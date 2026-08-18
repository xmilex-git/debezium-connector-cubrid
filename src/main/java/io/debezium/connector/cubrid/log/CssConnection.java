/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid.log;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;

/**
 * One CSS connection speaking the CDC log-extraction protocol: TCP to the CUBRID master
 * port, magic + {@code DATA_REQUEST} handoff to the {@code cub_server} process, then a
 * strict request/reply sequence of CDC commands.
 *
 * <p>Framing (measured against the live server, fixtures under
 * {@code src/test/resources/wire/}): every wire chunk is {@code [int32 length][bytes]}.
 * The connect exchange (magic, command, dbname) is sent plain; every CDC request after the
 * handoff uses the padded chunk form the server's receiver expects —
 * {@code [int32 4+n+pad][int32 pad][n bytes][pad zero bytes]} with pad filling n to a
 * multiple of 8. Server replies are always plain chunks: a 32-byte {@code NET_HEADER}
 * chunk followed by one payload chunk of {@code buffer_size} bytes.
 */
final class CssConnection implements Closeable {

    private Socket socket;
    private DataInputStream in;
    private OutputStream out;

    private int nextRequestId = 1;
    /** Echoed back into request headers, adopted from every server reply (C client parity). */
    private int tranIndex = WireConstants.NULL_TRAN_INDEX;
    private int dbError;

    /**
     * TCP connect + magic + {@code DATA_REQUEST(dbname)} + {@code SERVER_CONNECTED} reply.
     * The master hands the accepted socket off to the database server process, so the same
     * connection carries the CDC session afterwards.
     */
    void connect(String host, int port, String dbname, int timeoutSeconds) {
        int timeoutMs = timeoutSeconds > 0 ? timeoutSeconds * 1000 : 0;
        try {
            socket = new Socket();
            socket.setTcpNoDelay(true);
            socket.connect(new InetSocketAddress(host, port), timeoutMs);
            in = new DataInputStream(socket.getInputStream());
            out = socket.getOutputStream();

            byte[] magic = new byte[WireConstants.NET_HEADER_SIZE];
            System.arraycopy(WireConstants.NET_MAGIC, 0, magic, 0, WireConstants.NET_MAGIC.length);
            writePlainChunk(magic);

            byte[] name = (dbname + '\0').getBytes(StandardCharsets.UTF_8);
            int rid = allocRequestId();
            writePlainChunk(header(WireConstants.COMMAND_TYPE, WireConstants.DATA_REQUEST, rid, name.length));
            writePlainChunk(header(WireConstants.DATA_TYPE, 0, rid, name.length));
            writePlainChunk(name);
            out.flush();

            byte[] reply = readReply(rid, timeoutMs);
            if (reply.length != 4) {
                throw connectFailure("unexpected connect reply size " + reply.length, null);
            }
            int reason = new OrReader(reply).readInt();
            if (reason != WireConstants.SERVER_CONNECTED) {
                throw connectFailure("server refused the connection (reason " + reason + ")", null);
            }
        }
        catch (IOException e) {
            throw connectFailure("I/O failure connecting to " + host + ":" + port, e);
        }
    }

    /** Send one CDC request (padded framing) and return the reply payload. */
    byte[] request(int opcode, byte[] payload, int timeoutSeconds) {
        int size = payload == null ? 0 : payload.length;
        int rid = allocRequestId();
        try {
            writePaddedChunk(header(WireConstants.COMMAND_TYPE, opcode, rid, size));
            if (size > 0) {
                writePaddedChunk(header(WireConstants.DATA_TYPE, 0, rid, size));
                writePaddedChunk(payload);
            }
            out.flush();
            return readReply(rid, timeoutSeconds > 0 ? timeoutSeconds * 1000 : 0);
        }
        catch (IOException e) {
            throw connectFailure("I/O failure on CDC request " + opcode, e);
        }
    }

    private byte[] readReply(int rid, int timeoutMs) throws IOException {
        // The C client keeps waiting past its timeout as long as the server process stays
        // alive; a bounded wait with slack keeps the same practical behavior but cannot
        // hang a Connect worker forever.
        socket.setSoTimeout(timeoutMs > 0 ? timeoutMs + 5000 : 0);
        while (true) {
            byte[] head;
            try {
                head = readChunk();
            }
            catch (SocketTimeoutException e) {
                throw connectFailure("timed out waiting for a server reply (rid " + rid + ")", e);
            }
            if (head.length != WireConstants.NET_HEADER_SIZE) {
                throw connectFailure("expected a 32-byte packet header, got " + head.length + " bytes", null);
            }
            OrReader h = new OrReader(head);
            int type = h.readInt();
            h.readInt(); // version (uninitialized junk from the server; ignored)
            h.readInt(); // host_id
            int tran = h.readInt();
            int replyRid = h.readInt();
            int dbErr = h.readInt();
            h.readShortAsInt(); // function_code + flags read below from raw positions
            int bufSize = ((head[28] & 0xff) << 24) | ((head[29] & 0xff) << 16) | ((head[30] & 0xff) << 8) | (head[31] & 0xff);

            if (type == WireConstants.CLOSE_TYPE) {
                throw connectFailure("server closed the connection", null);
            }
            if (type == WireConstants.DATA_TYPE) {
                tranIndex = tran;
                dbError = dbErr;
                byte[] body = readChunk();
                if (replyRid == rid) {
                    return body;
                }
                continue; // stale reply for another request id — drop it (C client queues these)
            }
            if (type == WireConstants.ABORT_TYPE) {
                throw connectFailure("server aborted request " + rid, null);
            }
            // ERROR_TYPE or anything unexpected: consume its payload and fail loudly —
            // the CDC exchange is strictly request/reply, so this is a protocol breach.
            if (bufSize > 0) {
                readChunk();
            }
            throw connectFailure("unexpected packet type " + type + " while waiting for rid " + rid, null);
        }
    }

    private byte[] readChunk() throws IOException {
        int len = in.readInt();
        if (len < 0 || len > 64 * 1024 * 1024) {
            throw connectFailure("implausible chunk length " + len, null);
        }
        byte[] buf = new byte[len];
        in.readFully(buf);
        return buf;
    }

    private void writePlainChunk(byte[] b) throws IOException {
        writeInt(b.length);
        out.write(b);
    }

    private void writePaddedChunk(byte[] b) throws IOException {
        int pad = (8 - (b.length % 8)) & 7;
        writeInt(4 + b.length + pad);
        writeInt(pad);
        out.write(b);
        for (int i = 0; i < pad; i++) {
            out.write(0);
        }
    }

    private void writeInt(int v) throws IOException {
        out.write((v >>> 24) & 0xff);
        out.write((v >>> 16) & 0xff);
        out.write((v >>> 8) & 0xff);
        out.write(v & 0xff);
    }

    private byte[] header(int type, int functionCode, int rid, int bufferSize) {
        byte[] h = new byte[WireConstants.NET_HEADER_SIZE];
        putInt(h, 0, type);
        // version(4), host_id(8) stay 0
        putInt(h, 12, tranIndex);
        putInt(h, 16, rid);
        putInt(h, 20, dbError);
        h[24] = (byte) ((functionCode >>> 8) & 0xff);
        h[25] = (byte) (functionCode & 0xff);
        h[26] = (byte) ((WireConstants.FLAG_INVALIDATE_SNAPSHOT >>> 8) & 0xff);
        h[27] = (byte) (WireConstants.FLAG_INVALIDATE_SNAPSHOT & 0xff);
        putInt(h, 28, bufferSize);
        return h;
    }

    private static void putInt(byte[] b, int off, int v) {
        b[off] = (byte) (v >>> 24);
        b[off + 1] = (byte) (v >>> 16);
        b[off + 2] = (byte) (v >>> 8);
        b[off + 3] = (byte) v;
    }

    private int allocRequestId() {
        int rid = nextRequestId++;
        if (nextRequestId > 0xffff) {
            nextRequestId = 1;
        }
        return rid;
    }

    private CubridLogException connectFailure(String detail, Exception cause) {
        CubridLogException e = new CubridLogException("cubrid_log wire: " + detail, CubridLogException.FAILED_CONNECT);
        if (cause != null) {
            e.initCause(cause);
        }
        return e;
    }

    boolean isOpen() {
        return socket != null && socket.isConnected() && !socket.isClosed();
    }

    @Override
    public void close() {
        if (socket != null) {
            try {
                socket.close();
            }
            catch (IOException ignored) {
            }
            socket = null;
        }
    }
}
