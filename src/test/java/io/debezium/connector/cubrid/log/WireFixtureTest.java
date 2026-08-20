/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid.log;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.debezium.connector.cubrid.log.RawLogItem.ColumnValue;

/**
 * Wire-dump fixture tests (ADR 0012 D5②): {@code src/test/resources/wire/cdc-session.hexlog}
 * is a byte-exact capture of a real {@code cubrid_log} C-client session against a live
 * cub_server (engine build 11.5.0/htap-cdc **wire v2**, workspace#84 build bdbeaf3f1,
 * re-recorded through a TCP proxy on 2026-08-20 for the temporal wire v2 contract —
 * DATETIME payloads now carry {@code YYYY-MM-DD HH24:MI:SS.FF3} ISO text).
 * The session configured three name-based extraction targets and streamed one committed
 * transaction: INSERT(t_order 990022) → UPDATE(t_item SKU-A) → INSERT(990023, undone via
 * savepoint) → ROLLBACK_TO → DELETE(990022) → COMMIT, plus RELATION and TIMER items, an
 * idle extraction-timeout round, and a clean END_SESSION.
 *
 * <p>The tests pin both directions: the request encoder must reproduce the captured
 * C-client bytes exactly, and the reply parser must decode the captured server bytes into
 * the items the C client demonstrably decoded (cross-checked against its dump output).
 */
class WireFixtureTest {

    /** One length-prefixed wire chunk. */
    private record Chunk(byte[] bytes) {
    }

    /** A parsed 32-byte NET_HEADER. */
    private record Header(int type, int tran, int rid, int dbError, int functionCode, int flags, int bufferSize) {
        static Header of(byte[] h) {
            return new Header(be32(h, 0), be32(h, 12), be32(h, 16), be32(h, 20),
                    ((h[24] & 0xff) << 8) | (h[25] & 0xff), ((h[26] & 0xff) << 8) | (h[27] & 0xff), be32(h, 28));
        }
    }

    private record Request(int opcode, byte[] payload) {
    }

    private static List<Chunk> c2sChunks;
    private static List<Chunk> s2cChunks;
    /** rid → request (opcode + unpadded payload), in capture order. */
    private static Map<Integer, Request> requests;
    /** rid → reply payload. */
    private static Map<Integer, byte[]> replies;
    private static List<Integer> requestRids;

    @BeforeAll
    static void load() throws IOException {
        byte[] c2s = direction("C2S");
        byte[] s2c = direction("S2C");
        c2sChunks = chunks(c2s);
        s2cChunks = chunks(s2c);

        // pair rid → opcode/payload from the C2S stream (chunks 0..3 are the connect
        // exchange; everything after uses the padded framing)
        requests = new HashMap<>();
        requestRids = new ArrayList<>();
        for (int i = 4; i < c2sChunks.size(); i++) {
            byte[] cmd = unpad(c2sChunks.get(i).bytes());
            Header h = Header.of(cmd);
            assertEquals(1, h.type(), "expected a COMMAND header at padded chunk " + i);
            byte[] payload = new byte[0];
            if (h.bufferSize() > 0) {
                byte[] dataHeader = unpad(c2sChunks.get(++i).bytes());
                assertEquals(2, Header.of(dataHeader).type());
                payload = unpad(c2sChunks.get(++i).bytes());
                assertEquals(h.bufferSize(), payload.length);
            }
            requests.put(h.rid(), new Request(h.functionCode(), payload));
            requestRids.add(h.rid());
        }

        replies = new HashMap<>();
        for (int i = 0; i < s2cChunks.size(); i += 2) {
            Header h = Header.of(s2cChunks.get(i).bytes());
            assertEquals(2, h.type(), "server replies are DATA packets");
            byte[] payload = s2cChunks.get(i + 1).bytes();
            assertEquals(h.bufferSize(), payload.length);
            replies.put(h.rid(), payload);
        }
    }

    @Test
    void connectExchangeFraming() {
        byte[] magic = c2sChunks.get(0).bytes();
        assertEquals(32, magic.length);
        assertArrayEquals(WireConstants.NET_MAGIC, java.util.Arrays.copyOf(magic, 8));

        Header cmd = Header.of(c2sChunks.get(1).bytes());
        assertEquals(WireConstants.COMMAND_TYPE, cmd.type());
        assertEquals(WireConstants.DATA_REQUEST, cmd.functionCode());
        assertEquals(WireConstants.NULL_TRAN_INDEX, cmd.tran());
        assertEquals(WireConstants.FLAG_INVALIDATE_SNAPSHOT, cmd.flags());

        assertArrayEquals("htapdb\0".getBytes(StandardCharsets.UTF_8), c2sChunks.get(3).bytes());

        // connect reply: SERVER_CONNECTED for the same rid
        byte[] reason = replies.get(cmd.rid());
        assertNotNull(reason);
        assertEquals(WireConstants.SERVER_CONNECTED, new OrReader(reason).readInt());
    }

    @Test
    void paddedChunksCarryTheDocumentedFraming() {
        byte[] raw = c2sChunks.get(4).bytes(); // first padded chunk (START_SESSION command header)
        int pad = be32(raw, 0);
        assertTrue(pad >= 0 && pad < 8);
        assertEquals(0, (raw.length - 4 - pad) % 8, "payload+pad must fill to a multiple of 8");
        for (int i = raw.length - pad; i < raw.length; i++) {
            assertEquals(0, raw[i], "padding bytes are zero");
        }
    }

    @Test
    void startSessionRequestMatchesTheEncoder() {
        Request r = requestOf(WireConstants.NET_SERVER_CDC_START_SESSION);
        OrWriter w = new OrWriter();
        w.writeInt(512); // cdclogdump defaults: max_log_item
        w.writeInt(2); // extraction timeout used in the capture
        w.writeInt(1); // all_in_cond
        w.writeInt(0); // users
        w.writeInt(0); // classoid tables
        w.writeInt(3);
        w.writeString("dba.t_order");
        w.writeString("dba.t_item");
        w.writeString("dba.t_audit");
        assertArrayEquals(r.payload(), w.toByteArray(), "START_SESSION payload must be byte-identical to the C client");
    }

    @Test
    void findLsaRequestAndReplyRoundTrip() {
        Request r = requestOf(WireConstants.NET_SERVER_CDC_FIND_LSA);
        long ts = new OrReader(r.payload()).readInt64();
        assertTrue(ts > 1_700_000_000L && ts < 2_000_000_000L, "captured timestamp is a plausible epoch");
        OrWriter w = new OrWriter();
        w.writeInt64(ts);
        assertArrayEquals(r.payload(), w.toByteArray());

        byte[] reply = replies.get(ridOf(WireConstants.NET_SERVER_CDC_FIND_LSA));
        OrReader rr = new OrReader(reply);
        int code = rr.readInt();
        assertTrue(code == WireConstants.NO_ERROR || code == WireConstants.ER_CDC_ADJUSTED_LSA);
        long lsa = rr.readLogLsaRaw();
        assertTrue(CubridLogClient.lsaPageId(lsa) > 0);
    }

    @Test
    void metadataRequestRoundTripsThroughTheLsaCodec() {
        Request r = requestOf(WireConstants.NET_SERVER_CDC_GET_LOGINFO_METADATA);
        long flat = new OrReader(r.payload()).readLogLsaRaw();
        OrWriter w = new OrWriter();
        w.writeLogLsaRaw(flat);
        byte[] encoded = w.toByteArray();
        assertEquals(r.payload().length, encoded.length);
        // bytes 10-11 are the C LOG_LSA struct's trailing padding — undefined content (the C
        // client sends stack garbage there; the 2026-08-20 re-capture measured 0x20 0x00). Only
        // the 8-byte pageid + 2-byte offset are contractual; the Java encoder always sends zeros.
        for (int i = 0; i < 10; i++) {
            assertEquals(r.payload()[i], encoded[i], "LSA byte " + i);
        }
    }

    @Test
    void logInfoStreamDecodesTheCapturedTransaction() {
        List<RawLogItem> all = allItems();
        assertTrue(all.size() > 100, "capture contains the timer stream plus the transaction");

        // the committed transaction measured in the C client's dump output
        RawLogItem insert990022 = findDml(all, RawLogItem.DmlType.INSERT, 990022);
        assertEquals("DBA", insert990022.user());
        assertEquals(4, insert990022.changedColumns().size());
        assertEquals(0, insert990022.condColumns().size());
        assertArrayEquals(new byte[]{ 0x46, 0x1b, 0x0f, 0x00 }, insert990022.changedColumns().get(0).data(),
                "int column bytes keep the little-endian JNA-era contract");
        assertArrayEquals("fix-insert".getBytes(StandardCharsets.UTF_8), insert990022.changedColumns().get(1).data());
        assertArrayEquals("1.0000".getBytes(StandardCharsets.UTF_8), insert990022.changedColumns().get(2).data());
        assertTrue(insert990022.lsaKey() > 0);

        RawLogItem update = all.stream()
                .filter(it -> it.type() == RawLogItem.ItemType.DML && it.dmlType() == RawLogItem.DmlType.UPDATE)
                .filter(it -> it.condColumns().size() == 3)
                .findFirst().orElseThrow();
        assertArrayEquals("SKU-A".getBytes(StandardCharsets.UTF_8), update.condColumns().get(0).data(),
                "all_in_cond full before-image");
        assertEquals(1, update.changedColumns().size());

        RawLogItem delete = findDml(all, RawLogItem.DmlType.DELETE, 990022);
        assertEquals(4, delete.condColumns().size(), "all_in_cond full before-image on DELETE");

        RawLogItem insert990023 = findDml(all, RawLogItem.DmlType.INSERT, 990023);
        RawLogItem rollbackTo = all.stream()
                .filter(it -> it.type() == RawLogItem.ItemType.ROLLBACK_TO)
                .filter(it -> it.transactionId() == insert990023.transactionId())
                .findFirst().orElseThrow();
        assertTrue(rollbackTo.lsaKey() > 0);
        // the savepoint rewind target lies below the undone insert's record
        assertTrue(insert990023.lsaKey() > rollbackTo.lsaKey(),
                "the undone insert is above the rollback target, so consumers drop it");

        assertTrue(all.stream().anyMatch(it -> it.type() == RawLogItem.ItemType.DCL
                && it.dclType() == RawLogItem.DclType.COMMIT && it.timestamp() > 1_700_000_000L));
        assertTrue(all.stream().anyMatch(it -> it.type() == RawLogItem.ItemType.TIMER && it.timestamp() > 1_700_000_000L));
        // RELATION dictionary announces (workspace#67) surface owner/table split by the
        // engine (workspace#70) and carry the classoid their DML items are routed by
        RawLogItem orderRelation = all.stream()
                .filter(it -> it.type() == RawLogItem.ItemType.RELATION)
                .filter(it -> "t_order".equals(it.relationTable()))
                .findFirst().orElseThrow();
        assertEquals("dba", orderRelation.relationOwner());
        assertEquals(orderRelation.classoid(), insert990022.classoid(),
                "the announce precedes its classoid's first DML and routes it (ADR 0011 D4)");
        assertTrue(all.stream().anyMatch(it -> it.type() == RawLogItem.ItemType.RELATION
                && "t_item".equals(it.relationTable())));
    }

    @Test
    void extractionTimeoutAndEndSessionRepliesDecode() {
        // the last METADATA reply of the capture is the idle round
        int lastMetadataRid = -1;
        for (int rid : requestRids) {
            if (requests.get(rid).opcode() == WireConstants.NET_SERVER_CDC_GET_LOGINFO_METADATA) {
                lastMetadataRid = rid;
            }
        }
        assertEquals(WireConstants.ER_CDC_EXTRACTION_TIMEOUT, new OrReader(replies.get(lastMetadataRid)).readInt());

        int endRid = ridOf(WireConstants.NET_SERVER_CDC_END_SESSION);
        assertEquals(WireConstants.NO_ERROR, new OrReader(replies.get(endRid)).readInt());
    }

    /* ---- helpers ---- */

    private static List<RawLogItem> allItems() {
        List<RawLogItem> all = new ArrayList<>();
        for (int rid : requestRids) {
            if (requests.get(rid).opcode() != WireConstants.NET_SERVER_CDC_GET_LOGINFO_METADATA) {
                continue;
            }
            OrReader meta = new OrReader(replies.get(rid));
            if (meta.readInt() != WireConstants.NO_ERROR) {
                continue;
            }
            meta.readLogLsaRaw();
            int numInfos = meta.readInt();
            int totalLength = meta.readInt();
            if (numInfos == 0) {
                continue;
            }
            // the matching GET_LOGINFO is the next request in capture order
            int idx = requestRids.indexOf(rid);
            int loginfoRid = requestRids.get(idx + 1);
            assertEquals(WireConstants.NET_SERVER_CDC_GET_LOGINFO, requests.get(loginfoRid).opcode());
            byte[] payload = replies.get(loginfoRid);
            assertEquals(totalLength, payload.length);
            all.addAll(LogItemParser.parse(payload, numInfos));
        }
        return all;
    }

    private static RawLogItem findDml(List<RawLogItem> items, RawLogItem.DmlType dmlType, int idLe) {
        byte[] key = { (byte) idLe, (byte) (idLe >>> 8), (byte) (idLe >>> 16), (byte) (idLe >>> 24) };
        return items.stream()
                .filter(it -> it.type() == RawLogItem.ItemType.DML && it.dmlType() == dmlType)
                .filter(it -> {
                    List<ColumnValue> cols = dmlType == RawLogItem.DmlType.DELETE ? it.condColumns() : it.changedColumns();
                    return !cols.isEmpty() && java.util.Arrays.equals(key, cols.get(0).data());
                })
                .findFirst().orElseThrow();
    }

    private static Request requestOf(int opcode) {
        return requests.get(ridOf(opcode));
    }

    private static int ridOf(int opcode) {
        for (int rid : requestRids) {
            if (requests.get(rid).opcode() == opcode) {
                return rid;
            }
        }
        throw new AssertionError("no captured request with opcode " + opcode);
    }

    private static byte[] unpad(byte[] chunk) {
        int pad = be32(chunk, 0);
        assertTrue(pad >= 0 && pad < 8, "padded chunk starts with the pad indicator");
        byte[] out = new byte[chunk.length - 4 - pad];
        System.arraycopy(chunk, 4, out, 0, out.length);
        return out;
    }

    private static List<Chunk> chunks(byte[] stream) {
        List<Chunk> out = new ArrayList<>();
        int o = 0;
        while (o < stream.length) {
            int len = be32(stream, o);
            o += 4;
            byte[] b = new byte[len];
            System.arraycopy(stream, o, b, 0, len);
            o += len;
            out.add(new Chunk(b));
        }
        return out;
    }

    private static byte[] direction(String tag) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(
                WireFixtureTest.class.getResourceAsStream("/wire/cdc-session.hexlog"), StandardCharsets.US_ASCII))) {
            String current = null;
            String line;
            while ((line = r.readLine()) != null) {
                if (line.startsWith("C2S") || line.startsWith("S2C")) {
                    current = line.substring(0, 3);
                }
                else if (tag.equals(current)) {
                    out.write(hex(line));
                }
            }
        }
        return out.toByteArray();
    }

    private static byte[] hex(String s) {
        byte[] b = new byte[s.length() / 2];
        for (int i = 0; i < b.length; i++) {
            b[i] = (byte) Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16);
        }
        return b;
    }

    private static int be32(byte[] b, int o) {
        return ((b[o] & 0xff) << 24) | ((b[o + 1] & 0xff) << 16) | ((b[o + 2] & 0xff) << 8) | (b[o + 3] & 0xff);
    }
}
