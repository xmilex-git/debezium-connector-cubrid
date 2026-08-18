/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid.log;

import java.nio.charset.StandardCharsets;

/**
 * Sequential reader for CUBRID {@code OR_} packed buffers (big-endian, C-side
 * {@code object_representation.c} semantics). Alignment is relative to the buffer start,
 * which matches the C client exactly: every OR buffer it packs or parses is
 * {@code PTR_ALIGN}ed to {@code MAX_ALIGNMENT} (8) first, so absolute-address alignment
 * and offset-from-start alignment coincide.
 */
final class OrReader {

    private final byte[] buf;
    private int pos;

    OrReader(byte[] buf) {
        this.buf = buf;
    }

    int position() {
        return pos;
    }

    boolean hasRemaining() {
        return pos < buf.length;
    }

    void align(int boundary) {
        int rem = pos % boundary;
        if (rem != 0) {
            pos += boundary - rem;
        }
    }

    int readInt() {
        int v = ((buf[pos] & 0xff) << 24) | ((buf[pos + 1] & 0xff) << 16) | ((buf[pos + 2] & 0xff) << 8) | (buf[pos + 3] & 0xff);
        pos += 4;
        return v;
    }

    /** {@code or_unpack_int64}: aligns to 8 first. */
    long readInt64() {
        align(8);
        return readRawInt64();
    }

    /** 8 big-endian bytes at the current position, no alignment (LOG_LSA pageid field). */
    long readRawInt64() {
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (buf[pos + i] & 0xff);
        }
        pos += 8;
        return v;
    }

    /** {@code or_unpack_short}: packed as a 4-byte int. */
    short readShortAsInt() {
        return (short) readInt();
    }

    float readFloat() {
        return Float.intBitsToFloat(readInt());
    }

    /** {@code or_unpack_double}: aligns to 8 first. */
    double readDouble() {
        align(8);
        return Double.longBitsToDouble(readRawInt64());
    }

    /**
     * {@code or_unpack_log_lsa}: int64 pageid (no 8-alignment — raw at current offset) +
     * short offset, advancing {@code OR_LOG_LSA_ALIGNED_SIZE} (12) in total.
     */
    long readLogLsaRaw() {
        int start = pos;
        long pageid = readRawInt64();
        int offset = ((buf[pos] & 0xff) << 8) | (buf[pos + 1] & 0xff);
        pos = start + 12;
        // flat uint64 layout used by cubrid_log_find_lsa()/extract(): low 48 = pageid, high 16 = offset
        return (((long) offset) << 48) | (pageid & 0x0000FFFFFFFFFFFFL);
    }

    /**
     * {@code or_unpack_string_nocopy}: 4-byte padded length (-1 = NULL), then the bytes.
     * Returns the C string up to (excluding) its NUL terminator.
     */
    byte[] readStringBytes() {
        int len = readInt();
        if (len == -1) {
            return null;
        }
        int end = pos;
        int limit = pos + len;
        while (end < limit && buf[end] != 0) {
            end++;
        }
        byte[] out = new byte[end - pos];
        System.arraycopy(buf, pos, out, 0, out.length);
        pos += len;
        return out;
    }

    String readString() {
        byte[] b = readStringBytes();
        return b == null ? null : new String(b, StandardCharsets.UTF_8);
    }
}
