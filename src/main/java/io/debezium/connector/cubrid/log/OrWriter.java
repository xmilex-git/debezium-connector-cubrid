/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid.log;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Sequential writer for CUBRID {@code OR_} packed buffers — the request-side mirror of
 * {@link OrReader} (big-endian, alignment relative to the buffer start, matching the
 * C client's {@code PTR_ALIGN}ed request buffers).
 */
final class OrWriter {

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    byte[] toByteArray() {
        return out.toByteArray();
    }

    void align(int boundary) {
        while (out.size() % boundary != 0) {
            out.write(0);
        }
    }

    OrWriter writeInt(int v) {
        out.write((v >>> 24) & 0xff);
        out.write((v >>> 16) & 0xff);
        out.write((v >>> 8) & 0xff);
        out.write(v & 0xff);
        return this;
    }

    /** {@code or_pack_int64}: aligns to 8 first. */
    OrWriter writeInt64(long v) {
        align(8);
        writeRawInt64(v);
        return this;
    }

    private void writeRawInt64(long v) {
        for (int i = 7; i >= 0; i--) {
            out.write((int) (v >>> (i * 8)) & 0xff);
        }
    }

    /**
     * {@code or_pack_log_lsa} from the flat uint64 layout (low 48 = pageid, high 16 =
     * offset): int64 pageid (48-bit field sign-extended) + short offset + 2 pad bytes,
     * {@code OR_LOG_LSA_ALIGNED_SIZE} (12) in total.
     */
    OrWriter writeLogLsaRaw(long flat) {
        long pageid = (flat << 16) >> 16; // sign-extend the 48-bit pageid (NULL LSA = -1)
        short offset = (short) (flat >>> 48);
        writeRawInt64(pageid);
        out.write((offset >>> 8) & 0xff);
        out.write(offset & 0xff);
        out.write(0);
        out.write(0);
        return this;
    }

    /** {@code or_pack_string}: NUL-terminated, length (incl. NUL + pad) prefixed, 4-padded. */
    OrWriter writeString(String s) {
        if (s == null) {
            return writeInt(-1);
        }
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        int len = b.length + 1;
        int pad = (4 - (len & 3)) & 3;
        writeInt(len + pad);
        out.write(b, 0, b.length);
        for (int i = 0; i < 1 + pad; i++) {
            out.write(0);
        }
        return this;
    }
}
