/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid.log;

import java.util.ArrayList;
import java.util.List;

import io.debezium.connector.cubrid.log.RawLogItem.ColumnValue;

/**
 * Parses a {@code NET_SERVER_CDC_GET_LOGINFO} reply payload into {@link RawLogItem}s —
 * the Java port of {@code cubrid_log_make_log_item_list()} and friends.
 *
 * <p>Byte-layout contract for {@link ColumnValue#data()}: identical to the JNA client,
 * which handed out the C client's <em>post-unpack in-place</em> bytes — numeric values as
 * native little-endian arrays (int/float 4B, bigint/double 8B, short 2B), strings as their
 * bytes without the NUL. The wire itself is big-endian; this parser converts.
 */
final class LogItemParser {

    /* DATA_ITEM_TYPE (cubrid_log.c) */
    private static final int TYPE_DDL = 0;
    private static final int TYPE_DML = 1;
    private static final int TYPE_DCL = 2;
    private static final int TYPE_TIMER = 3;
    private static final int TYPE_ROLLBACK_TO = 4;
    private static final int TYPE_RELATION = 5;

    private LogItemParser() {
    }

    static List<RawLogItem> parse(byte[] payload, int numInfos) {
        OrReader r = new OrReader(payload);
        List<RawLogItem> items = new ArrayList<>(numInfos);
        for (int i = 0; i < numInfos; i++) {
            items.add(readItem(r));
            r.align(8); // items start MAX_ALIGNMENT-aligned (cubrid_log_make_log_item_list)
        }
        return items;
    }

    private static RawLogItem readItem(OrReader r) {
        r.readInt(); // log_info_len — the C client reads and ignores it; parsing is structural
        int transactionId = r.readInt();
        String user = r.readString();
        int type = r.readInt();

        int ddlType = -1;
        int ddlObjectType = -1;
        String ddlStatement = null;
        RawLogItem.DmlType dmlType = RawLogItem.DmlType.UNKNOWN;
        long classoid = 0;
        List<ColumnValue> changed = List.of();
        List<ColumnValue> cond = List.of();
        RawLogItem.DclType dclType = RawLogItem.DclType.UNKNOWN;
        long timestamp = 0;
        long lsaKey = 0;

        switch (type) {
            case TYPE_DDL -> {
                ddlType = r.readInt();
                ddlObjectType = r.readInt();
                r.readInt64(); // oid — not surfaced (JNA parity)
                classoid = r.readInt64();
                r.readInt(); // statement_length
                ddlStatement = r.readString();
            }
            case TYPE_DML -> {
                dmlType = RawLogItem.DmlType.of(r.readInt());
                classoid = r.readInt64();
                changed = readColumns(r);
                cond = readColumns(r);
                lsaKey = r.readInt64();
            }
            case TYPE_DCL -> {
                dclType = RawLogItem.DclType.of(r.readInt());
                timestamp = r.readInt64();
            }
            case TYPE_TIMER -> timestamp = r.readInt64();
            case TYPE_ROLLBACK_TO -> lsaKey = r.readInt64();
            case TYPE_RELATION -> {
                // consumed structurally; surfacing owner/table to the connector is workspace#70
                r.readInt64(); // classoid
                r.readStringBytes(); // owner
                r.readStringBytes(); // table
            }
            default -> throw new CubridLogException("cubrid_log parse: unknown data item type " + type,
                    CubridLogException.FAILED_EXTRACT);
        }
        return new RawLogItem(transactionId, user, RawLogItem.ItemType.of(type),
                ddlType, ddlObjectType, ddlStatement,
                dmlType, classoid, changed, cond,
                dclType, timestamp, lsaKey);
    }

    private static List<ColumnValue> readColumns(OrReader r) {
        int n = r.readInt();
        if (n <= 0) {
            return List.of();
        }
        int[] indexes = new int[n];
        for (int i = 0; i < n; i++) {
            indexes[i] = r.readInt();
        }
        List<ColumnValue> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(new ColumnValue(indexes[i], readValue(r)));
        }
        return out;
    }

    private static byte[] readValue(OrReader r) {
        int packCode = r.readInt();
        return switch (packCode) {
            case 0 -> le(r.readInt(), 4); // int
            case 1 -> le(r.readInt64(), 8); // bigint
            case 2 -> le(r.readInt(), 4); // float (bit pattern)
            case 3 -> le(r.readInt64(), 8); // double (bit pattern; readInt64 8-aligns like or_unpack_double)
            case 4 -> le(r.readShortAsInt(), 2); // short (wire: 4-byte int)
            case 5, 8 -> stringBytes(r, false);
            case 7 -> stringBytes(r, true); // nullable variant
            default -> throw new CubridLogException("cubrid_log parse: unknown column pack code " + packCode,
                    CubridLogException.FAILED_EXTRACT);
        };
    }

    private static byte[] stringBytes(OrReader r, boolean nullable) {
        byte[] b = r.readStringBytes();
        if (b == null) {
            if (nullable) {
                return null;
            }
            // pack codes 5/8 never carry NULL (the C client would strlen(NULL) and crash)
            throw new CubridLogException("cubrid_log parse: NULL string for a non-nullable pack code",
                    CubridLogException.FAILED_EXTRACT);
        }
        return b;
    }

    /** Little-endian byte image of the low {@code len} bytes — the JNA-era value contract. */
    private static byte[] le(long v, int len) {
        byte[] b = new byte[len];
        for (int i = 0; i < len; i++) {
            b[i] = (byte) (v >>> (i * 8));
        }
        return b;
    }
}
