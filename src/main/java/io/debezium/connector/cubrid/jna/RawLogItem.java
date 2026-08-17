/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid.jna;

import java.util.List;

/**
 * A {@code CUBRID_LOG_ITEM} copied out of native memory into plain Java values, so it
 * stays valid after {@code cubrid_log_clear_log_item()} frees the batch.
 *
 * <p>Column values arrive as raw bytes without the server-side pack code: only the byte
 * length is known, so len==4 (int|float) and len==8 (bigint|double|8-char string) are
 * ambiguous at this layer. Typed decoding needs the JDBC schema and belongs to the value
 * converter layer, not here (P0 finding, workspace#33). A NULL column is a null
 * {@code data} array — distinguishable from an empty string only by the pointer
 * (ADR 0003).
 */
public final class RawLogItem {

    public enum ItemType {
        DDL, DML, DCL, TIMER, ROLLBACK_TO, UNKNOWN;

        static ItemType of(int code) {
            return switch (code) {
                case 0 -> DDL;
                case 1 -> DML;
                case 2 -> DCL;
                case 3 -> TIMER;
                case 4 -> ROLLBACK_TO;
                default -> UNKNOWN;
            };
        }
    }

    public enum DmlType {
        INSERT, UPDATE, DELETE, TRIGGER_INSERT, TRIGGER_UPDATE, TRIGGER_DELETE, UNKNOWN;

        static DmlType of(int code) {
            return switch (code) {
                case 0 -> INSERT;
                case 1 -> UPDATE;
                case 2 -> DELETE;
                case 3 -> TRIGGER_INSERT;
                case 4 -> TRIGGER_UPDATE;
                case 5 -> TRIGGER_DELETE;
                default -> UNKNOWN;
            };
        }
    }

    public enum DclType {
        COMMIT, ABORT, UNKNOWN;

        static DclType of(int code) {
            return switch (code) {
                case 0 -> COMMIT;
                case 1 -> ABORT;
                default -> UNKNOWN;
            };
        }
    }

    /** One column slot of a DML item: table column index + raw packed bytes (null = SQL NULL). */
    public record ColumnValue(int columnIndex, byte[] data) {

        public String toDisplayString() {
            if (data == null) {
                return "col[" + columnIndex + "]=NULL";
            }
            StringBuilder sb = new StringBuilder("col[").append(columnIndex).append("] len=").append(data.length).append(" hex=");
            for (int i = 0; i < Math.min(data.length, 32); i++) {
                sb.append(String.format("%02x", data[i]));
            }
            if (data.length > 32) {
                sb.append("..");
            }
            sb.append(" str=\"");
            for (int i = 0; i < Math.min(data.length, 64); i++) {
                char c = (char) (data[i] & 0xff);
                sb.append(c >= 0x20 && c < 0x7f ? c : '.');
            }
            sb.append('"');
            return sb.toString();
        }
    }

    private final int transactionId;
    private final String user;
    private final ItemType type;

    /* DDL */
    private final int ddlType;
    private final int ddlObjectType;
    private final String ddlStatement;

    /* DML */
    private final DmlType dmlType;
    private final long classoid;
    private final List<ColumnValue> changedColumns;
    private final List<ColumnValue> condColumns;

    /* DCL / TIMER */
    private final DclType dclType;
    private final long timestamp;

    /* DML: orderable lsa key of the source record; ROLLBACK_TO: rewind target.
     * Key layout (pageid << 16 | offset) — numeric order == log order. */
    private final long lsaKey;

    RawLogItem(int transactionId, String user, ItemType type,
               int ddlType, int ddlObjectType, String ddlStatement,
               DmlType dmlType, long classoid, List<ColumnValue> changedColumns, List<ColumnValue> condColumns,
               DclType dclType, long timestamp, long lsaKey) {
        this.transactionId = transactionId;
        this.user = user;
        this.type = type;
        this.ddlType = ddlType;
        this.ddlObjectType = ddlObjectType;
        this.ddlStatement = ddlStatement;
        this.dmlType = dmlType;
        this.classoid = classoid;
        this.changedColumns = changedColumns;
        this.condColumns = condColumns;
        this.dclType = dclType;
        this.timestamp = timestamp;
        this.lsaKey = lsaKey;
    }

    public int transactionId() {
        return transactionId;
    }

    public String user() {
        return user;
    }

    public ItemType type() {
        return type;
    }

    public int ddlType() {
        return ddlType;
    }

    public int ddlObjectType() {
        return ddlObjectType;
    }

    public String ddlStatement() {
        return ddlStatement;
    }

    public DmlType dmlType() {
        return dmlType;
    }

    public long classoid() {
        return classoid;
    }

    public List<ColumnValue> changedColumns() {
        return changedColumns;
    }

    public List<ColumnValue> condColumns() {
        return condColumns;
    }

    public DclType dclType() {
        return dclType;
    }

    public long timestamp() {
        return timestamp;
    }

    /** DML: this record's lsa key; ROLLBACK_TO: the rewind target key; 0 otherwise. */
    public long lsaKey() {
        return lsaKey;
    }

    public String toDisplayString() {
        StringBuilder sb = new StringBuilder("trid=").append(transactionId).append(" user=").append(user).append(' ').append(type);
        switch (type) {
            case DDL -> sb.append(" ddl_type=").append(ddlType).append(" obj_type=").append(ddlObjectType)
                    .append(" stmt=").append(ddlStatement);
            case DML -> {
                sb.append(' ').append(dmlType).append(" classoid=").append(classoid)
                        .append(" rec_lsa=").append(lsaKey)
                        .append(" changed=").append(changedColumns.size()).append(" cond=").append(condColumns.size());
                for (ColumnValue c : changedColumns) {
                    sb.append("\n      changed ").append(c.toDisplayString());
                }
                for (ColumnValue c : condColumns) {
                    sb.append("\n      cond    ").append(c.toDisplayString());
                }
            }
            case DCL -> sb.append(' ').append(dclType).append(" ts=").append(timestamp);
            case TIMER -> sb.append(" ts=").append(timestamp);
            case ROLLBACK_TO -> sb.append(" rollback_to_lsa=").append(lsaKey);
            default -> {
            }
        }
        return sb.toString();
    }
}
