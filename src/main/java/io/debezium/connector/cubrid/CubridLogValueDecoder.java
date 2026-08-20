/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.List;

import io.debezium.DebeziumException;
import io.debezium.connector.cubrid.log.RawLogItem;
import io.debezium.relational.Column;
import io.debezium.relational.Table;

/**
 * Decodes the raw column bytes of a {@code cubrid_log} DML item into plain Java values, using the
 * JDBC-read table model to resolve the type (the log carries no pack code — P0 finding,
 * workspace#33).
 * <p>
 * The engine serializes each value in {@code cdc_make_dml_loginfo} (log_manager.c): fixed-size
 * numerics as host-order (little-endian) binary, NUMERIC as its decimal string, CHAR/VARCHAR as
 * raw bytes, and every date/time type as wire v2 ISO text — the byte-exact contract is
 * {@code docs/htap-cdc-wire-v2.md} §3.2 ({@code YYYY-MM-DD HH24:MI:SS[.FF][ ±TZH:TZM]}, with
 * TIMESTAMP wall-clocks rendered in UTC by the engine's CDC daemon timezone pin). Temporal
 * parsing is strict ({@link CubridTemporal}) — v1 locale-default AM/PM text fails loudly, the
 * lockstep safety net of #76-D5. A SQL NULL arrives as a null data pointer — distinguishable
 * from {@code ''} only by the pointer (ADR 0003).
 */
final class CubridLogValueDecoder {

    private CubridLogValueDecoder() {
    }

    /**
     * @return a row array indexed by column position (0-based def order = the log's column index),
     *         with {@code null} for columns absent from {@code columnValues}
     */
    static Object[] toRow(Table table, List<RawLogItem.ColumnValue> columnValues) {
        final List<Column> columns = table.columns();
        final Object[] row = new Object[columns.size()];
        for (RawLogItem.ColumnValue cv : columnValues) {
            if (cv.columnIndex() < 0 || cv.columnIndex() >= row.length) {
                throw new DebeziumException("Log column index " + cv.columnIndex() + " out of range for table "
                        + table.id() + " (" + row.length + " columns) — was the table altered? CDC requires a fixed schema.");
            }
            row[cv.columnIndex()] = decode(columns.get(cv.columnIndex()), cv.data());
        }
        return row;
    }

    /**
     * Overlays the changed columns on top of the full before-image:
     * {@code full after = cond(before) ⊕ changed(after)} (ADR 0003).
     */
    static Object[] merge(Table table, List<RawLogItem.ColumnValue> condColumns, List<RawLogItem.ColumnValue> changedColumns) {
        final Object[] row = toRow(table, condColumns);
        for (RawLogItem.ColumnValue cv : changedColumns) {
            row[cv.columnIndex()] = decode(table.columns().get(cv.columnIndex()), cv.data());
        }
        return row;
    }

    private static Object decode(Column column, byte[] data) {
        if (data == null) {
            return null;
        }
        switch (column.jdbcType()) {
            case Types.INTEGER:
                return le(data).getInt();
            case Types.SMALLINT:
            case Types.TINYINT:
                return le(data).getShort();
            case Types.BIGINT:
                return le(data).getLong();
            case Types.REAL:
            case Types.FLOAT:
                // no ternary: its numeric promotion would widen the float to a Double,
                // breaking the FLOAT32 schema (found by the workspace#58 boundary corpus)
                if (data.length == 4) {
                    return le(data).getFloat();
                }
                return le(data).getDouble();
            case Types.DOUBLE:
                return le(data).getDouble();
            case Types.NUMERIC:
            case Types.DECIMAL:
                return new BigDecimal(str(data));
            case Types.TIMESTAMP:
                // both CUBRID types report jdbcType TIMESTAMP; the contract differs by typeName
                // (#76-D3): TIMESTAMP is an instant (UTC wall-clock on the wire), DATETIME is
                // zone-less.
                if ("TIMESTAMP".equalsIgnoreCase(column.typeName())) {
                    return CubridTemporal.parseTimestampUtc(str(data));
                }
                return CubridTemporal.parseDatetime(str(data));
            case Types.TIMESTAMP_WITH_TIMEZONE:
                // TZ family (workspace#86): the wire carries the ± TZH:TZM offset suffix (§3.2);
                // the DATETIME* variants additionally carry the .FF3 fraction
                if (column.typeName().toUpperCase(java.util.Locale.ROOT).startsWith("DATETIME")) {
                    return CubridTemporal.parseDatetimeTz(str(data));
                }
                return CubridTemporal.parseTimestampTz(str(data));
            case Types.DATE:
                return CubridTemporal.parseDate(str(data));
            case Types.TIME:
                return CubridTemporal.parseTime(str(data));
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.NCHAR:
            default:
                return str(data);
        }
    }

    private static ByteBuffer le(byte[] data) {
        return ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
    }

    private static String str(byte[] data) {
        return new String(data, StandardCharsets.UTF_8);
    }
}
