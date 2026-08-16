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
import io.debezium.connector.cubrid.jna.RawLogItem;
import io.debezium.relational.Column;
import io.debezium.relational.Table;

/**
 * Decodes the raw column bytes of a {@code cubrid_log} DML item into plain Java values, using the
 * JDBC-read table model to resolve the type (the log carries no pack code — P0 finding,
 * workspace#33).
 * <p>
 * The engine serializes each value in {@code cdc_make_dml_loginfo} (log_manager.c): fixed-size
 * numerics as host-order (little-endian) binary, NUMERIC as its decimal string, CHAR/VARCHAR as
 * raw bytes, and every date/time type as a formatted string ({@code YYYY-MM-DD HH24:MI:SS[.FF]}).
 * A SQL NULL arrives as a null data pointer — distinguishable from {@code ''} only by the pointer
 * (ADR 0003).
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
                return data.length == 4 ? le(data).getFloat() : le(data).getDouble();
            case Types.DOUBLE:
                return le(data).getDouble();
            case Types.NUMERIC:
            case Types.DECIMAL:
                return new BigDecimal(str(data));
            case Types.TIMESTAMP:
                return parseDateTime(str(data).trim());
            case Types.DATE:
                return java.sql.Date.valueOf(java.time.LocalDate.parse(str(data).trim(), CUBRID_DATE));
            case Types.TIME:
                return java.sql.Time.valueOf(java.time.LocalTime.parse(str(data).trim(), CUBRID_TIME));
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.NCHAR:
            default:
                return str(data);
        }
    }

    // the engine serializes date/time values in CUBRID's default output format
    // (measured, workspace#40): DATETIME "10:00:00.000 AM 08/16/2026",
    // TIMESTAMP "10:00:00 AM 08/16/2026"
    private static final java.time.format.DateTimeFormatter CUBRID_DATETIME = java.time.format.DateTimeFormatter
            .ofPattern("hh:mm:ss[.SSS] a MM/dd/uuuu", java.util.Locale.ENGLISH);
    private static final java.time.format.DateTimeFormatter CUBRID_DATE = java.time.format.DateTimeFormatter
            .ofPattern("MM/dd/uuuu", java.util.Locale.ENGLISH);
    private static final java.time.format.DateTimeFormatter CUBRID_TIME = java.time.format.DateTimeFormatter
            .ofPattern("hh:mm:ss[.SSS] a", java.util.Locale.ENGLISH);

    private static java.sql.Timestamp parseDateTime(String value) {
        try {
            return java.sql.Timestamp.valueOf(java.time.LocalDateTime.parse(value, CUBRID_DATETIME));
        }
        catch (java.time.format.DateTimeParseException e) {
            // fall back to the JDBC escape format ("2026-08-16 10:00:00[.fff]")
            return java.sql.Timestamp.valueOf(value);
        }
    }

    private static ByteBuffer le(byte[] data) {
        return ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
    }

    private static String str(byte[] data) {
        return new String(data, StandardCharsets.UTF_8);
    }
}
