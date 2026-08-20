/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.connector.cubrid.log.RawLogItem.ColumnValue;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * Type-mapping boundary corpus for the streaming decoder (workspace#58).
 * <p>
 * Every raw byte fixture below is a MEASURED engine serialization — captured with the
 * {@code cdclogdump} harness against a CUBRID 11.5 {@code supplemental_log=1} server
 * (engine build with the workspace#47 patch, 2026-08-18) from INSERTs of boundary values.
 * The engine serializes in {@code cdc_make_dml_loginfo} (log_manager.c): fixed-size numerics
 * as little-endian binary, NUMERIC as its decimal string, CHAR/VARCHAR as raw UTF-8 bytes,
 * every date/time type as wire v2 ISO text ({@code docs/htap-cdc-wire-v2.md} §3.2 — temporal
 * fixtures re-measured 2026-08-19 against the workspace#84 engine, TIMESTAMP wall-clocks UTC),
 * and SQL NULL as a null data pointer (ADR 0003 — distinguishable from {@code ''} only by the
 * pointer).
 * <p>
 * Types NOT in this corpus are the documented-unsupported set (see {@code docs/type-support.md}):
 * MONETARY, BIT/BIT VARYING, TIMESTAMPTZ/TIMESTAMPLTZ/DATETIMETZ/DATETIMELTZ, SET/MULTISET/LIST,
 * BLOB/CLOB, JSON. Their fixtures must not be added here as "green" until the connector actually
 * supports them — the measured hazards (MONETARY decodes garbage via the DOUBLE path, collections
 * and JSON arrive as silent NULL) are recorded in that document and guarded by workspace#58's
 * follow-up ticket.
 */
class CubridLogValueDecoderCorpusTest {

    private static final TableId TABLE_ID = new TableId(null, "htapdb", "t_corpus");

    private static Column col(String name, int pos, int jdbcType, String typeName) {
        return Column.editor().name(name).position(pos).jdbcType(jdbcType).type(typeName).create();
    }

    private static Table table(Column... columns) {
        var editor = Table.editor().tableId(TABLE_ID);
        for (Column c : columns) {
            editor.addColumn(c);
        }
        return editor.create();
    }

    private static byte[] hex(String s) {
        byte[] out = new byte[s.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(s.substring(2 * i, 2 * i + 2), 16);
        }
        return out;
    }

    private static ColumnValue cv(int index, String rawHex) {
        return new ColumnValue(index, hex(rawHex));
    }

    private static ColumnValue cvStr(int index, String value) {
        return new ColumnValue(index, value.getBytes(StandardCharsets.UTF_8));
    }

    private static ColumnValue cvNull(int index) {
        return new ColumnValue(index, null);
    }

    // ---------------------------------------------------------------- numeric family

    private static final Table NUM = table(
            col("v_short", 1, Types.SMALLINT, "SMALLINT"),
            col("v_int", 2, Types.INTEGER, "INTEGER"),
            col("v_bigint", 3, Types.BIGINT, "BIGINT"),
            col("v_num", 4, Types.NUMERIC, "NUMERIC"),
            col("v_float", 5, Types.REAL, "FLOAT"),
            col("v_double", 6, Types.DOUBLE, "DOUBLE"));

    @Test
    void numericMinima() {
        Object[] row = CubridLogValueDecoder.toRow(NUM, List.of(
                cv(0, "0080"), // SMALLINT -32768
                cv(1, "00000080"), // INT -2147483648
                cv(2, "0000000000000080"), // BIGINT min
                cvStr(3, "-9999999999999999999999999999.9999999999"), // NUMERIC(38,10) min
                cv(4, "fdff7f00"), // FLOAT 1.175494e-38 (min normal, measured bits)
                cv(5, "0000000000001000"))); // DOUBLE 2.2250738585072014e-308 (min normal)
        assertEquals((short) -32768, row[0]);
        assertEquals(-2147483648, row[1]);
        assertEquals(Long.MIN_VALUE, row[2]);
        assertEquals(new BigDecimal("-9999999999999999999999999999.9999999999"), row[3]);
        assertEquals(1.175494e-38f, row[4]);
        assertEquals(2.2250738585072014e-308, row[5]);
    }

    @Test
    void numericMaxima() {
        Object[] row = CubridLogValueDecoder.toRow(NUM, List.of(
                cv(0, "ff7f"), // SMALLINT 32767
                cv(1, "ffffff7f"), // INT 2147483647
                cv(2, "ffffffffffffff7f"), // BIGINT max
                cvStr(3, "9999999999999999999999999999.9999999999"),
                cv(4, "fdff7f7f"), // FLOAT 3.402823e+38 (measured bits)
                cv(5, "ffffffffffffef7f"))); // DOUBLE 1.7976931348623157e+308
        assertEquals((short) 32767, row[0]);
        assertEquals(2147483647, row[1]);
        assertEquals(Long.MAX_VALUE, row[2]);
        assertEquals(new BigDecimal("9999999999999999999999999999.9999999999"), row[3]);
        assertEquals(3.402823e+38f, row[4]);
        assertEquals(1.7976931348623157e+308, row[5]);
    }

    @Test
    void numericZeros() {
        // the engine normalizes -0.0 DOUBLE to +0.0 on insert (measured) — no signed-zero case
        Object[] row = CubridLogValueDecoder.toRow(NUM, List.of(
                cv(0, "0000"),
                cv(1, "00000000"),
                cv(2, "0000000000000000"),
                cvStr(3, "0.0000000000"), // NUMERIC keeps declared scale
                cv(4, "00000000"),
                cv(5, "0000000000000000")));
        assertEquals((short) 0, row[0]);
        assertEquals(0, row[1]);
        assertEquals(0L, row[2]);
        assertEquals(new BigDecimal("0.0000000000"), row[3]);
        assertEquals(0.0f, row[4]);
        assertEquals(0.0d, row[5]);
    }

    @Test
    void numericSmallScalePreserved() {
        Object[] row = CubridLogValueDecoder.toRow(
                table(col("v_num2", 1, Types.NUMERIC, "NUMERIC")),
                List.of(cvStr(0, "0.0001")));
        assertEquals(new BigDecimal("0.0001"), row[0]);
    }

    @Test
    void numericNulls() {
        Object[] row = CubridLogValueDecoder.toRow(NUM, List.of(
                cvNull(0), cvNull(1), cvNull(2), cvNull(3), cvNull(4), cvNull(5)));
        for (Object v : row) {
            assertNull(v);
        }
    }

    // ---------------------------------------------------------------- string family

    private static final Table STR = table(
            col("v_char", 1, Types.CHAR, "CHAR"),
            col("v_varchar", 2, Types.VARCHAR, "VARCHAR"));

    @Test
    void charIsSpacePaddedToDeclaredLength() {
        // CHAR(10) 'a' arrives space-padded — measured raw 61 20*9
        Object[] row = CubridLogValueDecoder.toRow(STR, List.of(
                cv(0, "61202020202020202020"), cvStr(1, "0123456789")));
        assertEquals("a         ", row[0]);
        assertEquals("0123456789", row[1]);
    }

    @Test
    void charPaddingCountsCharactersNotBytes() {
        // CHAR(10) '한글패딩' = 4 Hangul chars + 6 pad spaces (18 bytes UTF-8) — measured
        Object[] row = CubridLogValueDecoder.toRow(STR, List.of(
                cv(0, "ed959ceab880ed8ca8eb94a9202020202020"), cvNull(1)));
        assertEquals("한글패딩      ", row[0]);
    }

    @Test
    void emptyStringIsNotNull() {
        // ADR 0003: '' arrives as a zero-length non-null pointer, SQL NULL as a null pointer
        Object[] row = CubridLogValueDecoder.toRow(STR, List.of(
                cvNull(0), new ColumnValue(1, new byte[0])));
        assertNull(row[0]);
        assertEquals("", row[1]);
    }

    @Test
    void varcharUnicodeAndSpecialCharsRoundTrip() {
        String v = "유니코드 문자열 — quotes 'single' \"double\" |~| pipe, tab\t, backslash \\";
        Object[] row = CubridLogValueDecoder.toRow(STR, List.of(cvNull(0), cvStr(1, v)));
        assertEquals(v, row[1]);
    }

    @Test
    void varcharAtMaxDeclaredLength() {
        String v = "x".repeat(255);
        Object[] row = CubridLogValueDecoder.toRow(STR, List.of(cvNull(0), cvStr(1, v)));
        assertEquals(v, row[1]);
    }

    // ---------------------------------------------------------------- date/time family

    private static final Table DT = table(
            col("v_date", 1, Types.DATE, "DATE"),
            col("v_time", 2, Types.TIME, "TIME"),
            col("v_ts", 3, Types.TIMESTAMP, "TIMESTAMP"),
            col("v_dtm", 4, Types.TIMESTAMP, "DATETIME"));

    @Test
    void wireV2MeasuredBytesDecodeExactly() {
        // MEASURED raw hex (workspace#84 conformance evidence, cdclogdump 2026-08-19, server tz
        // Asia/Seoul): KST session input TIMESTAMP'2026-01-02 03:04:05' arrives as the UTC
        // wall-clock "2026-01-01 18:04:05" — the instant, not the session digits
        Object[] row = CubridLogValueDecoder.toRow(DT, List.of(
                cv(0, "323032362d30312d3032"), // "2026-01-02"
                cv(1, "31353a30343a3035"), // "15:04:05"
                cv(2, "323032362d30312d30312031383a30343a3035"), // "2026-01-01 18:04:05"
                cv(3, "323032362d30312d30322030333a30343a30352e363030"))); // "2026-01-02 03:04:05.600"
        assertEquals(java.time.LocalDate.parse("2026-01-02"), row[0]);
        assertEquals(java.time.LocalTime.parse("15:04:05"), row[1]);
        assertEquals(java.time.OffsetDateTime.parse("2026-01-01T18:04:05Z"), row[2]);
        assertEquals(java.time.LocalDateTime.parse("2026-01-02T03:04:05.600"), row[3]);
    }

    @Test
    void dateTimeMinima() {
        // TIMESTAMP epoch floor: raw hex measured (#84 §2, FROM_UNIXTIME(1) — epoch 0 is below
        // CUBRID's TIMESTAMP range); the v1 corpus' KST assumption ("09:00:01") is gone
        Object[] row = CubridLogValueDecoder.toRow(DT, List.of(
                cvStr(0, "0001-01-01"),
                cvStr(1, "00:00:00"),
                cv(2, "313937302d30312d30312030303a30303a3031"), // "1970-01-01 00:00:01"
                cvStr(3, "0001-01-01 00:00:00.000")));
        assertEquals(java.time.LocalDate.parse("0001-01-01"), row[0]);
        assertEquals(java.time.LocalTime.MIDNIGHT, row[1]);
        assertEquals(java.time.Instant.ofEpochSecond(1), ((java.time.OffsetDateTime) row[2]).toInstant());
        assertEquals(java.time.LocalDateTime.parse("0001-01-01T00:00:00"), row[3]);
    }

    @Test
    void dateTimeMaxima() {
        // 32-bit TIMESTAMP epoch max renders as its UTC wall-clock 2038-01-19 03:14:07
        Object[] row = CubridLogValueDecoder.toRow(DT, List.of(
                cvStr(0, "9999-12-31"),
                cvStr(1, "23:59:59"),
                cvStr(2, "2038-01-19 03:14:07"),
                cvStr(3, "9999-12-31 23:59:59.999")));
        assertEquals(java.time.LocalDate.parse("9999-12-31"), row[0]);
        assertEquals(java.time.LocalTime.parse("23:59:59"), row[1]);
        assertEquals(java.time.Instant.ofEpochSecond(Integer.MAX_VALUE), ((java.time.OffsetDateTime) row[2]).toInstant());
        assertEquals(java.time.LocalDateTime.parse("9999-12-31T23:59:59.999"), row[3]);
    }

    @Test
    void leapDayAndMillisecondEdge() {
        Object[] row = CubridLogValueDecoder.toRow(DT, List.of(
                cvStr(0, "2028-02-29"),
                cvStr(1, "12:00:00"),
                cvStr(2, "2026-08-16 10:00:00"),
                cvStr(3, "2026-08-16 10:00:00.001")));
        assertEquals(java.time.LocalDate.parse("2028-02-29"), row[0]);
        assertEquals(java.time.LocalTime.NOON, row[1]);
        assertEquals(java.time.OffsetDateTime.parse("2026-08-16T10:00:00Z"), row[2]);
        assertEquals(java.time.LocalDateTime.parse("2026-08-16T10:00:00.001"), row[3]);
    }

    @Test
    void v1EngineTextIsRejectedLoudly() {
        // lockstep safety net (#76-D5 / wire v2 §3.2): a v1 engine's locale-default AM/PM text
        // must fail the strict parser — never silently produce a value
        assertThrows(DebeziumException.class, () -> CubridLogValueDecoder.toRow(DT,
                List.of(cvStr(2, "09:00:01 AM 01/01/1970"))));
        assertThrows(DebeziumException.class, () -> CubridLogValueDecoder.toRow(DT,
                List.of(cvStr(3, "10:00:00.001 AM 08/16/2026"))));
        assertThrows(DebeziumException.class, () -> CubridLogValueDecoder.toRow(DT,
                List.of(cvStr(0, "01/01/0001"))));
        assertThrows(DebeziumException.class, () -> CubridLogValueDecoder.toRow(DT,
                List.of(cvStr(1, "12:00:00 AM"))));
        // no lenient fallback either: the wire fraction is fixed at 3 digits
        assertThrows(DebeziumException.class, () -> CubridLogValueDecoder.toRow(DT,
                List.of(cvStr(3, "2026-08-16 10:00:00.5"))));
    }

    @Test
    void dateTimeNulls() {
        Object[] row = CubridLogValueDecoder.toRow(DT, List.of(
                cvNull(0), cvNull(1), cvNull(2), cvNull(3)));
        for (Object v : row) {
            assertNull(v);
        }
    }

    // ---------------------------------------------------------------- ENUM (maps to string)

    @Test
    void enumArrivesAsLabelString() {
        // ENUM('red','green','blue') — the log carries the label, JDBC reports VARCHAR(12)
        Table t = table(col("v_enum", 1, Types.VARCHAR, "ENUM"));
        assertEquals("red", CubridLogValueDecoder.toRow(t, List.of(cvStr(0, "red")))[0]);
        assertEquals("blue", CubridLogValueDecoder.toRow(t, List.of(cvStr(0, "blue")))[0]);
        assertNull(CubridLogValueDecoder.toRow(t, List.of(cvNull(0)))[0]);
    }

    // ---------------------------------------------------------------- structural invariants

    @Test
    void mergeOverlaysChangedOnCondWithBoundaryValues() {
        // full after = cond(before) ⊕ changed(after) (ADR 0003) — with boundary values
        Object[] row = CubridLogValueDecoder.merge(NUM,
                List.of(cv(0, "0080"), cv(1, "00000080"), cvStr(3, "0.0000000000")),
                List.of(cv(1, "ffffff7f")));
        assertEquals((short) -32768, row[0]); // untouched cond value survives
        assertEquals(2147483647, row[1]); // changed overlays cond
        assertEquals(new BigDecimal("0.0000000000"), row[3]);
        assertNull(row[2]); // column absent from both images stays null
    }

    @Test
    void absentColumnsStayNull() {
        Object[] row = CubridLogValueDecoder.toRow(NUM, List.of(cv(2, "0100000000000000")));
        assertArrayEquals(new Object[]{ null, null, 1L, null, null, null }, row);
    }

    @Test
    void outOfRangeColumnIndexFailsLoudly() {
        assertThrows(DebeziumException.class,
                () -> CubridLogValueDecoder.toRow(STR, List.of(cvStr(2, "x"))));
        assertThrows(DebeziumException.class,
                () -> CubridLogValueDecoder.toRow(STR, List.of(cvStr(-1, "x"))));
    }
}
