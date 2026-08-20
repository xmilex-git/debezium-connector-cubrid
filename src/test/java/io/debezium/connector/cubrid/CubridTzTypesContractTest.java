/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.TimeZone;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.connector.cubrid.log.RawLogItem.ColumnValue;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.time.ZonedTimestamp;

/**
 * The TZ-family Kafka contract of workspace#86 (#76-D3 third bullet) through the full
 * decode→convert chain, under a NON-UTC default JVM zone (Asia/Seoul — #76-D4: no step may
 * consult the worker JVM's default zone). All four types are instants; the wire and the
 * TO_CHAR-projected snapshot speak the same {@code ±TZH:TZM} grammar
 * ({@code docs/htap-cdc-wire-v2.md} §3.2), so both paths land on the same {@link ZonedTimestamp}
 * value with the value's offset preserved.
 */
class CubridTzTypesContractTest {

    private static TimeZone savedZone;

    @BeforeAll
    static void pinNonUtcJvmZone() {
        savedZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Seoul"));
    }

    @AfterAll
    static void restoreJvmZone() {
        TimeZone.setDefault(savedZone);
    }

    private static final CubridValueConverters CONVERTERS = new CubridValueConverters(
            DecimalMode.DOUBLE, TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS, BinaryHandlingMode.BYTES);

    private static final Column TSTZ = Column.editor().name("v_tstz").position(1)
            .jdbcType(Types.TIMESTAMP_WITH_TIMEZONE).type("TIMESTAMPTZ").optional(true).create();
    private static final Column TSLTZ = Column.editor().name("v_tsltz").position(2)
            .jdbcType(Types.TIMESTAMP_WITH_TIMEZONE).type("TIMESTAMPLTZ").optional(true).create();
    private static final Column DTTZ = Column.editor().name("v_dttz").position(3)
            .jdbcType(Types.TIMESTAMP_WITH_TIMEZONE).type("DATETIMETZ").optional(true).create();
    private static final Column DTLTZ = Column.editor().name("v_dtltz").position(4)
            .jdbcType(Types.TIMESTAMP_WITH_TIMEZONE).type("DATETIMELTZ").optional(true).create();

    private static final Table TABLE = Table.editor()
            .tableId(new TableId(null, "htapdb", "t_tz"))
            .addColumn(TSTZ)
            .addColumn(TSLTZ)
            .addColumn(DTTZ)
            .addColumn(DTLTZ)
            .create();

    private static Object convert(Column column, Object raw) {
        Schema schema = CONVERTERS.schemaBuilder(column).optional().build();
        Field field = new Field(column.name(), 0, schema);
        return CONVERTERS.converter(column, field).convert(raw);
    }

    private static ColumnValue wire(int index, String text) {
        return new ColumnValue(index, text.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void allFourTypesMapToZonedTimestamp() {
        for (Column column : List.of(TSTZ, TSLTZ, DTTZ, DTLTZ)) {
            assertEquals(ZonedTimestamp.SCHEMA_NAME, CONVERTERS.schemaBuilder(column).name(), column.typeName());
        }
    }

    /** Wire v2 §3.2 examples, byte-exact (engine TO_CHAR measured 2026-08-19/20). */
    @Test
    void wireTextDecodesWithTheValueOffsetPreserved() {
        Object[] row = CubridLogValueDecoder.toRow(TABLE, List.of(
                wire(0, "2026-01-02 03:04:05 +09:00"),
                wire(1, "2026-01-01 18:04:05 +00:00"),
                wire(2, "2026-01-02 03:04:05.670 +09:00"),
                wire(3, "2026-01-01 18:04:05.670 +00:00")));
        // the four cells above are the SAME two instants seen from KST and from the UTC-rendered
        // LTZ side — instant identity must survive the differing offsets
        assertEquals(((OffsetDateTime) row[0]).toInstant(), ((OffsetDateTime) row[1]).toInstant());
        assertEquals(((OffsetDateTime) row[2]).toInstant(), ((OffsetDateTime) row[3]).toInstant());
        assertEquals(Instant.parse("2026-01-01T18:04:05Z"), ((OffsetDateTime) row[0]).toInstant());
        assertEquals(Instant.parse("2026-01-01T18:04:05.670Z"), ((OffsetDateTime) row[2]).toInstant());

        assertEquals("2026-01-02T03:04:05+09:00", convert(TSTZ, row[0]));
        assertEquals("2026-01-01T18:04:05Z", convert(TSLTZ, row[1]));
        assertEquals("2026-01-02T03:04:05.670+09:00", convert(DTTZ, row[2]));
        assertEquals("2026-01-01T18:04:05.670Z", convert(DTLTZ, row[3]));
    }

    @Test
    void negativeAndHalfHourOffsetsSurvive() {
        Object[] row = CubridLogValueDecoder.toRow(TABLE, List.of(
                wire(2, "1970-01-01 00:00:00.001 -05:30")));
        assertEquals(Instant.ofEpochMilli(1L + 5 * 3600_000 + 30 * 60_000), ((OffsetDateTime) row[2]).toInstant());
        assertEquals("1970-01-01T00:00:00.001-05:30", convert(DTTZ, row[2]));
    }

    /**
     * Snapshot parity: the TO_CHAR projection makes the JDBC text identical to the wire text,
     * and {@link CubridConnection#getColumnValue} routes it through the same parsers — same
     * digits, same Kafka value on both paths.
     */
    @Test
    void snapshotToCharTextAndWireTextConverge() {
        assertEquals(
                convert(TSTZ, CubridTemporal.parseTimestampTz("2026-06-15 12:00:00 +09:00")),
                convert(TSTZ, CubridLogValueDecoder.toRow(TABLE, List.of(wire(0, "2026-06-15 12:00:00 +09:00")))[0]));
        assertEquals(
                convert(DTLTZ, CubridTemporal.parseDatetimeTz("2026-06-15 03:00:00.123 +00:00")),
                convert(DTLTZ, CubridLogValueDecoder.toRow(TABLE, List.of(wire(3, "2026-06-15 03:00:00.123 +00:00")))[3]));
    }

    /**
     * Strict grammar (#76-D4/D5): only {@code ±TZH:TZM} after exactly one space. Everything the
     * driver's native object path could have produced — trimmed fractions, region tokens, 'Z' —
     * must fail loudly rather than be guessed at (workspace#86 D1 rejected that path).
     */
    @Test
    void nonWireTextFailsLoudly() {
        for (String bad : List.of(
                "2026-01-02 03:04:05", // offset missing
                "2026-01-02 03:04:05 Z", // 'Z' is not on the wire; zero offset is '+00:00'
                "2026-01-02 03:04:05Z",
                "2026-01-02 03:04:05 +09", // minutes missing
                "2026-06-15 12:00:00 Asia/Seoul KST", // driver toString region token
                "03:04:05 AM 01/02/2026 +09:00")) { // v1 locale-default
            assertThrows(DebeziumException.class, () -> CubridTemporal.parseTimestampTz(bad), bad);
        }
        for (String bad : List.of(
                "2026-01-02 03:04:05.67 +09:00", // driver toString trims trailing zeros — reject
                "2026-01-02 03:04:05 +09:00", // fraction missing for DATETIME family
                "2026-01-02 03:04:05.670 UTC")) {
            assertThrows(DebeziumException.class, () -> CubridTemporal.parseDatetimeTz(bad), bad);
        }
    }

    @Test
    void snapshotProjectionWrapsOnlyTzColumnsAndAliasesBack() {
        final Column id = Column.editor().name("id").position(1)
                .jdbcType(Types.INTEGER).type("INTEGER").optional(false).create();
        final Table table = Table.editor()
                .tableId(new TableId(null, "dba", "t_tz"))
                .addColumn(id)
                .addColumn(TSTZ)
                .addColumn(DTLTZ)
                .create();
        assertEquals(
                "\"id\""
                        + ", TO_CHAR(\"v_tstz\", 'YYYY-MM-DD HH24:MI:SS TZH:TZM') AS \"v_tstz\""
                        + ", TO_CHAR(\"v_dtltz\", 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM') AS \"v_dtltz\"",
                CubridSnapshotChangeEventSource.projectColumns(table,
                        List.of("\"id\"", "\"v_tstz\"", "\"v_dtltz\"")));
    }

    @Test
    void nullsStayNull() {
        for (Column column : List.of(TSTZ, TSLTZ, DTTZ, DTLTZ)) {
            assertNull(convert(column, null));
        }
    }
}
