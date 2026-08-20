/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.List;
import java.util.TimeZone;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.connector.cubrid.log.RawLogItem.ColumnValue;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.time.ZonedTimestamp;

/**
 * The temporal Kafka contract of #76-D3 through the full decode→convert chain, exercised under a
 * NON-UTC default JVM zone (Asia/Seoul — #76-D4: the pipeline must never consult the worker JVM's
 * default zone). The whole class runs with the default zone flipped; if any step implicitly used
 * it, the expected UTC-anchored outputs below would shift by the +09:00 offset.
 */
class CubridTemporalContractTest {

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

    private static final Column TS = Column.editor().name("v_ts").position(1)
            .jdbcType(Types.TIMESTAMP).type("TIMESTAMP").optional(true).create();
    private static final Column DTM = Column.editor().name("v_dtm").position(2)
            .jdbcType(Types.TIMESTAMP).type("DATETIME").optional(true).create();

    private static final Table TABLE = Table.editor()
            .tableId(new TableId(null, "htapdb", "t_temporal"))
            .addColumn(TS)
            .addColumn(DTM)
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
    void schemaSplitsByTypeName() {
        assertEquals(ZonedTimestamp.SCHEMA_NAME, CONVERTERS.schemaBuilder(TS).name());
        assertNull(CONVERTERS.schemaBuilder(DTM).name(), "DATETIME is a plain zone-less string");
        assertEquals(Schema.Type.STRING, CONVERTERS.schemaBuilder(DTM).build().type());
    }

    /**
     * Same displayed digits, different contracts (#76-D6): a TIMESTAMP whose UTC wall-clock reads
     * {@code 2026-01-02 03:04:05} is the instant {@code ...T03:04:05Z}, while a DATETIME with the
     * same digits stays a zone-less local value with no offset token at all.
     */
    @Test
    void sameDisplayedValueDivergesByContract() {
        Object[] row = CubridLogValueDecoder.toRow(TABLE, List.of(
                wire(0, "2026-01-02 03:04:05"),
                wire(1, "2026-01-02 03:04:05.000")));
        Object ts = convert(TS, row[0]);
        Object dtm = convert(DTM, row[1]);
        assertEquals("2026-01-02T03:04:05Z", ts);
        assertEquals("2026-01-02T03:04:05.000", dtm);
        assertNotEquals(ts, dtm, "instant and zone-less contracts must stay distinguishable");
    }

    @Test
    void wireTimestampRestoresTheTrueInstantUnderNonUtcJvm() {
        // measured epoch floor (#84 §2): wire "1970-01-01 00:00:01" IS epoch second 1; a JVM-zone
        // leak would shift this by -09:00 (the v1 wall-clock passthrough bug this map fixes)
        Object[] row = CubridLogValueDecoder.toRow(TABLE, List.of(wire(0, "1970-01-01 00:00:01")));
        assertEquals(java.time.Instant.ofEpochSecond(1), ((java.time.OffsetDateTime) row[0]).toInstant());
        assertEquals("1970-01-01T00:00:01Z", convert(TS, row[0]));
    }

    @Test
    void datetimeKeepsMillisecondDigitsVerbatim() {
        Object[] row = CubridLogValueDecoder.toRow(TABLE, List.of(wire(1, "2299-12-31 23:59:59.999")));
        assertEquals("2299-12-31T23:59:59.999", convert(DTM, row[1]));
    }

    @Test
    void snapshotJdbcDigitsAndWireDigitsConvergeToTheSameKafkaValue() {
        // snapshot parity (#76-D6): the UTC-pinned JDBC session yields the same digit strings the
        // wire carries, and CubridConnection.getColumnValue parses them with the same parser —
        // same row, same Kafka value on both paths
        Object snapshotTs = convert(TS, CubridTemporal.parseTimestampUtc("2026-01-01 18:04:05"));
        Object streamTs = convert(TS,
                CubridLogValueDecoder.toRow(TABLE, List.of(wire(0, "2026-01-01 18:04:05")))[0]);
        assertEquals(snapshotTs, streamTs);
        assertEquals("2026-01-01T18:04:05Z", streamTs);

        Object snapshotDtm = convert(DTM, CubridTemporal.parseDatetime("2026-01-02 03:04:05.600"));
        Object streamDtm = convert(DTM,
                CubridLogValueDecoder.toRow(TABLE, List.of(wire(1, "2026-01-02 03:04:05.600")))[1]);
        assertEquals(snapshotDtm, streamDtm);
        assertEquals("2026-01-02T03:04:05.600", streamDtm);
    }

    @Test
    void nullsStayNull() {
        assertNull(convert(TS, null));
        assertNull(convert(DTM, null));
    }
}
