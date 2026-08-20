/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.Locale;

import io.debezium.DebeziumException;

/**
 * The single strict parser/renderer for CUBRID temporal text (workspace#76, wire v2 —
 * {@code docs/htap-cdc-wire-v2.md} §3.2). Both temporal text sources speak the same grammar,
 * so one parser serves both:
 * <ul>
 * <li><b>CDC wire v2</b>: the engine renders every temporal as
 * {@code YYYY-MM-DD HH24:MI:SS[.FF][ ±TZH:TZM]}, with TIMESTAMP/LTZ wall-clocks always UTC
 * (the CDC daemon session timezone is pinned to UTC engine-side).</li>
 * <li><b>JDBC snapshot</b>: {@code ResultSet.getString} formats the driver-transported digits as
 * {@code yyyy-MM-dd HH:mm:ss} (TIMESTAMP) / {@code yyyy-MM-dd HH:mm:ss.SSS} (DATETIME); with the
 * session pinned to UTC ({@link CubridConnection}), TIMESTAMP digits are UTC too. Reading the
 * digits as text sidesteps the driver's default-zone {@code java.sql.Timestamp} construction —
 * no implicit JVM zone anywhere on this path.</li>
 * </ul>
 * Parsing is STRICT (#76-D4/D5): only this grammar is accepted, there is no bypass switch and no
 * lenient fallback. v1 engine text (locale-default {@code hh:mm:ss AM MM/dd/yyyy}) fails loudly —
 * the lockstep safety net for a mispaired old engine.
 */
final class CubridTemporal {

    private static final DateTimeFormatter DATE = DateTimeFormatter
            .ofPattern("uuuu-MM-dd", Locale.ROOT).withResolverStyle(ResolverStyle.STRICT);
    private static final DateTimeFormatter TIME = DateTimeFormatter
            .ofPattern("HH:mm:ss", Locale.ROOT).withResolverStyle(ResolverStyle.STRICT);
    private static final DateTimeFormatter TIMESTAMP = DateTimeFormatter
            .ofPattern("uuuu-MM-dd HH:mm:ss", Locale.ROOT).withResolverStyle(ResolverStyle.STRICT);
    private static final DateTimeFormatter DATETIME = DateTimeFormatter
            .ofPattern("uuuu-MM-dd HH:mm:ss.SSS", Locale.ROOT).withResolverStyle(ResolverStyle.STRICT);
    private static final DateTimeFormatter DATETIME_ISO = DateTimeFormatter
            .ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSS", Locale.ROOT);

    private CubridTemporal() {
    }

    /** Wire v2 / JDBC {@code YYYY-MM-DD} → {@link LocalDate}. */
    static LocalDate parseDate(String text) {
        return parse(text, DATE, LocalDate::from, "DATE", "YYYY-MM-DD");
    }

    /** Wire v2 / JDBC {@code HH24:MI:SS} → {@link LocalTime}. */
    static LocalTime parseTime(String text) {
        return parse(text, TIME, LocalTime::from, "TIME", "HH24:MI:SS");
    }

    /**
     * Wire v2 / UTC-pinned JDBC {@code YYYY-MM-DD HH24:MI:SS} → the {@link OffsetDateTime} of the
     * true instant: the digits ARE the UTC wall-clock (wire: engine-side daemon tz pin; snapshot:
     * connector-side {@code SET TIME ZONE 'UTC'}), so attaching {@link ZoneOffset#UTC} restores
     * the instant exactly (#76-D3, MySQL binlog TIMESTAMP model).
     */
    static OffsetDateTime parseTimestampUtc(String text) {
        return parse(text, TIMESTAMP, LocalDateTime::from, "TIMESTAMP", "YYYY-MM-DD HH24:MI:SS")
                .atOffset(ZoneOffset.UTC);
    }

    /** Wire v2 / JDBC {@code YYYY-MM-DD HH24:MI:SS.FF3} → zone-less {@link LocalDateTime}. */
    static LocalDateTime parseDatetime(String text) {
        return parse(text, DATETIME, LocalDateTime::from, "DATETIME", "YYYY-MM-DD HH24:MI:SS.FF3");
    }

    /**
     * The Kafka value of a zone-less DATETIME (#76-D3): ISO-8601 local date-time with the
     * millisecond precision fixed at 3 digits, NO offset — the type carries no instant, and the
     * v1 contract's fabricated {@code Z} was the P0-3 corruption. Sinks bind it to a zone
     * explicitly (e.g. ClickHouse {@code DateTime64(3,'UTC')} + {@code best_effort}).
     */
    static String toIsoDatetimeString(LocalDateTime value) {
        return DATETIME_ISO.format(value);
    }

    private static <T> T parse(String text, DateTimeFormatter format, java.time.temporal.TemporalQuery<T> query,
                               String type, String grammar) {
        try {
            return format.parse(text.trim(), query);
        }
        catch (DateTimeParseException e) {
            throw new DebeziumException(
                    "CUBRID " + type + " text '" + text + "' does not match the wire v2 grammar '" + grammar
                            + "' (docs/htap-cdc-wire-v2.md §3.2). The engine and this connector must be a lockstep"
                            + " wire v2 pair — a v1 engine renders locale-default AM/PM text and MUST fail here."
                            + " Action: (1) confirm the paired engine build implements wire v2 (workspace#84);"
                            + " (2) if this value came from a JDBC snapshot, confirm the session timezone pin"
                            + " succeeded (the connector issues SET TIME ZONE 'UTC' on every connection);"
                            + " (3) after fixing the pairing, recover by re-snapshotting.",
                    e);
        }
    }
}
