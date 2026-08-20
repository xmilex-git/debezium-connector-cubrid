/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.time.ZonedTimestamp;

/**
 * Conversion of CUBRID specific datatypes.
 * <p>
 * CUBRID DATETIME and TIMESTAMP both report {@link Types#TIMESTAMP}, but their contracts differ —
 * the split keys on the catalog typeName (#76-D3, resolving ADR 0005's "TYPE_NAME 분기" follow-up):
 * <ul>
 * <li><b>TIMESTAMP</b> stores an instant (UTC epoch) → {@link ZonedTimestamp}. Both paths deliver
 * an {@link java.time.OffsetDateTime} restored from UTC wall-clock digits (wire v2 §3.1 pins the
 * CDC daemon timezone to UTC; the snapshot session is pinned by {@code SET TIME ZONE 'UTC'}), so
 * the emitted ISO string is the true instant — never a fabricated {@code Z} on local digits.</li>
 * <li><b>DATETIME</b> is zone-less → offset-less ISO-8601 string ({@link CubridTemporal}), the
 * PostgreSQL {@code timestamp_out} shape. The sink binds the zone explicitly (validated path:
 * ClickHouse {@code DateTime64(3,'UTC')} + {@code date_time_input_format=best_effort}).</li>
 * </ul>
 * TZ-carrying types (TIMESTAMPTZ/LTZ, DATETIMETZ/LTZ) are rejected by the
 * {@link UnsupportedTypeGuard} until workspace#86.
 * <p>
 * TODO: BLOB/CLOB fidelity is out of POC scope; the generic JDBC conversions apply to everything
 * else.
 */
public class CubridValueConverters extends JdbcValueConverters {

    public CubridValueConverters(DecimalMode decimalMode, TemporalPrecisionMode temporalPrecisionMode, BinaryHandlingMode binaryHandlingMode) {
        super(decimalMode, temporalPrecisionMode, ZoneOffset.UTC, null, null, binaryHandlingMode);
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        if (column.jdbcType() == Types.TIMESTAMP) {
            if (isInstantTimestamp(column)) {
                return ZonedTimestamp.builder();
            }
            return SchemaBuilder.string();
        }
        return super.schemaBuilder(column);
    }

    @Override
    public ValueConverter converter(Column column, org.apache.kafka.connect.data.Field fieldDefn) {
        if (column.jdbcType() == Types.TIMESTAMP) {
            if (isInstantTimestamp(column)) {
                return data -> convertTimestampWithZone(column, fieldDefn, data);
            }
            return data -> convertZonelessDatetime(column, fieldDefn, data);
        }
        return super.converter(column, fieldDefn);
    }

    private static boolean isInstantTimestamp(Column column) {
        return "TIMESTAMP".equalsIgnoreCase(column.typeName());
    }

    private Object convertZonelessDatetime(Column column, org.apache.kafka.connect.data.Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, CubridTemporal.toIsoDatetimeString(LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC)), (r) -> {
            if (data instanceof LocalDateTime) {
                r.deliver(CubridTemporal.toIsoDatetimeString((LocalDateTime) data));
            }
        });
    }
}
