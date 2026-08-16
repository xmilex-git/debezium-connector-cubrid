/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import java.sql.Types;
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
 * CUBRID DATETIME/TIMESTAMP (both {@link Types#TIMESTAMP}) are emitted as {@link ZonedTimestamp}
 * ISO-8601 UTC strings — the sink contract of workspace#39 (ClickHouse
 * {@code DateTime64(3,'UTC')} + {@code date_time_input_format=best_effort}). Values pass through
 * as wall-clock: both the snapshot (JDBC {@code java.sql.Timestamp}) and the streaming decoder
 * produce timestamps in the worker JVM's default zone, and the UTC default offset renders the
 * same wall-clock digits back out.
 * <p>
 * TODO: BLOB/CLOB, ENUM, DATETIMETZ fidelity is out of POC scope; the generic JDBC conversions
 * apply to everything else.
 */
public class CubridValueConverters extends JdbcValueConverters {

    public CubridValueConverters(DecimalMode decimalMode, TemporalPrecisionMode temporalPrecisionMode, BinaryHandlingMode binaryHandlingMode) {
        super(decimalMode, temporalPrecisionMode, ZoneOffset.UTC, null, null, binaryHandlingMode);
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        if (column.jdbcType() == Types.TIMESTAMP) {
            return ZonedTimestamp.builder();
        }
        return super.schemaBuilder(column);
    }

    @Override
    public ValueConverter converter(Column column, org.apache.kafka.connect.data.Field fieldDefn) {
        if (column.jdbcType() == Types.TIMESTAMP) {
            return data -> convertTimestampWithZone(column, fieldDefn, data);
        }
        return super.converter(column, fieldDefn);
    }
}
