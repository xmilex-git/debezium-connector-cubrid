/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import java.time.ZoneOffset;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;

/**
 * Conversion of CUBRID specific datatypes.
 * <p>
 * TODO(workspace#38): CUBRID-specific type fidelity (BLOB/CLOB, ENUM, DATETIMETZ) is not handled yet;
 * the POC relies on the generic JDBC conversions.
 */
public class CubridValueConverters extends JdbcValueConverters {

    public CubridValueConverters(DecimalMode decimalMode, TemporalPrecisionMode temporalPrecisionMode, BinaryHandlingMode binaryHandlingMode) {
        super(decimalMode, temporalPrecisionMode, ZoneOffset.UTC, null, null, binaryHandlingMode);
    }
}
