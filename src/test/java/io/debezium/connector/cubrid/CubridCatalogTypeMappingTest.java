/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Types;

import org.junit.jupiter.api.Test;

/**
 * {@code db_attribute.data_type} string -> {@code java.sql.Types} mapping (ADR 0011 D9).
 * <p>
 * Every string below is a measured catalog value (CUBRID 11.5, workspace#69 probe); the expected
 * jdbcType mirrors what the JDBC driver reports for the same column ({@code docs/type-support.md}),
 * so the streaming decoder and value converters behave identically to the previous
 * driver-metadata discovery path.
 */
class CubridCatalogTypeMappingTest {

    @Test
    void supportedTypesMirrorTheDriverReport() {
        assertEquals(Types.SMALLINT, CubridConnection.jdbcTypeFor("SHORT"));
        assertEquals(Types.INTEGER, CubridConnection.jdbcTypeFor("INTEGER"));
        assertEquals(Types.BIGINT, CubridConnection.jdbcTypeFor("BIGINT"));
        assertEquals(Types.NUMERIC, CubridConnection.jdbcTypeFor("NUMERIC"));
        assertEquals(Types.REAL, CubridConnection.jdbcTypeFor("FLOAT"));
        assertEquals(Types.DOUBLE, CubridConnection.jdbcTypeFor("DOUBLE"));
        assertEquals(Types.CHAR, CubridConnection.jdbcTypeFor("CHAR"));
        assertEquals(Types.VARCHAR, CubridConnection.jdbcTypeFor("STRING"));
        assertEquals(Types.VARCHAR, CubridConnection.jdbcTypeFor("ENUM"));
        assertEquals(Types.DATE, CubridConnection.jdbcTypeFor("DATE"));
        assertEquals(Types.TIME, CubridConnection.jdbcTypeFor("TIME"));
        assertEquals(Types.TIMESTAMP, CubridConnection.jdbcTypeFor("TIMESTAMP"));
        assertEquals(Types.TIMESTAMP, CubridConnection.jdbcTypeFor("DATETIME"));
    }

    @Test
    void unsupportedTypesMirrorTheDriverReportButKeepDistinctTypeNames() {
        // jdbcType overlaps a supported type on purpose (driver parity) — the fail-fast guard
        // (workspace#73) must therefore key on the typeName string, never on the jdbcType.
        assertEquals(Types.DOUBLE, CubridConnection.jdbcTypeFor("MONETARY"));
        assertEquals(Types.VARCHAR, CubridConnection.jdbcTypeFor("JSON"));
        assertEquals(Types.TIMESTAMP, CubridConnection.jdbcTypeFor("TIMESTAMPTZ"));
        assertEquals(Types.TIMESTAMP, CubridConnection.jdbcTypeFor("TIMESTAMPLTZ"));
        assertEquals(Types.TIMESTAMP, CubridConnection.jdbcTypeFor("DATETIMETZ"));
        assertEquals(Types.TIMESTAMP, CubridConnection.jdbcTypeFor("DATETIMELTZ"));
        assertEquals(Types.BINARY, CubridConnection.jdbcTypeFor("BIT"));
        assertEquals(Types.VARBINARY, CubridConnection.jdbcTypeFor("VARBIT"));
        assertEquals(Types.BLOB, CubridConnection.jdbcTypeFor("BLOB"));
        assertEquals(Types.CLOB, CubridConnection.jdbcTypeFor("CLOB"));
        assertEquals(Types.OTHER, CubridConnection.jdbcTypeFor("SET"));
        assertEquals(Types.OTHER, CubridConnection.jdbcTypeFor("MULTISET"));
        assertEquals(Types.OTHER, CubridConnection.jdbcTypeFor("SEQUENCE"));
    }

    @Test
    void unknownFutureTypesFallToOther() {
        assertEquals(Types.OTHER, CubridConnection.jdbcTypeFor("SOME_FUTURE_TYPE"));
    }
}
