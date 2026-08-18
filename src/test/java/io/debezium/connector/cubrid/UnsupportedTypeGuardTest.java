/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * Unsupported-type fail-fast guard (workspace#73). The offending types below are the measured
 * silently-dangerous ones of {@code docs/type-support.md}: MONETARY (garbage double) and JSON
 * (silent NULL loss) share a jdbcType with supported types, which is exactly why the guard must
 * key on the catalog typeName.
 */
class UnsupportedTypeGuardTest {

    private static Table table(Column... columns) {
        return Table.editor()
                .tableId(new TableId(null, "app", "t_guard"))
                .addColumns(List.of(columns))
                .create();
    }

    private static Column col(String name, String typeName) {
        return Column.editor()
                .name(name)
                .type(typeName)
                .jdbcType(CubridConnection.jdbcTypeFor(typeName))
                .create();
    }

    @Test
    void supportedOnlyTablePasses() {
        assertDoesNotThrow(() -> UnsupportedTypeGuard.checkTable(table(
                col("c_short", "SHORT"), col("c_int", "INTEGER"), col("c_bigint", "BIGINT"),
                col("c_num", "NUMERIC"), col("c_float", "FLOAT"), col("c_double", "DOUBLE"),
                col("c_char", "CHAR"), col("c_str", "STRING"), col("c_enum", "ENUM"),
                col("c_date", "DATE"), col("c_time", "TIME"),
                col("c_ts", "TIMESTAMP"), col("c_dt", "DATETIME"))));
    }

    @Test
    void unsupportedColumnsFailNamingEveryTypeAndColumn() {
        final DebeziumException e = assertThrows(DebeziumException.class,
                () -> UnsupportedTypeGuard.checkTable(table(
                        col("c_int", "INTEGER"),
                        col("c_money", "MONETARY"),
                        col("c_doc", "JSON"))));
        assertTrue(e.getMessage().contains("app.t_guard.c_money (MONETARY)"), e.getMessage());
        assertTrue(e.getMessage().contains("app.t_guard.c_doc (JSON)"), e.getMessage());
        assertTrue(e.getMessage().contains("type-support.md"), e.getMessage());
    }

    @Test
    void everyUnsupportedMatrixTypeFails() {
        for (String typeName : List.of("MONETARY", "BIT", "VARBIT",
                "TIMESTAMPTZ", "TIMESTAMPLTZ", "DATETIMETZ", "DATETIMELTZ",
                "SET", "MULTISET", "SEQUENCE", "BLOB", "CLOB", "JSON")) {
            assertThrows(DebeziumException.class,
                    () -> UnsupportedTypeGuard.checkTable(table(col("c", typeName))),
                    typeName);
        }
    }

    @Test
    void unknownFutureTypeFailsInsteadOfPassingSilently() {
        assertThrows(DebeziumException.class,
                () -> UnsupportedTypeGuard.checkTable(table(col("c", "SOME_FUTURE_TYPE"))));
    }
}
