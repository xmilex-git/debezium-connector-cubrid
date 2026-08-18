/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import io.debezium.DebeziumException;
import io.debezium.relational.Column;
import io.debezium.relational.Table;

/**
 * Fail-fast guard against unsupported column types in captured tables (workspace#73, the spirit of
 * ADR 0008's DDL halt): several unsupported types corrupt or lose data <b>silently</b> when
 * streamed — SET/MULTISET/LIST and JSON arrive as NULL in the supplemental log, MONETARY is
 * decoded as a garbage double — so the connector refuses to start rather than let them through.
 * <p>
 * The check keys on the catalog {@code data_type} string ({@link Column#typeName()}), never on the
 * jdbcType: MONETARY/JSON/the TZ family share a jdbcType with supported types by design (driver
 * parity, {@link CubridConnection#jdbcTypeFor}). The supported set below is the 1.0 matrix of
 * {@code docs/type-support.md} — an allow-list, so a type the matrix does not know also fails
 * instead of passing silently.
 */
final class UnsupportedTypeGuard {

    private static final Set<String> SUPPORTED_TYPE_NAMES = Set.of(
            "SHORT", "INTEGER", "BIGINT", "NUMERIC", "FLOAT", "DOUBLE",
            "CHAR", "STRING", "ENUM",
            "DATE", "TIME", "TIMESTAMP", "DATETIME");

    private UnsupportedTypeGuard() {
    }

    /**
     * @throws DebeziumException naming every unsupported column of the captured table
     */
    static void checkTable(Table table) {
        final List<String> offending = new ArrayList<>();
        for (Column column : table.columns()) {
            final String typeName = column.typeName() == null ? "" : column.typeName().toUpperCase(Locale.ROOT);
            if (!SUPPORTED_TYPE_NAMES.contains(typeName)) {
                offending.add(table.id() + "." + column.name() + " (" + column.typeName() + ")");
            }
        }
        if (!offending.isEmpty()) {
            throw new DebeziumException(
                    "Captured table contains column types the CUBRID connector does not support in 1.0"
                            + " (values would be silently lost or corrupted in the change stream): "
                            + String.join(", ", offending)
                            + ". Remove these tables from table.include.list or drop/convert the columns."
                            + " See docs/type-support.md for the supported type matrix.");
        }
    }
}
