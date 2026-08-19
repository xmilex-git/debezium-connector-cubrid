/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import io.debezium.DebeziumException;

/**
 * Fail-fast guard: the connector supports UTF-8 databases only in 1.0 (workspace#77, review
 * §4.14 option A). The engine ships column values, owner/table names and DDL statements as the
 * database codeset's raw bytes with no charset tag, and the connector decodes all of them as
 * UTF-8 ({@code OrReader}, {@code CubridLogValueDecoder}) while the snapshot goes through JDBC —
 * on a non-UTF-8 database both paths corrupt non-ASCII data <b>silently</b>, and differently
 * from each other. So the connector refuses to start against such a database.
 * <p>
 * The charset id is the engine's {@code INTL_CODESET} enum (intl_support.h), read once at
 * startup from {@code db_root} ({@link CubridConnection#readDatabaseCharsetId}).
 */
final class DatabaseCharsetGuard {

    /** {@code INTL_CODESET_UTF8} — the only codeset the 1.0 connector accepts. */
    static final int UTF8_CHARSET_ID = 5;

    private DatabaseCharsetGuard() {
    }

    /**
     * @throws DebeziumException when the database codeset is anything but UTF-8
     */
    static void check(int charsetId) {
        if (charsetId == UTF8_CHARSET_ID) {
            return;
        }
        throw new DebeziumException(
                "The database charset is " + charsetName(charsetId)
                        + " but the CUBRID connector supports UTF-8 databases only in 1.0."
                        + " The engine streams strings as raw database-codeset bytes without a charset tag"
                        + " and the connector decodes them as UTF-8, so every non-ASCII column value,"
                        + " identifier and DDL statement would be corrupted silently — and the JDBC snapshot"
                        + " would disagree with the change stream."
                        + " Capture from a database created with a UTF-8 locale instead"
                        + " (cubrid createdb ... <locale>.utf8; an existing database must be migrated,"
                        + " e.g. unloaddb/loaddb into a new UTF-8 database)."
                        + " See docs/support-scope.md §5 known limitations.");
    }

    /** Human-readable name of an {@code INTL_CODESET} id, for the refusal message. */
    static String charsetName(int charsetId) {
        switch (charsetId) {
            case 0:
                return "ASCII (id 0)";
            case 1:
                return "RAW-BITS (id 1)";
            case 2:
                return "BINARY (id 2)";
            case 3:
                return "ISO-8859-1 (id 3)";
            case 4:
                return "EUC-KR (id 4)";
            case UTF8_CHARSET_ID:
                return "UTF-8 (id 5)";
            default:
                return "unknown (id " + charsetId + ")";
        }
    }
}
