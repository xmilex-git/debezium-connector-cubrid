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

/**
 * UTF-8-only startup guard (workspace#77, review §4.14 option A): any codeset id other than
 * {@code INTL_CODESET_UTF8} (5) refuses to start — including ids the mapping does not know,
 * so a future engine codeset fails instead of passing silently.
 */
class DatabaseCharsetGuardTest {

    @Test
    void utf8Passes() {
        assertDoesNotThrow(() -> DatabaseCharsetGuard.check(5));
    }

    @Test
    void everyNonUtf8KnownCodesetFails() {
        for (int id : List.of(0, 1, 2, 3, 4)) {
            assertThrows(DebeziumException.class, () -> DatabaseCharsetGuard.check(id), "id " + id);
        }
    }

    @Test
    void unknownFutureCodesetFailsInsteadOfPassingSilently() {
        assertThrows(DebeziumException.class, () -> DatabaseCharsetGuard.check(6));
        assertThrows(DebeziumException.class, () -> DatabaseCharsetGuard.check(-1));
    }

    @Test
    void euckrRefusalNamesTheCharsetAndTheRemedy() {
        final DebeziumException e = assertThrows(DebeziumException.class, () -> DatabaseCharsetGuard.check(4));
        assertTrue(e.getMessage().contains("EUC-KR (id 4)"), e.getMessage());
        assertTrue(e.getMessage().contains("UTF-8 databases only"), e.getMessage());
        assertTrue(e.getMessage().contains("cubrid createdb"), e.getMessage());
        assertTrue(e.getMessage().contains("support-scope.md"), e.getMessage());
    }
}
