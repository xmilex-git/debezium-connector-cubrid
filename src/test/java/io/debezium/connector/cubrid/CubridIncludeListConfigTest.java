/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;

/**
 * {@code table.include.list} is mandatory and literal (workspace#70, ADR 0011 D2): the
 * entries double as the server-side extraction targets and the per-table SELECT list, so
 * an unset list (whole-log = DBA-only) and regex patterns are rejected at startup.
 */
class CubridIncludeListConfigTest {

    private static CubridConnectorConfig config(String includeList) {
        Configuration.Builder builder = Configuration.create()
                .with("topic.prefix", "htapcdc")
                .with("database.dbname", "htapdb");
        if (includeList != null) {
            builder = builder.with("table.include.list", includeList);
        }
        return new CubridConnectorConfig(builder.build());
    }

    @Test
    void includeListIsMandatory() {
        assertThrows(DebeziumException.class, () -> config(null));
        assertThrows(DebeziumException.class, () -> config("   "));
    }

    @Test
    void entriesMustBeLiteralOwnerTableNames() {
        assertThrows(DebeziumException.class, () -> config("dba.*"));
        assertThrows(DebeziumException.class, () -> config("t_order"));
        assertThrows(DebeziumException.class, () -> config("dba.t_(order|item)"));
    }

    @Test
    void entriesAreLowercasedToTheConnectorNormalForm() {
        assertEquals(List.of("dba.t_order", "app.t_item"),
                config(" DBA.t_order , app.T_ITEM ").getExtractionTableNames());
    }
}
