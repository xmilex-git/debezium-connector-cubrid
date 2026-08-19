/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * Include-list bootstrap fail-fast (workspace#82 D4): startup must verify every literal include
 * entry exists with a loadable schema — the only observation point for a table dropped or renamed
 * while the connector was stopped (S3: the server silently filters its lagging log, so nothing
 * ever reaches the stream). A missing entry fails startup non-retriably; the "pre-include a
 * table, CREATE it later" workflow is retired.
 */
class CubridIncludeBootstrapTest {

    private static final TableId ORDER = new TableId(null, "dba", "t_order");
    private static final TableId ITEM = new TableId(null, "dba", "t_item");

    private static Table table(TableId id) {
        return Table.editor().tableId(id)
                .addColumn(io.debezium.relational.Column.editor().name("id").type("INTEGER").create())
                .create();
    }

    @Test
    void allEntriesPresentRefreshEveryTable() {
        final Map<TableId, Table> catalog = Map.of(ORDER, table(ORDER), ITEM, table(ITEM));
        final List<TableId> refreshed = new ArrayList<>();

        CubridConnectorTask.bootstrapIncludedTables(List.of(ORDER, ITEM),
                id -> Optional.ofNullable(catalog.get(id)),
                t -> refreshed.add(t.id()));

        assertEquals(List.of(ORDER, ITEM), refreshed);
    }

    @Test
    void missingEntryFailsStartupNonRetriablyWithTheThreePartMessage() {
        // S3: t_item was dropped (or renamed) while the connector was stopped
        final Map<TableId, Table> catalog = Map.of(ORDER, table(ORDER));
        final List<TableId> refreshed = new ArrayList<>();

        final DebeziumException e = assertThrows(DebeziumException.class,
                () -> CubridConnectorTask.bootstrapIncludedTables(List.of(ORDER, ITEM),
                        id -> Optional.ofNullable(catalog.get(id)),
                        t -> refreshed.add(t.id())));

        assertTrue(e.getMessage().contains("dba.t_item"), e.getMessage());
        assertTrue(e.getMessage().contains("table.include.list"), e.getMessage());
        assertTrue(e.getMessage().contains("resnapshot"), e.getMessage());
        assertTrue(e.getMessage().contains("Relation identity halt recovery"), e.getMessage());
        assertFalse(org.apache.kafka.connect.errors.RetriableException.class.isInstance(e), "must be non-retriable (D6)");
    }

    @Test
    void infrastructureFailureIsWrappedNotMisreportedAsAMissingTable() {
        final DebeziumException e = assertThrows(DebeziumException.class,
                () -> CubridConnectorTask.bootstrapIncludedTables(List.of(ORDER),
                        id -> {
                            throw new SQLException("connection reset");
                        },
                        t -> {
                        }));

        assertTrue(e.getMessage().contains("Failed to bootstrap"), e.getMessage());
        assertEquals("connection reset", e.getCause().getMessage());
    }
}
