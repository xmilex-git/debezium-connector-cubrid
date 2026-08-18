/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import static io.debezium.connector.cubrid.log.TestRawLogItems.commit;
import static io.debezium.connector.cubrid.log.TestRawLogItems.insert;
import static io.debezium.connector.cubrid.log.TestRawLogItems.relation;
import static io.debezium.connector.cubrid.log.TestRawLogItems.timer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;
import io.debezium.connector.cubrid.CubridStreamingChangeEventSource.BufferPolicy;
import io.debezium.connector.cubrid.CubridStreamingChangeEventSource.StreamState;
import io.debezium.connector.cubrid.CubridStreamingChangeEventSource.TxnBufferMetrics;
import io.debezium.connector.cubrid.log.RawLogItem;
import io.debezium.relational.TableId;

/**
 * In-stream relation dictionary consumption (workspace#70, ADR 0011 D4/D6): announces build
 * the classoid route, are excluded from the event counter exactly like TIMER, bypass the
 * transaction buffer, and a DML without a preceding announce is a protocol-contract error.
 * The counter exclusion is what keeps {@code _version} byte-identical across a reconnect
 * (the server re-announces per session) — the #41 RMT convergence proof depends on it.
 */
class CubridRelationDictionaryTest {

    private static final long ORDER_OID = 7;
    private static final long ITEM_OID = 8;

    private StreamState state;
    private final List<Long> publishedSeqs = new ArrayList<>();
    private final List<TableId> publishedTables = new ArrayList<>();

    @BeforeEach
    void setUp() {
        state = new StreamState(0);
        publishedSeqs.clear();
        publishedTables.clear();
    }

    private void runBatch(long batchInLsa, long batchOutLsa, RawLogItem... items) throws InterruptedException {
        CubridStreamingChangeEventSource.processBatch(state, BufferPolicy.UNLIMITED, List.of(items), batchInLsa, batchOutLsa,
                tableId -> true,
                (lsa, seq) -> {
                },
                (buffer, commitDcl) -> buffer.changes.forEach(c -> {
                    publishedSeqs.add(c.seq());
                    publishedTables.add(c.tableId());
                }),
                TxnBufferMetrics.NO_OP);
    }

    @Test
    void announceBuildsTheRouteAndIsNotCounted() throws InterruptedException {
        runBatch(100, 200, relation(ORDER_OID, "DBA", "t_order"), insert(1, ORDER_OID), commit(1), timer());

        assertEquals(new TableId(null, "dba", "t_order"), publishedTables.get(0),
                "owner arrives split from the engine and is lowercased to the connector normal form");
        assertEquals(1, publishedSeqs.get(0), "the DML is event 1: the announce before it was not counted (D6)");
        assertEquals(2, state.counter, "only the DML and the COMMIT counted — announce and TIMER did not");
    }

    @Test
    void reconnectReplayWithResentAnnouncesYieldsIdenticalSeqs() throws InterruptedException {
        runBatch(100, 200, relation(ORDER_OID, "dba", "t_order"), insert(1, ORDER_OID), insert(1, ORDER_OID), commit(1));
        final List<Long> firstRun = List.copyOf(publishedSeqs);

        // reconnect: fresh session state, the server re-announces before the replayed items —
        // and may interleave the announce differently; seqs must not move (ADR 0011 D6)
        setUp();
        runBatch(100, 200, relation(ORDER_OID, "dba", "t_order"),
                insert(1, ORDER_OID), relation(ORDER_OID, "dba", "t_order"), insert(1, ORDER_OID), commit(1));

        assertEquals(firstRun, publishedSeqs, "re-sent announces must not shift the deterministic counter");
    }

    @Test
    void announceBypassesTheTransactionBufferAndSurvivesAbandon() throws InterruptedException {
        final BufferPolicy oneEvent = new BufferPolicy(1, 0, () -> 0);
        final List<RawLogItem> items = List.of(
                relation(ORDER_OID, "dba", "t_order"),
                insert(1, ORDER_OID), insert(1, ORDER_OID), // trid 1 exceeds threshold 1 → abandoned
                relation(ITEM_OID, "dba", "t_item"), // announce arriving amid the abandoned txn
                insert(2, ITEM_OID), commit(2), commit(1));
        CubridStreamingChangeEventSource.processBatch(state, oneEvent, items, 100, 200,
                tableId -> true,
                (lsa, seq) -> {
                },
                (buffer, commitDcl) -> buffer.changes.forEach(c -> publishedTables.add(c.tableId())),
                TxnBufferMetrics.NO_OP);

        assertEquals(List.of(new TableId(null, "dba", "t_item")), publishedTables,
                "the announce was not swept away with the abandoned transaction's buffer");
        assertTrue(state.relationDictionary.containsKey(ITEM_OID));
    }

    @Test
    void dmlWithoutAnnounceIsAProtocolContractError() {
        assertThrows(DebeziumException.class, () -> runBatch(100, 200, insert(1, ORDER_OID)),
                "the server announces before first use (#67); a miss means version skew (ADR 0011 D10)");
    }

    @Test
    void emptyNamesAnnounceMarksTheClassoidUnroutableInsteadOfErroring() throws InterruptedException {
        // lagging log of an already-dropped class: announced with empty names (invalid_class)
        runBatch(100, 200, relation(ORDER_OID, "", ""), insert(1, ORDER_OID), commit(1));

        assertTrue(publishedTables.isEmpty(), "unroutable DML is skipped, not published");
        assertEquals(2, state.counter, "but it stays counted — it is position-derived");
        assertTrue(state.relationDictionary.containsKey(ORDER_OID));
        assertNull(state.relationDictionary.get(ORDER_OID));
    }
}
