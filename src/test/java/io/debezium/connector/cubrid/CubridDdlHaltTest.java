/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import static io.debezium.connector.cubrid.log.TestRawLogItems.commit;
import static io.debezium.connector.cubrid.log.TestRawLogItems.ddl;
import static io.debezium.connector.cubrid.log.TestRawLogItems.insert;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.debezium.connector.cubrid.CubridStreamingChangeEventSource.BufferPolicy;
import io.debezium.connector.cubrid.CubridStreamingChangeEventSource.StreamState;
import io.debezium.connector.cubrid.CubridStreamingChangeEventSource.TxnBufferMetrics;
import io.debezium.connector.cubrid.log.RawLogItem;
import io.debezium.relational.TableId;

/**
 * Unit tests for DDL halt (ADR 0008, workspace#63): a captured-table ALTER/DROP/RENAME/TRUNCATE
 * DDL item fails the stream fast (D1·D2), mid-stream CREATE TABLE only warns and counts (D3),
 * events committed before the DDL publish while the anchor never passes the DDL so a restart
 * re-halts deterministically (D4), and the error surface carries table + ddl_type + statement
 * without being retriable (D5).
 */
class CubridDdlHaltTest {

    private static final int CDC_TABLE = 0;
    private static final int CDC_INDEX = 1;
    private static final long CAPTURED_CLASSOID = 7;
    private static final long OTHER_CLASSOID = 99;
    private static final TableId TABLE = new TableId(null, "testdb", "t1");

    private record Published(int trid, long seq) {
    }

    /** Recording {@link TxnBufferMetrics} stub for the DDL hooks. */
    private static final class RecordingMetrics implements TxnBufferMetrics {
        final List<String> ddlHalts = new ArrayList<>();
        int midStreamCreates;

        @Override
        public void onOversizedAbandon(int trid) {
        }

        @Override
        public void onRetentionAbandon(int trid) {
        }

        @Override
        public void onBatchEnd(int activeCount, long oldestAgeMs) {
        }

        @Override
        public void onDdlHalt(String table, String ddlType, String statement) {
            ddlHalts.add(table + "|" + ddlType + "|" + statement);
        }

        @Override
        public void onMidStreamCreateTable(String statement) {
            midStreamCreates++;
        }
    }

    private StreamState state;
    private long anchorLsa;
    private long anchorSeq;
    private final List<Published> published = new ArrayList<>();
    private final RecordingMetrics metrics = new RecordingMetrics();

    @BeforeEach
    void setUp() {
        state = new StreamState(0);
        published.clear();
        metrics.ddlHalts.clear();
        metrics.midStreamCreates = 0;
    }

    private void runBatch(long batchInLsa, long batchOutLsa, RawLogItem... items) throws InterruptedException {
        CubridStreamingChangeEventSource.processBatch(state, BufferPolicy.UNLIMITED, List.of(items), batchInLsa, batchOutLsa,
                classoid -> classoid == CAPTURED_CLASSOID ? TABLE : null,
                (lsa, seq) -> {
                    anchorLsa = lsa;
                    anchorSeq = seq;
                },
                (buffer, commitDcl) -> buffer.changes.forEach(
                        c -> published.add(new Published(c.item().transactionId(), c.seq()))),
                metrics);
    }

    // ---- D1·D2: the four schema-changing DDL types halt on a captured table ----

    @ParameterizedTest(name = "ddl_type {0} = {1}")
    @CsvSource({ "1, ALTER", "2, DROP", "3, RENAME", "4, TRUNCATE" })
    void capturedTableSchemaChangingDdlHalts(int ddlTypeCode, String ddlTypeName) {
        final String stmt = ddlTypeName + " TABLE t1 ...";
        final DdlHaltException e = assertThrows(DdlHaltException.class,
                () -> runBatch(100, 200, ddl(1, ddlTypeCode, CDC_TABLE, CAPTURED_CLASSOID, stmt)));

        // D5: the error surface names the table, the ddl type, the full statement, and the guide
        assertTrue(e.getMessage().contains("testdb.t1"), e.getMessage());
        assertTrue(e.getMessage().contains(ddlTypeName), e.getMessage());
        assertTrue(e.getMessage().contains(stmt), e.getMessage());
        assertTrue(e.getMessage().contains("DDL halt recovery"), e.getMessage());
        // D5 non-retriable: Debezium's ErrorHandler only retries Kafka Connect RetriableExceptions
        // (and registered communication exceptions) — a plain DebeziumException fails the task
        assertFalse(org.apache.kafka.connect.errors.RetriableException.class.isAssignableFrom(DdlHaltException.class),
                "must be non-retriable (D5)");
        assertEquals(List.of("testdb.t1|" + ddlTypeName + "|" + stmt), metrics.ddlHalts);
    }

    @Test
    void unknownFutureDdlTypeOnCapturedTableHaltsFailSafe() {
        assertThrows(DdlHaltException.class,
                () -> runBatch(100, 200, ddl(1, 9, CDC_TABLE, CAPTURED_CLASSOID, "FUTURE DDL")));
    }

    // ---- D4: publish-before-halt and deterministic re-halt ----

    @Test
    void commitsBeforeTheDdlPublishAndTheAnchorStaysBeforeTheDdl() {
        assertThrows(DdlHaltException.class, () -> runBatch(100, 200,
                insert(1, CAPTURED_CLASSOID), commit(1),
                insert(2, CAPTURED_CLASSOID),
                ddl(3, 1, CDC_TABLE, CAPTURED_CLASSOID, "ALTER TABLE t1 ADD COLUMN c INT")));

        // T1, committed before the DDL, published normally
        assertEquals(List.of(new Published(1, 1)), published);
        // in-flight T2 never published, and its buffer is still pinned in state
        assertTrue(state.inflight.containsKey(2));
        // the last anchor advance (T1's commit) is the batch-in boundary — never past the DDL,
        // because the batch-end advance to batchOutLsa is unreachable after the halt
        assertEquals(100, anchorLsa);
        assertEquals(0, anchorSeq);
    }

    @Test
    void unassistedRestartHaltsAtTheSameDdlWithTheSameCounter() throws InterruptedException {
        final RawLogItem[] items = {
                insert(1, CAPTURED_CLASSOID), commit(1),
                ddl(2, 2, CDC_TABLE, CAPTURED_CLASSOID, "DROP TABLE t1")
        };
        final DdlHaltException first = assertThrows(DdlHaltException.class, () -> runBatch(100, 200, items));
        final long counterAtHalt = state.counter;
        final List<Published> firstPublished = new ArrayList<>(published);

        // restart: replay from the same anchor re-derives the same counters and re-halts (D4)
        setUp();
        final DdlHaltException second = assertThrows(DdlHaltException.class, () -> runBatch(100, 200, items));
        assertEquals(counterAtHalt, state.counter);
        assertEquals(first.getMessage(), second.getMessage());
        assertEquals(firstPublished, published);
    }

    // ---- D3: mid-stream CREATE TABLE continues ----

    @Test
    void midStreamCreateTableWarnsCountsAndContinues() throws InterruptedException {
        runBatch(100, 200,
                ddl(1, 0, CDC_TABLE, OTHER_CLASSOID, "CREATE TABLE t_new (id INT)"),
                insert(2, CAPTURED_CLASSOID), commit(2));

        assertEquals(1, metrics.midStreamCreates);
        assertTrue(metrics.ddlHalts.isEmpty());
        // the CREATE DDL item stayed counted (ADR 0004 determinism): insert carries counter 2
        assertEquals(List.of(new Published(2, 2)), published);
        assertEquals(200, anchorLsa);
        assertEquals(3, anchorSeq);
    }

    // ---- D1: out-of-scope DDL is ignored ----

    @Test
    void nonTableObjectDdlIsIgnored() throws InterruptedException {
        runBatch(100, 200,
                ddl(1, 1, CDC_INDEX, CAPTURED_CLASSOID, "ALTER INDEX i1 REBUILD"),
                insert(2, CAPTURED_CLASSOID), commit(2));

        assertTrue(metrics.ddlHalts.isEmpty());
        assertEquals(1, published.size());
    }

    @Test
    void nonCapturedTableDdlIsIgnored() throws InterruptedException {
        runBatch(100, 200,
                ddl(1, 1, CDC_TABLE, OTHER_CLASSOID, "ALTER TABLE other ADD COLUMN c INT"),
                insert(2, CAPTURED_CLASSOID), commit(2));

        assertTrue(metrics.ddlHalts.isEmpty());
        assertEquals(1, published.size());
    }
}
