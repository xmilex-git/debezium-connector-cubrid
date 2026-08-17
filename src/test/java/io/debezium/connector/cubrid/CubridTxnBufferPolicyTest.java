/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import static io.debezium.connector.cubrid.jna.TestRawLogItems.abort;
import static io.debezium.connector.cubrid.jna.TestRawLogItems.commit;
import static io.debezium.connector.cubrid.jna.TestRawLogItems.insert;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.cubrid.CubridStreamingChangeEventSource.BufferPolicy;
import io.debezium.connector.cubrid.CubridStreamingChangeEventSource.StreamState;
import io.debezium.connector.cubrid.CubridStreamingChangeEventSource.TxnBufferMetrics;
import io.debezium.connector.cubrid.jna.RawLogItem;
import io.debezium.relational.TableId;

/**
 * Unit tests for the transaction buffer policy (ADR 0007, workspace#60): opt-in per-transaction
 * event-count threshold (D2) and retention age (D3), both abandoning the transaction — which is an
 * <em>intended, permanent downstream loss</em> (recovery = re-snapshot) — while advancing the
 * restart anchor past it and keeping replay counters deterministic.
 */
class CubridTxnBufferPolicyTest {

    private static final long CLASSOID = 7;
    private static final TableId TABLE = new TableId(null, "testdb", "t1");

    private record Published(int trid, long seq, long anchorLsa, long anchorSeq) {
    }

    /** Recording {@link TxnBufferMetrics} stub. */
    private static final class RecordingMetrics implements TxnBufferMetrics {
        final List<Integer> oversized = new ArrayList<>();
        final List<Integer> retentionAbandoned = new ArrayList<>();
        int lastActiveCount = -1;
        long lastOldestAgeMs = -1;

        @Override
        public void onOversizedAbandon(int trid) {
            oversized.add(trid);
        }

        @Override
        public void onRetentionAbandon(int trid) {
            retentionAbandoned.add(trid);
        }

        @Override
        public void onBatchEnd(int activeCount, long oldestAgeMs) {
            lastActiveCount = activeCount;
            lastOldestAgeMs = oldestAgeMs;
        }
    }

    private StreamState state;
    private long nowMs;
    private long anchorLsa;
    private long anchorSeq;
    private final List<Published> published = new ArrayList<>();
    private final RecordingMetrics metrics = new RecordingMetrics();

    @BeforeEach
    void setUp() {
        state = new StreamState(0);
        nowMs = 0;
        published.clear();
        metrics.oversized.clear();
        metrics.retentionAbandoned.clear();
    }

    private BufferPolicy policy(long eventsThreshold, long retentionMs) {
        return new BufferPolicy(eventsThreshold, retentionMs, () -> nowMs);
    }

    private void runBatch(BufferPolicy policy, long batchInLsa, long batchOutLsa, RawLogItem... items) throws InterruptedException {
        CubridStreamingChangeEventSource.processBatch(state, policy, List.of(items), batchInLsa, batchOutLsa,
                classoid -> TABLE,
                (lsa, seq) -> {
                    anchorLsa = lsa;
                    anchorSeq = seq;
                },
                (buffer, commitDcl) -> buffer.changes.forEach(
                        c -> published.add(new Published(c.item().transactionId(), c.seq(), anchorLsa, anchorSeq))),
                metrics);
    }

    // ---- D2: transaction.events.threshold ----

    @Test
    void transactionAtExactlyTheThresholdIsNotAbandoned() throws InterruptedException {
        final BufferPolicy p = policy(2, 0);
        runBatch(p, 100, 200, insert(1, CLASSOID), insert(1, CLASSOID), commit(1));

        assertEquals(2, published.size());
        assertTrue(metrics.oversized.isEmpty());
    }

    @Test
    void oversizedTransactionIsAbandonedAndIntentionallyLostDownstream() throws InterruptedException {
        final BufferPolicy p = policy(2, 0);
        // 3rd event exceeds the threshold of 2 — the whole transaction is abandoned, so its later
        // COMMIT publishes nothing: the loss is the documented ADR 0007 D1/D2 behavior
        runBatch(p, 100, 200, insert(1, CLASSOID), insert(1, CLASSOID), insert(1, CLASSOID), commit(1));

        assertTrue(published.isEmpty());
        assertEquals(List.of(1), metrics.oversized);
        // nothing left in flight: the anchor advances to the batch end past the abandoned txn
        assertEquals(200, anchorLsa);
        assertEquals(4, anchorSeq);
    }

    @Test
    void eventsOfAnAbandonedTransactionAreSkippedButStillCounted() throws InterruptedException {
        final BufferPolicy p = policy(1, 0);
        runBatch(p, 100, 200, insert(1, CLASSOID), insert(1, CLASSOID)); // 2nd event abandons T1
        runBatch(p, 200, 300, insert(1, CLASSOID)); // must not re-open a buffer for the abandoned trid
        runBatch(p, 300, 400, insert(2, CLASSOID), commit(1), commit(2));

        // only T2 publishes, and its counter reflects every skipped item having been counted
        assertEquals(1, published.size());
        assertEquals(2, published.get(0).trid());
        assertEquals(4, published.get(0).seq());
        assertTrue(state.inflight.isEmpty());
    }

    @Test
    void abandonedTridIsReusableAfterItsTerminalDcl() throws InterruptedException {
        final BufferPolicy p = policy(1, 0);
        runBatch(p, 100, 200, insert(1, CLASSOID), insert(1, CLASSOID), abort(1)); // abandon, then txn ends
        runBatch(p, 200, 300, insert(1, CLASSOID), commit(1)); // same trid, new transaction

        assertEquals(1, published.size());
        assertEquals(1, published.get(0).trid());
        assertTrue(state.abandoned.isEmpty());
    }

    @Test
    void oversizedAbandonOfOldestInflightReleasesAnchorToNextOldest() throws InterruptedException {
        final BufferPolicy p = policy(2, 0);
        runBatch(p, 100, 200, insert(1, CLASSOID)); // T1 oldest @ (100, 0)
        runBatch(p, 200, 300, insert(2, CLASSOID), insert(1, CLASSOID), insert(1, CLASSOID)); // T2 @ (200, 1); T1 exceeds

        // with T1 abandoned, the batch-end anchor is bounded by T2's start, not T1's
        assertEquals(200, anchorLsa);
        assertEquals(1, anchorSeq);
        assertEquals(1, metrics.lastActiveCount);
    }

    // ---- D3: transaction.retention.ms ----

    @Test
    void retentionExpiredTransactionIsAbandonedAndIntentionallyLostDownstream() throws InterruptedException {
        final BufferPolicy p = policy(0, 1000);
        runBatch(p, 100, 200, insert(1, CLASSOID)); // T1 first buffered at t=0
        nowMs = 500;
        runBatch(p, 200, 300, insert(2, CLASSOID)); // T2 first buffered at t=500
        nowMs = 1500;
        runBatch(p, 300, 400); // T1 age 1500 > 1000 → abandoned; T2 age 1000 → kept

        assertEquals(List.of(1), metrics.retentionAbandoned);
        // the anchor advanced past abandoned T1 to the next oldest in-flight start (T2)
        assertEquals(200, anchorLsa);
        assertEquals(1, anchorSeq);

        // T1's later COMMIT publishes nothing — permanent downstream loss is the intended D3 behavior
        runBatch(p, 400, 500, commit(1), commit(2));
        assertEquals(1, published.size());
        assertEquals(2, published.get(0).trid());
    }

    @Test
    void retentionAbandonWithNothingElseInflightAdvancesAnchorToBatchEnd() throws InterruptedException {
        final BufferPolicy p = policy(0, 1000);
        runBatch(p, 100, 200, insert(1, CLASSOID));
        nowMs = 2000;
        runBatch(p, 200, 300);

        assertEquals(List.of(1), metrics.retentionAbandoned);
        assertEquals(300, anchorLsa);
        assertEquals(1, anchorSeq);
        assertEquals(0, metrics.lastActiveCount);
        assertEquals(0, metrics.lastOldestAgeMs);
    }

    @Test
    void transactionAtExactlyTheRetentionAgeIsNotAbandoned() throws InterruptedException {
        final BufferPolicy p = policy(0, 1000);
        runBatch(p, 100, 200, insert(1, CLASSOID));
        nowMs = 1000;
        runBatch(p, 200, 300);

        assertTrue(metrics.retentionAbandoned.isEmpty());
        assertEquals(1, metrics.lastActiveCount);
        assertEquals(1000, metrics.lastOldestAgeMs);

        runBatch(p, 300, 400, commit(1));
        assertEquals(1, published.size());
    }

    // ---- defaults and replay determinism ----

    @Test
    void defaultUnlimitedPolicyNeverAbandons() throws InterruptedException {
        final BufferPolicy p = policy(0, 0);
        runBatch(p, 100, 200, insert(1, CLASSOID), insert(1, CLASSOID), insert(1, CLASSOID));
        nowMs = Long.MAX_VALUE / 2;
        runBatch(p, 200, 300, commit(1));

        assertEquals(3, published.size());
        assertTrue(metrics.oversized.isEmpty());
        assertTrue(metrics.retentionAbandoned.isEmpty());
    }

    @Test
    void replayFromTheAdvancedAnchorAfterRetentionAbandonReassignsIdenticalCounters() throws InterruptedException {
        final BufferPolicy p = policy(0, 1000);
        runBatch(p, 100, 200, insert(1, CLASSOID)); // T1 @ t=0 (before the eventual anchor)
        nowMs = 500;
        runBatch(p, 200, 300, insert(2, CLASSOID)); // T2 @ (200, seq 1)
        nowMs = 1500;
        runBatch(p, 300, 400); // T1 abandoned; anchor = T2.start (200, 1)
        assertEquals(200, anchorLsa);
        assertEquals(1, anchorSeq);
        runBatch(p, 400, 500, commit(1), insert(2, CLASSOID), commit(2));
        final List<Published> firstRun = List.copyOf(published);
        assertEquals(List.of(2, 2), firstRun.stream().map(Published::trid).toList());

        // a restart resumes at the advanced anchor: T1's pre-anchor DML never replays, its stray
        // COMMIT is a no-op, and T2's records re-derive the exact same counters (_version)
        published.clear();
        state = new StreamState(1);
        runBatch(p, 200, 300, insert(2, CLASSOID));
        runBatch(p, 300, 400);
        runBatch(p, 400, 500, commit(1), insert(2, CLASSOID), commit(2));

        assertEquals(firstRun.stream().map(pub -> pub.trid() + ":" + pub.seq()).toList(),
                published.stream().map(pub -> pub.trid() + ":" + pub.seq()).toList());
    }
}
