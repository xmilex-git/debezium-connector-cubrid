/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import static io.debezium.connector.cubrid.jna.TestRawLogItems.abort;
import static io.debezium.connector.cubrid.jna.TestRawLogItems.commit;
import static io.debezium.connector.cubrid.jna.TestRawLogItems.insert;
import static io.debezium.connector.cubrid.jna.TestRawLogItems.insertAt;
import static io.debezium.connector.cubrid.jna.TestRawLogItems.rollbackTo;
import static io.debezium.connector.cubrid.jna.TestRawLogItems.timer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.cubrid.CubridStreamingChangeEventSource.StreamState;
import io.debezium.connector.cubrid.jna.RawLogItem;
import io.debezium.relational.TableId;

/**
 * Unit tests for the offset/anchor invariant (workspace#45, ADR 0004): until the last change
 * record of a transaction has been enqueued, no record is given a source offset (anchor) past
 * that transaction's first-DML batch boundary — so a worker crash with a commit only partially
 * acked always restarts at a position that replays the whole transaction.
 */
class CubridStreamingAnchorInvariantTest {

    private static final long CLASSOID = 7;
    private static final TableId TABLE = new TableId(null, "testdb", "t1");

    /** One published change with the anchor that was in effect when it was enqueued. */
    private record Published(int trid, long seq, long anchorLsa, long anchorSeq) {
    }

    private StreamState state;
    private long anchorLsa;
    private long anchorSeq;
    private final List<Published> published = new ArrayList<>();

    @BeforeEach
    void setUp() {
        state = new StreamState(0);
        published.clear();
    }

    private void runBatch(long batchInLsa, long batchOutLsa, RawLogItem... items) throws InterruptedException {
        CubridStreamingChangeEventSource.processBatch(state, List.of(items), batchInLsa, batchOutLsa,
                classoid -> TABLE,
                (lsa, seq) -> {
                    anchorLsa = lsa;
                    anchorSeq = seq;
                },
                (buffer, commitDcl) -> buffer.changes.forEach(
                        c -> published.add(new Published(c.item().transactionId(), c.seq(), anchorLsa, anchorSeq))));
    }

    @Test
    void multiBatchTransactionAnchorsAtItsOwnStart() throws InterruptedException {
        runBatch(100, 200, insert(1, CLASSOID)); // T1 starts at batch boundary (100, seq 0)
        runBatch(200, 300, insert(1, CLASSOID));
        runBatch(300, 400, commit(1)); // T1 commits two batches later

        // both records enqueue under T1's own start anchor — not the COMMIT batch start (300)
        assertEquals(2, published.size());
        for (Published p : published) {
            assertEquals(100, p.anchorLsa());
            assertEquals(0, p.anchorSeq());
        }
        // only after the whole transaction is enqueued may the batch-end heartbeat advance
        assertEquals(400, anchorLsa);
        assertEquals(3, anchorSeq);
    }

    @Test
    void interleavedCommitAnchorsAtCommitterNotAtSurvivor() throws InterruptedException {
        runBatch(100, 200, insert(1, CLASSOID)); // T1 start @ (100, 0)
        runBatch(200, 300, insert(2, CLASSOID), commit(1)); // T2 start @ (200, 1), then T1 COMMIT

        // T1's record must carry T1.start (100, 0) — not T2.start (200, 1)
        assertEquals(1, published.size());
        assertEquals(1, published.get(0).trid());
        assertEquals(100, published.get(0).anchorLsa());
        assertEquals(0, published.get(0).anchorSeq());

        // once T1 is fully enqueued, the anchor is bounded by T2, still in flight
        assertEquals(200, anchorLsa);
        assertEquals(1, anchorSeq);
    }

    @Test
    void noRecordEverCarriesAnchorPastItsTransactionStart() throws InterruptedException {
        runBatch(100, 200, insert(1, CLASSOID), insert(2, CLASSOID)); // T1 @ (100,0), T2 @ (100,0)
        runBatch(200, 300, insert(3, CLASSOID), commit(2)); // T3 @ (200,2)
        runBatch(300, 400, insert(1, CLASSOID), commit(1), timer());
        runBatch(400, 500, commit(3));

        assertEquals(4, published.size());
        final Map<Integer, Long> startSeqByTrid = Map.of(1, 0L, 2, 0L, 3, 2L);
        for (Published p : published) {
            assertTrue(p.anchorSeq() <= startSeqByTrid.get(p.trid()),
                    "record of trid " + p.trid() + " carried anchor seq " + p.anchorSeq()
                            + " past its transaction start " + startSeqByTrid.get(p.trid()));
        }
    }

    @Test
    void rollbackToDropsOnlyChangesInsideTheUndoneRange() throws InterruptedException {
        // T1: DML @key 10, DML @key 20, then ROLLBACK_TO key 10 (statement/savepoint rollback),
        // then DML @key 30, COMMIT — only keys 10 and 30 survive (workspace#47)
        runBatch(100, 200,
                insertAt(1, CLASSOID, 10),
                insertAt(1, CLASSOID, 20),
                rollbackTo(1, 10),
                insertAt(1, CLASSOID, 30),
                commit(1));

        assertEquals(2, published.size());
        assertEquals(1, published.get(0).seq()); // key 10 (counter 1)
        assertEquals(4, published.get(1).seq()); // key 30 (counter 4; key-20 DML and marker consumed 2..3)
    }

    @Test
    void rollbackToWholeTransactionLeavesNothingToPublish() throws InterruptedException {
        // the undone range covers every buffered change (e.g. failed statement was the only one);
        // the emptied buffer releases the anchor like an ABORT does
        runBatch(100, 200, insertAt(1, CLASSOID, 10), rollbackTo(1, 5), commit(1));

        assertTrue(published.isEmpty());
        assertEquals(200, anchorLsa);
        assertEquals(3, anchorSeq);
    }

    @Test
    void repeatedRollbackToSameSavepointIsIdempotent() throws InterruptedException {
        runBatch(100, 200,
                insertAt(1, CLASSOID, 10),
                insertAt(1, CLASSOID, 20),
                rollbackTo(1, 10),
                rollbackTo(1, 10),
                insertAt(1, CLASSOID, 30),
                commit(1));

        assertEquals(2, published.size());
    }

    @Test
    void rollbackToOfUnbufferedTridIsIgnored() throws InterruptedException {
        // internal server aborts of transactions that never produced captured DML arrive as
        // markers with no buffer — they must be a no-op
        runBatch(100, 200, rollbackTo(9, 10), insert(1, CLASSOID), commit(1));

        assertEquals(1, published.size());
        assertEquals(1, published.get(0).trid());
    }

    @Test
    void abortDiscardsBufferAndReleasesAnchor() throws InterruptedException {
        runBatch(100, 200, insert(1, CLASSOID), abort(1));

        assertTrue(published.isEmpty());
        // nothing left in flight, so the anchor advances to the batch end
        assertEquals(200, anchorLsa);
        assertEquals(2, anchorSeq); // DML and ABORT DCL both counted, TIMER-free batch
    }

    @Test
    void replayFromCarriedAnchorReassignsIdenticalCounters() throws InterruptedException {
        runBatch(100, 200, insert(1, CLASSOID), insert(2, CLASSOID), timer());
        runBatch(200, 300, insert(1, CLASSOID), commit(2));
        runBatch(300, 400, commit(1));
        final List<Published> firstRun = List.copyOf(published);

        // a worker that dies with only some of T1's records acked restarts at the anchor those
        // records carried — T1.start — and must re-derive the exact same counters (_version)
        final Published t1Record = firstRun.stream().filter(p -> p.trid() == 1).findFirst().orElseThrow();
        assertEquals(100, t1Record.anchorLsa());
        published.clear();
        state = new StreamState(t1Record.anchorSeq());

        runBatch(100, 200, insert(1, CLASSOID), insert(2, CLASSOID), timer());
        runBatch(200, 300, insert(1, CLASSOID), commit(2));
        runBatch(300, 400, commit(1));

        assertEquals(firstRun.stream().map(p -> p.trid() + ":" + p.seq()).toList(),
                published.stream().map(p -> p.trid() + ":" + p.seq()).toList());
    }
}
