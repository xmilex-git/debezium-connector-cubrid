/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.cubrid.jna.CubridLogClient;
import io.debezium.connector.cubrid.jna.RawLogItem;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/**
 * Streams changes out of the CUBRID transaction log through the {@code cubrid_log} JNA client.
 * <p>
 * Positioning follows ADR 0004: every non-TIMER item advances a deterministic counter which is the
 * event position ({@code source.lsn} → {@code _version}); DML items are buffered per transaction
 * and published in log order on COMMIT DCL, discarded on ABORT DCL. The persisted offset is the
 * <em>anchor</em> — the batch-boundary LSA/counter of the oldest in-flight transaction — so a
 * restart replays whole transactions and re-derives identical counters (at-least-once).
 * <p>
 * Offset invariant (workspace#45): a committing transaction stays in the in-flight set until every
 * one of its events has been enqueued, so no record ever carries an anchor past that transaction's
 * own first-DML batch boundary — a partially-acked commit is always fully replayable on restart.
 */
public class CubridStreamingChangeEventSource implements StreamingChangeEventSource<CubridPartition, CubridOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CubridStreamingChangeEventSource.class);

    private final CubridConnectorConfig connectorConfig;
    private final CubridConnection connection;
    private final EventDispatcher<CubridPartition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final CubridDatabaseSchema schema;
    private final TxnBufferMetrics metrics;

    public CubridStreamingChangeEventSource(CubridConnectorConfig connectorConfig, CubridConnection connection,
                                            EventDispatcher<CubridPartition, TableId> dispatcher,
                                            ErrorHandler errorHandler, Clock clock, CubridDatabaseSchema schema,
                                            TxnBufferMetrics metrics) {
        this.connectorConfig = connectorConfig;
        this.connection = connection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.metrics = metrics;
    }

    /** One buffered DML with the counter it was assigned when read from the stream. */
    record BufferedChange(long seq, TableId tableId, RawLogItem item) {
    }

    /** Per-transaction buffer, remembering the batch boundary at which its first item arrived. */
    static final class TxnBuffer {
        final long startLsaRaw;
        final long startSeq;
        final long firstBufferedAtMs;
        final List<BufferedChange> changes = new ArrayList<>();

        TxnBuffer(long startLsaRaw, long startSeq, long firstBufferedAtMs) {
            this.startLsaRaw = startLsaRaw;
            this.startSeq = startSeq;
            this.firstBufferedAtMs = firstBufferedAtMs;
        }
    }

    /** Buffering/anchor state carried across batches — package-private so {@link #processBatch} is unit-testable. */
    static final class StreamState {
        // insertion order = first-DML order, so the first entry is the oldest in-flight txn
        final LinkedHashMap<Integer, TxnBuffer> inflight = new LinkedHashMap<>();
        // trids abandoned by the buffer policy (ADR 0007) whose later items must be skipped;
        // an entry is dropped when the transaction's terminal DCL arrives (trids are reused)
        final Set<Integer> abandoned = new HashSet<>();
        long counter;

        StreamState(long counter) {
            this.counter = counter;
        }
    }

    /**
     * Opt-in per-transaction buffer caps (ADR 0007). Both caps default to 0 = disabled; the clock
     * is injectable so retention is unit-testable.
     */
    record BufferPolicy(long eventsThreshold, long retentionMs, LongSupplier nowMs) {
        static final BufferPolicy UNLIMITED = new BufferPolicy(0, 0, System::currentTimeMillis);
    }

    /** Buffer-policy observability hooks; the production sink is {@link CubridStreamingChangeEventSourceMetrics}. */
    interface TxnBufferMetrics {

        TxnBufferMetrics NO_OP = new TxnBufferMetrics() {
            @Override
            public void onOversizedAbandon(int trid) {
            }

            @Override
            public void onRetentionAbandon(int trid) {
            }

            @Override
            public void onBatchEnd(int activeCount, long oldestAgeMs) {
            }
        };

        /** A transaction exceeded {@code transaction.events.threshold} and was abandoned (D2). */
        void onOversizedAbandon(int trid);

        /** A transaction exceeded {@code transaction.retention.ms} and was abandoned (D3). */
        void onRetentionAbandon(int trid);

        /** Batch-end gauges: in-flight transaction count and the oldest in-flight age (0 when none). */
        void onBatchEnd(int activeCount, long oldestAgeMs);
    }

    /** Receives anchor advances; the production sink is {@link CubridOffsetContext#setAnchor}. */
    @FunctionalInterface
    interface AnchorSink {
        void setAnchor(long lsaRaw, long seq);
    }

    /** Publishes the buffered changes of a committed transaction. */
    @FunctionalInterface
    interface CommitSink {
        void publish(TxnBuffer buffer, RawLogItem commitDcl) throws InterruptedException;
    }

    @Override
    public void execute(ChangeEventSourceContext context, CubridPartition partition, CubridOffsetContext offsetContext) throws InterruptedException {
        final CubridLogClient client = new CubridLogClient();
        try {
            final Map<Long, TableId> tableByClassoid = readClassOidTableIds();

            client.setAllInCond(true);
            client.connect(
                    connectorConfig.getJdbcConfig().getHostname(),
                    connectorConfig.getCdcPort(),
                    connectorConfig.getDatabaseName(),
                    connectorConfig.getJdbcConfig().getUser(),
                    connectorConfig.getJdbcConfig().getPassword());

            long cursor;
            long counter;
            final Lsa anchor = offsetContext.getAnchorLsa();
            if (anchor.isAvailable()) {
                cursor = anchor.toRaw();
                counter = offsetContext.getAnchorSeq();
                LOGGER.info("Resuming CUBRID CDC stream at anchor {} with counter {}", anchor, counter);
            }
            else {
                cursor = client.findLsa(Instant.now().getEpochSecond());
                counter = 0;
                offsetContext.setAnchor(Lsa.fromRaw(cursor), 0);
                LOGGER.info("No prior offset — starting CUBRID CDC stream at current log end {}", Lsa.fromRaw(cursor));
            }

            final StreamState state = new StreamState(counter);
            final BufferPolicy policy = new BufferPolicy(
                    connectorConfig.getTransactionEventsThreshold(),
                    connectorConfig.getTransactionRetentionMs(),
                    System::currentTimeMillis);

            while (context.isRunning()) {
                final long batchInLsaRaw = cursor;

                final CubridLogClient.ExtractBatch batch = client.extract(cursor);
                cursor = batch.lsaOut();

                processBatch(state, policy, batch.items(), batchInLsaRaw, cursor,
                        classoid -> {
                            final TableId tableId = tableByClassoid.get(classoid);
                            return tableId != null && schema.tableFor(tableId) != null ? tableId : null;
                        },
                        (lsaRaw, seq) -> offsetContext.setAnchor(Lsa.fromRaw(lsaRaw), seq),
                        (buffer, commitDcl) -> publishTransaction(partition, offsetContext, buffer, commitDcl),
                        metrics);
                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
            }
        }
        catch (InterruptedException e) {
            throw e;
        }
        catch (Exception e) {
            errorHandler.setProducerThrowable(e);
        }
        finally {
            try {
                client.finalizeClient();
            }
            catch (Exception e) {
                LOGGER.warn("Failed to finalize the cubrid_log client", e);
            }
        }
    }

    /**
     * Consumes one extract batch: assigns the deterministic counter, buffers DML per transaction,
     * publishes on COMMIT and discards on ABORT (ADR 0004). A ROLLBACK_TO marker (workspace#47)
     * rewinds the transaction's buffer: every change whose {@code rec_lsa} key is greater than
     * the marker's key was undone server-side (savepoint/statement rollback) and is dropped
     * before it can ever publish. A committing transaction is removed
     * from {@code state.inflight} only <em>after</em> {@code commitSink} returns, so the anchor it
     * emits is bounded by its own start (workspace#45).
     * <p>
     * Buffer policy (ADR 0007, opt-in via {@code policy}): a transaction buffering more than
     * {@code eventsThreshold} events, or staying in flight longer than {@code retentionMs}, is
     * <em>abandoned</em> — its buffer is discarded, its later items are skipped via
     * {@code state.abandoned}, and the anchor advances past it at batch end. Abandoned changes are
     * permanently lost downstream; recovery is a re-snapshot. Items of abandoned transactions stay
     * counted, so counter determinism on replay is unaffected.
     */
    static void processBatch(StreamState state, BufferPolicy policy, List<RawLogItem> items, long batchInLsaRaw, long batchOutLsaRaw,
                             java.util.function.LongFunction<TableId> capturedTableFor,
                             AnchorSink anchorSink, CommitSink commitSink, TxnBufferMetrics metrics)
            throws InterruptedException {
        final long batchStartCounter = state.counter;

        for (RawLogItem item : items) {
            if (item.type() == RawLogItem.ItemType.TIMER) {
                continue; // not counted (ADR 0004) — batch-level heartbeat advances the offset
            }
            state.counter++;

            switch (item.type()) {
                case DML -> {
                    if (state.abandoned.contains(item.transactionId())) {
                        continue; // counted for determinism, but the transaction was abandoned (ADR 0007)
                    }
                    final TableId tableId = capturedTableFor.apply(item.classoid());
                    if (tableId == null) {
                        continue; // not captured — counted but never buffered/published
                    }
                    final TxnBuffer buffer = state.inflight.computeIfAbsent(item.transactionId(),
                            trid -> new TxnBuffer(batchInLsaRaw, batchStartCounter, policy.nowMs().getAsLong()));
                    buffer.changes.add(new BufferedChange(state.counter, tableId, item));
                    if (policy.eventsThreshold() > 0 && buffer.changes.size() > policy.eventsThreshold()) {
                        state.inflight.remove(item.transactionId());
                        state.abandoned.add(item.transactionId());
                        metrics.onOversizedAbandon(item.transactionId());
                        LOGGER.warn("Abandoning trid {}: buffered {} events, over transaction.events.threshold {} — "
                                + "its changes are permanently lost downstream; recover with a re-snapshot (ADR 0007 D2)",
                                item.transactionId(), buffer.changes.size(), policy.eventsThreshold());
                    }
                }
                case DCL -> {
                    final TxnBuffer buffer = state.inflight.get(item.transactionId());
                    if (buffer == null) {
                        state.abandoned.remove(item.transactionId()); // transaction over — the trid may be reused
                        continue;
                    }
                    if (item.dclType() == RawLogItem.DclType.COMMIT) {
                        // anchor while publishing = oldest in-flight start, the committing
                        // transaction itself included — never past its own first DML (workspace#45)
                        final TxnBuffer oldest = state.inflight.values().iterator().next();
                        anchorSink.setAnchor(oldest.startLsaRaw, oldest.startSeq);
                        commitSink.publish(buffer, item);
                    }
                    else {
                        LOGGER.debug("Discarding {} buffered changes of aborted trid {}", buffer.changes.size(), item.transactionId());
                    }
                    state.inflight.remove(item.transactionId());
                }
                case ROLLBACK_TO -> {
                    final TxnBuffer buffer = state.inflight.get(item.transactionId());
                    if (buffer == null) {
                        continue; // no captured DML inside the undone range
                    }
                    final long rewindKey = item.lsaKey();
                    final int before = buffer.changes.size();
                    buffer.changes.removeIf(change -> change.item().lsaKey() > rewindKey);
                    LOGGER.debug("Rolled back {} of {} buffered changes of trid {} (rewind key {})",
                            before - buffer.changes.size(), before, item.transactionId(), rewindKey);
                    if (buffer.changes.isEmpty()) {
                        state.inflight.remove(item.transactionId());
                    }
                }
                default -> {
                    // DDL — counted for determinism, never emitted (fixed-schema POC)
                }
            }
        }

        // retention abandon (ADR 0007 D3): dropping an expired transaction here, before the anchor
        // is derived, is what advances the anchor past it — to the next oldest in-flight start, or
        // to the batch end when nothing else is in flight
        final long nowMs = policy.nowMs().getAsLong();
        if (policy.retentionMs() > 0) {
            for (Iterator<Map.Entry<Integer, TxnBuffer>> it = state.inflight.entrySet().iterator(); it.hasNext();) {
                final Map.Entry<Integer, TxnBuffer> entry = it.next();
                final long ageMs = nowMs - entry.getValue().firstBufferedAtMs;
                if (ageMs > policy.retentionMs()) {
                    it.remove();
                    state.abandoned.add(entry.getKey());
                    metrics.onRetentionAbandon(entry.getKey());
                    LOGGER.warn("Abandoning trid {} after {} ms in flight, over transaction.retention.ms {} — "
                            + "the restart anchor advances past it and its changes are permanently lost downstream; "
                            + "recover with a re-snapshot (ADR 0007 D3)",
                            entry.getKey(), ageMs, policy.retentionMs());
                }
            }
        }

        // whole batch consumed: with nothing in flight the anchor may advance to the batch end
        if (state.inflight.isEmpty()) {
            anchorSink.setAnchor(batchOutLsaRaw, state.counter);
            metrics.onBatchEnd(0, 0);
        }
        else {
            final TxnBuffer oldest = state.inflight.values().iterator().next();
            anchorSink.setAnchor(oldest.startLsaRaw, oldest.startSeq);
            metrics.onBatchEnd(state.inflight.size(), Math.max(0, nowMs - oldest.firstBufferedAtMs));
        }
    }

    private void publishTransaction(CubridPartition partition, CubridOffsetContext offsetContext,
                                    TxnBuffer buffer, RawLogItem commitDcl)
            throws InterruptedException {
        final Instant commitTs = Instant.ofEpochSecond(commitDcl.timestamp());

        for (BufferedChange change : buffer.changes) {
            final RawLogItem item = change.item();
            final Table table = schema.tableFor(change.tableId());
            if (table == null) {
                continue;
            }

            final Operation operation;
            Object[] before = null;
            Object[] after = null;
            switch (item.dmlType()) {
                case INSERT, TRIGGER_INSERT -> {
                    operation = Operation.CREATE;
                    after = CubridLogValueDecoder.toRow(table, item.changedColumns());
                }
                case UPDATE, TRIGGER_UPDATE -> {
                    operation = Operation.UPDATE;
                    before = CubridLogValueDecoder.toRow(table, item.condColumns());
                    after = CubridLogValueDecoder.merge(table, item.condColumns(), item.changedColumns());
                }
                case DELETE, TRIGGER_DELETE -> {
                    operation = Operation.DELETE;
                    before = CubridLogValueDecoder.toRow(table, item.condColumns());
                }
                default -> {
                    LOGGER.warn("Skipping DML item with unknown type {} (trid {})", item.dmlType(), item.transactionId());
                    continue;
                }
            }

            offsetContext.setEventSeq(change.seq());
            offsetContext.setTxId(item.transactionId());
            offsetContext.event(change.tableId(), commitTs);
            dispatcher.dispatchDataChangeEvent(partition, change.tableId(),
                    new CubridChangeRecordEmitter(partition, offsetContext, operation, before, after, clock, connectorConfig));
        }
    }

    private Map<Long, TableId> readClassOidTableIds() throws Exception {
        final Map<Long, TableId> result = new HashMap<>();
        connection.readClassOidMap().forEach(
                (classoid, tableName) -> result.put(classoid, new TableId(null, connectorConfig.getDatabaseName(), tableName)));
        LOGGER.info("Resolved {} classoid -> table mappings from _db_class", result.size());
        return result;
    }
}
