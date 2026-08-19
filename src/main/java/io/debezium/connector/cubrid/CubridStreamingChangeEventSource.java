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

import io.debezium.connector.cubrid.log.CubridLogClient;
import io.debezium.connector.cubrid.log.RawLogItem;
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
        // classoid -> owner.table from CDC_RELATION announces (ADR 0011 D4). Session-scoped by
        // design: the state is rebuilt from the stream after every (re)connect and never
        // persisted; the server re-announces per session. Every value is a member of the literal
        // include list (workspace#82 D5) — an empty-name or non-included announce halts the
        // stream instead of ever entering the dictionary, so DML routing through it IS literal
        // include-list matching (D4).
        final Map<Long, TableId> relationDictionary = new HashMap<>();
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

        /** A DDL halt fired on a captured table (ADR 0008 D5); the task is about to fail. */
        default void onDdlHalt(String table, String ddlType, String statement) {
        }

        /** A mid-stream CREATE TABLE was observed and skipped (ADR 0008 D3). */
        default void onMidStreamCreateTable(String statement) {
        }

        /** The HA halt guard fired (ADR 0010 D2); the task is about to fail. */
        default void onHaHalt(String reason) {
        }

        /** An empty/half-empty relation announce halted the stream (workspace#82 D2). */
        default void onEmptyAnnounceHalt(long classoid) {
        }

        /** An announce named a table outside the include list (workspace#82 D5); halting. */
        default void onAnnounceIncludeMismatchHalt(String ownerTable) {
        }
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

    private CubridOffsetContext effectiveOffsetContext;

    @Override
    public void init(CubridOffsetContext offsetContext) {
        // The coordinator hands this to SignalProcessor.setContext(): without it the
        // partition->offset map stays empty and every external (Kafka/JMX/File) signal is
        // silently dropped in getOffsets() — the blocking-snapshot signal never fires (#65).
        this.effectiveOffsetContext = offsetContext;
    }

    @Override
    public CubridOffsetContext getOffsetContext() {
        return effectiveOffsetContext;
    }

    @Override
    public void execute(ChangeEventSourceContext context, CubridPartition partition, CubridOffsetContext offsetContext) throws InterruptedException {
        final CubridLogClient client = new CubridLogClient();
        try {
            client.setAllInCond(true);
            // name-based extraction (ADR 0011 D3/D5): the server resolves the configured
            // owner.table names, scopes both delivery and the relation dictionary to them,
            // and the JDBC gate below checks per-table SELECT on the same list — a non-DBA
            // account with those grants is sufficient (workspace#70)
            client.setExtractionTableNames(connectorConfig.getExtractionTableNames());
            // the C client's db_login() authorization pass, reproduced over JDBC (#68 → #72)
            client.setAuthorizationGate(CubridCdcAuthorization.gate(connection));
            client.connect(
                    connectorConfig.getJdbcConfig().getHostname(),
                    connectorConfig.getCdcPort(),
                    connectorConfig.getDatabaseName(),
                    connectorConfig.getJdbcConfig().getUser(),
                    connectorConfig.getJdbcConfig().getPassword());

            // HA halt guard (ADR 0010 D2): before touching the log, verify the node just
            // connected to is the one the stored offset belongs to (path A) and that it is in
            // a capturable HA state (path B). The facts arrive in-band in the START_SESSION
            // reply (workspace#70) — no DBA-only SHOW LOG HEADER, and they describe the very
            // server the log stream comes from. Fails closed: an old server without facts is
            // rejected inside connect().
            final CubridLogClient.NodeFacts nodeFacts = client.nodeFacts();
            offsetContext.setSourceNode(HaNodeGuard.verifyAndStamp(
                    offsetContext.getSourceNode(),
                    offsetContext.getAnchorLsa().isAvailable(),
                    HaNodeGuard.identity(connectorConfig.getJdbcConfig().getHostname(), nodeFacts.dbCreationSeconds() * 1000L),
                    nodeFacts.haServerState(),
                    metrics::onHaHalt));

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
            // literal include-list routing (workspace#82 D4): every entry was verified to exist
            // with a loadable schema at task bootstrap, so membership alone decides routing
            final Set<TableId> includeTables = Set.copyOf(connectorConfig.getExtractionTableIds());
            final BufferPolicy policy = new BufferPolicy(
                    connectorConfig.getTransactionEventsThreshold(),
                    connectorConfig.getTransactionRetentionMs(),
                    System::currentTimeMillis);

            while (context.isRunning()) {
                // Blocking-snapshot pause handshake (ADR 0009 D4, binlog-connector pattern):
                // the coordinator's doBlockingSnapshot() blocks on waitStreamingPaused() until
                // this acknowledgment, runs the snapshot, then resumes us. Pausing only at a
                // batch boundary keeps the anchor and in-flight buffers untouched across the
                // pause; extract() returns periodically via TIMER items, bounding the latency.
                if (context.isPaused()) {
                    LOGGER.info("Streaming paused for an on-demand blocking snapshot");
                    context.streamingPaused();
                    context.waitSnapshotCompletion();
                    LOGGER.info("Streaming resumed after the blocking snapshot");
                }

                final long batchInLsaRaw = cursor;

                final CubridLogClient.ExtractBatch batch = client.extract(cursor);
                cursor = batch.lsaOut();

                processBatch(state, policy, batch.items(), batchInLsaRaw, cursor,
                        includeTables::contains,
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
     * <p>
     * DDL halt (ADR 0008, amended by workspace#82 D3): any TABLE ALTER/DROP/RENAME/TRUNCATE DDL
     * item that reaches the connector throws {@link DdlHaltException} the moment it is seen —
     * unconditionally, because passing the server-side extraction filter proves it concerns a
     * capture target. Committed events before it publish normally, in-flight buffers never
     * publish, and the anchor stays before the DDL so a restart deterministically re-halts.
     * Mid-stream CREATE TABLE only warns (ADR 0008 D3).
     */
    static void processBatch(StreamState state, BufferPolicy policy, List<RawLogItem> items, long batchInLsaRaw, long batchOutLsaRaw,
                             java.util.function.Predicate<TableId> included,
                             AnchorSink anchorSink, CommitSink commitSink, TxnBufferMetrics metrics)
            throws InterruptedException {
        final long batchStartCounter = state.counter;

        for (RawLogItem item : items) {
            if (item.type() == RawLogItem.ItemType.TIMER) {
                continue; // not counted (ADR 0004) — batch-level heartbeat advances the offset
            }
            if (item.type() == RawLogItem.ItemType.RELATION) {
                // NOT counted, exactly like TIMER (ADR 0011 D6): a dictionary announce is a
                // session event re-sent after every reconnect, not derived from a log position —
                // counting it would give the same row event a different counter (= _version) on
                // replay and void the #41 RMT convergence proof. It also bypasses the transaction
                // buffer: it belongs to no transaction and must survive an ADR 0007 abandon.
                // Contrast ROLLBACK_TO (#47), which IS position-derived and deterministic on
                // replay, so it stays counted.
                if (item.relationOwner().isEmpty() || item.relationTable().isEmpty()) {
                    // D2 (workspace#82): the engine resolves announce names at extraction time,
                    // so empty names mean the class was dropped server-side while its committed
                    // log lagged behind the read cursor — every buffered change of that classoid
                    // would silently skip and the sink would diverge. Fail loud instead.
                    metrics.onEmptyAnnounceHalt(item.classoid());
                    throw new io.debezium.DebeziumException(
                            "CDC relation announce for classoid " + item.classoid() + " arrived with empty names — "
                                    + "the table was dropped server-side before its lagging committed log was read, "
                                    + "so its changes can no longer be routed and the sink would silently diverge. "
                                    + "Remove the dropped table from 'table.include.list', then run the resnapshot procedure. "
                                    + "See the CUBRID connector setup guide, section 'Relation identity halt recovery'.");
                }
                final TableId announced = new TableId(null, item.relationOwner().toLowerCase(java.util.Locale.ROOT),
                        item.relationTable().toLowerCase(java.util.Locale.ROOT));
                if (!included.test(announced)) {
                    // D5 (workspace#82): the server only announces extraction targets, so an
                    // announce outside the include list means the table was renamed while its
                    // log lagged (the old name's changes now travel under the new name), or the
                    // engine/connector extraction contract is broken.
                    metrics.onAnnounceIncludeMismatchHalt(announced.identifier());
                    throw new io.debezium.DebeziumException(
                            "CDC relation announce named '" + announced.identifier() + "' (classoid " + item.classoid()
                                    + ") which is not in 'table.include.list' — the table was renamed server-side while "
                                    + "its committed log lagged, so its changes would be misattributed or lost. "
                                    + "Update 'table.include.list' to the current schema, then run the resnapshot procedure. "
                                    + "See the CUBRID connector setup guide, section 'Relation identity halt recovery'.");
                }
                state.relationDictionary.put(item.classoid(), announced);
                continue;
            }
            state.counter++;

            switch (item.type()) {
                case DML -> {
                    if (state.abandoned.contains(item.transactionId())) {
                        continue; // counted for determinism, but the transaction was abandoned (ADR 0007)
                    }
                    if (!state.relationDictionary.containsKey(item.classoid())) {
                        // the server filters delivery to the extraction targets and announces each
                        // target's dictionary entry before its first use (workspace#67), so a DML
                        // without an entry is a protocol-contract breach. A restart opens a fresh
                        // session whose dictionary is re-sent from the anchor, which heals a
                        // transient miss; a persistent failure means engine/connector version skew
                        // (ADR 0011 D10 — no _db_class fallback).
                        throw new io.debezium.DebeziumException(
                                "CDC stream delivered a DML item for classoid " + item.classoid()
                                        + " without a preceding relation dictionary announce (ADR 0011 D4). "
                                        + "Restart the connector; if this persists, the engine and connector "
                                        + "releases do not match (ADR 0011 D10).");
                    }
                    // dictionary membership was include-verified at announce time (D5), so this
                    // route IS literal include-list matching (D4) — no schema-based predicate
                    final TableId tableId = state.relationDictionary.get(item.classoid());
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
                case DDL -> {
                    // counted for determinism, never emitted; on halt the batch-end anchor advance
                    // below never runs, so the anchor stays before this DDL and an unassisted
                    // restart replays into the same halt (ADR 0008 D4)
                    if (!item.isTableDdl()) {
                        continue; // non-TABLE object DDL never affects row encoding/identity (ADR 0008 D1)
                    }
                    final RawLogItem.DdlType ddlType = item.decodedDdlType();
                    if (ddlType == RawLogItem.DdlType.CREATE) {
                        metrics.onMidStreamCreateTable(item.ddlStatement());
                        LOGGER.warn("Mid-stream CREATE TABLE observed and skipped — capturing a new table requires "
                                + "the documented restart+snapshot procedure (ADR 0008 D3): {}", item.ddlStatement());
                        continue;
                    }
                    // D3 (workspace#82): a TABLE DDL that reached the connector passed the
                    // server-side extraction filter — that alone proves it concerns a capture
                    // target, so halt unconditionally with no dictionary or schema lookup
                    // (a lookup could miss and silently skip the halt, e.g. a NULL-classoid
                    // statement). ALTER/DROP/RENAME/TRUNCATE — and, fail-safe, any unknown
                    // future ddl_type. The dictionary is consulted only to label the error.
                    final TableId announced = state.relationDictionary.get(item.classoid());
                    final String tableLabel = announced != null ? announced.identifier()
                            : "<unannounced classoid " + item.classoid() + ">";
                    metrics.onDdlHalt(tableLabel, ddlType.name(), item.ddlStatement());
                    throw new DdlHaltException(tableLabel, ddlType, item.ddlStatement());
                }
                default -> {
                    // UNKNOWN item type — counted, ignored
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

}
