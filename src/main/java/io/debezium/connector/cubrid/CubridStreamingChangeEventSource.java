/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
 */
public class CubridStreamingChangeEventSource implements StreamingChangeEventSource<CubridPartition, CubridOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CubridStreamingChangeEventSource.class);

    private final CubridConnectorConfig connectorConfig;
    private final CubridConnection connection;
    private final EventDispatcher<CubridPartition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final CubridDatabaseSchema schema;

    public CubridStreamingChangeEventSource(CubridConnectorConfig connectorConfig, CubridConnection connection,
                                            EventDispatcher<CubridPartition, TableId> dispatcher,
                                            ErrorHandler errorHandler, Clock clock, CubridDatabaseSchema schema) {
        this.connectorConfig = connectorConfig;
        this.connection = connection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
    }

    /** One buffered DML with the counter it was assigned when read from the stream. */
    private record BufferedChange(long seq, TableId tableId, RawLogItem item) {
    }

    /** Per-transaction buffer, remembering the batch boundary at which its first item arrived. */
    private static final class TxnBuffer {
        final long startLsaRaw;
        final long startSeq;
        final List<BufferedChange> changes = new ArrayList<>();

        TxnBuffer(long startLsaRaw, long startSeq) {
            this.startLsaRaw = startLsaRaw;
            this.startSeq = startSeq;
        }
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

            // insertion order = first-DML order, so the first entry is the oldest in-flight txn
            final LinkedHashMap<Integer, TxnBuffer> inflight = new LinkedHashMap<>();

            while (context.isRunning()) {
                final long batchInLsaRaw = cursor;
                final long batchStartCounter = counter;

                final CubridLogClient.ExtractBatch batch = client.extract(cursor);
                cursor = batch.lsaOut();

                for (RawLogItem item : batch.items()) {
                    if (item.type() == RawLogItem.ItemType.TIMER) {
                        continue; // not counted (ADR 0004) — batch-level heartbeat advances the offset
                    }
                    counter++;

                    switch (item.type()) {
                        case DML -> {
                            final TableId tableId = tableByClassoid.get(item.classoid());
                            if (tableId == null || schema.tableFor(tableId) == null) {
                                continue; // not captured — counted but never buffered/published
                            }
                            inflight.computeIfAbsent(item.transactionId(), trid -> new TxnBuffer(batchInLsaRaw, batchStartCounter))
                                    .changes.add(new BufferedChange(counter, tableId, item));
                        }
                        case DCL -> {
                            final TxnBuffer buffer = inflight.remove(item.transactionId());
                            if (buffer == null) {
                                continue;
                            }
                            if (item.dclType() == RawLogItem.DclType.COMMIT) {
                                publishTransaction(partition, offsetContext, inflight, buffer, item, batchInLsaRaw, batchStartCounter);
                            }
                            else {
                                LOGGER.debug("Discarding {} buffered changes of aborted trid {}", buffer.changes.size(), item.transactionId());
                            }
                        }
                        default -> {
                            // DDL — counted for determinism, never emitted (fixed-schema POC)
                        }
                    }
                }

                // whole batch consumed: with nothing in flight the anchor may advance to the batch end
                if (inflight.isEmpty()) {
                    offsetContext.setAnchor(Lsa.fromRaw(cursor), counter);
                }
                else {
                    final TxnBuffer oldest = inflight.values().iterator().next();
                    offsetContext.setAnchor(Lsa.fromRaw(oldest.startLsaRaw), oldest.startSeq);
                }
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

    private void publishTransaction(CubridPartition partition, CubridOffsetContext offsetContext,
                                    LinkedHashMap<Integer, TxnBuffer> inflight, TxnBuffer buffer,
                                    RawLogItem commitDcl, long batchInLsaRaw, long batchStartCounter)
            throws InterruptedException {
        // The anchor while publishing must not pass the oldest transaction still in flight, nor
        // the start of the current (partially processed) batch.
        final Lsa anchorLsa;
        final long anchorSeq;
        if (!inflight.isEmpty()) {
            final TxnBuffer oldest = inflight.values().iterator().next();
            anchorLsa = Lsa.fromRaw(oldest.startLsaRaw);
            anchorSeq = oldest.startSeq;
        }
        else {
            anchorLsa = Lsa.fromRaw(batchInLsaRaw);
            anchorSeq = batchStartCounter;
        }
        offsetContext.setAnchor(anchorLsa, anchorSeq);

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
