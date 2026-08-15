/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.common.OffsetUtils;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;

/**
 * The connector offset, persisted as the four flat numeric keys mandated by ADR 0004
 * ({@code page_id}, {@code lsa_offset}, {@code seq}, {@code epoch}) plus the standard snapshot keys.
 */
public class CubridOffsetContext extends CommonOffsetContext<SourceInfo> {

    private static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";

    private final Schema sourceInfoSchema;
    private final TransactionContext transactionContext;
    private final IncrementalSnapshotContext<TableId> incrementalSnapshotContext;
    private boolean snapshotCompleted;

    public CubridOffsetContext(CubridConnectorConfig connectorConfig, Lsa lsa, long seq, int epoch,
                               boolean snapshot, boolean snapshotCompleted,
                               TransactionContext transactionContext,
                               IncrementalSnapshotContext<TableId> incrementalSnapshotContext) {
        super(new SourceInfo(connectorConfig));

        sourceInfo.setLsa(lsa);
        sourceInfo.setSeq(seq);
        sourceInfo.setEpoch(epoch);
        sourceInfoSchema = sourceInfo.schema();

        this.snapshotCompleted = snapshotCompleted;
        if (this.snapshotCompleted) {
            postSnapshotCompletion();
        }
        else {
            sourceInfo.setSnapshot(snapshot ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }

        this.transactionContext = transactionContext;
        this.incrementalSnapshotContext = incrementalSnapshotContext;
    }

    public CubridOffsetContext(CubridConnectorConfig connectorConfig, Lsa lsa, long seq, int epoch,
                               boolean snapshot, boolean snapshotCompleted) {
        this(connectorConfig, lsa, seq, epoch, snapshot, snapshotCompleted,
                new TransactionContext(), new SignalBasedIncrementalSnapshotContext<>(false));
    }

    @Override
    public Map<String, ?> getOffset() {
        if (sourceInfo.isSnapshot()) {
            return Collect.hashMapOf(
                    SourceInfo.SNAPSHOT_KEY, true,
                    SNAPSHOT_COMPLETED_KEY, snapshotCompleted,
                    SourceInfo.PAGE_ID_KEY, sourceInfo.getPageId(),
                    SourceInfo.LSA_OFFSET_KEY, sourceInfo.getLsaOffset());
        }
        return incrementalSnapshotContext.store(transactionContext.store(Collect.hashMapOf(
                SourceInfo.PAGE_ID_KEY, sourceInfo.getPageId(),
                SourceInfo.LSA_OFFSET_KEY, sourceInfo.getLsaOffset(),
                SourceInfo.SEQ_KEY, sourceInfo.getSeq(),
                SourceInfo.EPOCH_KEY, (long) sourceInfo.getEpoch())));
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    public Lsa getLsa() {
        return sourceInfo.getLsa();
    }

    public void setLsa(Lsa lsa) {
        sourceInfo.setLsa(lsa);
    }

    public long getSeq() {
        return sourceInfo.getSeq();
    }

    /**
     * Advances the event counter that serves as the connector position (ADR 0004).
     */
    public long incrementSeq() {
        long next = sourceInfo.getSeq() + 1;
        sourceInfo.setSeq(next);
        return next;
    }

    public void setTxId(int txId) {
        sourceInfo.setTxId(txId);
    }

    @Override
    public boolean isSnapshotRunning() {
        return sourceInfo.isSnapshot() && !snapshotCompleted;
    }

    public boolean isSnapshotCompleted() {
        return snapshotCompleted;
    }

    @Override
    public void preSnapshotStart() {
        sourceInfo.setSnapshot(SnapshotRecord.TRUE);
        snapshotCompleted = false;
    }

    @Override
    public void preSnapshotCompletion() {
        snapshotCompleted = true;
    }

    @Override
    public void event(DataCollectionId tableId, Instant timestamp) {
        sourceInfo.setTimestamp(timestamp);
        sourceInfo.setTableId((TableId) tableId);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return incrementalSnapshotContext;
    }

    @Override
    public String toString() {
        return "CubridOffsetContext [sourceInfo=" + sourceInfo + ", snapshotCompleted=" + snapshotCompleted + "]";
    }

    public static class Loader implements OffsetContext.Loader<CubridOffsetContext> {

        private final CubridConnectorConfig connectorConfig;

        public Loader(CubridConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public CubridOffsetContext load(Map<String, ?> offset) {
            final long pageId = OffsetUtils.longOffsetValue(offset, SourceInfo.PAGE_ID_KEY);
            final long lsaOffset = OffsetUtils.longOffsetValue(offset, SourceInfo.LSA_OFFSET_KEY);
            final long seq = OffsetUtils.longOffsetValue(offset, SourceInfo.SEQ_KEY);
            final long epoch = OffsetUtils.longOffsetValue(offset, SourceInfo.EPOCH_KEY);

            final boolean snapshot = Boolean.TRUE.equals(offset.get(SourceInfo.SNAPSHOT_KEY));
            final boolean snapshotCompleted = Boolean.TRUE.equals(offset.get(SNAPSHOT_COMPLETED_KEY));

            return new CubridOffsetContext(connectorConfig, new Lsa(pageId, lsaOffset), seq, (int) epoch,
                    snapshot, snapshotCompleted,
                    TransactionContext.load(offset), SignalBasedIncrementalSnapshotContext.load(offset, false));
        }
    }
}
