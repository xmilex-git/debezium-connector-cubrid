/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.metrics.DefaultStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * JMX implementation of the CUBRID buffer-policy (ADR 0007) and DDL-halt (ADR 0008) streaming metrics. Updated from the
 * streaming thread through the {@link CubridStreamingChangeEventSource.TxnBufferMetrics} hooks,
 * read from JMX threads — all state is atomics or synchronized.
 */
public class CubridStreamingChangeEventSourceMetrics extends DefaultStreamingChangeEventSourceMetrics<CubridPartition>
        implements CubridStreamingChangeEventSourceMetricsMXBean, CubridStreamingChangeEventSource.TxnBufferMetrics {

    /** How many abandoned transaction ids to retain for JMX inspection (mirrors Oracle's small LRU). */
    private static final int ABANDONED_IDS_CAPACITY = 10;

    private final AtomicLong activeTransactions = new AtomicLong();
    private final AtomicLong oversizedTransactions = new AtomicLong();
    private final AtomicLong abandonedTransactions = new AtomicLong();
    private final AtomicLong oldestInflightAgeMs = new AtomicLong();
    private final LinkedHashSet<String> abandonedTransactionIds = new LinkedHashSet<>();
    private final AtomicLong ddlHalts = new AtomicLong();
    private final AtomicLong midStreamCreateTables = new AtomicLong();
    private final AtomicLong haHalts = new AtomicLong();
    private final AtomicLong emptyAnnounceHalts = new AtomicLong();
    private final AtomicLong announceIncludeMismatchHalts = new AtomicLong();
    private volatile String lastDdlHaltTable = "";
    private volatile String lastDdlHaltStatement = "";
    private volatile String lastHaHaltReason = "";
    private volatile String lastEmptyAnnounceHaltClassoid = "";
    private volatile String lastAnnounceIncludeMismatchTable = "";

    public <T extends CdcSourceTaskContext> CubridStreamingChangeEventSourceMetrics(T taskContext,
                                                                                    ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                    EventMetadataProvider metadataProvider,
                                                                                    CapturedTablesSupplier capturedTablesSupplier) {
        super(taskContext, changeEventQueueMetrics, metadataProvider, capturedTablesSupplier);
    }

    @Override
    public void onOversizedAbandon(int trid) {
        oversizedTransactions.incrementAndGet();
        rememberAbandoned(trid);
    }

    @Override
    public void onRetentionAbandon(int trid) {
        abandonedTransactions.incrementAndGet();
        rememberAbandoned(trid);
    }

    @Override
    public void onBatchEnd(int activeCount, long oldestAgeMs) {
        activeTransactions.set(activeCount);
        oldestInflightAgeMs.set(oldestAgeMs);
    }

    @Override
    public void onDdlHalt(String table, String ddlType, String statement) {
        ddlHalts.incrementAndGet();
        lastDdlHaltTable = table;
        lastDdlHaltStatement = ddlType + ": " + (statement == null ? "" : statement);
    }

    @Override
    public void onMidStreamCreateTable(String statement) {
        midStreamCreateTables.incrementAndGet();
    }

    private void rememberAbandoned(int trid) {
        synchronized (abandonedTransactionIds) {
            abandonedTransactionIds.remove(String.valueOf(trid));
            abandonedTransactionIds.add(String.valueOf(trid));
            if (abandonedTransactionIds.size() > ABANDONED_IDS_CAPACITY) {
                abandonedTransactionIds.remove(abandonedTransactionIds.iterator().next());
            }
        }
    }

    @Override
    public long getNumberOfActiveTransactions() {
        return activeTransactions.get();
    }

    @Override
    public long getNumberOfOversizedTransactions() {
        return oversizedTransactions.get();
    }

    @Override
    public long getAbandonedTransactionCount() {
        return abandonedTransactions.get();
    }

    @Override
    public Set<String> getAbandonedTransactionIds() {
        synchronized (abandonedTransactionIds) {
            return new LinkedHashSet<>(abandonedTransactionIds);
        }
    }

    @Override
    public long getOldestInflightAgeInMilliseconds() {
        return oldestInflightAgeMs.get();
    }

    @Override
    public long getDdlHaltCount() {
        return ddlHalts.get();
    }

    @Override
    public String getLastDdlHaltTable() {
        return lastDdlHaltTable;
    }

    @Override
    public String getLastDdlHaltStatement() {
        return lastDdlHaltStatement;
    }

    @Override
    public long getMidStreamCreateTableCount() {
        return midStreamCreateTables.get();
    }

    @Override
    public void onHaHalt(String reason) {
        haHalts.incrementAndGet();
        lastHaHaltReason = reason == null ? "" : reason;
    }

    @Override
    public long getHaHaltCount() {
        return haHalts.get();
    }

    @Override
    public String getLastHaHaltReason() {
        return lastHaHaltReason;
    }

    @Override
    public void onEmptyAnnounceHalt(long classoid) {
        emptyAnnounceHalts.incrementAndGet();
        lastEmptyAnnounceHaltClassoid = String.valueOf(classoid);
    }

    @Override
    public void onAnnounceIncludeMismatchHalt(String ownerTable) {
        announceIncludeMismatchHalts.incrementAndGet();
        lastAnnounceIncludeMismatchTable = ownerTable == null ? "" : ownerTable;
    }

    @Override
    public long getEmptyAnnounceHaltCount() {
        return emptyAnnounceHalts.get();
    }

    @Override
    public String getLastEmptyAnnounceHaltClassoid() {
        return lastEmptyAnnounceHaltClassoid;
    }

    @Override
    public long getAnnounceIncludeMismatchHaltCount() {
        return announceIncludeMismatchHalts.get();
    }

    @Override
    public String getLastAnnounceIncludeMismatchTable() {
        return lastAnnounceIncludeMismatchTable;
    }

    @Override
    public void reset() {
        super.reset();
        activeTransactions.set(0);
        oversizedTransactions.set(0);
        abandonedTransactions.set(0);
        oldestInflightAgeMs.set(0);
        ddlHalts.set(0);
        midStreamCreateTables.set(0);
        haHalts.set(0);
        emptyAnnounceHalts.set(0);
        announceIncludeMismatchHalts.set(0);
        lastDdlHaltTable = "";
        lastDdlHaltStatement = "";
        lastHaHaltReason = "";
        lastEmptyAnnounceHaltClassoid = "";
        lastAnnounceIncludeMismatchTable = "";
        synchronized (abandonedTransactionIds) {
            abandonedTransactionIds.clear();
        }
    }
}
