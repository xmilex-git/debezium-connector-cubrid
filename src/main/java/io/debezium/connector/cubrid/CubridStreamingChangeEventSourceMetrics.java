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
import io.debezium.pipeline.metrics.DefaultStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * JMX implementation of the CUBRID buffer-policy streaming metrics (ADR 0007). Updated from the
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

    public <T extends CdcSourceTaskContext> CubridStreamingChangeEventSourceMetrics(T taskContext,
                                                                                    ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                    EventMetadataProvider metadataProvider) {
        super(taskContext, changeEventQueueMetrics, metadataProvider);
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
    public void reset() {
        super.reset();
        activeTransactions.set(0);
        oversizedTransactions.set(0);
        abandonedTransactions.set(0);
        oldestInflightAgeMs.set(0);
        synchronized (abandonedTransactionIds) {
            abandonedTransactionIds.clear();
        }
    }
}
