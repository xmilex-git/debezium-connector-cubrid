/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * Hands the coordinator the pre-built streaming metrics instance that the streaming source also
 * updates (Oracle pattern), so the buffer-policy gauges (ADR 0007) surface on the standard
 * streaming MBean.
 */
public class CubridChangeEventSourceMetricsFactory extends DefaultChangeEventSourceMetricsFactory<CubridPartition> {

    private final CubridStreamingChangeEventSourceMetrics streamingMetrics;

    public CubridChangeEventSourceMetricsFactory(CubridStreamingChangeEventSourceMetrics streamingMetrics) {
        this.streamingMetrics = streamingMetrics;
    }

    @Override
    public <T extends CdcSourceTaskContext> StreamingChangeEventSourceMetrics<CubridPartition> getStreamingMetrics(T taskContext,
                                                                                                                   ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                                   EventMetadataProvider eventMetadataProvider) {
        return streamingMetrics;
    }
}
