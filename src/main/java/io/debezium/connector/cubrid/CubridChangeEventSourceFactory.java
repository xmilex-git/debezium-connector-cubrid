/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import java.util.Optional;

import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;

public class CubridChangeEventSourceFactory implements ChangeEventSourceFactory<CubridPartition, CubridOffsetContext> {

    private final CubridConnectorConfig configuration;
    private final MainConnectionProvidingConnectionFactory<CubridConnection> connectionFactory;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<CubridPartition, TableId> dispatcher;
    private final Clock clock;
    private final CubridDatabaseSchema schema;
    private final SnapshotterService snapshotterService;
    private final CubridStreamingChangeEventSourceMetrics streamingMetrics;

    public CubridChangeEventSourceFactory(CubridConnectorConfig configuration,
                                          MainConnectionProvidingConnectionFactory<CubridConnection> connectionFactory,
                                          ErrorHandler errorHandler, EventDispatcher<CubridPartition, TableId> dispatcher,
                                          Clock clock, CubridDatabaseSchema schema, SnapshotterService snapshotterService,
                                          CubridStreamingChangeEventSourceMetrics streamingMetrics) {
        this.configuration = configuration;
        this.connectionFactory = connectionFactory;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.snapshotterService = snapshotterService;
        this.streamingMetrics = streamingMetrics;
    }

    @Override
    public SnapshotChangeEventSource<CubridPartition, CubridOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener<CubridPartition> snapshotProgressListener,
                                                                                                        NotificationService<CubridPartition, CubridOffsetContext> notificationService) {
        return new CubridSnapshotChangeEventSource(
                configuration,
                connectionFactory,
                schema,
                dispatcher,
                clock,
                snapshotProgressListener,
                notificationService,
                snapshotterService);
    }

    @Override
    public StreamingChangeEventSource<CubridPartition, CubridOffsetContext> getStreamingChangeEventSource() {
        return new CubridStreamingChangeEventSource(configuration, connectionFactory.mainConnection(), dispatcher, errorHandler, clock, schema, streamingMetrics);
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<CubridPartition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(CubridOffsetContext offsetContext,
                                                                                                                                              SnapshotProgressListener<CubridPartition> snapshotProgressListener,
                                                                                                                                              DataChangeEventListener<CubridPartition> dataChangeEventListener,
                                                                                                                                              NotificationService<CubridPartition, CubridOffsetContext> notificationService) {
        // Incremental snapshots are out of scope for the POC.
        return Optional.empty();
    }
}
