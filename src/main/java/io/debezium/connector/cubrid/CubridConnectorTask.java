/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.bean.StandardBeanNames;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.document.DocumentReader;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

/**
 * The main task executing streaming from CUBRID.
 */
public class CubridConnectorTask extends BaseSourceTask<CubridPartition, CubridOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CubridConnectorTask.class);

    private static final String CONTEXT_NAME = "cubrid-connector-task";

    private volatile CubridConnectorConfig connectorConfig;
    private volatile CubridTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile CubridConnection dataConnection;
    private volatile ErrorHandler errorHandler;
    private volatile CubridDatabaseSchema schema;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    protected String connectorName() {
        return Module.name();
    }

    @Override
    public CdcSourceTaskContext<CubridConnectorConfig> preStart(Configuration config) {
        connectorConfig = new CubridConnectorConfig(config);
        taskContext = new CubridTaskContext(config, connectorConfig);
        return taskContext;
    }

    @Override
    protected ChangeEventSourceCoordinator<CubridPartition, CubridOffsetContext> start(Configuration config) {
        final CubridConnectorConfig connectorConfig = this.connectorConfig;
        final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY);
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();

        final MainConnectionProvidingConnectionFactory<CubridConnection> connectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(
                () -> new CubridConnection(connectorConfig.getJdbcConfig()));
        dataConnection = connectionFactory.mainConnection();

        registerServiceProviders(connectorConfig.getServiceRegistry());

        final CustomConverterRegistry customConverterRegistry = connectorConfig.getServiceRegistry().tryGetService(CustomConverterRegistry.class);
        final CubridValueConverters valueConverters = new CubridValueConverters(connectorConfig.getDecimalMode(),
                connectorConfig.getTemporalPrecisionMode(), connectorConfig.binaryHandlingMode());
        schema = new CubridDatabaseSchema(connectorConfig, topicNamingStrategy, valueConverters, customConverterRegistry, taskContext);

        final Offsets<CubridPartition, CubridOffsetContext> previousOffsets = getPreviousOffsets(
                new CubridPartition.Provider(connectorConfig),
                new CubridOffsetContext.Loader(connectorConfig));

        // Non-historized schema bootstrap (Postgres model): read the captured tables' structure
        // from the database on every start, so a restart that skips the snapshot phase can stream.
        try {
            for (TableId tableId : dataConnection.readUserTableIds(connectorConfig.getDatabaseName())) {
                if (connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                    dataConnection.readTable(tableId).ifPresent(schema::refresh);
                }
            }
        }
        catch (SQLException e) {
            throw new io.debezium.DebeziumException("Failed to bootstrap the table schemas from the database", e);
        }

        // Manual bean registration
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, config);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.DATABASE_SCHEMA, schema);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.JDBC_CONNECTION, dataConnection);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.VALUE_CONVERTER, valueConverters);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.OFFSETS, previousOffsets);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, taskContext);

        final SnapshotterService snapshotterService = connectorConfig.getServiceRegistry().tryGetService(SnapshotterService.class);

        final Clock clock = Clock.system();

        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        errorHandler = new CubridErrorHandler(connectorConfig, queue, errorHandler);

        final CubridEventMetadataProvider metadataProvider = new CubridEventMetadataProvider();

        final SignalProcessor<CubridPartition, CubridOffsetContext> signalProcessor = new SignalProcessor<>(
                CubridConnector.class,
                connectorConfig,
                Map.of(),
                getAvailableSignalChannels(),
                DocumentReader.defaultReader(),
                previousOffsets);

        final EventDispatcher<CubridPartition, TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicNamingStrategy,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                metadataProvider,
                schemaNameAdjuster,
                signalProcessor,
                connectorConfig.getServiceRegistry().tryGetService(DebeziumHeaderProducer.class));

        final NotificationService<CubridPartition, CubridOffsetContext> notificationService = new NotificationService<>(
                getNotificationChannels(),
                connectorConfig,
                CubridSchemaFactory.get(),
                dispatcher::enqueueNotification);

        // the streaming source updates the buffer-policy gauges on the same instance the
        // coordinator registers as the streaming MBean (ADR 0007, Oracle pattern)
        final CubridStreamingChangeEventSourceMetrics streamingMetrics = new CubridStreamingChangeEventSourceMetrics(
                taskContext, queue, metadataProvider, schema::dataCollectionIds);

        final ChangeEventSourceCoordinator<CubridPartition, CubridOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                previousOffsets,
                errorHandler,
                CubridConnector.class,
                connectorConfig,
                new CubridChangeEventSourceFactory(connectorConfig, connectionFactory, errorHandler, dispatcher, clock, schema, snapshotterService, streamingMetrics),
                new CubridChangeEventSourceMetricsFactory(streamingMetrics),
                dispatcher,
                schema,
                signalProcessor,
                notificationService,
                snapshotterService);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    @Override
    protected List<SourceRecord> doPoll() throws InterruptedException {
        return queue.poll().stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());
    }

    @Override
    protected Optional<ErrorHandler> getErrorHandler() {
        return Optional.ofNullable(errorHandler);
    }

    @Override
    protected void doStop() {
        try {
            if (dataConnection != null) {
                dataConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC connection", e);
        }

        if (schema != null) {
            schema.close();
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return CubridConnectorConfig.ALL_FIELDS;
    }
}
