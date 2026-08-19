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
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.CustomConverterRegistry;
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

        // UTF-8-only guard (workspace#77, review §4.14-A): runs first, before anything decodes
        // strings — a non-UTF-8 database corrupts silently on both the JDBC and the log path.
        try {
            DatabaseCharsetGuard.check(dataConnection.readDatabaseCharsetId());
        }
        catch (SQLException e) {
            throw new io.debezium.DebeziumException("Failed to read the database charset from db_root", e);
        }

        // Non-historized schema bootstrap (Postgres model): read the captured tables' structure
        // from the database on every start, so a restart that skips the snapshot phase can stream.
        // Fail-fast (workspace#82 D4): every include-list entry must exist with a loadable schema
        // — this is the only observation point for a table dropped or renamed while the connector
        // was stopped (the server would silently filter its lagging log otherwise). The
        // unsupported-type guard (workspace#73) runs here because the include list is a fixed
        // literal (ADR 0011 D10): every table a later blocking snapshot may touch is checked now.
        bootstrapIncludedTables(connectorConfig.getExtractionTableIds(), dataConnection::readTable, table -> {
            UnsupportedTypeGuard.checkTable(table);
            schema.refresh(table);
        });

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

    /** Reads one include-list table's relational model; a thrown SQLException is infrastructure failure. */
    @FunctionalInterface
    interface IncludedTableReader {
        java.util.Optional<io.debezium.relational.Table> read(TableId tableId) throws SQLException;
    }

    /**
     * Include-list bootstrap fail-fast (workspace#82 D4): every literal include entry must exist
     * and load its schema, or startup fails non-retriably. This retires the "pre-include a table,
     * CREATE it later" workflow (ADR 0011 amendment) — a missing entry is indistinguishable from
     * a table dropped/renamed while the connector was stopped, which must halt, not silently skip.
     */
    static void bootstrapIncludedTables(List<TableId> includeTableIds, IncludedTableReader reader,
                                        java.util.function.Consumer<io.debezium.relational.Table> refresher) {
        for (TableId tableId : includeTableIds) {
            final java.util.Optional<io.debezium.relational.Table> table;
            try {
                table = reader.read(tableId);
            }
            catch (SQLException e) {
                throw new io.debezium.DebeziumException(
                        "Failed to bootstrap the table schema of '" + tableId.identifier() + "' from the database", e);
            }
            if (table.isEmpty()) {
                throw new io.debezium.DebeziumException(
                        "'table.include.list' entry '" + tableId.identifier() + "' does not exist in the database "
                                + "(or is not readable by this account) — a capture target must exist with a loadable "
                                + "schema at startup; a table dropped or renamed while the connector was stopped is caught here. "
                                + "Update 'table.include.list' to the current schema, then run the resnapshot procedure. "
                                + "See the CUBRID connector setup guide, section 'Relation identity halt recovery'.");
            }
            refresher.accept(table.get());
        }
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
