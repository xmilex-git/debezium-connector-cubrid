/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.cubrid.CubridOffsetContext.Loader;
import io.debezium.connector.cubrid.jna.CubridLogClient;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

/**
 * Initial snapshot source (ADR 0005): reuses the Debezium JDBC snapshot with CUBRID JDBC.
 * <ul>
 * <li>No locking — the write stop on captured tables is an operator procedure (D2); the snapshot
 * transaction is merely promoted to REPEATABLE READ for a consistent multi-statement view.</li>
 * <li>The barrier LSA is captured by the connector itself over JNA in
 * {@link #determineSnapshotOffset} (D3), before any table is read.</li>
 * <li>Snapshot rows carry {@code source.lsn = 0} so any CDC event wins in the
 * ReplacingMergeTree (D4); the handover offset is {@code {barrier, seq=0, epoch=0}} (D5).</li>
 * </ul>
 */
public class CubridSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<CubridPartition, CubridOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CubridSnapshotChangeEventSource.class);

    private final CubridConnectorConfig connectorConfig;
    private final CubridConnection connection;
    private final CubridDatabaseSchema schema;

    public CubridSnapshotChangeEventSource(CubridConnectorConfig connectorConfig,
                                           MainConnectionProvidingConnectionFactory<CubridConnection> connectionFactory,
                                           CubridDatabaseSchema schema, EventDispatcher<CubridPartition, TableId> dispatcher,
                                           Clock clock, SnapshotProgressListener<CubridPartition> snapshotProgressListener,
                                           NotificationService<CubridPartition, CubridOffsetContext> notificationService,
                                           SnapshotterService snapshotterService) {
        super(connectorConfig, connectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService, snapshotterService);
        this.connectorConfig = connectorConfig;
        this.connection = connectionFactory.mainConnection();
        this.schema = schema;
    }

    @Override
    protected SnapshotContext<CubridPartition, CubridOffsetContext> prepare(CubridPartition partition, boolean onDemand) {
        return new CubridSnapshotContext(partition, connectorConfig.getDatabaseName(), onDemand);
    }

    @Override
    protected void connectionCreated(RelationalSnapshotContext<CubridPartition, CubridOffsetContext> snapshotContext) throws Exception {
        // Cost-free double protection on top of the operator write stop: a consistent
        // multi-statement view plus DDL blockage while the snapshot reads (ADR 0005 D2).
        connection.connection().setAutoCommit(false);
        connection.connection().setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        LOGGER.info("Snapshot connection promoted to REPEATABLE READ");
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<CubridPartition, CubridOffsetContext> ctx) throws Exception {
        final Set<TableId> tableIds = connection.readUserTableIds(connectorConfig.getDatabaseName());
        LOGGER.info("Found user tables: {}", tableIds);
        return tableIds;
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<CubridPartition, CubridOffsetContext> snapshotContext) {
        // No-op by decision (ADR 0005 D2): CUBRID has no LOCK TABLE statement and the write stop
        // on captured tables is an operator responsibility.
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<CubridPartition, CubridOffsetContext> snapshotContext) {
        // No locks are taken (ADR 0005 D2).
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<CubridPartition, CubridOffsetContext> ctx,
                                           CubridOffsetContext previousOffset) {
        if (previousOffset != null && previousOffset.getAnchorLsa().isAvailable()) {
            ctx.offset = previousOffset;
            return;
        }
        // Capture the barrier LSA over JNA (ADR 0005 D3). The framework calls this before any
        // table scan, matching the §8.1 "stop -> record barrier -> scan" order; streaming later
        // resumes exactly at the barrier with counter 0 and epoch 0 (D5).
        final CubridLogClient client = new CubridLogClient();
        try {
            client.connect(
                    connectorConfig.getJdbcConfig().getHostname(),
                    connectorConfig.getCdcPort(),
                    connectorConfig.getDatabaseName(),
                    connectorConfig.getJdbcConfig().getUser(),
                    connectorConfig.getJdbcConfig().getPassword());
            final long barrier = client.findLsa(Instant.now().getEpochSecond());
            final Lsa barrierLsa = Lsa.fromRaw(barrier);
            LOGGER.info("Captured snapshot barrier LSA {}", barrierLsa);
            ctx.offset = new CubridOffsetContext(connectorConfig, barrierLsa, 0L, 0, false, false);
        }
        finally {
            try {
                client.finalizeClient();
            }
            catch (Exception e) {
                LOGGER.warn("Failed to finalize the barrier cubrid_log client", e);
            }
        }
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<CubridPartition, CubridOffsetContext> snapshotContext,
                                      CubridOffsetContext offsetContext, SnapshottingTask snapshottingTask)
            throws Exception {
        for (TableId tableId : snapshotContext.capturedTables) {
            final Table table = connection.readTable(tableId)
                    .orElseThrow(() -> new DebeziumException("No columns found for captured table " + tableId));
            snapshotContext.tables.overwriteTable(table);
            schema.refresh(table);
            LOGGER.info("Read structure of {}: columns={}, pk={}", tableId,
                    table.columns().size(), table.primaryKeyColumnNames());
        }
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<CubridPartition, CubridOffsetContext> snapshotContext, Table table) {
        return SchemaChangeEvent.ofSnapshotCreate(snapshotContext.partition, snapshotContext.offset, snapshotContext.catalogName, table);
    }

    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<CubridPartition, CubridOffsetContext> snapshotContext,
                                                 TableId tableId, List<String> columns) {
        return Optional.of(String.format("SELECT %s FROM %s",
                columns.stream().collect(Collectors.joining(", ")),
                connection.quotedTableIdString(tableId)));
    }

    @Override
    protected CubridOffsetContext copyOffset(RelationalSnapshotContext<CubridPartition, CubridOffsetContext> snapshotContext) {
        return new Loader(connectorConfig).load(snapshotContext.offset.getOffset());
    }

    @Override
    protected ResultSet resultSetForDataEvents(String selectStatement, Statement statement) throws SQLException {
        return statement.executeQuery(selectStatement);
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class CubridSnapshotContext extends RelationalSnapshotContext<CubridPartition, CubridOffsetContext> {

        CubridSnapshotContext(CubridPartition partition, String catalogName, boolean onDemand) {
            super(partition, catalogName, onDemand);
        }
    }
}
