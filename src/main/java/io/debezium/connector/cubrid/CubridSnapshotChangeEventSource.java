/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import io.debezium.connector.cubrid.CubridOffsetContext.Loader;
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
 * Initial snapshot source.
 * <p>
 * The POC does not snapshot: every hook below is a no-op so that startup falls straight through to
 * streaming. TODO(workspace#39)/TODO(workspace#40) implement table discovery, structure reading and
 * consistent offset capture.
 */
public class CubridSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<CubridPartition, CubridOffsetContext> {

    private final CubridConnectorConfig connectorConfig;

    public CubridSnapshotChangeEventSource(CubridConnectorConfig connectorConfig,
                                           MainConnectionProvidingConnectionFactory<CubridConnection> connectionFactory,
                                           CubridDatabaseSchema schema, EventDispatcher<CubridPartition, TableId> dispatcher,
                                           Clock clock, SnapshotProgressListener<CubridPartition> snapshotProgressListener,
                                           NotificationService<CubridPartition, CubridOffsetContext> notificationService,
                                           SnapshotterService snapshotterService) {
        super(connectorConfig, connectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService, snapshotterService);
        this.connectorConfig = connectorConfig;
    }

    @Override
    protected SnapshotContext<CubridPartition, CubridOffsetContext> prepare(CubridPartition partition, boolean onDemand) {
        return new CubridSnapshotContext(partition, connectorConfig.getDatabaseName(), onDemand);
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<CubridPartition, CubridOffsetContext> ctx) {
        // TODO(workspace#39): read the captured tables from the catalog.
        return Collections.emptySet();
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<CubridPartition, CubridOffsetContext> snapshotContext) {
        // TODO(workspace#39): no locking is performed while the snapshot is a no-op.
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<CubridPartition, CubridOffsetContext> snapshotContext) {
        // TODO(workspace#39): no locks are taken, so nothing to release.
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<CubridPartition, CubridOffsetContext> ctx,
                                           CubridOffsetContext previousOffset) {
        // TODO(workspace#39): capture the LSA that the snapshot is consistent with; the streaming
        // source currently anchors itself off the wall clock instead.
        if (ctx.offset == null) {
            ctx.offset = previousOffset != null
                    ? previousOffset
                    : new CubridOffsetContext(connectorConfig, Lsa.NULL, 0L, 0, false, false);
        }
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<CubridPartition, CubridOffsetContext> snapshotContext,
                                      CubridOffsetContext offsetContext, SnapshottingTask snapshottingTask) {
        // TODO(workspace#39): read the table structure of the captured tables.
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<CubridPartition, CubridOffsetContext> snapshotContext, Table table) {
        return SchemaChangeEvent.ofSnapshotCreate(snapshotContext.partition, snapshotContext.offset, snapshotContext.catalogName, table);
    }

    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<CubridPartition, CubridOffsetContext> snapshotContext,
                                                 TableId tableId, List<String> columns) {
        // TODO(workspace#40): emit the snapshot SELECT once table discovery exists.
        return Optional.empty();
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
