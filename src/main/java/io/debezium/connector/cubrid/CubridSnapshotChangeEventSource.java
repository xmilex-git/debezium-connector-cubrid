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
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

/**
 * Initial snapshot source, online without a write stop (ADR 0009 D1 on top of ADR 0005).
 * <ul>
 * <li>No locking and no write stop — writes may continue during the snapshot; the operator
 * write-stop checklist of ADR 0005 survives only as a conservative fallback procedure. The
 * snapshot transaction is promoted to REPEATABLE READ for a consistent multi-statement view.</li>
 * <li>The barrier LSA is captured by the connector itself in {@link #determineSnapshotOffset}
 * (ADR 0005 D3), and the consistent read view is (re-)established strictly <em>after</em> the
 * barrier — see the invariant note in that method. Any commit the view does not contain then has
 * an LSA at or above the barrier and is replayed by streaming; any commit the view does contain
 * that streaming replays too converges because snapshot rows lose to CDC rows (D4).</li>
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
        // HA halt guard, state axis only (ADR 0010 D2-2): never snapshot a non-master — a
        // standby's data plus a later stream would mix two nodes' histories. The identity axis
        // needs a stored offset to compare against, so it lives in the streaming source, which
        // stamps the identity on its first run after this snapshot.
        HaNodeGuard.assertCapturableState(connection.readHaNodeInfo().haServerState(), reason -> {
        });

        // The consistency mechanism of the online snapshot (ADR 0009 D1): a REPEATABLE READ
        // multi-statement view. Known constraints (documented, ADR 0009 D2): the RR reader
        // blocks DDL for the duration of the scan, and large tables prolong that window.
        connection.connection().setAutoCommit(false);
        connection.connection().setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        // Discard whatever view the shared main connection may still hold (it stays in
        // autoCommit=false after a previous snapshot): a blocking snapshot skips
        // determineSnapshotOffset, so without this its scan could read a stale view predating
        // the changes it was asked to re-read. The next statement opens a fresh view.
        connection.connection().commit();
        LOGGER.info("Snapshot connection promoted to REPEATABLE READ (fresh view)");
    }

    @Override
    public SnapshottingTask getBlockingSnapshottingTask(CubridPartition partition, CubridOffsetContext previousOffset,
                                                        SnapshotConfiguration snapshotConfiguration) {
        // The blocking-snapshot path (ADR 0009 D4) skips determineSnapshotOffset and reuses the
        // live streaming offset context as-is, so re-assert the D4 invariant here: snapshot rows
        // must carry source.lsn = 0 so replayed CDC always wins in the ReplacingMergeTree. The
        // sourceInfo still holds the last emitted event counter; streaming re-stamps it per
        // event on resume, so zeroing it while streaming is paused is safe.
        if (previousOffset != null) {
            previousOffset.setEventSeq(0);
            LOGGER.info("Blocking snapshot reuses anchor {} as its barrier; snapshot rows carry seq 0",
                    previousOffset.getAnchorLsa());
        }
        return super.getBlockingSnapshottingTask(partition, previousOffset, snapshotConfiguration);
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
        // No-op by decision (ADR 0005 D2 / ADR 0009 D1): CUBRID has no LOCK TABLE statement and
        // the snapshot is online — concurrent writes converge via barrier + RR view + version 0.
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<CubridPartition, CubridOffsetContext> snapshotContext) {
        // No locks are taken (ADR 0005 D2).
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<CubridPartition, CubridOffsetContext> ctx,
                                           CubridOffsetContext previousOffset) throws Exception {
        if (previousOffset != null && previousOffset.getAnchorLsa().isAvailable()) {
            // Interrupted-snapshot rerun (ADR 0009 D2 ④): reuse the original barrier. The rescan's
            // view is established now, i.e. after that (older) barrier, so the ordering invariant
            // below holds trivially and streaming replay from the barrier covers every difference.
            // (The on-demand blocking snapshot never reaches this method — core skips step 4 for
            // onDemand tasks; its D4 invariant is re-asserted in getBlockingSnapshottingTask.)
            ctx.offset = previousOffset;
            return;
        }
        testPause("before barrier capture", connectorConfig.getSnapshotTestPauseBeforeBarrierMs());
        // Capture the barrier LSA over JNA (ADR 0005 D3). The framework calls this before any
        // table scan; streaming later resumes exactly at the barrier with counter 0 and epoch 0 (D5).
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
        // INVARIANT (ADR 0009 D1): the REPEATABLE READ view the data scan reads from must be
        // established strictly AFTER the barrier. The framework's metadata queries
        // (getAllTableIds) already opened a view on this connection BEFORE the barrier was
        // captured — a commit landing between that view and the barrier would be in neither the
        // snapshot nor the stream (fault test ②'s loss window). Ending the transaction here
        // discards the pre-barrier view; the next statement (readTableStructure / the data scan,
        // same connection since snapshot.max.threads=1) opens a fresh view above the barrier.
        connection.connection().commit();
        LOGGER.info("Discarded pre-barrier REPEATABLE READ view; scan view will be established after the barrier");
        testPause("after barrier capture", connectorConfig.getSnapshotTestPauseAfterBarrierMs());
    }

    private static void testPause(String where, long ms) throws InterruptedException {
        if (ms > 0) {
            LOGGER.warn("TEST PAUSE {} ms {} (fault-injection hook, never set in production)", ms, where);
            Thread.sleep(ms);
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
