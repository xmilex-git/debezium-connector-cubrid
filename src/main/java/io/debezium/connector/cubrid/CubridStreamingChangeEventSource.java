/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.cubrid.jna.CubridLogClient;
import io.debezium.connector.cubrid.jna.RawLogItem;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/**
 * Streams changes out of the CUBRID transaction log through the {@code cubrid_log} JNA client.
 * <p>
 * TODO(workspace#38): the loop currently only logs the raw items and advances the offset counter;
 * transaction buffering and dispatching change events to the queue still has to be built.
 */
public class CubridStreamingChangeEventSource implements StreamingChangeEventSource<CubridPartition, CubridOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CubridStreamingChangeEventSource.class);

    private final CubridConnectorConfig connectorConfig;
    private final EventDispatcher<CubridPartition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final CubridDatabaseSchema schema;

    public CubridStreamingChangeEventSource(CubridConnectorConfig connectorConfig,
                                            EventDispatcher<CubridPartition, TableId> dispatcher,
                                            ErrorHandler errorHandler, Clock clock, CubridDatabaseSchema schema) {
        this.connectorConfig = connectorConfig;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
    }

    @Override
    public void execute(ChangeEventSourceContext context, CubridPartition partition, CubridOffsetContext offsetContext) throws InterruptedException {
        final CubridLogClient client = new CubridLogClient();
        try {
            client.setAllInCond(true);
            client.connect(
                    connectorConfig.getJdbcConfig().getHostname(),
                    connectorConfig.getCdcPort(),
                    connectorConfig.getDatabaseName(),
                    connectorConfig.getJdbcConfig().getUser(),
                    connectorConfig.getJdbcConfig().getPassword());

            long cursor = resolveStartCursor(client, offsetContext);
            LOGGER.info("Starting CUBRID CDC stream at {}", CubridLogClient.lsaDisplay(cursor));

            while (context.isRunning()) {
                final CubridLogClient.ExtractBatch batch = client.extract(cursor);
                cursor = batch.lsaOut();

                for (RawLogItem item : batch.items()) {
                    if (item.type() == RawLogItem.ItemType.TIMER) {
                        // TODO(heartbeat): translate TIMER items into Debezium heartbeats.
                        continue;
                    }
                    // TODO(workspace#38): buffer per transaction and dispatch real change events.
                    LOGGER.info("raw item: {}", item.toDisplayString());
                    offsetContext.incrementSeq();
                    offsetContext.setTxId(item.transactionId());
                }
                offsetContext.setLsa(Lsa.fromRaw(cursor));
            }
        }
        catch (Exception e) {
            errorHandler.setProducerThrowable(e);
        }
        finally {
            try {
                client.finalizeClient();
            }
            catch (Exception e) {
                LOGGER.warn("Failed to finalize the cubrid_log client", e);
            }
        }
    }

    /**
     * Resumes from the persisted LSA when one exists, otherwise anchors on the current wall clock.
     * <p>
     * TODO(workspace#38): the true anchor semantics (what "resume exactly after the last emitted
     * event" means for a counter-based position) are still open.
     */
    private long resolveStartCursor(CubridLogClient client, CubridOffsetContext offsetContext) {
        final Lsa persisted = offsetContext.getLsa();
        if (persisted.isAvailable()) {
            return persisted.toRaw();
        }
        return client.findLsa(Instant.now().getEpochSecond());
    }
}
