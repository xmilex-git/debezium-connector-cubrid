/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.relational.TableId;

/**
 * Pre-wiring (a) of ADR 0009 D4: the offset serialization must round-trip the core
 * {@link AbstractIncrementalSnapshotContext} keys, so a post-1.0 incremental snapshot can pick
 * up a stored offset without an offset-format migration. workspace#65.
 */
class CubridOffsetIncrementalContextRoundTripTest {

    /** The core serialization key of the in-progress data-collection queue (private in core). */
    private static final String COLLECTIONS_KEY = AbstractIncrementalSnapshotContext.INCREMENTAL_SNAPSHOT_KEY + "_collections";

    private static CubridConnectorConfig config() {
        return new CubridConnectorConfig(Configuration.create()
                .with("topic.prefix", "htapcdc")
                .with("database.dbname", "htapdb")
                .with("table.include.list", "dba.t_order,dba.t_item")
                .build());
    }

    @Test
    void incrementalSnapshotContextKeysSurviveStoreAndLoad() {
        final CubridConnectorConfig config = config();
        final CubridOffsetContext offset = new CubridOffsetContext(config, new Lsa(3, 7), 42L, 0, false, false);

        @SuppressWarnings("unchecked")
        final IncrementalSnapshotContext<TableId> incremental = (IncrementalSnapshotContext<TableId>) offset.getIncrementalSnapshotContext();
        incremental.addDataCollectionNamesToSnapshot("corr-1", List.of("dba.t_order"), List.of(), "");
        assertTrue(incremental.snapshotRunning());

        final Map<String, ?> stored = offset.getOffset();
        assertTrue(stored.containsKey(COLLECTIONS_KEY), "stored offset must carry the core incremental-snapshot key");

        final CubridOffsetContext reloaded = new CubridOffsetContext.Loader(config).load(stored);
        assertEquals(new Lsa(3, 7), reloaded.getAnchorLsa());
        assertEquals(42L, reloaded.getAnchorSeq());
        assertTrue(reloaded.getIncrementalSnapshotContext().snapshotRunning());
        assertEquals(1, reloaded.getIncrementalSnapshotContext().dataCollectionsToBeSnapshottedCount());

        // storing again yields the same incremental-snapshot payload — no lossy round trip
        assertEquals(stored.get(COLLECTIONS_KEY), reloaded.getOffset().get(COLLECTIONS_KEY));
    }

    @Test
    void plainStreamingOffsetStaysFlatWithoutIncrementalKeys() {
        final CubridOffsetContext offset = new CubridOffsetContext(config(), new Lsa(3, 7), 42L, 0, false, false);

        // no incremental snapshot in progress: exactly the four flat ADR 0004 keys
        assertEquals(
                Set.of(SourceInfo.PAGE_ID_KEY, SourceInfo.LSA_OFFSET_KEY, SourceInfo.SEQ_KEY, SourceInfo.EPOCH_KEY),
                offset.getOffset().keySet());
    }
}
