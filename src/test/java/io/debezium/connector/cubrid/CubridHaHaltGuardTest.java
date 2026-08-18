/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

/**
 * HA halt guard (ADR 0010 D2) — reproduction of the two silent-corruption paths as pure-logic
 * scenarios, plus the offset round trip of the stamped node identity. Path A (reconnecting to a
 * different node with an old anchor) and path B (reconnecting to a demoted old master) must both
 * fail fast; a plain restart against the same healthy master must pass untouched.
 */
class CubridHaHaltGuardTest {

    private static final String NODE_A = HaNodeGuard.identity("ha-node-1", 1755410328000L);
    private static final String NODE_B = HaNodeGuard.identity("ha-node-2", 1755410328000L);

    private final List<String> haltReasons = new ArrayList<>();

    private static CubridConnectorConfig config() {
        return new CubridConnectorConfig(Configuration.create()
                .with("topic.prefix", "htapcdc")
                .with("database.dbname", "htapdb")
                .build());
    }

    // -- normal paths -------------------------------------------------------------------------

    @Test
    void freshStartOnActiveMasterStampsIdentity() {
        assertEquals(NODE_A, HaNodeGuard.verifyAndStamp(null, NODE_A, "active", haltReasons::add));
        assertTrue(haltReasons.isEmpty());
    }

    @Test
    void freshStartOnNonHaIdleServerStampsIdentity() {
        // measured on a live non-HA 11.5 server: the disk log header reports 'idle'
        assertEquals(NODE_A, HaNodeGuard.verifyAndStamp(null, NODE_A, "idle", haltReasons::add));
        assertTrue(haltReasons.isEmpty());
    }

    @Test
    void restartAgainstSameMasterPasses() {
        assertEquals(NODE_A, HaNodeGuard.verifyAndStamp(NODE_A, NODE_A, "active", haltReasons::add));
        assertTrue(haltReasons.isEmpty());
    }

    // -- path A: reconnect to a different node (new master) -----------------------------------

    @Test
    void reconnectToNewMasterHaltsOnNodeMismatch() {
        final HaHaltException halt = assertThrows(HaHaltException.class,
                () -> HaNodeGuard.verifyAndStamp(NODE_A, NODE_B, "active", haltReasons::add));
        assertTrue(halt.getMessage().contains("resnapshot"), "recovery must point at the resnapshot procedure");
        assertTrue(halt.getMessage().contains(NODE_A) && halt.getMessage().contains(NODE_B));
        assertEquals(1, haltReasons.size());
    }

    @Test
    void createdbBuiltClusterDiffersByCreationTimeEvenBehindSameHostname() {
        // a VIP that follows the master keeps the hostname stable; independently created
        // databases still differ by creation time, so the identity axis catches the switch
        final String behindVipOld = HaNodeGuard.identity("ha-vip", 1755410328000L);
        final String behindVipNew = HaNodeGuard.identity("ha-vip", 1755499999000L);
        assertThrows(HaHaltException.class,
                () -> HaNodeGuard.verifyAndStamp(behindVipOld, behindVipNew, "active", haltReasons::add));
    }

    // -- path B: reconnect to a node that is not a capturable master --------------------------

    @Test
    void demotedOldMasterHaltsOnStandbyState() {
        // same node, anchor replays fine — exactly the silent applylogdb re-emission path
        final HaHaltException halt = assertThrows(HaHaltException.class,
                () -> HaNodeGuard.verifyAndStamp(NODE_A, NODE_A, "standby", haltReasons::add));
        assertTrue(halt.getMessage().contains("standby"));
        assertEquals(1, haltReasons.size());
    }

    @Test
    void transitionalAndDegenerateStatesAllHalt() {
        for (String state : new String[]{ "to-be-active", "to-be-standby", "maintenance", "dead", null }) {
            assertThrows(HaHaltException.class,
                    () -> HaNodeGuard.verifyAndStamp(NODE_A, NODE_A, state, haltReasons::add));
        }
        assertEquals(5, haltReasons.size());
    }

    @Test
    void snapshotSideStateAxisRejectsStandby() {
        assertThrows(HaHaltException.class, () -> HaNodeGuard.assertCapturableState("standby", haltReasons::add));
        HaNodeGuard.assertCapturableState("active", haltReasons::add);
        HaNodeGuard.assertCapturableState("idle", haltReasons::add);
        assertEquals(1, haltReasons.size());
    }

    // -- offset round trip --------------------------------------------------------------------

    @Test
    void stampedNodeIdentitySurvivesOffsetStoreAndLoad() {
        final CubridConnectorConfig config = config();
        final CubridOffsetContext offset = new CubridOffsetContext(config, new Lsa(3, 7), 42L, 0, false, false);
        offset.setSourceNode(NODE_A);

        final Map<String, ?> stored = offset.getOffset();
        assertEquals(NODE_A, stored.get(SourceInfo.NODE_KEY));

        final CubridOffsetContext reloaded = new CubridOffsetContext.Loader(config).load(stored);
        assertEquals(NODE_A, reloaded.getSourceNode());
        assertEquals(new Lsa(3, 7), reloaded.getAnchorLsa());
        assertEquals(42L, reloaded.getAnchorSeq());
    }

    @Test
    void legacyOffsetWithoutNodeKeyLoadsAsUnstamped() {
        final CubridOffsetContext offset = new CubridOffsetContext(config(), new Lsa(3, 7), 42L, 0, false, false);
        final Map<String, ?> legacy = offset.getOffset();
        assertTrue(!legacy.containsKey(SourceInfo.NODE_KEY));

        final CubridOffsetContext reloaded = new CubridOffsetContext.Loader(config()).load(legacy);
        assertNull(reloaded.getSourceNode());
        // ...and the guard then stamps instead of halting (upgrade path)
        assertEquals(NODE_A, HaNodeGuard.verifyAndStamp(reloaded.getSourceNode(), NODE_A, "active", haltReasons::add));
        assertTrue(haltReasons.isEmpty());
    }
}
