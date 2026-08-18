/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import java.util.Set;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The HA halt guard (ADR 0010 D2), pure logic so it is unit-testable without JDBC.
 * <p>
 * The node facts arrive in-band in the CDC START_SESSION reply (workspace#70; formerly the
 * DBA-only JDBC {@code SHOW LOG HEADER}): the live HA server state ('idle' on a non-HA server,
 * 'active'/'standby'/... on HA nodes) and the database creation time ({@code db_creation}).
 * The node identity stored in the offset is {@code <configured hostname>@<creation millis>}:
 * the hostname catches the standard failover procedure (operator repoints the connector at the
 * new master), the creation time additionally catches clusters whose nodes were created
 * independently via {@code createdb}. Known residual gap (documented in ADR 0010): a VIP/DNS
 * name that follows the master combined with a backup/restore-built slave (which preserves the
 * creation time) makes a node switch invisible to this identity — the setup guide mandates the
 * stop-and-resnapshot procedure for that topology.
 */
final class HaNodeGuard {

    private static final Logger LOGGER = LoggerFactory.getLogger(HaNodeGuard.class);

    /**
     * HA states a capture may run against: 'active' is the HA master, 'idle' is the initial
     * state a non-HA server stays in ({@code boot.h}; measured on a live 11.5 server).
     * Everything else — standby, to-be-*, maintenance, dead — fails fast (path B).
     */
    private static final Set<String> CAPTURABLE_STATES = Set.of("active", "idle");

    private HaNodeGuard() {
    }

    /** The node identity persisted in the offset under {@link SourceInfo#NODE_KEY}. */
    static String identity(String configuredHostname, long dbCreationMillis) {
        return configuredHostname + "@" + dbCreationMillis;
    }

    /**
     * Runs both guard axes against the node just connected to and returns the identity to stamp
     * into the offset. A {@code null} stored identity (fresh start, or an offset written before
     * this guard existed) passes and is stamped on the next offset flush.
     *
     * @throws HaHaltException on either violation (never retriable — ADR 0010 D3)
     */
    static String verifyAndStamp(String storedIdentity, String liveIdentity, String haServerState, Consumer<String> haltMetric) {
        assertCapturableState(haServerState, haltMetric);
        if (storedIdentity != null && !storedIdentity.equals(liveIdentity)) {
            final HaHaltException halt = HaHaltException.nodeMismatch(storedIdentity, liveIdentity);
            haltMetric.accept(halt.getMessage());
            throw halt;
        }
        if (storedIdentity == null) {
            LOGGER.info("Stamping source node identity '{}' into the offset (ADR 0010 D2)", liveIdentity);
        }
        return liveIdentity;
    }

    /** Path B alone — also used by the snapshot side, which has no stored identity to compare. */
    static void assertCapturableState(String haServerState, Consumer<String> haltMetric) {
        if (haServerState == null || !CAPTURABLE_STATES.contains(haServerState)) {
            final HaHaltException halt = HaHaltException.notCapturableState(haServerState);
            haltMetric.accept(halt.getMessage());
            throw halt;
        }
    }
}
