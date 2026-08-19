/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import io.debezium.DebeziumException;

/**
 * HA halt (ADR 0010): the connector detected that it is no longer attached to the same master
 * node it was streaming from — either the source node changed under its stored offset (path A:
 * the old anchor would be misinterpreted against the new node's independent LSA coordinate
 * space), or the node it reconnected to is not in a capturable HA state (path B: a demoted old
 * master would silently replay the applylogdb re-application stream). Deliberately <em>not</em>
 * a {@code RetriableException} — same stop semantics as {@link DdlHaltException}: Kafka Connect
 * must not auto-restart into the same corruption path; recovery is the documented resnapshot
 * procedure against the current master (ADR 0010 D3).
 */
public class HaHaltException extends DebeziumException {

    private static final long serialVersionUID = 1L;

    private HaHaltException(String message) {
        super(message);
    }

    /** Path A — the stored offset was written on a different node than the one connected now. */
    public static HaHaltException nodeMismatch(String storedIdentity, String liveIdentity) {
        return new HaHaltException("HA halt (ADR 0010 D2-1): the stored offset belongs to source node '" + storedIdentity
                + "' but the connector is now attached to '" + liveIdentity + "'. The anchor LSA is meaningless in the new node's"
                + " log coordinate space, so streaming stopped before any corrupt read. Recover with the resnapshot procedure"
                + " against the current master — see the CUBRID connector setup guide, section 'HA failover recovery'.");
    }

    /** An anchored offset without a stamped node identity — provenance cannot be verified. */
    public static HaHaltException unstampedAnchor() {
        return new HaHaltException("HA halt (ADR 0010 D2-1): the stored offset carries a position but no source node identity,"
                + " so it cannot be verified against the node the connector is attached to now. Offsets written by this"
                + " connector version always carry the identity — a node-less anchor comes from a pre-guard build or a"
                + " tampered offset, and resuming it could silently read another node's log. Recover with the resnapshot"
                + " procedure against the current master — see the CUBRID connector setup guide, section 'HA failover recovery'.");
    }

    /** Path B — the connected node is not in a capturable HA state (e.g. a demoted old master). */
    public static HaHaltException notCapturableState(String haServerState) {
        return new HaHaltException("HA halt (ADR 0010 D2-2): the connected node reports HA server state '" + haServerState
                + "' — only 'active' (HA master) or 'idle' (non-HA) nodes are capturable. A demoted or standby node would"
                + " silently emit its applylogdb re-application stream, so streaming stopped. Reconfigure the connector"
                + " against the current master and recover with the resnapshot procedure — see the CUBRID connector setup"
                + " guide, section 'HA failover recovery'.");
    }
}
