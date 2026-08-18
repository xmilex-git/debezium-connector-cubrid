/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.cubrid.log.CdcAuthorizationGate;
import io.debezium.connector.cubrid.log.CubridLogException;

/**
 * JDBC-backed implementation of the CDC authorization pass the C client ran in
 * {@code cubrid_log_db_login()} (ADR 0011 D1/D2/D7, workspace#68 → #72): the pure-Java
 * log client cannot run the engine's {@code au_} checks in-process, so the connector
 * reproduces the same semantics through its JDBC connection — which authenticates with
 * the same account the CDC session is opened as, and consults the same authorization
 * catalog. Enforcement stays client-side (ADR 0011 D11), exactly as before.
 */
public final class CubridCdcAuthorization {

    private static final Logger LOGGER = LoggerFactory.getLogger(CubridCdcAuthorization.class);

    /* engine error codes surfaced through the CUBRID JDBC driver */
    private static final int ER_LC_UNKNOWN_CLASSNAME = -64;
    private static final int ER_AU_AUTHORIZATION_FAILURE = -156;
    private static final int ER_AU_SELECT_FAILURE = -157;

    /** Outcome of the per-table SELECT probe. */
    enum Probe {
        OK,
        NO_PRIVILEGE,
        UNKNOWN_TABLE
    }

    private CubridCdcAuthorization() {
    }

    public static CdcAuthorizationGate gate(CubridConnection connection) {
        return (user, tableNames) -> decide(
                isDbaGroupMember(connection, user), user, tableNames,
                name -> probeSelect(connection, name),
                message -> LOGGER.warn("{}", message));
    }

    /**
     * The authorization decision, mirroring {@code cubrid_log_db_login()}: a DBA-group
     * member passes unconditionally; anyone else must name capture targets (empty list =
     * whole log = DBA-only) and hold SELECT on each; an unresolvable name is skipped with
     * a warning (the server skips it at session start too, ADR 0011 D3).
     */
    static void decide(boolean dbaGroupMember, String user, List<String> tableNames,
                       Function<String, Probe> probe, Consumer<String> warn) {
        if (dbaGroupMember) {
            return;
        }
        if (tableNames.isEmpty()) {
            throw new CubridLogException(
                    "CDC authorization failed: user " + user + " is not a member of the DBA group and no "
                            + "extraction table names are specified — a non-DBA CDC session must name its capture targets",
                    CubridLogException.NO_TABLE_PRIVILEGE);
        }
        for (String name : tableNames) {
            switch (probe.apply(name)) {
                case NO_PRIVILEGE -> throw new CubridLogException(
                        "CDC authorization failed: user " + user + " has no SELECT privilege on extraction table " + name,
                        CubridLogException.NO_TABLE_PRIVILEGE);
                case UNKNOWN_TABLE -> warn.accept(
                        "extraction table " + name + " does not resolve; privilege check skipped (the server will also skip it)");
                case OK -> {
                }
            }
        }
    }

    static boolean isDbaGroupMember(CubridConnection connection, String user) {
        // db_user.groups holds the transitive group closure (au_compute_groups), so one
        // membership row — or being DBA itself — matches au_is_dba_group_member()
        String query = "SELECT 1 FROM db_user u WHERE u.name = UPPER(?) AND u.name = 'DBA'"
                + " UNION ALL SELECT 1 FROM db_user u, TABLE(u.groups) t(g) WHERE u.name = UPPER(?) AND t.g = 'DBA'";
        try {
            return connection.prepareQueryAndMap(query, st -> {
                st.setString(1, user);
                st.setString(2, user);
            }, rs -> rs.next());
        }
        catch (SQLException e) {
            throw asLoginFailure("could not verify DBA group membership of user " + user, e);
        }
    }

    static Probe probeSelect(CubridConnection connection, String tableName) {
        // authorization-exact probe with no catalog dependency (ADR 0011: _db_class is
        // DBA-only); the identifier comes from the connector's include-list config
        try {
            return connection.prepareQueryAndMap("SELECT 1 FROM " + tableName + " WHERE 1 = 0", st -> {
            }, rs -> Probe.OK);
        }
        catch (SQLException e) {
            String message = e.getMessage() == null ? "" : e.getMessage();
            if (e.getErrorCode() == ER_LC_UNKNOWN_CLASSNAME || message.contains("Unknown class")) {
                return Probe.UNKNOWN_TABLE;
            }
            if (e.getErrorCode() == ER_AU_SELECT_FAILURE || e.getErrorCode() == ER_AU_AUTHORIZATION_FAILURE
                    || message.contains("is not authorized")) {
                return Probe.NO_PRIVILEGE;
            }
            throw asLoginFailure("SELECT probe on " + tableName + " failed", e);
        }
    }

    private static CubridLogException asLoginFailure(String detail, SQLException cause) {
        CubridLogException e = new CubridLogException("CDC authorization: " + detail, CubridLogException.FAILED_LOGIN);
        e.initCause(cause);
        return e;
    }
}
