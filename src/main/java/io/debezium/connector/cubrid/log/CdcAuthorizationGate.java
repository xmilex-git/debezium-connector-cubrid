/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid.log;

import java.util.List;

/**
 * CDC authorization check run once inside {@link CubridLogClient#connect} before the wire
 * session is opened — the Java-side home of the C client's {@code cubrid_log_db_login()}
 * semantics (ADR 0011 D1/D2/D7, workspace#68):
 *
 * <ul>
 * <li>a DBA-group member may capture anything;</li>
 * <li>any other account must name its capture targets and hold SELECT on each — an empty
 * list means the whole log and stays DBA-only;</li>
 * <li>violations throw {@link CubridLogException} with
 * {@link CubridLogException#NO_TABLE_PRIVILEGE} (non-retriable) naming the table;</li>
 * <li>an unresolvable name is skipped with a warning (the server skips it too, ADR 0011 D3).</li>
 * </ul>
 *
 * <p>The CDC wire session itself carries no user identity, so like every CUBRID object
 * privilege this is client-side enforcement (ADR 0011 D11) checked once at login: a
 * REVOKE takes effect at the next reconnect. The pure-Java client cannot run the engine's
 * {@code au_} checks in-process; the connector supplies an implementation backed by its
 * JDBC connection, which authenticates with the same account and queries the same
 * authorization catalog.
 */
@FunctionalInterface
public interface CdcAuthorizationGate {

    /**
     * @param user the account the CDC session is opened as
     * @param extractionTableNames the configured {@code owner.table} capture targets
     *            (empty = whole log)
     * @throws CubridLogException {@link CubridLogException#NO_TABLE_PRIVILEGE} on an
     *             authorization failure, {@link CubridLogException#FAILED_LOGIN} when the
     *             account itself cannot be verified
     */
    void authorize(String user, List<String> extractionTableNames);
}
