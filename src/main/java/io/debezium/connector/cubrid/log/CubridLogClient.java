/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid.log;

import java.util.ArrayList;
import java.util.List;

/**
 * Pure-Java CUBRID CDC log-extraction client (ADR 0012): the
 * connect → find_lsa → extract-loop → finalize lifecycle of the engine's {@code cubrid_log}
 * C API, reimplemented over the CSS wire protocol with no native code. The public surface
 * is the frozen facade the JNA implementation exposed (ADR 0012 D2); the JNA path was
 * deleted after parity verification (D4) and survives only in git history.
 *
 * <p>Callers must confine one instance to one thread — which matches Debezium's single
 * streaming-source thread. Unlike the C library there is no process-global state: each
 * instance owns its connection.
 */
public class CubridLogClient {

    /** LSA is a flat uint64 whose raw value is NOT ordered: low 48 bits = pageid, high 16 = offset (ADR 0004). */
    public static long lsaPageId(long lsa) {
        return lsa & 0x0000FFFFFFFFFFFFL;
    }

    public static long lsaOffset(long lsa) {
        return lsa >>> 48;
    }

    public static String lsaDisplay(long lsa) {
        return String.format("0x%016x(page=%d,off=%d)", lsa, lsaPageId(lsa), lsaOffset(lsa));
    }

    /** One cubrid_log_extract() round: the advanced cursor plus the decoded log items. */
    public record ExtractBatch(long lsaIn, long lsaOut, int returnCode, List<RawLogItem> items) {
    }

    /**
     * Node facts of the server the CDC session is actually attached to, carried in-band in
     * the START_SESSION reply (workspace#70): the live HA server state and the database
     * creation time. They feed the HA halt guard (ADR 0010 D2) without the DBA-only JDBC
     * {@code SHOW LOG HEADER} — and, unlike a JDBC-side read, they describe the very server
     * the log stream comes from.
     */
    public record NodeFacts(String haServerState, long dbCreationSeconds) {
    }

    private static final long NULL_LSA = 0xFFFFFFFFFFFFFFFFL;

    private final CssConnection conn = new CssConnection();

    /* configuration (cubrid_log.c globals, per-instance here) */
    private int connectionTimeout = 300;
    private int extractionTimeout = 300;
    private int maxLogItem = 512;
    private boolean allInCond;
    private List<String> extractionTableNames = List.of();
    private CdcAuthorizationGate authorizationGate;

    private boolean connected;
    private long nextLsa = NULL_LSA;
    private NodeFacts nodeFacts;

    public void setConnectionTimeout(int seconds) {
        requireNotConnected();
        if (seconds < -1 || seconds > 360) {
            throw new CubridLogException("cubrid_log_set_connection_timeout", CubridLogException.INVALID_CONNECTION_TIMEOUT);
        }
        connectionTimeout = seconds;
    }

    public void setExtractionTimeout(int seconds) {
        requireNotConnected();
        if (seconds < -1 || seconds > 360) {
            throw new CubridLogException("cubrid_log_set_extraction_timeout", CubridLogException.INVALID_EXTRACTION_TIMEOUT);
        }
        extractionTimeout = seconds;
    }

    public void setMaxLogItem(int maxLogItem) {
        requireNotConnected();
        if (maxLogItem < 1 || maxLogItem > 1024) {
            throw new CubridLogException("cubrid_log_set_max_log_item", CubridLogException.INVALID_MAX_LOG_ITEM);
        }
        this.maxLogItem = maxLogItem;
    }

    /** all_in_cond=1 makes UPDATE/DELETE cond columns a full before-image (ADR 0003 requires it). */
    public void setAllInCond(boolean retrieveAll) {
        requireNotConnected();
        allInCond = retrieveAll;
    }

    /**
     * Name-based extraction targets ({@code owner.table}, ADR 0011 D3/D5): the server
     * resolves each name at session start, skipping (with a notification) names that do
     * not resolve. An empty list captures the whole log.
     */
    public void setExtractionTableNames(List<String> tableNames) {
        requireNotConnected();
        List<String> copy = new ArrayList<>(tableNames.size());
        for (String name : tableNames) {
            if (name == null) {
                throw new CubridLogException("cubrid_log_set_extraction_table_names", CubridLogException.INVALID_TABLE_NAME);
            }
            copy.add(name);
        }
        extractionTableNames = List.copyOf(copy);
    }

    /** CDC authorization check run inside {@link #connect} — see {@link CdcAuthorizationGate}. */
    public void setAuthorizationGate(CdcAuthorizationGate gate) {
        requireNotConnected();
        authorizationGate = gate;
    }

    public void connect(String host, int port, String dbname, String user, String password) {
        requireNotConnected();
        if (dbname == null) {
            throw new CubridLogException("cubrid_log_connect_server", CubridLogException.INVALID_DBNAME);
        }
        if (host == null) {
            throw new CubridLogException("cubrid_log_connect_server", CubridLogException.INVALID_HOST);
        }
        if (port <= 0 || port > 0xffff) {
            throw new CubridLogException("cubrid_log_connect_server", CubridLogException.INVALID_PORT);
        }
        if (user == null) {
            throw new CubridLogException("cubrid_log_connect_server", CubridLogException.INVALID_USER);
        }
        if (password == null) {
            throw new CubridLogException("cubrid_log_connect_server", CubridLogException.INVALID_PASSWORD);
        }
        // the C client runs its db_login() credential/authorization pass before the wire
        // session; the gate is that pass's Java home (workspace#68 → #72)
        if (authorizationGate != null) {
            authorizationGate.authorize(user, extractionTableNames);
        }
        try {
            conn.connect(host, port, dbname, connectionTimeout);
            sendConfigurations();
            connected = true;
        }
        finally {
            if (!connected) {
                conn.close();
            }
        }
    }

    /** NET_SERVER_CDC_START_SESSION — ships the configuration set the C client sends. */
    private void sendConfigurations() {
        OrWriter w = new OrWriter();
        w.writeInt(maxLogItem);
        w.writeInt(extractionTimeout);
        w.writeInt(allInCond ? 1 : 0);
        w.writeInt(0); // extraction users — not exposed by this facade (JNA parity)
        w.writeInt(0); // classoid-based extraction tables — superseded by names (ADR 0011 D5)
        w.writeInt(extractionTableNames.size());
        for (String name : extractionTableNames) {
            w.writeString(name);
        }
        byte[] reply = conn.request(WireConstants.NET_SERVER_CDC_START_SESSION, w.toByteArray(), connectionTimeout);
        nodeFacts = parseStartSessionReply(reply);
    }

    /**
     * A current server acknowledges START_SESSION with {@code error_code(0) + ha_server_state
     * + db_creation} (workspace#70). A bare 4-byte success reply is the pre-dictionary wire
     * format: that engine cannot serve the relation dictionary (ADR 0011 D4) or name-based
     * extraction (D3) either, so the client stops with an explicit version error instead of
     * proceeding into a session it cannot route (ADR 0011 D10 — no {@code _db_class}
     * fallback; server and connector ship in lockstep, #62).
     */
    static NodeFacts parseStartSessionReply(byte[] reply) {
        OrReader r = new OrReader(reply);
        int code = r.readInt();
        if (code == WireConstants.ER_CDC_NOT_AVAILABLE) {
            throw new CubridLogException("cubrid_log_connect_server: CDC unavailable — check the server's "
                    + "'supplemental_log' parameter", CubridLogException.UNAVAILABLE_CDC_SERVER);
        }
        if (code != WireConstants.NO_ERROR) {
            throw new CubridLogException("cubrid_log_connect_server: start session reply " + code,
                    CubridLogException.FAILED_CONNECT);
        }
        if (!r.hasRemaining()) {
            throw new CubridLogException("cubrid_log_connect_server: the server accepted the session but its "
                    + "reply carries no node facts — this engine predates the CDC relation dictionary "
                    + "(ADR 0011 D4) and cannot serve this connector. Server and connector must ship from "
                    + "the same release (ADR 0011 D10); upgrade the engine.",
                    CubridLogException.FAILED_CONNECT);
        }
        String state = r.readString();
        long dbCreation = r.readInt64();
        return new NodeFacts(state == null ? "" : state, dbCreation);
    }

    /** The node facts received at {@link #connect}; only available while connected. */
    public NodeFacts nodeFacts() {
        requireConnected();
        return nodeFacts;
    }

    /**
     * Resolves the LSA at/after {@code startTimestampSeconds} (epoch seconds).
     * The server may adjust the timestamp; the resolved LSA is returned.
     */
    public long findLsa(long startTimestampSeconds) {
        requireConnected();
        if (startTimestampSeconds < 0) {
            throw new CubridLogException("cubrid_log_find_lsa", CubridLogException.INVALID_TIMESTAMP);
        }
        if (nextLsa != NULL_LSA) {
            return nextLsa; // the C client resolves once and replays g_next_lsa afterwards
        }
        OrWriter w = new OrWriter();
        w.writeInt64(startTimestampSeconds);
        byte[] reply = conn.request(WireConstants.NET_SERVER_CDC_FIND_LSA, w.toByteArray(), extractionTimeout);
        OrReader r = new OrReader(reply);
        int code = r.readInt();
        if (code != WireConstants.NO_ERROR && code != WireConstants.ER_CDC_ADJUSTED_LSA) {
            throw new CubridLogException("cubrid_log_find_lsa: no LSA at timestamp " + startTimestampSeconds
                    + " (reply " + code + ")", CubridLogException.LSA_NOT_FOUND);
        }
        nextLsa = r.readLogLsaRaw();
        return nextLsa;
    }

    /**
     * One extraction round from {@code lsaCursor}; the returned batch carries the advanced
     * cursor to feed into the next call. Blocks up to the extraction timeout when idle —
     * an idle round surfaces as returnCode {@code -6} (EXTRACTION_TIMEOUT) with no items,
     * exactly like the C client.
     */
    public ExtractBatch extract(long lsaCursor) {
        requireConnected();
        nextLsa = lsaCursor;

        OrWriter w = new OrWriter();
        w.writeLogLsaRaw(lsaCursor);
        byte[] reply = conn.request(WireConstants.NET_SERVER_CDC_GET_LOGINFO_METADATA, w.toByteArray(), extractionTimeout);
        OrReader r = new OrReader(reply);
        int code = r.readInt();
        if (code == WireConstants.ER_CDC_EXTRACTION_TIMEOUT) {
            return new ExtractBatch(lsaCursor, lsaCursor, CubridLogException.EXTRACTION_TIMEOUT, List.of());
        }
        if (code == WireConstants.ER_CDC_INVALID_LOG_LSA) {
            throw new CubridLogException("cubrid_log_extract: invalid lsa " + lsaDisplay(lsaCursor),
                    CubridLogException.INVALID_LSA);
        }
        if (code != WireConstants.NO_ERROR) {
            throw new CubridLogException("cubrid_log_extract: metadata reply " + code, CubridLogException.FAILED_EXTRACT);
        }
        long advancedLsa = r.readLogLsaRaw();
        int numInfos = r.readInt();
        int totalLength = r.readInt();
        if (numInfos < 0 || totalLength < 0 || (numInfos > 0 && totalLength <= 0)) {
            throw new CubridLogException("cubrid_log_extract: invalid metadata num_infos=" + numInfos
                    + " total_length=" + totalLength, CubridLogException.FAILED_EXTRACT);
        }
        nextLsa = advancedLsa;
        if (numInfos == 0) {
            return new ExtractBatch(lsaCursor, advancedLsa, CubridLogException.SUCCESS_WITH_NO_LOGITEM, List.of());
        }

        byte[] infos = conn.request(WireConstants.NET_SERVER_CDC_GET_LOGINFO, null, extractionTimeout);
        if (infos.length != totalLength) {
            throw new CubridLogException("cubrid_log_extract: log info size " + infos.length
                    + " != announced " + totalLength, CubridLogException.FAILED_EXTRACT);
        }
        return new ExtractBatch(lsaCursor, advancedLsa, CubridLogException.SUCCESS, LogItemParser.parse(infos, numInfos));
    }

    public void finalizeClient() {
        requireConnected();
        try {
            byte[] reply = conn.request(WireConstants.NET_SERVER_CDC_END_SESSION, null, extractionTimeout);
            int code = new OrReader(reply).readInt();
            if (code != WireConstants.NO_ERROR) {
                throw new CubridLogException("cubrid_log_finalize: end session reply " + code,
                        CubridLogException.FAILED_DISCONNECT);
            }
        }
        finally {
            conn.close();
            connected = false;
            nextLsa = NULL_LSA;
            nodeFacts = null;
        }
    }

    private void requireConnected() {
        if (!connected) {
            throw new CubridLogException("cubrid_log stage: not connected", CubridLogException.INVALID_FUNC_CALL_STAGE);
        }
    }

    private void requireNotConnected() {
        if (connected) {
            throw new CubridLogException("cubrid_log stage: already connected", CubridLogException.INVALID_FUNC_CALL_STAGE);
        }
    }
}
