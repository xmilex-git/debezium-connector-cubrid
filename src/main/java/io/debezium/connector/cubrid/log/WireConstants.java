/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid.log;

/**
 * Wire-level constants of the CUBRID CSS protocol and the CDC log-extraction requests,
 * transcribed from the engine sources ({@code connection_defs.h}, {@code network.h},
 * {@code error_code.h}) and pinned by the captured wire fixtures under
 * {@code src/test/resources/wire/}.
 *
 * <p>The request opcodes are positions in the engine's {@code net_server_request} enum, so
 * they shift whenever a request is added upstream before the CDC block. That is accepted:
 * server and connector ship in lockstep (workspace#62 — no version negotiation by design),
 * and the wire fixture tests pin the values measured against the paired engine build.
 */
final class WireConstants {

    private WireConstants() {
    }

    /** First 8 bytes of the 32-byte magic packet ({@code css_Net_magic}). */
    static final byte[] NET_MAGIC = { 0x00, 0x00, 0x00, 0x01, 0x20, 0x08, 0x11, 0x22 };

    static final int NET_HEADER_SIZE = 32;

    /* packet types (connection_defs.h enum css_packet_type) */
    static final int COMMAND_TYPE = 1;
    static final int DATA_TYPE = 2;
    static final int ABORT_TYPE = 3;
    static final int CLOSE_TYPE = 4;
    static final int ERROR_TYPE = 5;

    /** Master connect command: attach as a data client of the named database. */
    static final int DATA_REQUEST = 2;

    /** Master/server connect reply ({@code enum css_status}). */
    static final int SERVER_CONNECTED = 0;

    /**
     * Every request header carries this flag: the C client connection is initialized with
     * {@code invalidate_snapshot = 1} and the flag simply echoes it (measured on the wire).
     */
    static final int FLAG_INVALIDATE_SNAPSHOT = 0x8000;

    static final int NULL_TRAN_INDEX = -1;

    /* CDC requests (net_server_request enum positions; NET_SERVER_REQUEST_START = 0) */
    static final int NET_SERVER_CDC_START_SESSION = 172;
    static final int NET_SERVER_CDC_FIND_LSA = 173;
    static final int NET_SERVER_CDC_GET_LOGINFO_METADATA = 174;
    static final int NET_SERVER_CDC_GET_LOGINFO = 175;
    static final int NET_SERVER_CDC_END_SESSION = 176;

    /* server reply codes (error_code.h) */
    static final int NO_ERROR = 0;
    static final int ER_CDC_EXTRACTION_TIMEOUT = -1287;
    static final int ER_CDC_INVALID_LOG_LSA = -1290;
    static final int ER_CDC_NOT_AVAILABLE = -1291;
    static final int ER_CDC_ADJUSTED_LSA = -1292;
}
