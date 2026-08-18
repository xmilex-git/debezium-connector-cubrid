/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid.log;

/** A cubrid_log_* call returned a non-success code (see cubrid_log.h for the code table). */
public class CubridLogException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /* cubrid_log.h return codes kept by the pure-Java client (ADR 0012) */
    public static final int SUCCESS = 0;
    public static final int SUCCESS_WITH_NO_LOGITEM = 1;
    public static final int SUCCESS_WITH_ADJUSTED_LSA = 2;
    public static final int FAILED_DISCONNECT = -3;
    public static final int INVALID_LSA = -5;
    public static final int EXTRACTION_TIMEOUT = -6;
    public static final int LSA_NOT_FOUND = -7;
    public static final int INVALID_OUT_PARAM = -8;
    public static final int INVALID_TIMESTAMP = -9;
    public static final int FAILED_CONNECT = -10;
    public static final int INVALID_PORT = -11;
    public static final int INVALID_HOST = -12;
    public static final int INVALID_DBNAME = -13;
    public static final int INVALID_USER = -16;
    public static final int INVALID_MAX_LOG_ITEM = -20;
    public static final int INVALID_EXTRACTION_TIMEOUT = -25;
    public static final int INVALID_CONNECTION_TIMEOUT = -26;
    public static final int INVALID_FUNC_CALL_STAGE = -27;
    public static final int FAILED_EXTRACT = -30;
    public static final int INVALID_PASSWORD = -32;
    public static final int FAILED_LOGIN = -33;
    public static final int UNAVAILABLE_CDC_SERVER = -34;
    public static final int INVALID_TABLE_NAME = -35;
    /** CDC authorization failure (ADR 0011 D7, workspace#68) — non-retriable. */
    public static final int NO_TABLE_PRIVILEGE = -37;

    private final int returnCode;

    public CubridLogException(String function, int returnCode) {
        super(function + " failed with rc=" + returnCode);
        this.returnCode = returnCode;
    }

    public int returnCode() {
        return returnCode;
    }
}
