/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid.jna;

/** A cubrid_log_* call returned a non-success code (see cubrid_log.h for the code table). */
public class CubridLogException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final int returnCode;

    public CubridLogException(String function, int returnCode) {
        super(function + " failed with rc=" + returnCode);
        this.returnCode = returnCode;
    }

    public int returnCode() {
        return returnCode;
    }
}
