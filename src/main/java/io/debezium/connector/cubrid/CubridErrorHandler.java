/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

/**
 * Error handler for the CUBRID connector.
 * <p>
 * TODO: classify retriable cubrid_log failures once the failure modes are known (post-POC).
 */
public class CubridErrorHandler extends ErrorHandler {

    public CubridErrorHandler(CubridConnectorConfig connectorConfig, ChangeEventQueue<?> queue, ErrorHandler replacedErrorHandler) {
        super(CubridConnector.class, connectorConfig, queue, replacedErrorHandler);
    }
}
