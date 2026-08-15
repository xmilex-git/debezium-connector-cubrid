/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import io.debezium.connector.common.CdcSourceTaskContext;

/**
 * A state (context) associated with a CUBRID task.
 */
public class CubridTaskContext extends CdcSourceTaskContext {

    public CubridTaskContext(CubridConnectorConfig config, CubridDatabaseSchema schema) {
        super(config, config.getCustomMetricTags(), schema::tableIds);
    }
}
