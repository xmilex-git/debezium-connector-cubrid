/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import io.debezium.schema.SchemaFactory;

public class CubridSchemaFactory extends SchemaFactory {

    private static final CubridSchemaFactory SCHEMA_FACTORY = new CubridSchemaFactory();

    public CubridSchemaFactory() {
        super();
    }

    public static CubridSchemaFactory get() {
        return SCHEMA_FACTORY;
    }
}
