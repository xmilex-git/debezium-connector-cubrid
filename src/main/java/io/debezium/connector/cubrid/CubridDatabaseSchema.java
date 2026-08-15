/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Logical representation of the CUBRID schema.
 * <p>
 * Non-historized: the schema is read from the database on connector start rather than replayed from
 * a schema history topic, following the Postgres model.
 */
@NotThreadSafe
public class CubridDatabaseSchema extends RelationalDatabaseSchema {

    public CubridDatabaseSchema(CubridConnectorConfig connectorConfig, TopicNamingStrategy<TableId> topicNamingStrategy,
                                CubridValueConverters valueConverters) {
        super(
                connectorConfig,
                topicNamingStrategy,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                connectorConfig.getColumnFilter(),
                new TableSchemaBuilder(
                        valueConverters,
                        new CubridDefaultValueConverter(),
                        connectorConfig.schemaNameAdjuster(),
                        connectorConfig.customConverterRegistry(),
                        connectorConfig.getSourceInfoStructMaker().schema(),
                        connectorConfig.getFieldNamer(),
                        false),
                false,
                connectorConfig.getKeyMapper());
    }

    @Override
    public Tables tables() {
        return super.tables();
    }

    @Override
    public Tables.TableFilter getTableFilter() {
        return super.getTableFilter();
    }
}
