/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import java.util.Optional;

import io.debezium.relational.Column;
import io.debezium.relational.DefaultValueConverter;

/**
 * Converter for table column default values.
 * <p>
 * TODO(workspace#39): column default values are not resolved during the POC snapshot.
 */
public class CubridDefaultValueConverter implements DefaultValueConverter {

    @Override
    public Optional<Object> parseDefaultValue(Column column, String defaultValue) {
        return Optional.empty();
    }
}
