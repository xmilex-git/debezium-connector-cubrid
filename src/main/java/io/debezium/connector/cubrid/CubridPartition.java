/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.AbstractPartition;
import io.debezium.util.Collect;

public class CubridPartition extends AbstractPartition implements Partition {

    private static final String PARTITION_KEY = "databaseName";

    public CubridPartition(String databaseName) {
        super(databaseName);
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collect.hashMapOf(PARTITION_KEY, databaseName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final CubridPartition other = (CubridPartition) obj;
        return Objects.equals(databaseName, other.databaseName);
    }

    @Override
    public int hashCode() {
        return databaseName.hashCode();
    }

    @Override
    public String toString() {
        return "CubridPartition [sourcePartition=" + getSourcePartition() + "]";
    }

    static class Provider implements Partition.Provider<CubridPartition> {

        private final CubridConnectorConfig connectorConfig;

        Provider(CubridConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Set<CubridPartition> getPartitions() {
            return Collections.singleton(new CubridPartition(connectorConfig.getLogicalName()));
        }
    }
}
