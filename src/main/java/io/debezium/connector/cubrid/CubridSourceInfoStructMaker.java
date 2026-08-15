/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfoStructMaker;

public class CubridSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    private Schema schema;

    @Override
    public void init(String connector, String version, CommonConnectorConfig connectorConfig) {
        super.init(connector, version, connectorConfig);
        schema = commonSchemaBuilder()
                .name("io.debezium.connector.cubrid.Source")
                .field(SourceInfo.SCHEMA_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.TABLE_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.LSN_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .field(SourceInfo.TX_ID_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.PAGE_ID_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .field(SourceInfo.LSA_OFFSET_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .field(SourceInfo.EPOCH_KEY, Schema.OPTIONAL_INT64_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        final Struct ret = super.commonStruct(sourceInfo);

        if (sourceInfo.getTableId() != null) {
            ret.put(SourceInfo.SCHEMA_NAME_KEY, sourceInfo.getTableId().schema());
            ret.put(SourceInfo.TABLE_NAME_KEY, sourceInfo.getTableId().table());
        }
        // 'lsn' carries the connector-local event counter, which is the ordering position (ADR 0004).
        ret.put(SourceInfo.LSN_KEY, sourceInfo.getSeq());
        ret.put(SourceInfo.PAGE_ID_KEY, sourceInfo.getPageId());
        ret.put(SourceInfo.LSA_OFFSET_KEY, sourceInfo.getLsaOffset());
        ret.put(SourceInfo.EPOCH_KEY, (long) sourceInfo.getEpoch());
        if (sourceInfo.getTxId() >= 0) {
            ret.put(SourceInfo.TX_ID_KEY, Integer.toString(sourceInfo.getTxId()));
        }
        return ret;
    }
}
