/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import io.debezium.data.Envelope.Operation;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;

/**
 * Emits change data based on a single (or two in case of updates) CDC data row(s).
 */
public class CubridChangeRecordEmitter extends RelationalChangeRecordEmitter<CubridPartition> {

    private final Operation operation;
    private final Object[] before;
    private final Object[] after;

    public CubridChangeRecordEmitter(CubridPartition partition, CubridOffsetContext offsetContext, Operation operation,
                                     Object[] before, Object[] after, Clock clock, CubridConnectorConfig connectorConfig) {
        super(partition, offsetContext, clock, connectorConfig);

        this.operation = operation;
        this.before = before;
        this.after = after;
    }

    @Override
    public Operation getOperation() {
        return operation;
    }

    @Override
    protected Object[] getOldColumnValues() {
        return before;
    }

    @Override
    protected Object[] getNewColumnValues() {
        return after;
    }

    @Override
    protected void emitTruncateRecord(Receiver<CubridPartition> receiver, TableSchema tableSchema) throws InterruptedException {
        receiver.changeRecord(getPartition(), tableSchema, Operation.TRUNCATE, null,
                tableSchema.getEnvelopeSchema().truncate(getOffset().getSourceInfo(), getClock().currentTimeAsInstant()),
                getOffset(), null);
    }
}
