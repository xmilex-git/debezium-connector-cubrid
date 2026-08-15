/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import java.time.Instant;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.relational.TableId;

/**
 * Coordinates from the CUBRID transaction log that relate a streamed change to its source log
 * position. Maps to the {@code source} field of the change event {@code Envelope}.
 */
@NotThreadSafe
public class SourceInfo extends BaseSourceInfo {

    public static final String LSN_KEY = "lsn";
    public static final String PAGE_ID_KEY = "page_id";
    public static final String LSA_OFFSET_KEY = "lsa_offset";
    public static final String SEQ_KEY = "seq";
    public static final String EPOCH_KEY = "epoch";
    public static final String TX_ID_KEY = "tx_id";

    private long pageId = -1L;
    private long lsaOffset = -1L;
    private long seq;
    private int epoch;
    private int txId = -1;
    private Instant timestamp;
    private TableId tableId;

    private final CubridConnectorConfig config;

    public SourceInfo(CubridConnectorConfig config) {
        super(config);
        this.config = config;
    }

    public long getPageId() {
        return pageId;
    }

    public void setPageId(long pageId) {
        this.pageId = pageId;
    }

    public long getLsaOffset() {
        return lsaOffset;
    }

    public void setLsaOffset(long lsaOffset) {
        this.lsaOffset = lsaOffset;
    }

    public Lsa getLsa() {
        return new Lsa(pageId, lsaOffset);
    }

    public void setLsa(Lsa lsa) {
        this.pageId = lsa.pageId();
        this.lsaOffset = lsa.offset();
    }

    /**
     * @return the connector-local event counter which serves as the ordering position (ADR 0004)
     */
    public long getSeq() {
        return seq;
    }

    public void setSeq(long seq) {
        this.seq = seq;
    }

    public int getEpoch() {
        return epoch;
    }

    public void setEpoch(int epoch) {
        this.epoch = epoch;
    }

    public int getTxId() {
        return txId;
    }

    public void setTxId(int txId) {
        this.txId = txId;
    }

    /**
     * @param timestamp the time at which the transaction commit was executed
     */
    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public TableId getTableId() {
        return tableId;
    }

    public void setTableId(TableId tableId) {
        this.tableId = tableId;
    }

    @Override
    protected Instant timestamp() {
        return timestamp;
    }

    @Override
    protected String database() {
        return config.getDatabaseName();
    }

    @Override
    public String toString() {
        return "SourceInfo [" +
                "serverName=" + serverName() +
                ", timestamp=" + timestamp +
                ", db=" + database() +
                ", snapshot=" + snapshotRecord +
                ", pageId=" + pageId +
                ", lsaOffset=" + lsaOffset +
                ", seq=" + seq +
                ", epoch=" + epoch +
                ", txId=" + txId + "]";
    }
}
