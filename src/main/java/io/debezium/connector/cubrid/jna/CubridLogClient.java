/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid.jna;

import java.util.ArrayList;
import java.util.List;

import com.sun.jna.Pointer;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.LongByReference;
import com.sun.jna.ptr.PointerByReference;

import io.debezium.connector.cubrid.jna.RawLogItem.ColumnValue;

/**
 * Thin stateful wrapper over {@link CubridLogLibrary} exposing the
 * connect → find_lsa → extract-loop → finalize lifecycle with Java-owned copies of every
 * log item (native batches are freed before each call returns).
 *
 * <p>The underlying C API keeps a single process-global connection; thread safety of
 * libcubridcs is unverified (workspace#32), so callers must confine one instance to one
 * thread — which matches Debezium's single streaming-source thread.
 */
public class CubridLogClient {

    /** LSA is a flat uint64 whose raw value is NOT ordered: low 48 bits = pageid, high 16 = offset (ADR 0004). */
    public static long lsaPageId(long lsa) {
        return lsa & 0x0000FFFFFFFFFFFFL;
    }

    public static long lsaOffset(long lsa) {
        return lsa >>> 48;
    }

    public static String lsaDisplay(long lsa) {
        return String.format("0x%016x(page=%d,off=%d)", lsa, lsaPageId(lsa), lsaOffset(lsa));
    }

    /** One cubrid_log_extract() round: the advanced cursor plus Java-owned items. */
    public record ExtractBatch(long lsaIn, long lsaOut, int returnCode, List<RawLogItem> items) {
    }

    private final CubridLogLibrary lib;

    public CubridLogClient() {
        this(CubridLogLibrary.INSTANCE);
    }

    CubridLogClient(CubridLogLibrary lib) {
        this.lib = lib;
    }

    public void setConnectionTimeout(int seconds) {
        check("cubrid_log_set_connection_timeout", lib.cubrid_log_set_connection_timeout(seconds));
    }

    public void setExtractionTimeout(int seconds) {
        check("cubrid_log_set_extraction_timeout", lib.cubrid_log_set_extraction_timeout(seconds));
    }

    public void setMaxLogItem(int maxLogItem) {
        check("cubrid_log_set_max_log_item", lib.cubrid_log_set_max_log_item(maxLogItem));
    }

    /** all_in_cond=1 makes UPDATE/DELETE cond columns a full before-image (ADR 0003 requires it). */
    public void setAllInCond(boolean retrieveAll) {
        check("cubrid_log_set_all_in_cond", lib.cubrid_log_set_all_in_cond(retrieveAll ? 1 : 0));
    }

    public void connect(String host, int port, String dbname, String user, String password) {
        check("cubrid_log_connect_server", lib.cubrid_log_connect_server(host, port, dbname, user, password));
    }

    /**
     * Resolves the LSA at/after {@code startTimestampSeconds} (epoch seconds).
     * The server may adjust the timestamp; the resolved LSA is returned.
     */
    public long findLsa(long startTimestampSeconds) {
        LongByReference ts = new LongByReference(startTimestampSeconds);
        LongByReference lsa = new LongByReference(0);
        int rc = lib.cubrid_log_find_lsa(ts, lsa);
        if (rc != CubridLogLibrary.CUBRID_LOG_SUCCESS && rc != CubridLogLibrary.CUBRID_LOG_SUCCESS_WITH_ADJUSTED_LSA) {
            throw new CubridLogException("cubrid_log_find_lsa", rc);
        }
        return lsa.getValue();
    }

    /**
     * One extraction round from {@code lsaCursor}; the returned batch carries the advanced
     * cursor to feed into the next call. Blocks up to the extraction timeout when idle.
     */
    public ExtractBatch extract(long lsaCursor) {
        LongByReference lsa = new LongByReference(lsaCursor);
        PointerByReference listRef = new PointerByReference();
        IntByReference sizeRef = new IntByReference();

        int rc = lib.cubrid_log_extract(lsa, listRef, sizeRef);
        // -6 (EXTRACTION_TIMEOUT) is the normal idle outcome when extraction_timeout elapses
        // with nothing to read — a polling loop must treat it as an empty round, not a failure.
        if (rc != CubridLogLibrary.CUBRID_LOG_SUCCESS && rc != CubridLogLibrary.CUBRID_LOG_SUCCESS_WITH_NO_LOGITEM
                && rc != CubridLogLibrary.CUBRID_LOG_EXTRACTION_TIMEOUT) {
            throw new CubridLogException("cubrid_log_extract", rc);
        }

        Pointer head = listRef.getValue();
        List<RawLogItem> items = new ArrayList<>(Math.max(sizeRef.getValue(), 0));
        try {
            for (Pointer p = head; p != null; ) {
                CubridLogItemStruct node = new CubridLogItemStruct(p);
                node.read();
                items.add(copyItem(node));
                p = node.next;
            }
        }
        finally {
            if (head != null) {
                lib.cubrid_log_clear_log_item(head);
            }
        }
        return new ExtractBatch(lsaCursor, lsa.getValue(), rc, items);
    }

    public void finalizeClient() {
        check("cubrid_log_finalize", lib.cubrid_log_finalize());
    }

    private RawLogItem copyItem(CubridLogItemStruct node) {
        String user = node.user == null ? null : node.user.getString(0);
        RawLogItem.ItemType type = RawLogItem.ItemType.of(node.data_item_type);

        int ddlType = -1;
        int ddlObjectType = -1;
        String ddlStatement = null;
        RawLogItem.DmlType dmlType = RawLogItem.DmlType.UNKNOWN;
        long classoid = 0;
        List<ColumnValue> changed = List.of();
        List<ColumnValue> cond = List.of();
        RawLogItem.DclType dclType = RawLogItem.DclType.UNKNOWN;
        long timestamp = 0;
        long lsaKey = 0;

        switch (type) {
            case DDL -> {
                node.data_item.setType(CubridLogItemStruct.DdlStruct.class);
                node.data_item.read();
                CubridLogItemStruct.DdlStruct ddl = node.data_item.ddl;
                ddlType = ddl.ddl_type;
                ddlObjectType = ddl.object_type;
                classoid = ddl.classoid;
                ddlStatement = ddl.statement == null ? null : ddl.statement.getString(0);
            }
            case DML -> {
                node.data_item.setType(CubridLogItemStruct.DmlStruct.class);
                node.data_item.read();
                CubridLogItemStruct.DmlStruct dml = node.data_item.dml;
                dmlType = RawLogItem.DmlType.of(dml.dml_type);
                classoid = dml.classoid;
                lsaKey = dml.rec_lsa;
                changed = copyColumns(dml.num_changed_column, dml.changed_column_index, dml.changed_column_data,
                        dml.changed_column_data_len);
                cond = copyColumns(dml.num_cond_column, dml.cond_column_index, dml.cond_column_data,
                        dml.cond_column_data_len);
            }
            case DCL -> {
                node.data_item.setType(CubridLogItemStruct.DclStruct.class);
                node.data_item.read();
                dclType = RawLogItem.DclType.of(node.data_item.dcl.dcl_type);
                timestamp = node.data_item.dcl.timestamp;
            }
            case TIMER -> {
                node.data_item.setType(CubridLogItemStruct.TimerStruct.class);
                node.data_item.read();
                timestamp = node.data_item.timer.timestamp;
            }
            case ROLLBACK_TO -> {
                node.data_item.setType(CubridLogItemStruct.RollbackToStruct.class);
                node.data_item.read();
                lsaKey = node.data_item.rollback_to.lsa;
            }
            default -> {
            }
        }
        return new RawLogItem(node.transaction_id, user, type,
                ddlType, ddlObjectType, ddlStatement,
                dmlType, classoid, changed, cond,
                dclType, timestamp, lsaKey);
    }

    private static List<ColumnValue> copyColumns(int n, Pointer indexArr, Pointer dataArr, Pointer lenArr) {
        if (n <= 0) {
            return List.of();
        }
        int[] indexes = indexArr == null ? new int[n] : indexArr.getIntArray(0, n);
        int[] lens = lenArr == null ? new int[n] : lenArr.getIntArray(0, n);
        Pointer[] datas = dataArr == null ? new Pointer[n] : dataArr.getPointerArray(0, n);
        List<ColumnValue> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            byte[] bytes = datas[i] == null ? null : datas[i].getByteArray(0, Math.max(lens[i], 0));
            out.add(new ColumnValue(indexes[i], bytes));
        }
        return out;
    }

    private static void check(String func, int rc) {
        if (rc != CubridLogLibrary.CUBRID_LOG_SUCCESS) {
            throw new CubridLogException(func, rc);
        }
    }
}
