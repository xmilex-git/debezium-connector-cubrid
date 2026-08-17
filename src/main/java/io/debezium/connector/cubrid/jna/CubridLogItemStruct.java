/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid.jna;

import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.Structure.FieldOrder;
import com.sun.jna.Union;

/**
 * JNA mirror of {@code CUBRID_LOG_ITEM} (cubrid_log.h) — a singly linked list node whose
 * payload is a union discriminated by {@code data_item_type}
 * (0=DDL, 1=DML, 2=DCL, 3=TIMER, 4=ROLLBACK_TO). After {@link #read()}, callers must select the union
 * arm with {@code data_item.setType(...)} + {@code data_item.read()} before touching it.
 */
@FieldOrder({ "transaction_id", "user", "data_item_type", "data_item", "next" })
public class CubridLogItemStruct extends Structure {

    public static final int TYPE_DDL = 0;
    public static final int TYPE_DML = 1;
    public static final int TYPE_DCL = 2;
    public static final int TYPE_TIMER = 3;
    public static final int TYPE_ROLLBACK_TO = 4;

    public int transaction_id;
    public Pointer user;
    public int data_item_type;
    public DataItemUnion data_item;
    public Pointer next;

    public CubridLogItemStruct(Pointer p) {
        super(p);
    }

    public static class DataItemUnion extends Union {
        public DdlStruct ddl;
        public DmlStruct dml;
        public DclStruct dcl;
        public TimerStruct timer;
        public RollbackToStruct rollback_to;
    }

    @FieldOrder({ "ddl_type", "object_type", "oid", "classoid", "statement", "statement_length" })
    public static class DdlStruct extends Structure {
        public int ddl_type;
        public int object_type;
        public long oid;
        public long classoid;
        public Pointer statement;
        public int statement_length;
    }

    @FieldOrder({ "dml_type", "rec_lsa", "classoid",
            "num_changed_column", "changed_column_index", "changed_column_data", "changed_column_data_len",
            "num_cond_column", "cond_column_index", "cond_column_data", "cond_column_data_len" })
    public static class DmlStruct extends Structure {
        public int dml_type;
        public long rec_lsa; /* orderable lsa key (pageid:48 | offset:16) of the source log record */
        public long classoid;
        public int num_changed_column;
        public Pointer changed_column_index; /* int[num_changed_column] */
        public Pointer changed_column_data; /* char*[num_changed_column] */
        public Pointer changed_column_data_len; /* int[num_changed_column] */
        public int num_cond_column;
        public Pointer cond_column_index;
        public Pointer cond_column_data;
        public Pointer cond_column_data_len;
    }

    @FieldOrder({ "dcl_type", "timestamp" })
    public static class DclStruct extends Structure {
        public int dcl_type;
        public long timestamp; /* time_t, 8 bytes on linux x86_64 */
    }

    @FieldOrder({ "timestamp" })
    public static class TimerStruct extends Structure {
        public long timestamp;
    }

    /** Partial rollback marker: buffered DML of the trid with {@code rec_lsa > lsa} was undone. */
    @FieldOrder({ "lsa" })
    public static class RollbackToStruct extends Structure {
        public long lsa;
    }
}
