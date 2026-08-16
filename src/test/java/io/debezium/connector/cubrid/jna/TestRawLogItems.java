/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid.jna;

import java.util.List;

/**
 * Test-only factories for {@link RawLogItem}, whose constructor is package-private because
 * production instances are only ever copied out of native {@code CUBRID_LOG_ITEM} memory.
 */
public final class TestRawLogItems {

    private TestRawLogItems() {
    }

    public static RawLogItem insert(int trid, long classoid) {
        return new RawLogItem(trid, "dba", RawLogItem.ItemType.DML, 0, 0, null,
                RawLogItem.DmlType.INSERT, classoid, List.of(), List.of(),
                RawLogItem.DclType.UNKNOWN, 0);
    }

    public static RawLogItem commit(int trid) {
        return new RawLogItem(trid, "dba", RawLogItem.ItemType.DCL, 0, 0, null,
                RawLogItem.DmlType.UNKNOWN, 0, List.of(), List.of(),
                RawLogItem.DclType.COMMIT, 1_700_000_000L);
    }

    public static RawLogItem abort(int trid) {
        return new RawLogItem(trid, "dba", RawLogItem.ItemType.DCL, 0, 0, null,
                RawLogItem.DmlType.UNKNOWN, 0, List.of(), List.of(),
                RawLogItem.DclType.ABORT, 1_700_000_000L);
    }

    public static RawLogItem timer() {
        return new RawLogItem(-1, null, RawLogItem.ItemType.TIMER, 0, 0, null,
                RawLogItem.DmlType.UNKNOWN, 0, List.of(), List.of(),
                RawLogItem.DclType.UNKNOWN, 1_700_000_000L);
    }
}
