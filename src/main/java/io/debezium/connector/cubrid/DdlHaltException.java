/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import io.debezium.DebeziumException;
import io.debezium.connector.cubrid.log.RawLogItem;

/**
 * DDL halt (ADR 0008, amended by workspace#82 D3): a schema-changing TABLE DDL reached the
 * connector, so the streaming task fails fast instead of silently misdecoding rows against a
 * stale schema. Deliberately <em>not</em> a {@code RetriableException} (D5) — Kafka Connect must
 * not auto-restart into the same DDL forever; the restart anchor never passes the DDL (D4), so an
 * unassisted restart halts deterministically at the same position until the documented resnapshot
 * procedure is run.
 */
public class DdlHaltException extends DebeziumException {

    private static final long serialVersionUID = 1L;

    public DdlHaltException(String tableLabel, RawLogItem.DdlType ddlType, String statement) {
        super("DDL halt (ADR 0008): " + ddlType + " detected on captured table '" + tableLabel
                + "': [" + (statement == null ? "<no statement text>" : statement.strip()) + "]. "
                + "Streaming stopped to prevent silent misdecoding; a plain restart halts at this DDL again. "
                + "Fix 'table.include.list' if the schema changed, then run the resnapshot procedure — "
                + "see the CUBRID connector setup guide, section 'DDL halt recovery'.");
    }
}
