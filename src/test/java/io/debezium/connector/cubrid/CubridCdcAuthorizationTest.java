/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.connector.cubrid.CubridCdcAuthorization.Probe;
import io.debezium.connector.cubrid.log.CubridLogException;

/**
 * The CDC authorization decision (ADR 0011 D1/D2/D7) ported from the C client's
 * cubrid_log_db_login() — scenarios mirror workspace#68's measured cases A–E.
 */
class CubridCdcAuthorizationTest {

    private final List<String> warnings = new ArrayList<>();

    private void decide(boolean dba, List<String> names, Map<String, Probe> probes) {
        CubridCdcAuthorization.decide(dba, "cdcpriv", names, probes::get, warnings::add);
    }

    @Test
    void grantedTablesPass() { // #68 case A
        assertDoesNotThrow(() -> decide(false, List.of("dba.t_order"), Map.of("dba.t_order", Probe.OK)));
        assertTrue(warnings.isEmpty());
    }

    @Test
    void oneUngrantedTableFailsNonRetriablyNamingTheTable() { // #68 case B
        CubridLogException e = assertThrows(CubridLogException.class,
                () -> decide(false, List.of("dba.t_order", "dba.t_item"),
                        Map.of("dba.t_order", Probe.OK, "dba.t_item", Probe.NO_PRIVILEGE)));
        assertEquals(CubridLogException.NO_TABLE_PRIVILEGE, e.returnCode());
        assertTrue(e.getMessage().contains("dba.t_item"));
    }

    @Test
    void nonDbaWithoutNamesIsRejected() { // #68 case C
        CubridLogException e = assertThrows(CubridLogException.class,
                () -> decide(false, List.of(), Map.of()));
        assertEquals(CubridLogException.NO_TABLE_PRIVILEGE, e.returnCode());
    }

    @Test
    void dbaPassesUnconditionally() { // #68 case D
        assertDoesNotThrow(() -> decide(true, List.of(), Map.of()));
    }

    @Test
    void unresolvableNameIsSkippedWithAWarning() { // #68 case E
        assertDoesNotThrow(() -> decide(false, List.of("dba.no_such_table", "dba.t_order"),
                Map.of("dba.no_such_table", Probe.UNKNOWN_TABLE, "dba.t_order", Probe.OK)));
        assertEquals(1, warnings.size());
        assertTrue(warnings.get(0).contains("dba.no_such_table"));
    }
}
