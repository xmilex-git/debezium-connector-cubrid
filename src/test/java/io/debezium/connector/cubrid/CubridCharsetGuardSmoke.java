/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import io.debezium.DebeziumException;
import io.debezium.jdbc.JdbcConfiguration;

/**
 * Manual smoke for the UTF-8-only startup guard (workspace#77): read the real codeset of a live
 * database over JDBC — the exact probe the connector task runs at startup — and assert the guard
 * verdict. Run once against a UTF-8 database expecting {@code pass} and once against a real
 * EUC-KR database expecting {@code refuse}.
 *
 * <pre>
 * java -cp ... io.debezium.connector.cubrid.CubridCharsetGuardSmoke \
 *      &lt;host&gt; &lt;jdbcPort&gt; &lt;dbname&gt; &lt;user&gt; &lt;password&gt; &lt;pass|refuse&gt;
 * </pre>
 *
 * Exits 0 when the guard verdict matches the expectation.
 */
public final class CubridCharsetGuardSmoke {

    private CubridCharsetGuardSmoke() {
    }

    public static void main(String[] args) throws Exception {
        final String host = args[0];
        final int port = Integer.parseInt(args[1]);
        final String dbname = args[2];
        final String user = args[3];
        final String password = args[4];
        final String expect = args[5];

        final JdbcConfiguration config = JdbcConfiguration.create()
                .with(JdbcConfiguration.HOSTNAME, host)
                .with(JdbcConfiguration.PORT, port)
                .with(JdbcConfiguration.DATABASE, dbname)
                .with(JdbcConfiguration.USER, user)
                .with(JdbcConfiguration.PASSWORD, password)
                .build();

        try (CubridConnection connection = new CubridConnection(config)) {
            final int charsetId = connection.readDatabaseCharsetId();
            System.out.println("CHARSET " + DatabaseCharsetGuard.charsetName(charsetId));
            String verdict;
            try {
                DatabaseCharsetGuard.check(charsetId);
                verdict = "pass";
                System.out.println("GUARD pass");
            }
            catch (DebeziumException e) {
                verdict = "refuse";
                System.out.println("GUARD refuse: " + e.getMessage());
            }
            System.exit(verdict.equals(expect) ? 0 : 1);
        }
    }
}
