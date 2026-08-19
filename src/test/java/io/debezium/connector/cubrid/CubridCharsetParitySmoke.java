/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import io.debezium.connector.cubrid.log.CubridLogClient;
import io.debezium.connector.cubrid.log.RawLogItem;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * Manual smoke for Korean-data snapshot/streaming parity on a UTF-8 database (workspace#77):
 * insert rows with Korean strings over JDBC, read them back over JDBC (the snapshot path), and
 * decode the same rows from the supplemental log ({@code CubridLogValueDecoder}, the streaming
 * path). All three views — the literal, the JDBC read, the log decode — must be identical.
 *
 * <pre>
 * java -cp ... io.debezium.connector.cubrid.CubridCharsetParitySmoke \
 *      &lt;host&gt; &lt;jdbcPort&gt; &lt;cdcPort&gt; &lt;dbname&gt; &lt;user&gt; &lt;password&gt;
 * </pre>
 *
 * Requires {@code supplemental_log=1} and a user allowed to create a table in its own schema.
 * Exits 0 on full parity.
 */
public final class CubridCharsetParitySmoke {

    private static final String TABLE = "charset_parity";
    private static final List<String> KOREAN = List.of(
            "한글 데이터", "낱말·문장, 그리고 UTF-8 밖 아님: 판교테크노밸리", "emoji 아님 — 옛한글 ᄒᆞᆫ글");

    private CubridCharsetParitySmoke() {
    }

    public static void main(String[] args) throws Exception {
        final String host = args[0];
        final int jdbcPort = Integer.parseInt(args[1]);
        final int cdcPort = Integer.parseInt(args[2]);
        final String dbname = args[3];
        final String user = args[4];
        final String password = args[5];

        final JdbcConfiguration config = JdbcConfiguration.create()
                .with(JdbcConfiguration.HOSTNAME, host)
                .with(JdbcConfiguration.PORT, jdbcPort)
                .with(JdbcConfiguration.DATABASE, dbname)
                .with(JdbcConfiguration.USER, user)
                .with(JdbcConfiguration.PASSWORD, password)
                .build();

        try (CubridConnection connection = new CubridConnection(config)) {
            DatabaseCharsetGuard.check(connection.readDatabaseCharsetId());

            final long startSeconds = System.currentTimeMillis() / 1000 - 5;
            prepareTable(connection);
            final TableId tableId = new TableId(null, user.toLowerCase(), TABLE);
            final Table table = connection.readTable(tableId).orElseThrow();

            // Snapshot path: JDBC read-back.
            final List<String> jdbcValues = new ArrayList<>();
            connection.query("SELECT s FROM \"" + user.toLowerCase() + "\".\"" + TABLE + "\" ORDER BY id",
                    rs -> collect(rs, jdbcValues));
            check("snapshot(JDBC)", jdbcValues);

            // Streaming path: decode the INSERTs from the supplemental log.
            final List<String> logValues = extractLogValues(host, cdcPort, dbname, user, password, table, startSeconds);
            check("streaming(log)", logValues);

            System.out.println("PARITY ok: literal == snapshot == streaming (" + KOREAN.size() + " rows)");
            System.exit(0);
        }
    }

    private static void prepareTable(CubridConnection connection) throws SQLException {
        try (Statement st = connection.connection().createStatement()) {
            try {
                st.execute("DROP TABLE " + TABLE);
            }
            catch (SQLException ignored) {
            }
            st.execute("CREATE TABLE " + TABLE + " (id INTEGER PRIMARY KEY, s STRING)");
            for (int i = 0; i < KOREAN.size(); i++) {
                st.execute("INSERT INTO " + TABLE + " VALUES (" + i + ", '" + KOREAN.get(i) + "')");
            }
        }
        if (!connection.connection().getAutoCommit()) {
            connection.connection().commit();
        }
    }

    private static void collect(ResultSet rs, List<String> into) throws SQLException {
        while (rs.next()) {
            into.add(rs.getString(1));
        }
    }

    private static List<String> extractLogValues(String host, int cdcPort, String dbname, String user,
                                                 String password, Table table, long startSeconds) {
        final CubridLogClient client = new CubridLogClient();
        client.setExtractionTimeout(3);
        client.setAllInCond(true);
        client.setExtractionTableNames(List.of(user.toLowerCase() + "." + TABLE));
        client.connect(host, cdcPort, dbname, user, password);
        try {
            final String[] byId = new String[KOREAN.size()];
            int found = 0;
            long cursor = client.findLsa(startSeconds);
            for (int round = 0; round < 30 && found < KOREAN.size(); round++) {
                final CubridLogClient.ExtractBatch batch = client.extract(cursor);
                cursor = batch.lsaOut();
                for (RawLogItem item : batch.items()) {
                    if (item.type() != RawLogItem.ItemType.DML || item.dmlType() != RawLogItem.DmlType.INSERT) {
                        continue;
                    }
                    final Object[] row = CubridLogValueDecoder.toRow(table, item.changedColumns());
                    final int id = ((Number) row[0]).intValue();
                    if (id >= 0 && id < byId.length && byId[id] == null) {
                        byId[id] = (String) row[1];
                        found++;
                    }
                }
            }
            return Arrays.asList(byId);
        }
        finally {
            client.finalizeClient();
        }
    }

    private static void check(String path, List<String> actual) {
        for (int i = 0; i < KOREAN.size(); i++) {
            final String expected = KOREAN.get(i);
            final String got = i < actual.size() ? actual.get(i) : null;
            if (!Objects.equals(expected, got)) {
                System.out.println("PARITY FAIL [" + path + "] row " + i
                        + " expected <" + expected + "> got <" + got + ">");
                System.exit(1);
            }
            System.out.println("PARITY [" + path + "] row " + i + " ok: " + got);
        }
    }
}
