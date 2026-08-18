/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid.log;

/**
 * Manual smoke (successor of the JNA-era CubridLogJnaSmoke, workspace#37 → #72): prove the
 * pure-Java wire client can drive the real connect → find_lsa → extract lifecycle against
 * a live cub_server and read actual log items — no native library, no CUBRID install.
 *
 * <pre>
 * java -cp ... io.debezium.connector.cubrid.log.CubridLogWireSmoke \
 *      [host] [port] [dbname] [user] [password] [lookbackSeconds] [minNonTimerItems]
 * </pre>
 *
 * Exits 0 once at least {@code minNonTimerItems} non-TIMER items were extracted.
 */
public final class CubridLogWireSmoke {

    private CubridLogWireSmoke() {
    }

    public static void main(String[] args) {
        String host = arg(args, 0, "localhost");
        int port = Integer.parseInt(arg(args, 1, "1523"));
        String dbname = arg(args, 2, "htapdb");
        String user = arg(args, 3, "dba");
        String password = arg(args, 4, "");
        long lookbackSeconds = Long.parseLong(arg(args, 5, "60"));
        int minNonTimerItems = Integer.parseInt(arg(args, 6, "1"));

        CubridLogClient client = new CubridLogClient();
        client.setExtractionTimeout(3);
        client.setAllInCond(true);
        client.connect(host, port, dbname, user, password);
        System.out.println("CONNECT ok");
        System.out.println("NODE_FACTS ha_state=" + client.nodeFacts().haServerState()
                + " db_creation=" + client.nodeFacts().dbCreationSeconds());

        long cursor = client.findLsa(System.currentTimeMillis() / 1000 - lookbackSeconds);
        System.out.println("FIND_LSA " + CubridLogClient.lsaDisplay(cursor));

        int nonTimer = 0;
        for (int round = 0; round < 30 && nonTimer < minNonTimerItems; round++) {
            CubridLogClient.ExtractBatch batch = client.extract(cursor);
            cursor = batch.lsaOut();
            for (RawLogItem item : batch.items()) {
                if (item.type() != RawLogItem.ItemType.TIMER) {
                    nonTimer++;
                    System.out.println(item.toDisplayString());
                }
            }
            System.out.println("EXTRACT rc=" + batch.returnCode() + " n=" + batch.items().size()
                    + " -> " + CubridLogClient.lsaDisplay(cursor));
        }
        client.finalizeClient();
        System.out.println("FINALIZE ok, nonTimer=" + nonTimer);
        System.exit(nonTimer >= minNonTimerItems ? 0 : 1);
    }

    private static String arg(String[] args, int i, String dflt) {
        return args.length > i ? args[i] : dflt;
    }
}
