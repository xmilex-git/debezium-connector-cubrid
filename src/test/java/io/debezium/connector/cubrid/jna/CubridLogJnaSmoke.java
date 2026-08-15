/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid.jna;

/**
 * Manual smoke for workspace#37: prove the JNA binding can drive the real
 * connect → find_lsa → extract lifecycle against a live cub_server and read actual
 * log items. Run with the .so on jna.library.path (or LD_LIBRARY_PATH):
 *
 * <pre>
 * java -Djna.library.path=$CUBRID/lib -cp ... io.debezium.connector.cubrid.jna.CubridLogJnaSmoke \
 *      [host] [port] [dbname] [user] [password] [lookbackSeconds] [minNonTimerItems]
 * </pre>
 *
 * Exits 0 once at least {@code minNonTimerItems} non-TIMER items were extracted.
 */
public final class CubridLogJnaSmoke {

    private CubridLogJnaSmoke() {
    }

    public static void main(String[] args) {
        String host = arg(args, 0, "localhost");
        int port = Integer.parseInt(arg(args, 1, "1523"));
        String dbname = arg(args, 2, "htapdb");
        String user = arg(args, 3, "dba");
        String password = arg(args, 4, "");
        long lookback = Long.parseLong(arg(args, 5, "600"));
        int minNonTimer = Integer.parseInt(arg(args, 6, "5"));
        int maxRounds = 120;

        CubridLogClient client = new CubridLogClient();
        client.setExtractionTimeout(2);
        client.setAllInCond(true);

        System.out.printf("SMOKE connect host=%s port=%d db=%s user=%s%n", host, port, dbname, user);
        client.connect(host, port, dbname, user, password);
        System.out.println("SMOKE connect OK");

        long startTs = System.currentTimeMillis() / 1000 - lookback;
        long lsa = client.findLsa(startTs);
        System.out.printf("SMOKE find_lsa start_ts=%d -> lsa=%s%n", startTs, CubridLogClient.lsaDisplay(lsa));

        int nonTimer = 0;
        int total = 0;
        int rc = 1;
        try {
            for (int round = 1; round <= maxRounds && nonTimer < minNonTimer; round++) {
                CubridLogClient.ExtractBatch batch = client.extract(lsa);
                lsa = batch.lsaOut();
                System.out.printf("EXTRACT round=%d rc=%d n=%d in=%s -> out=%s%n", round, batch.returnCode(),
                        batch.items().size(), CubridLogClient.lsaDisplay(batch.lsaIn()),
                        CubridLogClient.lsaDisplay(batch.lsaOut()));
                for (RawLogItem item : batch.items()) {
                    total++;
                    if (item.type() != RawLogItem.ItemType.TIMER) {
                        nonTimer++;
                        System.out.println("  ITEM #" + total + " " + item.toDisplayString());
                    }
                }
            }
            rc = nonTimer >= minNonTimer ? 0 : 1;
            System.out.printf("SMOKE %s non_timer_items=%d total_items=%d%n", rc == 0 ? "PASS" : "FAIL (not enough items)",
                    nonTimer, total);
        }
        finally {
            client.finalizeClient();
            System.out.println("SMOKE finalize OK");
        }
        System.exit(rc);
    }

    private static String arg(String[] args, int i, String dflt) {
        return args.length > i ? args[i] : dflt;
    }
}
