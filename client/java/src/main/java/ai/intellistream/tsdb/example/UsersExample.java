// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb.example;

import ai.intellistream.tsdb.ConnectOptions;
import ai.intellistream.tsdb.Grant;
import ai.intellistream.tsdb.Order;
import ai.intellistream.tsdb.Rights;
import ai.intellistream.tsdb.Selector;
import ai.intellistream.tsdb.TsdbClient;
import ai.intellistream.tsdb.TsdbException;
import ai.intellistream.tsdb.User;

import java.nio.file.Path;
import java.util.StringJoiner;

/**
 * Users and rights: {@code java -cp tsdb-client.jar ai.intellistream.tsdb.example.UsersExample [host] [port] [user]
 * [password-file]}, after the quick start. The README's "Users and rights" snippet is the body of {@link #main}.
 */
public final class UsersExample {
    private UsersExample() {
    }

    /**
     * Runs the example.
     *
     * @param args as for {@link Example}: host, port, a superuser and its password file; default 127.0.0.1 7070
     *     admin /tmp/tsdb-demo/password
     * @throws Exception if the server cannot be reached, the login fails or a request fails
     */
    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : TsdbClient.DEFAULT_PORT;
        String user = args.length > 2 ? args[2] : "admin";
        Path passwordFile = Path.of(args.length > 3 ? args[3] : "/tmp/tsdb-demo/password");

        ConnectOptions login = ConnectOptions.defaults().user(user).passwordFile(passwordFile);
        try (TsdbClient admin = TsdbClient.connect(host, port, login)) {
            // A user for a collector of the tenant "acme": it may add rows to the series whose names start
            // with "acme/", and do nothing else.
            admin.userCreate("acme/ingest", "collector-password-5".toCharArray(), false);
            admin.grant("acme/ingest", Selector.prefix("acme/"), Rights.INSERT);
            for (User u : admin.userList()) {
                StringJoiner grants = new StringJoiner(", ");
                for (Grant g : u.grants())
                    grants.add(g.rights() + " on " + g.selector());
                System.out.println(u.name() + ": " + (u.superuser() ? "superuser" : grants));
            }

            // The collector logs in with its own password and writes a row.
            ConnectOptions collectorLogin = ConnectOptions.defaults().user("acme/ingest")
                    .password("collector-password-5");
            try (TsdbClient collector = TsdbClient.connect(host, port, collectorLogin)) {
                long pump = collector.series("acme/pump1");
                collector.insert(pump, new long[] {1_700_000_000_000L}, new float[] {1.5f});

                // It may not read the row back, nor see a series of another tenant.
                try {
                    collector.queryPoints(pump, Long.MIN_VALUE, Long.MAX_VALUE, Order.ASC, 10);
                } catch (TsdbException e) {
                    System.out.println("read acme/pump1: " + e.getMessage());
                }
                try {
                    collector.series("globex/pump1");
                } catch (TsdbException e) {
                    System.out.println("globex/pump1: " + e.getMessage());
                }
            }

            // Drop the user again, so that the example can run once more.
            admin.userDrop("acme/ingest");
        }
    }
}
