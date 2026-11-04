package ai.intellistream.tsdb.example;

import ai.intellistream.tsdb.Bucket;
import ai.intellistream.tsdb.ConnectOptions;
import ai.intellistream.tsdb.Group;
import ai.intellistream.tsdb.Order;
import ai.intellistream.tsdb.Points;
import ai.intellistream.tsdb.TsdbClient;
import ai.intellistream.tsdb.Unit;

import java.nio.file.Path;
import java.time.Instant;

/**
 * Quick start: {@code java -cp tsdb-client.jar ai.intellistream.tsdb.example.Example [host] [port] [user]
 * [password-file]}.
 */
public final class Example {
    private Example() {
    }

    /**
     * Runs the example.
     *
     * @param args the host and port of a running tsdbd, the user to log in as and the file with its password;
     *     default 127.0.0.1 7070 admin /tmp/tsdb-demo/password
     * @throws Exception if the server cannot be reached, the login fails or a request fails
     */
    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : TsdbClient.DEFAULT_PORT;
        String user = args.length > 2 ? args[2] : "admin";
        Path passwordFile = Path.of(args.length > 3 ? args[3] : "/tmp/tsdb-demo/password");

        // Log in as user, with the password from the first line of passwordFile.
        ConnectOptions login = ConnectOptions.defaults().user(user).passwordFile(passwordFile);
        try (TsdbClient client = TsdbClient.connect(host, port, login)) {
            long pump = client.series("pump7.flow_m3h"); // get or create

            // One reading per minute for a day, from 2024-03-01T00:00Z, as a single batch.
            long t0 = Instant.parse("2024-03-01T00:00:00Z").toEpochMilli();
            int n = 24 * 60;
            long[] ts = new long[n];
            float[] values = new float[n];
            for (int i = 0; i < n; i++) {
                ts[i] = t0 + i * 60_000L;
                values[i] = 40 + (i % 60) / 10f;
            }
            client.insert(pump, ts, values);

            // Raw points: the three newest.
            Points newest = client.queryPoints(pump, Long.MIN_VALUE, Long.MAX_VALUE, Order.DESC, 3);
            for (int i = 0; i < newest.size(); i++)
                System.out.printf("%s %.1f%n", Instant.ofEpochMilli(newest.timestamp(i)), newest.value(i));

            // Aggregates over fixed 6-hour intervals.
            for (Bucket b : client.queryAgg(pump, t0, t0 + 86_400_000L, 6 * 3_600_000L))
                System.out.printf("%s count %d avg %.2f min %.1f max %.1f%n", Instant.ofEpochMilli(b.start()),
                        b.count(), b.avg(), b.min(), b.max());

            // Calendar days in Oslo (UTC+1 in March): the UTC day spans two local days.
            Group osloDays = Group.of(Unit.DAY).in("Europe/Oslo");
            for (Bucket b : client.queryGroup(pump, Long.MIN_VALUE, Long.MAX_VALUE, osloDays))
                System.out.printf("Oslo day from %s: %d points%n", Instant.ofEpochMilli(b.start()), b.count());
        }
    }
}
