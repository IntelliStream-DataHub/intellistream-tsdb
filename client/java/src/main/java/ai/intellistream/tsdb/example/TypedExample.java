package ai.intellistream.tsdb.example;

import ai.intellistream.tsdb.Bucket;
import ai.intellistream.tsdb.Column;
import ai.intellistream.tsdb.ConnectOptions;
import ai.intellistream.tsdb.TsdbClient;
import ai.intellistream.tsdb.ValuePoints;
import ai.intellistream.tsdb.ValueType;

import java.nio.file.Path;
import java.time.Instant;
import java.util.BitSet;
import java.util.StringJoiner;

/** Value types and nulls: {@code java -cp tsdb-client.jar ai.intellistream.tsdb.example.TypedExample}. */
public final class TypedExample {
    private TypedExample() {
    }

    /**
     * Runs the example.
     *
     * @param args as for {@link Example}: host, port, user and password file; default 127.0.0.1 7070 admin
     *     /tmp/tsdb-demo/password
     * @throws Exception if the server cannot be reached, the login fails or a request fails
     */
    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : TsdbClient.DEFAULT_PORT;
        String user = args.length > 2 ? args[2] : "admin";
        Path passwordFile = Path.of(args.length > 3 ? args[3] : "/tmp/tsdb-demo/password");

        ConnectOptions login = ConnectOptions.defaults().user(user).passwordFile(passwordFile);
        try (TsdbClient client = TsdbClient.connect(host, port, login)) {
            long t0 = Instant.parse("2024-03-01T00:00:00Z").toEpochMilli();
            long[] ts = {t0, t0 + 60_000, t0 + 120_000, t0 + 180_000};

            // An int64 meter reading per minute; the third one is missing (null).
            long energy = client.seriesCreate("pump7.energy_wh", ValueType.INT64);
            BitSet missing = new BitSet();
            missing.set(2);
            client.insertSeriesValues(energy, ts, Column.ofLongs(1_000, 1_250, 0, 1_900).withNulls(missing));

            ValuePoints points = client.queryValues(energy, Long.MIN_VALUE, Long.MAX_VALUE);
            long[] wh = points.values().longs();
            for (int i = 0; i < points.size(); i++) {
                String value = points.values().isNull(i) ? "null" : Long.toString(wh[i]);
                System.out.println(Instant.ofEpochMilli(points.timestamps()[i]) + " " + value);
            }

            // Aggregates skip nulls; integer sums are exact.
            Bucket all = client.queryAgg(energy, Long.MIN_VALUE, Long.MAX_VALUE, 0).get(0);
            System.out.println("known " + all.count() + ", sum " + all.isum() + ", max " + all.imax());

            // A state series stores each name once, as a code.
            long mode = client.seriesCreate("pump7.mode", ValueType.STATE);
            int running = client.state(mode, "running");
            int stopped = client.state(mode, "stopped");
            client.insertSeriesValues(mode, ts, Column.ofStates(running, running, stopped, running));

            StringJoiner names = new StringJoiner(" ");
            for (int code : client.queryValues(mode, Long.MIN_VALUE, Long.MAX_VALUE).values().states())
                names.add(client.stateName(mode, code));
            System.out.println(names);
        }
    }
}
