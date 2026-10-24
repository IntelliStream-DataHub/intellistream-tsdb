package ai.intellistream.tsdb.embedded;

import ai.intellistream.tsdb.Column;
import ai.intellistream.tsdb.Group;
import ai.intellistream.tsdb.Order;
import ai.intellistream.tsdb.TsdbClient;
import ai.intellistream.tsdb.Unit;
import ai.intellistream.tsdb.ValuePoints;
import ai.intellistream.tsdb.ValueType;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.BitSet;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The same data answers the same through both doors: written embedded, then served by build/tsdbd from the same
 * directory and read with the TCP client, every query is compared with the embedded answer.
 */
class TcpCompareTest {
    static final long T0 = 1_704_067_200_000L;

    @TempDir
    Path tmp;

    @Test
    void embeddedAndTcpAnswerAlike() throws Exception {
        Path bin = Paths.get(System.getProperty("tsdbd", "../../build/tsdbd"));
        Assumptions.assumeTrue(Files.isExecutable(bin), bin + " is missing: run make first");
        Path dir = tmp.resolve("db");
        int n = 50_000;
        long[] ts = new long[n];
        float[] f = new float[n];
        double[] d = new double[n];
        long[] l = new long[n];
        boolean[] b = new boolean[n];
        BitSet nulls = new BitSet();
        for (int i = 0; i < n; i++) {
            ts[i] = T0 + i * 60_000L;
            f[i] = (float) Math.sin(i / 100.0) * 50;
            d[i] = Math.cos(i / 77.0) * 1e6;
            l[i] = (long) i * i - 1_000_000_000L;
            b[i] = i % 3 == 0;
            if (i % 97 == 5)
                nulls.set(i);
        }
        f[7] = Float.NaN;
        f[8] = Float.NEGATIVE_INFINITY;
        String[] names = {"f32", "f64", "i64", "bool", "state"};
        int[] ids = new int[names.length];
        try (EmbeddedDb db = EmbeddedDb.open(dir.toString(), new EmbeddedOptions().fsync(false))) {
            ids[0] = db.series(names[0]);
            db.insert(ids[0], ts, f);
            ids[1] = db.seriesCreate(names[1], ValueType.FLOAT64);
            db.insertSeriesValues(ids[1], ts, Column.ofDoubles(d).withNulls(nulls));
            ids[2] = db.seriesCreate(names[2], ValueType.INT64);
            db.insertSeriesValues(ids[2], ts, Column.ofLongs(l).withNulls(nulls));
            ids[3] = db.seriesCreate(names[3], ValueType.BOOL);
            db.insertSeriesValues(ids[3], ts, Column.ofBools(b).withNulls(nulls));
            ids[4] = db.seriesCreate(names[4], ValueType.STATE);
            int[] codes = {db.state(ids[4], "idle"), db.state(ids[4], "run"), db.state(ids[4], "stop")};
            int[] st = new int[n];
            for (int i = 0; i < n; i++)
                st[i] = codes[i % 3];
            db.insertSeriesValues(ids[4], ts, Column.ofStates(st).withNulls(nulls));
            db.insert(ids[0], new long[] {T0}, new float[] {123}); /* a correction, in the memtable */
        }
        int port;
        try (ServerSocket s = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            port = s.getLocalPort();
        }
        Path log = tmp.resolve("tsdbd.log");
        Process server = new ProcessBuilder(bin.toString(), "--dir", dir.toString(), "--port", Integer.toString(port),
                "--no-fsync", "--workers", "2").redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            String ready = "on 127.0.0.1:" + port + ",";
            for (int i = 0; i < 500 && server.isAlive()
                    && !Files.readString(log, StandardCharsets.UTF_8).contains(ready); i++)
                Thread.sleep(20);
            assertTrue(server.isAlive(), () -> "tsdbd: " + read(log));
            try (EmbeddedDb db = EmbeddedDb.open(dir.toString(), new EmbeddedOptions().readOnly(true));
                 TsdbClient tcp = TsdbClient.connect("127.0.0.1", port)) {
                for (int k = 0; k < names.length; k++) {
                    int id = ids[k];
                    assertEquals(tcp.series(names[k], false), id);
                    assertEquals(tcp.seriesName(id), db.seriesName(id));
                    assertEquals(tcp.seriesType(id), db.seriesType(id));
                    for (Order o : Order.values()) {
                        for (long limit : new long[] {0, 1, 777}) {
                            ValuePoints a = db.queryValues(id, Long.MIN_VALUE, Long.MAX_VALUE, o, limit);
                            ValuePoints c = tcp.queryValues(id, Long.MIN_VALUE, Long.MAX_VALUE, o, limit);
                            assertArrayEquals(c.timestamps(), a.timestamps(), names[k]);
                            assertEquals(c.values().nulls(), a.values().nulls(), names[k]);
                            for (int i = 0; i < a.size(); i++)
                                if (!a.values().isNull(i))
                                    assertEquals(Double.doubleToRawLongBits(c.values().getDouble(i)),
                                            Double.doubleToRawLongBits(a.values().getDouble(i)), names[k] + " " + i);
                            assertEquals(tcp.queryAgg(id, T0, T0 + n * 60_000L, 3_600_000L, o, limit),
                                    db.queryAgg(id, T0, T0 + n * 60_000L, 3_600_000L, o, limit));
                            for (Group g : new Group[] {null, Group.of(Unit.DAY), Group.of(Unit.WEEK).in("Europe/Oslo"),
                                Group.of(Unit.HOUR, 5).in("America/St_Johns"), Group.of(Unit.MONTH).in("Asia/Kolkata")})
                                assertEquals(tcp.queryGroup(id, Long.MIN_VALUE, Long.MAX_VALUE, g, o, limit),
                                        db.queryGroup(id, Long.MIN_VALUE, Long.MAX_VALUE, g, o, limit), "" + g);
                            long[] bounds = {T0 - 1, T0 + 1_000_000, T0 + 9_999_999};
                            assertEquals(tcp.queryBounds(id, T0, T0 + 20_000_000L, bounds, o, limit),
                                    db.queryBounds(id, T0, T0 + 20_000_000L, bounds, o, limit));
                        }
                    }
                }
                var p = db.queryPoints(ids[0], Long.MIN_VALUE, Long.MAX_VALUE);
                var q = tcp.queryPoints(ids[0], Long.MIN_VALUE, Long.MAX_VALUE);
                assertArrayEquals(q.timestamps(), p.timestamps());
                assertArrayEquals(q.values(), p.values());
                assertEquals(123f, p.value(0));
                assertEquals(tcp.stateName(ids[4], 2), db.stateName(ids[4], 2));
                assertEquals(tcp.stateLookup(ids[4], "stop"), db.stateLookup(ids[4], "stop"));
            }
        } finally {
            server.destroy(); /* SIGTERM to this one process */
            if (!server.waitFor(60, TimeUnit.SECONDS))
                server.destroyForcibly().waitFor();
        }
    }

    private static String read(Path p) {
        try {
            return Files.readString(p, StandardCharsets.UTF_8);
        } catch (IOException e) {
            return e.toString();
        }
    }
}
