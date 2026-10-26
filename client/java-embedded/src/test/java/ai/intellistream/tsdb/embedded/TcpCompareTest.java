package ai.intellistream.tsdb.embedded;

import ai.intellistream.tsdb.Column;
import ai.intellistream.tsdb.Group;
import ai.intellistream.tsdb.Order;
import ai.intellistream.tsdb.TsdbClient;
import ai.intellistream.tsdb.TsdbException;
import ai.intellistream.tsdb.TwaOptions;
import ai.intellistream.tsdb.Unit;
import ai.intellistream.tsdb.ValuePoints;
import ai.intellistream.tsdb.ValueType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
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
            /* tombstones both doors must apply: a delete and a replace in every type, a retention limit */
            for (int k = 0; k < 4; k++) {
                db.deleteRange(ids[k], T0 + 1000 * 60_000L, T0 + 1500 * 60_000L);
                long[] rts = {T0 + 2000 * 60_000L + 1, T0 + 2000 * 60_000L + 30_000};
                Column rc = switch (k) {
                case 0 -> Column.ofFloats(new float[] {1.5f, -2});
                case 1 -> Column.ofDoubles(new double[] {1e9, -7}).withNulls(BitSet.valueOf(new long[] {2}));
                case 2 -> Column.ofLongs(new long[] {Long.MAX_VALUE - 1, 3});
                default -> Column.ofBools(new boolean[] {true, true});
                };
                db.replaceRange(ids[k], T0 + 2000 * 60_000L, T0 + 2600 * 60_000L, rts, rc);
            }
            db.setRetention(ids[1], 1000L * 86400 * 365 * 200); /* long enough to keep every row */
        }
        /* tsdbd picks a free port itself (--port 0) and writes "PORT PID" to the port file once it serves: a port
         * picked here and released could be taken by another tree's tsdbd, whose answers this test would then check */
        Path log = tmp.resolve("tsdbd.log");
        Path portFile = tmp.resolve("tsdbd.port");
        Process server = new ProcessBuilder(bin.toString(), "--dir", dir.toString(), "--port", "0", "--port-file",
                portFile.toString(), "--no-fsync", "--workers", "2").redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            int port = -1;
            for (int i = 0; i < 1500 && server.isAlive() && port < 0; i++) {
                if (Files.exists(portFile)) {
                    String[] pf = Files.readString(portFile, StandardCharsets.UTF_8).trim().split("\\s+");
                    assertEquals(server.pid(), Long.parseLong(pf[1]), "the port file names another process");
                    port = Integer.parseInt(pf[0]);
                } else {
                    Thread.sleep(20);
                }
            }
            assertTrue(server.isAlive() && port > 0, () -> "tsdbd did not serve: " + read(log));
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
                                Group.of(Unit.HOUR, 5).in("America/St_Johns"), Group.of(Unit.MONTH).in("Asia/Kolkata"),
                                /* zone ids of java.time that the engine's calendar does not know: the embedded
                                 * calls must compute the bounds in Java, as TsdbClient does */
                                Group.of(Unit.DAY).in("UTC+2"), Group.of(Unit.HOUR, 7).in("GMT-05:30")}) {
                                assertEquals(tcp.queryGroup(id, Long.MIN_VALUE, Long.MAX_VALUE, g, o, limit),
                                        db.queryGroup(id, Long.MIN_VALUE, Long.MAX_VALUE, g, o, limit), "" + g);
                                long gl = limit;
                                for (long[] r : new long[][] {{Long.MIN_VALUE, Long.MAX_VALUE},
                                    {T0 + 12_345_678L, T0 + 2_000_000_000L}}) {
                                    same(names[k] + " spread " + g, () -> tcp.querySpreadGroup(id, r[0], r[1], g, o,
                                            gl), () -> db.querySpreadGroup(id, r[0], r[1], g, o, gl));
                                    TwaOptions to = TwaOptions.linear().order(o).limit(gl);
                                    same(names[k] + " twa " + g, () -> tcp.queryTwaGroup(id, r[0], r[1], g, to),
                                            () -> db.queryTwaGroup(id, r[0], r[1], g, to));
                                }
                            }
                            long[] bounds = {T0 - 1, T0 + 1_000_000, T0 + 9_999_999};
                            assertEquals(tcp.queryBounds(id, T0, T0 + 20_000_000L, bounds, o, limit),
                                    db.queryBounds(id, T0, T0 + 20_000_000L, bounds, o, limit));
                            long ol = limit;
                            long lo = T0 + 999 * 60_000L, hi = T0 + 30_000 * 60_000L;
                            same(names[k] + " spread", () -> tcp.querySpread(id, lo, hi, 3_600_000L, o, ol),
                                    () -> db.querySpread(id, lo, hi, 3_600_000L, o, ol));
                            same(names[k] + " spread 0", () -> tcp.querySpread(id, Long.MIN_VALUE, Long.MAX_VALUE, 0,
                                    o, ol), () -> db.querySpread(id, Long.MIN_VALUE, Long.MAX_VALUE, 0, o, ol));
                            same(names[k] + " spread bounds", () -> tcp.querySpreadBounds(id, T0, hi, bounds, o, ol),
                                    () -> db.querySpreadBounds(id, T0, hi, bounds, o, ol));
                            for (TwaOptions t : new TwaOptions[] {TwaOptions.step(), TwaOptions.linear(),
                                TwaOptions.step().maxGap(90_000), TwaOptions.linear().maxGap(30 * 60_000L)}) {
                                TwaOptions to = t.order(o).limit(ol);
                                same(names[k] + " twa " + to, () -> tcp.queryTwa(id, lo + 7, hi, 3_600_000L, to),
                                        () -> db.queryTwa(id, lo + 7, hi, 3_600_000L, to));
                                same(names[k] + " twa 0 " + to, () -> tcp.queryTwa(id, Long.MIN_VALUE,
                                        Long.MAX_VALUE, 0, to), () -> db.queryTwa(id, Long.MIN_VALUE, Long.MAX_VALUE,
                                        0, to));
                                same(names[k] + " twa bounds " + to, () -> tcp.queryTwaBounds(id, T0, hi, bounds, to),
                                        () -> db.queryTwaBounds(id, T0, hi, bounds, to));
                            }
                            for (long th : new long[] {0, 2, 3, 100, 49_000, 1_000_000, -1})
                                same(names[k] + " lttb " + th, () -> points(tcp.queryLttb(id, Long.MIN_VALUE,
                                        Long.MAX_VALUE, th, o, ol)), () -> points(db.queryLttb(id, Long.MIN_VALUE,
                                        Long.MAX_VALUE, th, o, ol)));
                        }
                    }
                }
                for (long th : new long[] {2, 10}) /* an unknown series: no points, after the threshold check */
                    same("lttb of an unknown series " + th, () -> points(tcp.queryLttb(987_654, Long.MIN_VALUE,
                            Long.MAX_VALUE, th)), () -> points(db.queryLttb(987_654, Long.MIN_VALUE, Long.MAX_VALUE,
                            th)));
                var p = db.queryPoints(ids[0], Long.MIN_VALUE, Long.MAX_VALUE);
                var q = tcp.queryPoints(ids[0], Long.MIN_VALUE, Long.MAX_VALUE);
                assertArrayEquals(q.timestamps(), p.timestamps());
                assertArrayEquals(q.values(), p.values());
                assertEquals(123f, p.value(0));
                assertEquals(tcp.retention(ids[1]), db.retention(ids[1]));
                assertEquals(0, db.retention(ids[0]));
                assertEquals(tcp.stateName(ids[4], 2), db.stateName(ids[4], 2));
                assertEquals(tcp.stateLookup(ids[4], "stop"), db.stateLookup(ids[4], "stop"));
            }
        } finally {
            server.destroy(); /* SIGTERM to this one process */
            if (!server.waitFor(60, TimeUnit.SECONDS))
                server.destroyForcibly().waitFor();
        }
    }

    interface Query<T> {
        T run() throws TsdbException;
    }

    /** Both answer alike, or both fail with the same status. */
    private static <T> void same(String what, Query<T> tcp, Query<T> embedded) {
        Object a, b;
        try {
            a = tcp.run();
        } catch (TsdbException e) {
            a = "status " + e.status();
        }
        try {
            b = embedded.run();
        } catch (TsdbException e) {
            b = "status " + e.status();
        }
        assertEquals(a, b, what);
    }

    /** A comparable form of points: timestamps, then each value's bits or null. */
    private static List<Object> points(ValuePoints p) {
        List<Object> l = new ArrayList<>();
        l.add(p.values().type());
        for (int i = 0; i < p.size(); i++) {
            l.add(p.timestamps()[i]);
            l.add(p.values().isNull(i) ? null : Double.doubleToRawLongBits(p.values().getDouble(i)));
        }
        return l;
    }

    private static String read(Path p) {
        try {
            return Files.readString(p, StandardCharsets.UTF_8);
        } catch (IOException e) {
            return e.toString();
        }
    }
}
