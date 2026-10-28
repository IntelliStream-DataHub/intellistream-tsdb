package ai.intellistream.tsdb.embedded;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.intellistream.tsdb.Buckets;
import ai.intellistream.tsdb.Column;
import ai.intellistream.tsdb.Group;
import ai.intellistream.tsdb.MultiOptions;
import ai.intellistream.tsdb.Order;
import ai.intellistream.tsdb.SeriesResult;
import ai.intellistream.tsdb.Status;
import ai.intellistream.tsdb.TsdbClient;
import ai.intellistream.tsdb.TsdbException;
import ai.intellistream.tsdb.TwaOptions;
import ai.intellistream.tsdb.Unit;
import ai.intellistream.tsdb.ValuePoints;
import ai.intellistream.tsdb.ValueType;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Many series in one call (tsdb_query_multi_collect): every result equals the single call's, and tsdbd's
 * QUERY_MULTI answers the same.
 */
class EmbeddedMultiTest {
    static final long T0 = 1_704_067_200_000L;
    static final long MIN = Long.MIN_VALUE, MAX = Long.MAX_VALUE;
    static final int ROWS = 4 * 24 * 60;

    @TempDir
    Path tmp;

    interface Single<T> {
        T call(int i) throws TsdbException;
    }

    interface Multi<C> {
        Object call(C db) throws TsdbException;
    }

    /** A result as plain values, for comparison (rows as [ts, value bits or null]). */
    static Object plain(Object o) throws TsdbException {
        if (o instanceof ValuePoints p) {
            List<Object> rows = new ArrayList<>();
            rows.add(p.type());
            for (int i = 0; i < p.size(); i++)
                rows.add(List.of(p.timestamps()[i], p.values().isNull(i) ? "null"
                        : Double.doubleToRawLongBits(p.values().getDouble(i))));
            return rows;
        }
        if (o instanceof SeriesResult<?> r)
            return r.ok() ? plain(r.get()) : "status " + r.status();
        if (o instanceof List<?> l) {
            List<Object> out = new ArrayList<>();
            for (Object e : l)
                out.add(plain(e));
            return out;
        }
        return o;
    }

    static <T> void check(int[] ids, List<SeriesResult<T>> got, Single<T> single, String what) throws Exception {
        assertEquals(ids.length, got.size(), what);
        for (int i = 0; i < ids.length; i++) {
            Object want;
            try {
                want = plain(single.call(i));
            } catch (TsdbException e) {
                want = "status " + e.status();
            }
            assertEquals(want, plain(got.get(i)), what + " series " + i);
        }
    }

    @Test
    void multiEqualsSingleAndTsdbd() throws Exception {
        Path dir = tmp.resolve("db");
        long[] ts = new long[ROWS];
        float[] f = new float[ROWS];
        double[] d = new double[ROWS];
        long[] l = new long[ROWS];
        BitSet nulls = new BitSet();
        for (int i = 0; i < ROWS; i++) {
            ts[i] = T0 + i * 60_000L + (i % 5) * 1000;
            f[i] = (float) Math.sin(i / 60.0) * 50;
            d[i] = Math.cos(i / 77.0) * 1e6;
            l[i] = (long) i * i - 1_000_000L;
            if (i % 97 == 5)
                nulls.set(i);
        }
        int[] ids;
        Map<String, Multi<EmbeddedDb>> embedded = new LinkedHashMap<>();
        Map<String, Multi<TsdbClient>> tcp = new LinkedHashMap<>();
        try (EmbeddedDb db = EmbeddedDb.open(dir.toString(), new EmbeddedOptions().fsync(false).requestThreads(4))) {
            int a = db.series("f32");
            db.insert(a, ts, f);
            int b = db.seriesCreate("f64", ValueType.FLOAT64);
            db.insertSeriesValues(b, ts, Column.ofDoubles(d).withNulls(nulls));
            int c = db.seriesCreate("i64", ValueType.INT64);
            db.insertSeriesValues(c, java.util.Arrays.copyOf(ts, ROWS / 3),
                    Column.ofLongs(java.util.Arrays.copyOf(l, ROWS / 3)));
            int s = db.seriesCreate("state", ValueType.STATE);
            int[] st = new int[ROWS];
            int[] codes = {db.state(s, "idle"), db.state(s, "run")};
            for (int i = 0; i < ROWS; i++)
                st[i] = codes[i % 7 < 3 ? 0 : 1];
            db.insertSeriesValues(s, ts, Column.ofStates(st));
            int e = db.seriesCreate("empty", ValueType.BOOL);
            ids = new int[] {a, b, c, s, e, a};
            int[] known = ids;

            long[] iv = {3_600_000, 600_000, 86_400_000, 7_200_000, 60_000, 0};
            long[] bounds = {T0, T0 + 3_600_000, T0 + 86_400_000, T0 + 3 * 86_400_000L};
            long[] starts = new long[ids.length], ends = new long[ids.length];
            for (int i = 0; i < ids.length; i++) {
                starts[i] = T0 + i * 7_200_000L;
                ends[i] = starts[i] + (i + 1) * 3_600_000L;
            }
            MultiOptions dflt = MultiOptions.defaults(), ranges = dflt.starts(starts).ends(ends);
            Group oslo = Group.of(Unit.DAY).in("Europe/Oslo"), utc = Group.of(Unit.HOUR, 6);
            Group odd = Group.of(Unit.HOUR, 7).in("GMT-05:30"); /* a java.time zone the engine does not know */
            for (Order o : Order.values())
                for (long lim : new long[] {0, 2}) {
                    String w = o + " " + lim;
                    check(known, db.queryValuesMulti(known, MIN, MAX, o, lim, dflt),
                            i -> db.queryValues(known[i], MIN, MAX, o, lim), "values " + w);
                    check(known, db.queryValuesMulti(known, 0, 0, o, lim, ranges),
                            i -> db.queryValues(known[i], starts[i], ends[i], o, lim), "ranges " + w);
                    check(known, db.queryLttbMulti(known, MIN, MAX, 40, o, lim, dflt),
                            i -> db.queryLttb(known[i], MIN, MAX, 40, o, lim), "lttb " + w);
                    check(known, db.queryBucketsMulti(known, MIN, MAX, Buckets.every(3_600_000), o, lim, dflt),
                            i -> db.queryAgg(known[i], MIN, MAX, 3_600_000, o, lim), "every " + w);
                    check(known, db.queryBucketsMulti(known, MIN, MAX, Buckets.intervals(iv), o, lim, dflt),
                            i -> db.queryAgg(known[i], MIN, MAX, iv[i], o, lim), "intervals " + w);
                    check(known, db.queryBucketsMulti(known, MIN, MAX, Buckets.bounds(bounds), o, lim, dflt),
                            i -> db.queryBounds(known[i], MIN, MAX, bounds, o, lim), "bounds " + w);
                    for (Group g : new Group[] {utc, oslo, odd}) {
                        check(known, db.queryBucketsMulti(known, MIN, MAX, Buckets.group(g), o, lim, dflt),
                                i -> db.queryGroup(known[i], MIN, MAX, g, o, lim), g + " " + w);
                        check(known, db.querySpreadMulti(known, T0 + 1, MAX, Buckets.group(g), o, lim, dflt),
                                i -> db.querySpreadGroup(known[i], T0 + 1, MAX, g, o, lim), "spread " + g + w);
                        TwaOptions t = TwaOptions.linear().order(o).limit(lim);
                        check(known, db.queryTwaMulti(known, MIN, MAX, Buckets.group(g), t, dflt),
                                i -> db.queryTwaGroup(known[i], MIN, MAX, g, t), "twa " + g + w);
                        check(known, db.queryTwaMulti(known, 0, 0, Buckets.group(g), t, ranges),
                                i -> db.queryTwaGroup(known[i], starts[i], ends[i], g, t), "twa ranges " + g + w);
                    }
                    check(known, db.querySpreadMulti(known, MIN, MAX, Buckets.every(7_200_000), o, lim, dflt),
                            i -> db.querySpread(known[i], MIN, MAX, 7_200_000, o, lim), "spread " + w);
                    TwaOptions t = TwaOptions.step().maxGap(120_000).order(o).limit(lim);
                    check(known, db.queryTwaMulti(known, T0, T0 + 86_400_000, Buckets.every(3_600_000), t, dflt),
                            i -> db.queryTwa(known[i], T0, T0 + 86_400_000, 3_600_000, t), "twa " + w);
                    check(known, db.queryTwaMulti(known, MIN, MAX, Buckets.bounds(bounds), t, dflt),
                            i -> db.queryTwaBounds(known[i], MIN, MAX, bounds, t), "twa bounds " + w);
                }
            /* an unknown id has no rows (status 0), as over TCP; the cap on all rows fails the call */
            List<SeriesResult<ValuePoints>> u = db.queryValuesMulti(new int[] {999_999, a}, MIN, MAX, Order.ASC, 0,
                    dflt);
            assertTrue(u.get(0).ok() && u.get(0).get().size() == 0 && u.get(1).get().size() == ROWS);
            TsdbException x = assertThrows(TsdbException.class,
                    () -> db.queryValuesMulti(known, MIN, MAX, Order.ASC, 0, dflt.maxRows(100)));
            assertEquals(Status.ERR_TOO_MANY_BUCKETS, x.status());
            assertFalse(db.queryTwaMulti(known, MIN, MAX, Buckets.one(), TwaOptions.step(), dflt).get(3).ok());
            assertEquals(0, db.queryBucketsMulti(new int[0], MIN, MAX, Buckets.one(), Order.ASC, 0, dflt).size());

            /* the same calls through tsdbd */
            int[] all = {a, b, c, s, e, 999_999, a};
            long[] iv2 = {3_600_000, 600_000, 86_400_000, 7_200_000, 60_000, 0, -5};
            embedded.put("values", x2 -> ((EmbeddedDb) x2).queryValuesMulti(all, MIN, MAX, Order.DESC, 3, dflt));
            tcp.put("values", x2 -> x2.queryValuesMulti(all, MIN, MAX, Order.DESC, 3, dflt));
            embedded.put("lttb", x2 -> x2.queryLttbMulti(all, MIN, MAX, 30, Order.ASC, 0, dflt));
            tcp.put("lttb", x2 -> x2.queryLttbMulti(all, MIN, MAX, 30, Order.ASC, 0, dflt));
            embedded.put("intervals", x2 -> x2.queryBucketsMulti(all, MIN, MAX, Buckets.intervals(iv2), Order.ASC, 0,
                    dflt.threads(2)));
            tcp.put("intervals", x2 -> x2.queryBucketsMulti(all, MIN, MAX, Buckets.intervals(iv2), Order.ASC, 0,
                    dflt.threads(2)));
            for (Group g : new Group[] {utc, oslo, odd}) {
                embedded.put("spread " + g, x2 -> x2.querySpreadMulti(all, MIN, MAX, Buckets.group(g), Order.DESC, 5,
                        dflt));
                tcp.put("spread " + g, x2 -> x2.querySpreadMulti(all, MIN, MAX, Buckets.group(g), Order.DESC, 5,
                        dflt));
                embedded.put("twa " + g, x2 -> x2.queryTwaMulti(all, MIN, MAX, Buckets.group(g), TwaOptions.linear(),
                        dflt));
                tcp.put("twa " + g, x2 -> x2.queryTwaMulti(all, MIN, MAX, Buckets.group(g), TwaOptions.linear(),
                        dflt));
            }
        }
        Path bin = Paths.get(System.getProperty("tsdbd", "../../build/tsdbd"));
        Assumptions.assumeTrue(Files.isExecutable(bin), bin + " is missing: run make first");
        Path log = tmp.resolve("tsdbd.log"), portFile = tmp.resolve("tsdbd.port");
        Process server = new ProcessBuilder(bin.toString(), "--dir", dir.toString(), "--port", "0", "--port-file",
                portFile.toString(), "--no-fsync", "--workers", "2").redirectErrorStream(true)
                .redirectOutput(log.toFile()).start();
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
            assertTrue(server.isAlive() && port > 0, "tsdbd did not serve");
            try (EmbeddedDb db = EmbeddedDb.open(dir.toString(), new EmbeddedOptions().readOnly(true));
                 TsdbClient c = TsdbClient.connect("127.0.0.1", port)) {
                for (String k : embedded.keySet())
                    assertEquals(plain(tcp.get(k).call(c)), plain(embedded.get(k).call(db)), k);
            }
        } finally {
            server.destroy();
            server.waitFor();
        }
    }
}
