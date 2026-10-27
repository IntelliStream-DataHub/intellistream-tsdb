package ai.intellistream.tsdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Many series in one request (protocol 10): every result equals the single call's. */
class MultiTest {
    static final long T0 = 1_700_000_000_000L; /* 2023-11-14T22:13:20Z */
    static final long MIN = Long.MIN_VALUE, MAX = Long.MAX_VALUE;
    static final int ROWS = 3 * 24 * 60; /* three days at one minute */

    static Path scratch;
    static TsdbServer server;
    static TsdbClient c;
    static int f32, f64, i64, bool, state, empty;
    static int[] ids;

    @BeforeAll
    static void start() throws Exception {
        scratch = TsdbServer.scratch("tsdbd-java-multi-");
        server = TsdbServer.start(scratch.resolve("db"));
        c = server.client();
        long[] ts = new long[ROWS];
        float[] fv = new float[ROWS];
        double[] dv = new double[ROWS];
        long[] lv = new long[ROWS];
        boolean[] bv = new boolean[ROWS];
        int[] sv = new int[ROWS];
        BitSet nulls = new BitSet();
        for (int i = 0; i < ROWS; i++) {
            ts[i] = T0 + i * 60_000L + (i % 7) * 1000L;
            fv[i] = (float) (Math.sin(i / 50.0) * 100);
            dv[i] = Math.cos(i / 70.0) * 1e3;
            lv[i] = (long) i * i - 5000;
            bv[i] = i % 13 < 5;
            if (i % 101 == 0)
                nulls.set(i);
        }
        f32 = c.series("m.f32");
        c.insert(f32, ts, fv);
        f64 = c.seriesCreate("m.f64", ValueType.FLOAT64);
        c.insertSeriesValues(f64, ts, Column.ofDoubles(dv).withNulls(nulls));
        i64 = c.seriesCreate("m.i64", ValueType.INT64);
        long[] half = java.util.Arrays.copyOf(ts, ROWS / 2); /* a shorter series: other edges than the rest */
        c.insertSeriesValues(i64, half, Column.ofLongs(java.util.Arrays.copyOf(lv, ROWS / 2)));
        bool = c.seriesCreate("m.bool", ValueType.BOOL);
        c.insertSeriesValues(bool, ts, Column.ofBools(bv));
        state = c.seriesCreate("m.state", ValueType.STATE);
        int a = c.state(state, "on"), b = c.state(state, "off");
        for (int i = 0; i < ROWS; i++)
            sv[i] = i % 17 < 9 ? a : b;
        c.insertSeriesValues(state, ts, Column.ofStates(sv));
        empty = c.seriesCreate("m.empty", ValueType.FLOAT64);
        ids = new int[] {f32, f64, i64, bool, state, empty, 999_999, f32};
    }

    @AfterAll
    static void stop() throws Exception {
        try {
            if (c != null)
                c.close();
        } finally {
            if (server != null)
                assertEquals(0, server.stop(), () -> TsdbClientTest.logOf(server));
            TsdbServer.delete(scratch);
        }
    }

    static void same(ValuePoints a, ValuePoints b, String what) {
        assertArrayEquals(a.timestamps(), b.timestamps(), what);
        assertEquals(a.type(), b.type(), what);
        Column x = a.values(), y = b.values();
        assertEquals(x.size(), y.size(), what);
        for (int i = 0; i < x.size(); i++) {
            assertEquals(x.isNull(i), y.isNull(i), what + " null " + i);
            if (!x.isNull(i))
                assertEquals(Double.doubleToLongBits(x.getDouble(i)), Double.doubleToLongBits(y.getDouble(i)),
                        what + " row " + i);
        }
    }

    interface Single<T> {
        T call(int i) throws TsdbException;
    }

    /** Each result equals the single call for its series, or fails with the single call's status. */
    static <T> void check(List<SeriesResult<T>> got, Single<T> single, java.util.function.BiConsumer<T, T> eq,
                          String what) throws TsdbException {
        assertEquals(ids.length, got.size(), what);
        for (int i = 0; i < ids.length; i++) {
            T want;
            try {
                want = single.call(i);
            } catch (TsdbException e) {
                assertFalse(got.get(i).ok(), what + " series " + i + " should fail with " + e.status());
                assertEquals(e.status(), got.get(i).status(), what + " " + i);
                continue;
            }
            assertTrue(got.get(i).ok(), what + " series " + i + ": " + got.get(i));
            eq.accept(want, got.get(i).get());
        }
    }

    static <T> void eq(T a, T b) {
        assertEquals(a, b);
    }

    @Test
    void valuesAndLttb() throws Exception {
        long s = T0 + 3_600_000, e = T0 + 40 * 3_600_000L;
        for (Order o : Order.values())
            for (long limit : new long[] {0, 1, 100}) {
                String w = "values " + o + " " + limit;
                check(c.queryValuesMulti(ids, s, e, o, limit, MultiOptions.defaults()),
                        i -> c.queryValues(ids[i], s, e, o, limit), (x, y) -> same(x, y, w), w);
                check(c.queryLttbMulti(ids, MIN, MAX, 50, o, limit, MultiOptions.defaults()),
                        i -> c.queryLttb(ids[i], MIN, MAX, 50, o, limit), (x, y) -> same(x, y, "lttb"), "lttb " + w);
            }
        long[] starts = new long[ids.length], ends = new long[ids.length];
        for (int i = 0; i < ids.length; i++) {
            starts[i] = T0 + i * 3_600_000L;
            ends[i] = starts[i] + (i + 1) * 600_000L;
        }
        MultiOptions m = MultiOptions.defaults().starts(starts).ends(ends);
        check(c.queryValuesMulti(ids, 0, 0, Order.ASC, 0, m), i -> c.queryValues(ids[i], starts[i], ends[i]),
                (x, y) -> same(x, y, "ranges"), "per-series ranges");
    }

    @Test
    void buckets() throws Exception {
        long[] bounds = {T0, T0 + 3_600_000, T0 + 86_400_000, T0 + 2 * 86_400_000L};
        long[] iv = new long[ids.length];
        for (int i = 0; i < ids.length; i++)
            iv[i] = (i + 1) * 900_000L;
        Group oslo = Group.of(Unit.DAY).in("Europe/Oslo"), utc = Group.of(Unit.HOUR, 6);
        Group ny = Group.of(Unit.HOUR, 5).in("America/New_York");
        MultiOptions d = MultiOptions.defaults();
        for (Order o : Order.values())
            for (long limit : new long[] {0, 3}) {
                String w = o + " " + limit;
                check(c.queryBucketsMulti(ids, MIN, MAX, Buckets.one(), o, limit, d),
                        i -> c.queryAgg(ids[i], MIN, MAX, 0, o, limit), MultiTest::eq, "one " + w);
                check(c.queryBucketsMulti(ids, T0, MAX, Buckets.every(3_600_000), o, limit, d),
                        i -> c.queryAgg(ids[i], T0, MAX, 3_600_000, o, limit), MultiTest::eq, "every " + w);
                check(c.queryBucketsMulti(ids, MIN, MAX, Buckets.intervals(iv), o, limit, d),
                        i -> c.queryAgg(ids[i], MIN, MAX, iv[i], o, limit), MultiTest::eq, "intervals " + w);
                for (Group g : new Group[] {utc, oslo, ny}) {
                    check(c.queryBucketsMulti(ids, MIN, MAX, Buckets.group(g), o, limit, d),
                            i -> c.queryGroup(ids[i], MIN, MAX, g, o, limit), MultiTest::eq, g + " " + w);
                    check(c.querySpreadMulti(ids, T0 + 1000, T0 + 50 * 3_600_000L, Buckets.group(g), o, limit, d),
                            i -> c.querySpreadGroup(ids[i], T0 + 1000, T0 + 50 * 3_600_000L, g, o, limit),
                            MultiTest::eq, "spread " + g + " " + w);
                }
                check(c.queryBucketsMulti(ids, MIN, MAX, Buckets.bounds(bounds), o, limit, d),
                        i -> c.queryBounds(ids[i], MIN, MAX, bounds, o, limit), MultiTest::eq, "bounds " + w);
                check(c.querySpreadMulti(ids, MIN, MAX, Buckets.every(7_200_000), o, limit, d),
                        i -> c.querySpread(ids[i], MIN, MAX, 7_200_000, o, limit), MultiTest::eq, "spread " + w);
                check(c.querySpreadMulti(ids, MIN, MAX, Buckets.bounds(bounds), o, limit, d),
                        i -> c.querySpreadBounds(ids[i], MIN, MAX, bounds, o, limit), MultiTest::eq, "sbounds " + w);
            }
        iv[2] = -1; /* a negative interval is that series' error */
        List<SeriesResult<List<Bucket>>> r = c.queryBucketsMulti(ids, MIN, MAX, Buckets.intervals(iv), Order.ASC, 0, d);
        assertEquals(Status.ERR_INVALID, r.get(2).status());
        assertTrue(r.get(0).ok() && r.get(3).ok());
        TsdbException x = assertThrows(TsdbException.class,
                () -> c.queryBucketsMulti(ids, MIN, MAX, Buckets.every(-1), Order.ASC, 0, d));
        assertEquals(Status.ERR_INVALID, x.status());
    }

    @Test
    void twa() throws Exception {
        long[] bounds = {T0, T0 + 3_600_000, T0 + 86_400_000};
        Group oslo = Group.of(Unit.DAY).in("Europe/Oslo");
        MultiOptions d = MultiOptions.defaults();
        for (TwaOptions o : new TwaOptions[] {TwaOptions.step(), TwaOptions.linear().maxGap(120_000),
            TwaOptions.step().order(Order.DESC).limit(2)}) {
            check(c.queryTwaMulti(ids, T0, T0 + 86_400_000, Buckets.every(3_600_000), o, d),
                    i -> c.queryTwa(ids[i], T0, T0 + 86_400_000, 3_600_000, o), MultiTest::eq, "twa " + o);
            check(c.queryTwaMulti(ids, MIN, MAX, Buckets.bounds(bounds), o, d),
                    i -> c.queryTwaBounds(ids[i], MIN, MAX, bounds, o), MultiTest::eq, "twa bounds " + o);
            check(c.queryTwaMulti(ids, MIN, MAX, Buckets.group(oslo), o, d),
                    i -> c.queryTwaGroup(ids[i], MIN, MAX, oslo, o), MultiTest::eq, "twa oslo " + o);
            check(c.queryTwaMulti(ids, T0 + 5_000_000, MAX, Buckets.group(oslo), o, d),
                    i -> c.queryTwaGroup(ids[i], T0 + 5_000_000, MAX, oslo, o), MultiTest::eq, "twa closed lo " + o);
            /* closed sides that differ between series: one by one through the single calls */
            long[] starts = new long[ids.length];
            for (int i = 0; i < ids.length; i++)
                starts[i] = T0 + i * 1_000_000L;
            check(c.queryTwaMulti(ids, 0, MAX, Buckets.group(oslo), o, d.starts(starts)),
                    i -> c.queryTwaGroup(ids[i], starts[i], MAX, oslo, o), MultiTest::eq, "twa mixed " + o);
        }
        assertEquals(Status.ERR_INVALID,
                c.queryTwaMulti(ids, MIN, MAX, Buckets.one(), TwaOptions.step(), d).get(4).status()); /* a state */
    }

    @Test
    void splitCapAndCancel() throws Exception {
        int n = Protocol.MULTI_MAX_SERIES + 3;
        int[] many = new int[n];
        for (int i = 0; i < n; i++)
            many[i] = ids[i % 4];
        List<SeriesResult<ValuePoints>> r = c.queryValuesMulti(many, MIN, MAX, Order.DESC, 1, MultiOptions.defaults());
        assertEquals(n, r.size());
        for (int i = 0; i < 4; i++) {
            ValuePoints want = c.queryValues(ids[i], MIN, MAX, Order.DESC, 1);
            same(want, r.get(i).get(), "split " + i);
        }
        for (int i = n - 4; i < n; i++)
            same(c.queryValues(many[i], MIN, MAX, Order.DESC, 1), r.get(i).get(), "split tail " + i);

        /* the cap on all rows fails the call; the connection goes on */
        TsdbException x = assertThrows(TsdbException.class,
                () -> c.queryValuesMulti(ids, MIN, MAX, Order.ASC, 0, MultiOptions.defaults().maxRows(100)));
        assertEquals(Status.ERR_TOO_MANY_BUCKETS, x.status());
        assertEquals(ROWS, c.queryValues(f32, MIN, MAX).size());

        /* a consumer that stops cancels the rest; the connection goes on */
        List<Integer> seen = new ArrayList<>();
        boolean whole = c.queryValuesMulti(many, MIN, MAX, Order.ASC, 0, MultiOptions.defaults(), (ix, ts, v, k) -> {
            if (!seen.contains(ix))
                seen.add(ix);
            return ix < 2;
        });
        assertFalse(whole);
        assertEquals(List.of(0, 1, 2), seen);
        assertEquals(ROWS, c.queryValues(f32, MIN, MAX).size());
        List<SeriesResult<List<Bucket>>> b = c.queryBucketsMulti(ids, MIN, MAX, Buckets.one(), Order.ASC, 0,
                MultiOptions.defaults().threads(1));
        assertEquals(ROWS, b.get(0).get().get(0).count());

        /* a consumer that throws: the reply is still read to its end */
        assertThrows(IllegalStateException.class,
                () -> c.queryValuesMulti(many, MIN, MAX, Order.ASC, 0, MultiOptions.defaults(), (ix, ts, v, k) -> {
                    throw new IllegalStateException("stop");
                }));
        assertEquals(ROWS, c.queryValues(f32, MIN, MAX).size());
        assertTrue(c.queryValuesMulti(new int[0], MIN, MAX).isEmpty());
        assertThrows(IllegalArgumentException.class,
                () -> c.queryValuesMulti(ids, MIN, MAX, Order.ASC, 0, MultiOptions.defaults().starts(new long[1])));
    }
}
