package ai.intellistream.tsdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.BitSet;
import java.util.List;
import java.util.function.IntPredicate;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.AfterParameterizedClassInvocation;
import org.junit.jupiter.params.BeforeParameterizedClassInvocation;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Protocol version 2 against a live build/tsdbd: value types, nulls, the state dictionary, QUERY_BUCKETS. Every
 * test runs twice: with packed, compressed inserts (version 3, the default) and with plain arrays.
 */
@ParameterizedClass(name = "compression level {0}")
@ValueSource(ints = {1, 0})
class TypedValuesTest {
    static final long T0 = TsdbClientTest.T0;
    static final int CHUNK = 16384;        /* rows per QUERY_VALUES chunk of the server */
    static final int N = 2 * CHUNK + 5000; /* three chunks */
    /* nulls at the start, in the middle, around both chunk boundaries and at the end */
    static final int[] NULLS = {0, 1, 7, 8, 1000, CHUNK - 1, CHUNK, CHUNK + 1, 2 * CHUNK - 1, 2 * CHUNK, N - 1};

    static Path scratch;
    static TsdbServer server;
    static TsdbClient c;

    @Parameter
    int level; /* the zstd level of the client: 1 packs, 0 sends plain arrays */

    @BeforeParameterizedClassInvocation
    static void start(int level) throws Exception {
        scratch = TsdbServer.scratch("tsdbd-java-types-");
        server = TsdbServer.start(scratch.resolve("db"));
        c = server.client();
        c.setCompression(level);
    }

    @AfterParameterizedClassInvocation
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

    static BitSet nullSet() {
        BitSet b = new BitSet();
        for (int i : NULLS)
            b.set(i);
        return b;
    }

    static int status(Executable e) {
        return assertThrows(TsdbException.class, e).status();
    }

    /** A column of every type with N rows: distinct values, the reserved null values avoided. */
    static Column column(ValueType t, int[] states) {
        switch (t) {
        case FLOAT32: {
            float[] v = new float[N];
            for (int i = 0; i < N; i++)
                v[i] = i * 0.5f - 1000;
            return Column.ofFloats(v);
        }
        case FLOAT64: {
            double[] v = new double[N];
            for (int i = 0; i < N; i++)
                v[i] = 1e300 / (i + 1);
            return Column.ofDoubles(v);
        }
        case INT64: {
            long[] v = new long[N];
            for (int i = 0; i < N; i++)
                v[i] = (i & 1) == 0 ? Long.MAX_VALUE - i : Long.MIN_VALUE + 1 + i;
            return Column.ofLongs(v);
        }
        case BOOL: {
            boolean[] v = new boolean[N];
            for (int i = 0; i < N; i++)
                v[i] = i % 3 == 0;
            return Column.ofBools(v);
        }
        default: {
            int[] v = new int[N];
            for (int i = 0; i < N; i++)
                v[i] = states[i % states.length];
            return Column.ofStates(v);
        }
        }
    }

    /** Asserts that {@code got[0, n)} equals {@code want[from, from + n)}, values and nulls. */
    static void assertSlice(Column want, int from, Column got, int n) {
        assertEquals(want.type(), got.type());
        for (int i = 0; i < n; i++) {
            int w = from + i;
            assertEquals(want.isNull(w), got.isNull(i), "null at " + w);
            if (want.isNull(w))
                continue;
            String at = "value at " + w;
            switch (want.type()) {
            case FLOAT32: assertEquals(want.floats()[w], got.floats()[i], at); break;
            case FLOAT64: assertEquals(want.doubles()[w], got.doubles()[i], at); break;
            case INT64: assertEquals(want.longs()[w], got.longs()[i], at); break;
            case BOOL: assertEquals(want.bools()[w], got.bools()[i], at); break;
            default: assertEquals(want.states()[w], got.states()[i], at); break;
            }
        }
    }

    @Test
    void seriesCreateAndType() throws Exception {
        assertEquals(level, c.compression(), "the client of this invocation");
        for (ValueType t : ValueType.values()) {
            int id = c.seriesCreate("create." + t, t);
            assertEquals(t, c.seriesType(id));
            assertEquals(id, c.seriesCreate("create." + t, t));
            assertEquals(id, c.series("create." + t, false));
        }
        assertEquals(ValueType.FLOAT32, c.seriesType(c.series("create.default")));
        int d = c.seriesCreate("create.FLOAT64", ValueType.FLOAT64);
        assertEquals(Status.ERR_INVALID, status(() -> c.seriesCreate("create.FLOAT64", ValueType.INT64)));
        assertEquals(Status.ERR_INVALID, status(() -> c.seriesCreate("create.default", ValueType.BOOL)));
        assertEquals(Status.ERR_NOTFOUND, status(() -> c.seriesType(0x7ffffff0)));
        int onVolume = c.seriesCreate("create.on_volume", ValueType.INT64, scratch.resolve("volume").toString());
        assertEquals(ValueType.INT64, c.seriesType(onVolume));
        assertNotEquals(d, onVolume);
        assertTrue(c.isOpen());
    }

    @Test
    void everyTypeRoundTripsWithNulls() throws Exception {
        long[] ts = TsdbClientTest.seq(T0, 1000, N);
        for (ValueType t : ValueType.values()) {
            int id = c.seriesCreate("rt." + t, t);
            int[] codes = t == ValueType.STATE
                    ? new int[] {c.state(id, "idle"), c.state(id, "run"), c.state(id, "fault")}
                    : null;
            Column want = column(t, codes).withNulls(nullSet());
            c.insertSeriesValues(id, ts, want);

            ValuePoints p = c.queryValues(id, Long.MIN_VALUE, Long.MAX_VALUE);
            assertEquals(N, p.size(), t.toString());
            assertEquals(t, p.type());
            assertArrayEquals(ts, p.timestamps());
            assertEquals(nullSet(), p.values().nulls(), t + " nulls");
            assertSlice(want, 0, p.values(), N);

            /* streamed: the server's chunks, each with its own nulls */
            int[] seen = {0, 0};
            assertTrue(c.queryValues(id, Long.MIN_VALUE, Long.MAX_VALUE, (tt, col, n) -> {
                assertEquals(n, col.size());
                assertEquals(ts[seen[0]], tt[0]);
                assertSlice(want, seen[0], col, n);
                seen[0] += n;
                seen[1]++;
                return true;
            }));
            assertEquals(N, seen[0]);
            assertEquals(3, seen[1], "chunks of " + CHUNK);

            /* the newest, newest first, across a chunk boundary of the reverse scan */
            int k = CHUNK + 10;
            ValuePoints d = c.queryValues(id, Long.MIN_VALUE, Long.MAX_VALUE, Order.DESC, k);
            assertEquals(k, d.size());
            for (int i = 0; i < k; i++) {
                int w = N - 1 - i;
                assertEquals(ts[w], d.timestamps()[i]);
                assertEquals(want.isNull(w), d.values().isNull(i), t + " desc null at " + w);
                assertEquals(want.getDouble(w), d.values().getDouble(i), 0.0, t + " desc value at " + w);
            }
        }
    }

    @Test
    void offsetsShiftTheBitmap() throws Exception {
        int id = c.seriesCreate("offsets.typed", ValueType.FLOAT64);
        long[] ts = TsdbClientTest.seq(T0, 1000, 40);
        double[] v = new double[40];
        for (int i = 0; i < 40; i++)
            v[i] = i;
        BitSet nulls = new BitSet();
        nulls.set(3); /* outside the slice */
        nulls.set(7);
        nulls.set(13);
        nulls.set(34);
        Column col = Column.ofDoubles(v).withNulls(nulls);
        c.insertSeriesValues(id, ts, col, 5, 30); /* rows 5..34: not on a byte boundary */
        ValuePoints p = c.queryValues(id, Long.MIN_VALUE, Long.MAX_VALUE);
        assertEquals(30, p.size());
        assertEquals(ts[5], p.timestamps()[0]);
        BitSet want = new BitSet();
        want.set(7 - 5);
        want.set(13 - 5);
        want.set(34 - 5);
        assertEquals(want, p.values().nulls());
        for (int i = 0; i < 30; i++)
            if (!want.get(i))
                assertEquals(i + 5, p.values().doubles()[i]);
        /* a slice without nulls goes without a bitmap */
        int id2 = c.seriesCreate("offsets.typed2", ValueType.FLOAT64);
        c.insertSeriesValues(id2, ts, col, 14, 10);
        assertFalse(c.queryValues(id2, Long.MIN_VALUE, Long.MAX_VALUE).values().hasNulls());
    }

    @Test
    void manySeriesFramesArePadded() throws Exception {
        /* bools are one byte wide: the series ids after them need padding to 4 bytes */
        int a = c.seriesCreate("many.bool.a", ValueType.BOOL), b = c.seriesCreate("many.bool.b", ValueType.BOOL);
        for (int n = 1; n <= 9; n++) {
            int[] series = new int[n];
            long[] ts = new long[n];
            boolean[] v = new boolean[n];
            BitSet nulls = new BitSet();
            for (int i = 0; i < n; i++) {
                series[i] = i % 2 == 0 ? a : b;
                ts[i] = T0 + n * 100_000L + i;
                v[i] = i % 3 != 1;
            }
            nulls.set(n / 2);
            c.insertValues(series, ts, Column.ofBools(v).withNulls(nulls));
            for (int s : new int[] {a, b}) {
                ValuePoints p = c.queryValues(s, T0 + n * 100_000L, T0 + n * 100_000L + n);
                for (int j = 0, i = s == a ? 0 : 1; i < n; i += 2, j++) {
                    assertEquals(ts[i], p.timestamps()[j]);
                    assertEquals(i == n / 2, p.values().isNull(j), "n=" + n + " row " + i);
                    if (i != n / 2)
                        assertEquals(v[i], p.values().bools()[j], "n=" + n + " row " + i);
                }
            }
        }
    }

    @Test
    void exactIntegerAggregates() throws Exception {
        int id = c.seriesCreate("agg.int64", ValueType.INT64);
        long big = Long.MAX_VALUE - 10, neg = -(Long.MAX_VALUE - 20);
        long[] v = {big, neg, 7, Long.MAX_VALUE / 2, 0};
        BitSet nulls = new BitSet();
        nulls.set(4);
        c.insertSeriesValues(id, TsdbClientTest.seq(T0, 1000, 5), Column.ofLongs(v).withNulls(nulls));
        Bucket all = c.queryAgg(id, Long.MIN_VALUE, Long.MAX_VALUE, 0).get(0);
        assertEquals(4, all.count());
        assertEquals(10 + 7 + Long.MAX_VALUE / 2, all.isum());
        assertEquals(neg, all.imin());
        assertEquals(big, all.imax());
        assertEquals((double) big, all.max());
        assertEquals((double) neg, all.min());

        /* sums saturate */
        int up = c.seriesCreate("agg.int64.up", ValueType.INT64);
        c.insertSeriesValues(up, TsdbClientTest.seq(T0, 1000, 2), Column.ofLongs(Long.MAX_VALUE - 1, 5));
        assertEquals(Long.MAX_VALUE, c.queryAgg(up, Long.MIN_VALUE, Long.MAX_VALUE, 0).get(0).isum());
        int down = c.seriesCreate("agg.int64.down", ValueType.INT64);
        c.insertSeriesValues(down, TsdbClientTest.seq(T0, 1000, 2), Column.ofLongs(Long.MIN_VALUE + 1, -5));
        assertEquals(Long.MIN_VALUE, c.queryAgg(down, Long.MIN_VALUE, Long.MAX_VALUE, 0).get(0).isum());

        /* buckets of 2 s, newest first, and calendar seconds as the C test does */
        List<Bucket> two = c.queryAgg(id, Long.MIN_VALUE, Long.MAX_VALUE, 2000, Order.DESC, 2);
        assertEquals(2, two.size());
        assertEquals(T0 + 4000, two.get(0).start());
        assertEquals(0, two.get(0).count(), "a bucket of only nulls is returned with count 0");
        assertEquals(T0 + 2000, two.get(1).start());
        assertEquals(7 + Long.MAX_VALUE / 2, two.get(1).isum());
        List<Bucket> g = c.queryGroup(id, Long.MIN_VALUE, Long.MAX_VALUE, Group.of(Unit.SECOND, 4).in("UTC"),
                Order.DESC, 1);
        assertEquals(1, g.size());
        assertEquals(T0 + 4000, g.get(0).start());
        assertEquals(0, g.get(0).count());
    }

    @Test
    void boolAndFloatAggregates() throws Exception {
        int b = c.seriesCreate("agg.bool", ValueType.BOOL);
        BitSet nulls = new BitSet();
        nulls.set(2);
        c.insertSeriesValues(b, TsdbClientTest.seq(T0, 1000, 5),
                Column.ofBools(true, false, true, true, false).withNulls(nulls));
        Bucket bk = c.queryAgg(b, Long.MIN_VALUE, Long.MAX_VALUE, 0).get(0);
        assertEquals(4, bk.count());
        assertEquals(2, bk.isum());
        assertEquals(2.0, bk.sum());
        assertEquals(0, bk.imin());
        assertEquals(1, bk.imax());
        assertEquals(0.5, bk.avg());

        /* float64 min and max arrive unrounded (version 1 rows would make 1e300 infinite) */
        int d = c.seriesCreate("agg.f64", ValueType.FLOAT64);
        c.insertValues(new int[] {d, d, d, d}, TsdbClientTest.seq(T0, 1000, 4),
                Column.ofDoubles(1.5, 2.25, -3.0, 1e300));
        Bucket f = c.queryAgg(d, Long.MIN_VALUE, Long.MAX_VALUE, 0).get(0);
        assertEquals(4, f.count());
        assertEquals(-3.0, f.min());
        assertEquals(1e300, f.max());
        double precise = 0.1;
        int p = c.seriesCreate("agg.f64.precise", ValueType.FLOAT64);
        c.insertSeriesValues(p, new long[] {T0}, Column.ofDoubles(precise));
        assertEquals(precise, c.queryAgg(p, Long.MIN_VALUE, Long.MAX_VALUE, 0).get(0).min());

        /* float32 series keep working, now through QUERY_BUCKETS */
        int f32 = c.series("agg.f32");
        c.insert(f32, TsdbClientTest.seq(T0, 1000, 3), new float[] {1.25f, -2.5f, 4});
        Bucket x = c.queryAgg(f32, Long.MIN_VALUE, Long.MAX_VALUE, 0).get(0);
        assertEquals(3, x.count());
        assertEquals(-2.5, x.min());
        assertEquals(4.0, x.max());
        assertEquals(2.75, x.sum());
    }

    @Test
    void stateDictionary() throws Exception {
        int s = c.seriesCreate("state.a", ValueType.STATE);
        int s2 = c.seriesCreate("state.b", ValueType.STATE);
        int on = c.state(s, "on"), off = c.state(s, "off");
        assertNotEquals(on, off);
        assertTrue(on != 0 && off != 0, "code 0 is null");
        assertEquals(on, c.state(s, "on"));
        assertEquals(off, c.stateLookup(s, "off"));
        assertEquals(on, c.stateLookup(s2, "on"), "series of one volume share the dictionary");
        assertEquals(Status.ERR_NOTFOUND, status(() -> c.stateLookup(s, "standby")));
        assertEquals("on", c.stateName(s, on));
        String unicode = "läuft ✓";
        assertEquals(unicode, c.stateName(s, c.state(s, unicode)));
        assertEquals(Status.ERR_NOTFOUND, status(() -> c.stateName(s, 999)));
        assertThrows(IllegalArgumentException.class, () -> c.state(s, ""));

        c.insertValues(new int[] {s, s2, s}, TsdbClientTest.seq(T0, 1000, 3), Column.ofStates(on, off, off));
        assertArrayEquals(new int[] {on, off}, c.queryValues(s, Long.MIN_VALUE, Long.MAX_VALUE).values().states());
        assertArrayEquals(new int[] {off}, c.queryValues(s2, Long.MIN_VALUE, Long.MAX_VALUE).values().states());
        Bucket b = c.queryAgg(s, Long.MIN_VALUE, Long.MAX_VALUE, 0).get(0);
        assertEquals(2, b.count());
    }

    @Test
    void reservedValuesReadBackAsNull() throws Exception {
        int i = c.seriesCreate("reserved.i64", ValueType.INT64);
        c.insertSeriesValues(i, TsdbClientTest.seq(T0, 1000, 2), Column.ofLongs(Long.MIN_VALUE, 1));
        assertTrue(c.queryValues(i, Long.MIN_VALUE, Long.MAX_VALUE).values().isNull(0));

        int f = c.seriesCreate("reserved.f32", ValueType.FLOAT32);
        c.insertSeriesValues(f, TsdbClientTest.seq(T0, 1000, 2),
                Column.ofFloats(Float.intBitsToFloat(0x7fc01001), Float.NaN));
        Column got = c.queryValues(f, Long.MIN_VALUE, Long.MAX_VALUE).values();
        assertTrue(got.isNull(0));
        assertFalse(got.isNull(1), "other NaNs are values");
        assertTrue(Float.isNaN(got.floats()[1]));

        int s = c.seriesCreate("reserved.state", ValueType.STATE);
        c.insertSeriesValues(s, new long[] {T0}, Column.ofStates(0));
        assertTrue(c.queryValues(s, Long.MIN_VALUE, Long.MAX_VALUE).values().isNull(0));
    }

    @Test
    void lastWriteWinsWithNullsAndPipelining() throws Exception {
        int id = c.seriesCreate("lww.typed", ValueType.INT64);
        long[] ts = {T0, T0 + 1000};
        BitSet first = new BitSet();
        first.set(1);
        c.insertSeriesValuesAsync(id, ts, Column.ofLongs(1, 2).withNulls(first));
        c.insertSeriesValuesAsync(id, ts, Column.ofLongs(3, 4)); /* replaces the null */
        BitSet second = new BitSet();
        second.set(0);
        c.insertValuesAsync(new int[] {id}, new long[] {T0}, Column.ofLongs(5).withNulls(second)); /* a null wins */
        assertEquals(3, c.pending());
        c.await();
        Column v = c.queryValues(id, Long.MIN_VALUE, Long.MAX_VALUE).values();
        assertTrue(v.isNull(0));
        assertEquals(4, v.longs()[1]);
        c.flush(); /* also once in parts */
        v = c.queryValues(id, Long.MIN_VALUE, Long.MAX_VALUE).values();
        assertTrue(v.isNull(0));
        assertEquals(4, v.longs()[1]);
    }

    @Test
    void typeMismatches() throws Exception {
        int d = c.seriesCreate("mismatch.f64", ValueType.FLOAT64);
        int f = c.series("mismatch.f32");
        long[] ts = {T0};
        assertEquals(Status.ERR_INVALID, status(() -> c.insertSeriesValues(d, ts, Column.ofLongs(1))));
        assertEquals(Status.ERR_INVALID, status(() -> c.insert(d, ts, new float[] {1})));
        assertEquals(Status.ERR_INVALID, status(() -> c.queryPoints(d, Long.MIN_VALUE, Long.MAX_VALUE)));
        assertEquals(Status.ERR_INVALID,
                status(() -> c.insertValues(new int[] {d, f}, new long[] {T0, T0}, Column.ofDoubles(1, 2))));
        assertEquals(0, c.queryValues(d, Long.MIN_VALUE, Long.MAX_VALUE).size(), "nothing of the batch applied");
        assertEquals(0, c.queryValues(f, Long.MIN_VALUE, Long.MAX_VALUE).size());
        /* a pipelined mismatch is reported by await */
        c.insertSeriesValuesAsync(d, ts, Column.ofBools(true));
        assertEquals(Status.ERR_INVALID, status(c::await));
        /* the float32 calls and typed calls agree on float32 series */
        c.insertSeriesValues(f, ts, Column.ofFloats(2.5f));
        assertArrayEquals(new float[] {2.5f}, c.queryPoints(f, Long.MIN_VALUE, Long.MAX_VALUE).values());
        assertEquals(ValueType.FLOAT32, c.queryValues(f, Long.MIN_VALUE, Long.MAX_VALUE).type());

        Column longs = Column.ofLongs(1);
        assertThrows(IllegalStateException.class, longs::doubles);
        assertThrows(IllegalArgumentException.class, () -> c.insertSeriesValues(d, new long[2], Column.ofDoubles(1)));
        assertTrue(c.isOpen());
    }

    @Test
    void unknownSeriesAndEarlyStop() throws Exception {
        ValuePoints none = c.queryValues(0x7ffffff0, Long.MIN_VALUE, Long.MAX_VALUE);
        assertEquals(0, none.size());
        assertEquals(ValueType.FLOAT32, none.type());

        int id = c.seriesCreate("stop.typed", ValueType.INT64);
        c.insertSeriesValues(id, TsdbClientTest.seq(T0, 1000, N), Column.ofLongs(new long[N]));
        int[] calls = {0};
        assertFalse(c.queryValues(id, Long.MIN_VALUE, Long.MAX_VALUE, (t, v, n) -> ++calls[0] < 1));
        assertEquals(1, calls[0]);
        RuntimeException boom = new IllegalStateException("boom");
        assertSame(boom, assertThrows(IllegalStateException.class,
                () -> c.queryValues(id, Long.MIN_VALUE, Long.MAX_VALUE, (t, v, n) -> {
                    throw boom;
                })));
        assertEquals(N, c.queryValues(id, Long.MIN_VALUE, Long.MAX_VALUE).size());
    }

    @Test
    void columnBasics() {
        BitSet nulls = new BitSet();
        nulls.set(1);
        nulls.set(99); /* beyond the size: ignored */
        Column col = Column.ofBools(true, false, true).withNulls(nulls);
        assertEquals(3, col.size());
        assertEquals(1.0, col.getDouble(0));
        assertTrue(Double.isNaN(col.getDouble(1)));
        assertEquals(1, col.nulls().cardinality());
        nulls.set(0); /* withNulls copied the set */
        assertFalse(col.isNull(0));
        assertFalse(Column.ofStates(1).withNulls(new BitSet()).hasNulls());
        assertThrows(IndexOutOfBoundsException.class, () -> col.isNull(3));
        IntPredicate unsigned = code -> Column.ofStates(code).getDouble(0) == Integer.toUnsignedLong(code);
        assertTrue(unsigned.test(-1));
    }
}
