package ai.intellistream.tsdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** QUERY_LATEST (protocol 14): the newest row of many series, by id and by name, equal to what a read says. */
class LatestTest {
    static final long T0 = 1_700_000_000_000L;
    static final int ROWS = 100;
    static final long UNKNOWN = (1L << 32) | 999_999;

    static Path scratch;
    static TsdbServer server;
    static TsdbClient c;
    static long f32, f64, i64, bool, state, nul, empty;

    @BeforeAll
    static void start() throws Exception {
        scratch = TsdbServer.scratch("tsdbd-java-latest-");
        server = TsdbServer.start(scratch.resolve("db"));
        c = server.client();
        long[] ts = new long[ROWS];
        float[] fv = new float[ROWS];
        double[] dv = new double[ROWS];
        long[] lv = new long[ROWS];
        boolean[] bv = new boolean[ROWS];
        int[] sv = new int[ROWS];
        for (int i = 0; i < ROWS; i++) {
            ts[i] = T0 + i * 1000L;
            fv[i] = i / 4f;
            dv[i] = i / 3.0;
            lv[i] = (long) i << 40;
            bv[i] = i % 2 == 0;
        }
        f32 = c.series("l.f32");
        c.insert(f32, ts, fv);
        f64 = c.seriesCreate("l.f64", ValueType.FLOAT64);
        c.insertSeriesValues(f64, ts, Column.ofDoubles(dv));
        i64 = c.seriesCreate("l.i64", ValueType.INT64);
        c.insertSeriesValues(i64, ts, Column.ofLongs(lv));
        bool = c.seriesCreate("l.bool", ValueType.BOOL);
        c.insertSeriesValues(bool, ts, Column.ofBools(bv));
        state = c.seriesCreate("l.state", ValueType.STATE);
        int run = c.state(state, "run");
        Arrays.fill(sv, run);
        c.insertSeriesValues(state, ts, Column.ofStates(sv));
        nul = c.seriesCreate("l.null", ValueType.FLOAT64);
        BitSet nulls = new BitSet();
        nulls.set(7, 10);
        c.insertSeriesValues(nul, Arrays.copyOf(ts, 10), Column.ofDoubles(Arrays.copyOf(dv, 10)).withNulls(nulls));
        empty = c.seriesCreate("l.empty", ValueType.FLOAT64);
        c.flush();
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

    @Test
    void everyTypeNullsAndNone() throws Exception {
        long last = T0 + (ROWS - 1) * 1000L;
        List<SeriesResult<Latest>> r = c.latest(f32, f64, i64, bool, state, nul, empty);
        assertEquals(7, r.size());
        assertEquals(last, r.get(0).get().ts());
        assertEquals(99 / 4f, r.get(0).get().asDouble());
        assertEquals(99 / 3.0, r.get(1).get().asDouble());
        assertEquals(99L << 40, r.get(2).get().asLong());
        assertFalse(r.get(3).get().asBoolean());
        assertEquals(c.state(state, "run"), r.get(4).get().asState());
        assertEquals(ValueType.FLOAT32, r.get(0).get().type());
        assertEquals(T0 + 6000, r.get(5).get().ts()); /* the nulls after it are skipped */
        assertEquals(6 / 3.0, r.get(5).get().asDouble());
        assertEquals(Latest.Kind.NONE, r.get(6).get().kind());
        Latest none = r.get(6).get(), f = r.get(0).get();
        assertThrows(IllegalStateException.class, none::asDouble);
        assertThrows(IllegalStateException.class, f::asLong);

        /* with nulls the null is the answer */
        r = c.latest(new long[] {nul, f32}, LatestOptions.defaults().withNulls(true));
        assertEquals(Latest.Kind.NULL, r.get(0).get().kind());
        assertEquals(T0 + 9000, r.get(0).get().ts());
        assertEquals(Latest.Kind.VALUE, r.get(1).get().kind());

        /* rows before end, one for all and one each */
        r = c.latest(new long[] {f32, f64}, LatestOptions.defaults().end(T0 + 10_000));
        assertEquals(T0 + 9000, r.get(0).get().ts());
        assertEquals(9 / 3.0, r.get(1).get().asDouble());
        r = c.latest(new long[] {f32, f64}, LatestOptions.defaults().ends(new long[] {T0 + 10_000, T0}));
        assertEquals(T0 + 9000, r.get(0).get().ts());
        assertEquals(Latest.Kind.NONE, r.get(1).get().kind());
        assertThrows(IllegalArgumentException.class,
                () -> c.latest(new long[] {f32}, LatestOptions.defaults().ends(new long[] {1, 2})));
        assertThrows(IllegalArgumentException.class, () -> LatestOptions.defaults().end(5).ends(new long[] {1}).end(5)
                .withNulls(true).threads(1 << 20));
        assertTrue(c.latest(new long[0], LatestOptions.defaults()).isEmpty());

        /* equal to what a read says */
        ValuePoints v = c.queryValues(f64, Long.MIN_VALUE, Long.MAX_VALUE);
        long[] t = v.timestamps();
        assertEquals(t[t.length - 1], c.latest(f64).get(0).get().ts());
    }

    @Test
    void missingIdsAndNames() throws Exception {
        List<SeriesResult<Latest>> r = c.latest(f32, UNKNOWN, f64);
        assertTrue(r.get(0).ok());
        assertEquals(Status.ERR_NOTFOUND, r.get(1).status());
        assertTrue(r.get(2).ok());
        SeriesResult<Latest> bad = r.get(1);
        assertThrows(TsdbException.class, bad::get);
        assertTrue(c.isOpen());

        r = c.latestByName(new String[] {"l.f64", "l.nothing", "l.empty", ""}, LatestOptions.defaults());
        assertEquals(99 / 3.0, r.get(0).get().asDouble());
        assertEquals(Status.ERR_NOTFOUND, r.get(1).status());
        assertEquals(Latest.Kind.NONE, r.get(2).get().kind());
        assertEquals(Status.ERR_INVALID, r.get(3).status());
        /* the names are cached: the second call asks nothing but QUERY_LATEST, with per-name bounds */
        r = c.latestByName(new String[] {"l.f64", "l.nothing", "l.f32"},
                LatestOptions.defaults().ends(new long[] {T0 + 5000, T0, T0 + 3000}));
        assertEquals(T0 + 4000, r.get(0).get().ts());
        assertEquals(Status.ERR_NOTFOUND, r.get(1).status());
        assertEquals(T0 + 2000, r.get(2).get().ts());
    }

    @Test
    void manyIdsGoInSeveralFrames() throws Exception {
        int n = 70_000; /* more than one reply frame of 65,536 records */
        long[] ids = new long[n];
        for (int i = 0; i < n; i++)
            ids[i] = i % 3 == 0 ? UNKNOWN + i : i % 3 == 1 ? f64 : f32;
        List<SeriesResult<Latest>> r = c.latest(ids, LatestOptions.defaults());
        assertEquals(n, r.size());
        for (int i = 0; i < n; i++) {
            if (i % 3 == 0)
                assertEquals(Status.ERR_NOTFOUND, r.get(i).status(), "at " + i);
            else
                assertEquals(T0 + 99_000L, r.get(i).get().ts(), "at " + i);
        }
        assertArrayEquals(new long[] {T0 + 99_000L}, new long[] {c.latest(f32).get(0).get().ts()});
    }
}
