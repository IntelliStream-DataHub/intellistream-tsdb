package ai.intellistream.tsdb.embedded;

import ai.intellistream.tsdb.Column;
import ai.intellistream.tsdb.Points;
import ai.intellistream.tsdb.TsdbException;
import ai.intellistream.tsdb.ValuePoints;
import ai.intellistream.tsdb.ValueType;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The native scratch memory that inserts reuse from call to call (Scratch), alone and under EmbeddedDb. */
class ScratchTest {
    static final long T0 = 1_704_067_200_000L;

    @TempDir
    Path tmp;

    @Test
    void bumpSpillGrowAndReuse() {
        Scratch s = new Scratch();
        MemorySegment a = s.allocate(1000, 8); /* empty block: spills */
        a.set(JAVA_LONG, 0, 42);
        s.reset();                             /* grows to the 1000 bytes asked for */
        MemorySegment b = s.allocate(24, 8);
        MemorySegment c = s.allocate(16, 16);
        assertTrue(b.address() % 8 == 0 && c.address() % 16 == 0);
        assertTrue(c.address() >= b.address() + 24, "no overlap");
        MemorySegment big = s.allocate(Scratch.MAX + 1, 8); /* beyond the block: spills, and is never kept */
        assertTrue(big.address() < b.address() || big.address() >= b.address() + (1 << 20));
        s.reset();
        assertEquals(b.address(), s.allocate(24, 8).address(), "the same block again");
        s.reset();
        Scratch.Pool p = new Scratch.Pool();
        Scratch x = p.take();
        p.give(x);
        assertTrue(p.take() == x, "returned to the pool");
        p.give(x);
        p.clear();
        assertFalse(p.take() == x, "cleared");
    }

    /** Inserts whose sizes spill, fit, shrink, grow and pass the largest block kept, each read back exactly. */
    @Test
    void insertsOfChangingSizes() throws TsdbException {
        try (EmbeddedDb db = EmbeddedDb.open(tmp.resolve("db").toString(),
                new EmbeddedOptions().fsync(false).walSyncMs(-1))) {
            int f = db.series("f");
            int g = db.series("g");
            int d = db.seriesCreate("d", ValueType.FLOAT64);
            int b = db.seriesCreate("b", ValueType.BOOL);
            int[] sizes = {1000, 1000, 10, 300_000, 5, 2_200_000, 1000};
            long t = T0;
            for (int round = 0; round < sizes.length; round++) {
                int n = sizes[round];
                long[] ts = new long[n];
                float[] fv = new float[n];
                int[] many = new int[n];
                double[] dv = new double[n];
                boolean[] bv = new boolean[n];
                BitSet nulls = new BitSet();
                for (int i = 0; i < n; i++) {
                    ts[i] = t + i;
                    fv[i] = round * 1_000_000 + i;
                    many[i] = i % 2 == 0 ? f : g;
                    dv[i] = -(round * 1_000_000.0 + i);
                    bv[i] = (i + round) % 3 == 0;
                    if ((i + round) % 7 == 0)
                        nulls.set(i);
                }
                db.insert(f, ts, fv, 0, n);
                db.insert(many, ts, fv, 0, n); /* overwrites f's even rows with the same values, g's odd rows */
                db.insertSeriesValues(d, ts, Column.ofDoubles(dv).withNulls(nulls));
                db.insertSeriesValues(b, ts, Column.ofBools(bv));

                Points pf = db.queryPoints(f, t, t + n);
                assertArrayEquals(ts, pf.timestamps(), "round " + round);
                assertArrayEquals(fv, pf.values(), "round " + round);
                assertEquals(n / 2, db.queryPoints(g, t, t + n).size(), "round " + round);
                ValuePoints pd = db.queryValues(d, t, t + n);
                assertEquals(n, pd.size());
                for (int i = 0; i < n; i++) {
                    assertEquals(nulls.get(i), pd.values().isNull(i), "round " + round + " row " + i);
                    if (!nulls.get(i))
                        assertEquals(dv[i], pd.values().doubles()[i]);
                }
                assertArrayEquals(bv, db.queryValues(b, t, t + n).values().bools(), "round " + round);
                t += n;
            }
        }
    }

    /** Threads inserting at once each get their own scratch: no row lands in another thread's series. */
    @Test
    void concurrentInserts() throws Exception {
        int nt = 8, calls = 20, n = 50_000;
        try (EmbeddedDb db = EmbeddedDb.open(tmp.resolve("c").toString(),
                new EmbeddedOptions().fsync(false).walSyncMs(-1))) {
            int[] ids = new int[nt];
            for (int k = 0; k < nt; k++)
                ids[k] = db.series("s" + k);
            ExecutorService ex = Executors.newFixedThreadPool(nt);
            try {
                List<Future<?>> fs = new ArrayList<>();
                for (int k = 0; k < nt; k++) {
                    int id = ids[k], kk = k;
                    fs.add(ex.submit(() -> {
                        for (int c = 0; c < calls; c++) {
                            int m = n - c * 100; /* sizes vary: some calls fit, some spill */
                            long[] ts = new long[m];
                            float[] v = new float[m];
                            for (int i = 0; i < m; i++) {
                                ts[i] = T0 + (long) c * n + i;
                                v[i] = kk;
                            }
                            db.insert(id, ts, v, 0, m);
                        }
                        return null;
                    }));
                }
                for (Future<?> f : fs)
                    f.get();
            } finally {
                ex.shutdown();
            }
            for (int k = 0; k < nt; k++) {
                float[] v = db.queryPoints(ids[k], Long.MIN_VALUE, Long.MAX_VALUE).values();
                long expect = 0;
                for (int c = 0; c < calls; c++)
                    expect += n - c * 100;
                assertEquals(expect, v.length);
                for (float x : v)
                    assertEquals(k, x);
            }
        }
    }
}
