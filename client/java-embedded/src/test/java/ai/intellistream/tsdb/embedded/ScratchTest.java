package ai.intellistream.tsdb.embedded;

import ai.intellistream.tsdb.Column;
import ai.intellistream.tsdb.Points;
import ai.intellistream.tsdb.TsdbException;
import ai.intellistream.tsdb.ValuePoints;
import ai.intellistream.tsdb.ValueType;

import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The native scratch memory that inserts reuse from call to call (Scratch), alone and under EmbeddedDb. Every test has
 * a timeout on its own thread, and closes go through {@link #closeWithin}: an insert that leaked the database's read
 * lock makes close() block forever, and must fail the test rather than hang the build.
 */
@Timeout(value = 180, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ScratchTest {
    static final long T0 = 1_704_067_200_000L;
    static final long MIB = 1L << 20;

    @TempDir
    Path tmp;

    @AfterEach
    void noHook() {
        Scratch.growHook = null;
    }

    static EmbeddedOptions opts() {
        return new EmbeddedOptions().fsync(false).walSyncMs(-1);
    }

    static Scratch.Pool pool(EmbeddedDb db) throws Exception {
        Field f = EmbeddedDb.class.getDeclaredField("scratch");
        f.setAccessible(true);
        return (Scratch.Pool) f.get(db);
    }

    @SuppressWarnings("unchecked")
    static Collection<Scratch> free(Scratch.Pool p) throws Exception {
        Field f = Scratch.Pool.class.getDeclaredField("free");
        f.setAccessible(true);
        return (Collection<Scratch>) f.get(p);
    }

    /** Closes {@code db} on another thread and fails if that does not return within 30 s. */
    static void closeWithin(EmbeddedDb db) throws Exception {
        AtomicReference<Throwable> err = new AtomicReference<>();
        Thread c = new Thread(() -> {
            try {
                db.close();
            } catch (Throwable t) {
                err.set(t);
            }
        });
        c.setDaemon(true);
        c.start();
        c.join(TimeUnit.SECONDS.toMillis(30));
        assertFalse(c.isAlive(), "close() blocks: an insert kept the read lock");
        assertNull(err.get());
    }

    /** The pool holds no block twice, and its byte count is the sum of the blocks it keeps. */
    static void consistent(Scratch.Pool p) throws Exception {
        IdentityHashMap<Scratch, Boolean> seen = new IdentityHashMap<>();
        long sum = 0;
        for (Scratch s : free(p)) {
            assertNull(seen.put(s, true), "a scratch twice in the pool");
            assertTrue(s.size() > 0, "a scratch without a block kept");
            sum += s.size();
        }
        assertEquals(sum, p.bytes(), "bytes counted = bytes kept");
    }

    @Test
    void bumpSpillGrowAndReuse() {
        Scratch s = new Scratch();
        MemorySegment a = s.allocate(1000, 8); /* empty block: spills */
        a.set(JAVA_LONG, 0, 42);
        assertEquals(MIB, s.reset(), "a whole MiB would have held it");
        assertTrue(s.grow(MIB));
        MemorySegment b = s.allocate(24, 8);
        MemorySegment c = s.allocate(16, 16);
        assertTrue(b.address() % 8 == 0 && c.address() % 16 == 0);
        assertTrue(c.address() >= b.address() + 24, "no overlap");
        assertEquals(0, s.reset(), "fitted");
        assertEquals(b.address(), s.allocate(24, 8).address(), "the same block again");
        s.reset();
        s.free();
    }

    /** Growth within the limit only, decided before allocating; the block returned last is taken first. */
    @Test
    void poolLimitAndOrder() throws Exception {
        AtomicInteger grows = new AtomicInteger();
        Scratch.growHook = grows::incrementAndGet;

        Scratch.Pool p = new Scratch.Pool(3 * MIB);
        Scratch s1 = p.take(), s2 = p.take();
        s1.allocate(MIB + 1, 8);
        p.give(s1);                                    /* grows to 2 MiB */
        assertEquals(2 * MIB, s1.size());
        assertEquals(2 * MIB, p.bytes());
        s2.allocate(MIB + 1, 8);
        p.give(s2);                                    /* 4 MiB > 3: not grown, and without a block not kept */
        assertEquals(0, s2.size());
        assertEquals(1, grows.get(), "no block allocated beyond the limit");
        assertEquals(1, p.kept());
        assertSame(s1, p.take());
        s1.allocate(Scratch.MAX + 1, 8);               /* more than any block kept: spills, nothing grows */
        p.give(s1);
        assertEquals(2 * MIB, s1.size());
        assertEquals(1, grows.get());
        consistent(p);

        Scratch s3 = p.take(), s4 = p.take();          /* s1, then a new one */
        assertSame(s1, s3);
        s4.allocate(100, 8);
        p.give(s4);                                    /* 1 MiB: fits the limit */
        p.give(s3);
        assertEquals(3 * MIB, p.bytes());
        assertSame(s3, p.take(), "last in, first out");
        assertSame(s4, p.take());
        p.give(s4);
        p.give(s3);
        consistent(p);
        p.clear();
        assertEquals(0, p.bytes());
        assertEquals(0, p.kept());

        Scratch.Pool big = new Scratch.Pool(4 * Scratch.MAX);
        Scratch y = big.take();
        y.allocate(Scratch.MAX + 1, 8);                /* beyond the largest block even where the limit has room */
        big.give(y);
        assertEquals(0, big.bytes());
        assertEquals(0, big.kept());

        Scratch.Pool none = new Scratch.Pool(0);       /* keeps nothing: every call as before the pool */
        Scratch z = none.take();
        z.allocate(1000, 8);
        none.give(z);
        assertEquals(0, none.kept());
        assertEquals(0, none.bytes());
        assertEquals(2, grows.get());
    }

    /**
     * Regression: a block that cannot grow (malloc failing) used to throw out of the insert's finally before the read
     * lock was released, after the rows were stored, and close() then blocked forever. Now growth is best-effort, and
     * the lock is released even when returning the scratch throws.
     */
    @Test
    void failedGrowthNeitherFailsTheInsertNorWedgesClose() throws Exception {
        int n = 1_000_000;
        long[] ts = new long[n];
        int[] many = new int[n];
        for (int i = 0; i < n; i++)
            ts[i] = T0 + i;
        EmbeddedDb db = EmbeddedDb.open(tmp.resolve("o").toString(), opts());
        try {
            int s = db.series("s");
            int d = db.seriesCreate("d", ValueType.FLOAT64);
            java.util.Arrays.fill(many, s);
            Scratch.Pool p = pool(db);
            Scratch.growHook = () -> {
                throw new OutOfMemoryError("injected: malloc failed");
            };
            db.insert(s, ts, new float[n], 0, n);
            db.insert(many, ts, new float[n], 0, n);
            db.insertSeriesValues(d, ts, Column.ofDoubles(new double[n]));
            assertEquals(n, db.queryPoints(s, Long.MIN_VALUE, Long.MAX_VALUE).size());
            assertEquals(n, db.queryValues(d, Long.MIN_VALUE, Long.MAX_VALUE).size());
            assertEquals(0, p.bytes());
            assertEquals(0, p.kept());

            /* an error the cache does not catch reaches the caller, but never keeps the lock (all three paths) */
            Scratch.growHook = () -> {
                throw new AssertionError("injected");
            };
            assertThrows(AssertionError.class, () -> db.insert(s, ts, new float[n], 0, n));
            assertThrows(AssertionError.class, () -> db.insert(many, ts, new float[n], 0, n));
            assertThrows(AssertionError.class, () -> db.insertSeriesValues(d, ts, Column.ofDoubles(new double[n])));
            assertEquals(0, p.bytes(), "the reservation of a failed growth is given back");
            Scratch.growHook = null;
            db.insert(s, ts, new float[n], 0, n);       /* and the cache works again */
            assertEquals(1, p.kept());
            consistent(p);
        } finally {
            Scratch.growHook = null;
            closeWithin(db);
        }
    }

    /** Inserts whose sizes spill, fit, shrink, grow and pass the largest block kept, each read back exactly. */
    @Test
    void insertsOfChangingSizes() throws Exception {
        for (long limit : new long[] {EmbeddedOptions.DEFAULT_SCRATCH_BYTES, 0, 3 * MIB}) {
            EmbeddedDb db = EmbeddedDb.open(tmp.resolve("db" + limit).toString(), opts().scratchBytes(limit));
            try {
                insertsOfChangingSizes(db);
                consistent(pool(db));
                assertTrue(pool(db).bytes() <= limit);
            } finally {
                closeWithin(db);
            }
        }
    }

    private static void insertsOfChangingSizes(EmbeddedDb db) throws TsdbException {
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

    /** Threads inserting at once each get their own scratch: no row lands in another thread's series. */
    @Test
    void concurrentInserts() throws Exception {
        int nt = 8, calls = 20, n = 50_000;
        EmbeddedDb db = EmbeddedDb.open(tmp.resolve("c").toString(), opts());
        try {
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
                    f.get(60, TimeUnit.SECONDS);
            } finally {
                ex.shutdownNow();
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
            consistent(pool(db));
        } finally {
            closeWithin(db);
        }
    }

    /** More threads at once than the limit has blocks for: the data is right and the limit holds. */
    @Test
    void moreThreadsThanTheLimit() throws Exception {
        int nt = 80, n = 300_000; /* 3.6 MB per call: 4 MiB blocks, two fit the limit */
        EmbeddedDb db = EmbeddedDb.open(tmp.resolve("m").toString(), opts().scratchBytes(8 * MIB));
        try {
            int[] ids = new int[nt];
            for (int k = 0; k < nt; k++)
                ids[k] = db.series("s" + k);
            AtomicReference<Throwable> bad = new AtomicReference<>();
            CyclicBarrier go = new CyclicBarrier(nt);
            List<Thread> th = new ArrayList<>();
            for (int k = 0; k < nt; k++) {
                int kk = k;
                Thread t = new Thread(() -> {
                    try {
                        for (int c = 0; c < 3; c++) {
                            long[] ts = new long[n];
                            float[] v = new float[n];
                            for (int i = 0; i < n; i++) {
                                ts[i] = T0 + (long) c * n + i;
                                v[i] = kk;
                            }
                            go.await(60, TimeUnit.SECONDS);
                            db.insert(ids[kk], ts, v, 0, n);
                        }
                    } catch (Throwable e) {
                        bad.compareAndSet(null, e);
                    }
                });
                t.setDaemon(true);
                t.start();
                th.add(t);
            }
            for (Thread t : th)
                t.join(TimeUnit.SECONDS.toMillis(120));
            assertNull(bad.get());
            Scratch.Pool p = pool(db);
            consistent(p);
            assertEquals(8 * MIB, p.bytes(), "two blocks of 4 MiB");
            for (int k = 0; k < nt; k++) {
                float[] v = db.queryPoints(ids[k], Long.MIN_VALUE, Long.MAX_VALUE).values();
                assertEquals(3 * n, v.length);
                for (float x : v)
                    assertEquals(k, x);
            }
        } finally {
            closeWithin(db);
        }
    }

    /** Close races inserts of every size; afterwards the pool holds nothing. Only "closed" errors are allowed. */
    @Test
    void closeRacesInserts() throws Exception {
        int[] sizes = {1000, 300_000, 2_000_000, 3_000_000, 7};
        for (int round = 0; round < 12; round++) {
            EmbeddedDb db = EmbeddedDb.open(tmp.resolve("r" + round).toString(), opts());
            int nt = 24;
            int[] ids = new int[nt];
            for (int k = 0; k < nt; k++)
                ids[k] = k % 2 == 0 ? db.series("s" + k) : db.seriesCreate("d" + k, ValueType.FLOAT64);
            AtomicReference<Throwable> bad = new AtomicReference<>();
            CyclicBarrier go = new CyclicBarrier(nt + 1);
            List<Thread> th = new ArrayList<>();
            for (int k = 0; k < nt; k++) {
                int kk = k;
                Thread t = new Thread(() -> {
                    try {
                        go.await(60, TimeUnit.SECONDS);
                        for (int c = 0;; c++) {
                            int n = sizes[(c + kk) % sizes.length];
                            long[] ts = new long[n];
                            for (int i = 0; i < n; i++)
                                ts[i] = T0 + (long) c * 10_000_000 + i;
                            if (kk % 2 == 0) {
                                db.insert(ids[kk], ts, new float[n], 0, n);
                            } else {
                                BitSet nulls = new BitSet();
                                nulls.set(0);
                                db.insertSeriesValues(ids[kk], ts, Column.ofDoubles(new double[n]).withNulls(nulls));
                            }
                        }
                    } catch (TsdbException e) {
                        if (!String.valueOf(e.getMessage()).contains("closed"))
                            bad.compareAndSet(null, e);
                    } catch (Throwable e) {
                        bad.compareAndSet(null, e);
                    }
                });
                t.setDaemon(true);
                t.start();
                th.add(t);
            }
            go.await(60, TimeUnit.SECONDS);
            Thread.sleep(ThreadLocalRandom.current().nextInt(5, 120));
            Scratch.Pool p = pool(db);
            closeWithin(db);
            for (Thread t : th) {
                t.join(TimeUnit.SECONDS.toMillis(60));
                assertFalse(t.isAlive(), "an inserter still running after close");
            }
            assertNull(bad.get(), () -> "unexpected: " + bad.get());
            assertEquals(0, p.kept(), "kept after close");
            assertEquals(0, p.bytes(), "bytes after close");
        }
    }

    /** An insert the engine refuses still returns its scratch, once; repeated failures hold one block. */
    @Test
    void failedInsertsReturnTheirScratch() throws Exception {
        EmbeddedDb db = EmbeddedDb.open(tmp.resolve("f").toString(), opts());
        try {
            int d = db.seriesCreate("d", ValueType.FLOAT64);
            int n = 100_000;
            long[] ts = new long[n];
            for (int i = 0; i < n; i++)
                ts[i] = T0 + i;
            Scratch.Pool p = pool(db);
            for (int c = 0; c < 50; c++)
                assertThrows(TsdbException.class, () -> db.insert(d, ts, new float[n], 0, n), "float32 into float64");
            assertEquals(1, p.kept(), "one scratch, back in the pool");
            consistent(p);
            /* and the data path still works afterwards, with nulls on a reused block */
            BitSet nulls = new BitSet();
            nulls.set(3);
            double[] dv = new double[n];
            for (int i = 0; i < n; i++)
                dv[i] = i;
            db.insertSeriesValues(d, ts, Column.ofDoubles(dv).withNulls(nulls));
            ValuePoints pd = db.queryValues(d, T0, T0 + n);
            assertEquals(n, pd.size());
            for (int i = 0; i < n; i++)
                assertEquals(i == 3, pd.values().isNull(i));
            assertEquals(1, p.kept());
        } finally {
            closeWithin(db);
        }
    }
}
