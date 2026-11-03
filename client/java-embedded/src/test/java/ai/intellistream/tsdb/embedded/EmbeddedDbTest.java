package ai.intellistream.tsdb.embedded;

import ai.intellistream.tsdb.Bucket;
import ai.intellistream.tsdb.Column;
import ai.intellistream.tsdb.Group;
import ai.intellistream.tsdb.Order;
import ai.intellistream.tsdb.Points;
import ai.intellistream.tsdb.QueryStats;
import ai.intellistream.tsdb.Stats;
import ai.intellistream.tsdb.SpreadBucket;
import ai.intellistream.tsdb.Status;
import ai.intellistream.tsdb.TsdbException;
import ai.intellistream.tsdb.TwaBucket;
import ai.intellistream.tsdb.TwaOptions;
import ai.intellistream.tsdb.Unit;
import ai.intellistream.tsdb.ValuePoints;
import ai.intellistream.tsdb.ValueType;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The embedded database: the TCP client's tests, plus what only an in-process binding can get wrong. */
class EmbeddedDbTest {
    static final long T0 = 1_704_067_200_000L; /* 2024-01-01 UTC */

    @TempDir
    Path tmp;
    EmbeddedDb db;

    @BeforeEach
    void open() throws TsdbException {
        db = EmbeddedDb.open(tmp.resolve("db").toString(), new EmbeddedOptions().fsync(false).walSyncMs(-1));
    }

    @AfterEach
    void close() throws TsdbException {
        db.close();
        assertEquals(0, Native.OUTSTANDING.get(), "every array the library returned is freed");
    }

    static int status(ThrowingRunnable r) {
        return assertThrows(TsdbException.class, r::run).status();
    }

    interface ThrowingRunnable {
        void run() throws Exception;
    }

    @Test
    void libraryAndOptions() throws TsdbException {
        assertTrue(EmbeddedDb.librarySource().length() > 0);
        assertTrue(EmbeddedDb.simdLevel().length() > 0);
        assertEquals("not found", EmbeddedDb.strerror(Status.ERR_NOTFOUND).toLowerCase().replace("series ", ""),
                "the engine's text");
        assertTrue(new EmbeddedOptions().toString().contains("granuleRows=8192"));
        /* the fields after obsolete_part_lifetime_s, read from tsdb_options_init and written back at their offsets */
        String defaults = new EmbeddedOptions().toString();
        assertTrue(defaults.contains("verifyChecksums=false, pinIndex=false, retentionMergeS=3600, createVolumes=true,"),
                defaults);
        assertTrue(db.isOpen());
        assertTrue(db.toString().contains("db"));
    }

    @Test
    void series() throws TsdbException {
        long a = db.series("a");
        assertEquals(a, db.series("a"));
        assertEquals(a, db.series("a", false));
        assertEquals(Status.ERR_NOTFOUND, status(() -> db.series("missing", false)));
        assertEquals("a", db.seriesName(a));
        assertEquals(Status.ERR_NOTFOUND, status(() -> db.seriesName(12345)));
        assertThrows(IllegalArgumentException.class, () -> db.series(""));
        assertEquals(1, db.seriesCount());
        assertEquals(1, db.volumeCount());
        assertEquals(db.volumePath(0), db.seriesPath(a));
        long b = db.seriesAt("b", tmp.resolve("vol2").toString());
        assertEquals(2, db.volumeCount());
        assertTrue(db.seriesPath(b).endsWith("vol2"));
        assertEquals(ValueType.FLOAT32, db.seriesType(b));
        long[] ids = db.seriesIds(); /* no order across volumes */
        java.util.Arrays.sort(ids);
        long[] want = {a, b};
        java.util.Arrays.sort(want);
        assertArrayEquals(want, ids);
        assertEquals(2, db.seriesCount());
    }

    /** Enough ids that tsdb_series_foreach's upcall grows its array several times. */
    @Test
    void seriesIdsListsEverySeries() throws TsdbException {
        java.util.Set<Long> want = new java.util.HashSet<>();
        for (int i = 0; i < 1000; i++)
            want.add(db.series("s" + i));
        long[] ids = db.seriesIds();
        assertEquals(1000, ids.length);
        assertEquals(want, java.util.Arrays.stream(ids).boxed().collect(java.util.stream.Collectors.toSet()));
        assertEquals(1000, db.seriesCount());
    }

    /**
     * Ids are (volume + 1) << 32 | local: every real id is past 2^32 and taken whole, in the database directory and
     * in another volume. A malformed id (a zero volume field, as a truncated id has; any of bits 48-63; a volume the
     * database does not have; a local number past the volume's series) is unknown to every call, never the series of
     * its low 32 bits (here series a, which must not change). Queries answer INVALID for an id no volume can have.
     */
    @Test
    void idsCarryTheVolumeAndMalformedOnesAreUnknown() throws TsdbException {
        db.close();
        db = EmbeddedDb.open(tmp.resolve("db").toString(),
                new EmbeddedOptions().fsync(false).walSyncMs(-1).createVolumes(true));
        long a = db.series("a");
        long st = db.seriesCreate("st", ValueType.STATE);
        long c = db.seriesAt("c", tmp.resolve("volume").toString());
        assertEquals(1L << 32, a);
        assertEquals((1L << 32) | 1, st);
        assertEquals(2L << 32, c);
        db.insert(a, new long[] {T0}, new float[] {1});
        db.insert(c, new long[] {T0}, new float[] {3});
        assertEquals("c", db.seriesName(c));
        long[] ts = {T0, T0 + 1000};
        float[] v = {7, 8};
        for (long big : new long[] {a & 0xFFFF_FFFFL, st & 0xFFFF_FFFFL, a | 1L << 48, -1L, Long.MIN_VALUE | a,
                a + (2L << 32), c + 1, a + 1000}) {
            String what = Long.toUnsignedString(big);
            assertEquals(Status.ERR_INVALID, status(() -> db.insert(big, ts, v)), what);
            assertEquals(Status.ERR_INVALID, status(() -> db.insert(new long[] {a, big}, ts, v)), what);
            assertEquals(Status.ERR_INVALID, status(() -> db.insertSeriesValues(big, ts, Column.ofFloats(v))), what);
            assertEquals(Status.ERR_INVALID, status(() -> db.append(big, T0 + 5000, 9)), what);
            assertEquals(Status.ERR_INVALID, status(() -> db.deleteRange(big, Long.MIN_VALUE, Long.MAX_VALUE)), what);
            assertEquals(Status.ERR_NOTFOUND, status(() -> db.seriesName(big)), what);
            assertEquals(Status.ERR_NOTFOUND, status(() -> db.seriesPath(big)), what);
            assertEquals(Status.ERR_NOTFOUND, status(() -> db.seriesType(big)), what);
            assertEquals(Status.ERR_NOTFOUND, status(() -> db.retention(big)), what);
            assertEquals(Status.ERR_NOTFOUND, status(() -> db.state(big, "on")), what);
            var multi = db.queryValuesMulti(new long[] {big, a}, Long.MIN_VALUE, Long.MAX_VALUE, Order.ASC, 0,
                    ai.intellistream.tsdb.MultiOptions.defaults());
            assertEquals(1, multi.get(1).get().size(), what);
            long lo = Long.MIN_VALUE, hi = Long.MAX_VALUE;
            if ((big >>> 48) != 0 || (big >>> 32) == 0) { /* no volume can have it: INVALID, not an empty range */
                assertEquals(Status.ERR_INVALID, status(() -> db.queryValues(big, lo, hi)), what);
                assertEquals(Status.ERR_INVALID, status(() -> db.queryPoints(big, lo, hi)), what);
                assertEquals(Status.ERR_INVALID, status(() -> db.queryAgg(big, lo, hi, 0)), what);
                assertEquals(Status.ERR_INVALID, multi.get(0).status(), what);
            } else {
                assertEquals(Status.ERR_NOTFOUND, status(() -> db.queryValues(big, lo, hi)), what);
                assertEquals(0, multi.get(0).get().size(), what);
            }
        }
        assertArrayEquals(new float[] {1}, db.queryPoints(a, Long.MIN_VALUE, Long.MAX_VALUE).values());
        assertArrayEquals(new float[] {3}, db.queryPoints(c, Long.MIN_VALUE, Long.MAX_VALUE).values());
        assertEquals("a", db.seriesName(a));
        assertEquals(3, db.seriesCount());
    }

    @Test
    void insertAndStream() throws TsdbException {
        long s = db.series("x");
        int n = 100_000; /* more than CHUNK: several chunks */
        long[] ts = new long[n];
        float[] v = new float[n];
        for (int i = 0; i < n; i++) {
            ts[n - 1 - i] = T0 + i * 1000L; /* any order */
            v[n - 1 - i] = i * 0.5f;
        }
        db.insert(s, ts, v);
        Points p = db.queryPoints(s, Long.MIN_VALUE, Long.MAX_VALUE);
        assertEquals(n, p.size());
        for (int i = 0; i < n; i++) {
            assertEquals(T0 + i * 1000L, p.timestamp(i));
            assertEquals(i * 0.5f, p.value(i));
        }
        long[] seen = {0, 0};
        long[] last = {Long.MIN_VALUE};
        assertTrue(db.queryRaw(s, Long.MIN_VALUE, Long.MAX_VALUE, (t, x, k) -> {
            assertTrue(k <= EmbeddedDb.CHUNK && k > 0);
            for (int i = 0; i < k; i++) {
                assertTrue(t[i] > last[0]);
                last[0] = t[i];
                assertEquals((t[i] - T0) / 1000 * 0.5f, x[i]);
            }
            seen[0] += k;
            seen[1]++;
            return true;
        }));
        assertEquals(n, seen[0]);
        assertEquals((n + EmbeddedDb.CHUNK - 1) / EmbeddedDb.CHUNK, seen[1], "full chunks as over TCP");
        /* stopping */
        AtomicInteger calls = new AtomicInteger();
        assertFalse(db.queryRaw(s, Long.MIN_VALUE, Long.MAX_VALUE, (t, x, k) -> calls.incrementAndGet() > 5));
        assertEquals(1, calls.get());
        /* empty */
        assertTrue(db.queryRaw(s, 0, 1, (t, x, k) -> {
            throw new AssertionError("no rows");
        }));
        assertEquals(0, db.queryPoints(s, 0, 1).size());
    }

    @Test
    void offsetsManySeriesAndEmptyInserts() throws TsdbException {
        long a = db.series("a");
        long b = db.series("b");
        long[] ts = {T0, T0 + 1, T0 + 2, T0 + 3};
        float[] v = {1, 2, 3, 4};
        db.insert(a, ts, v, 1, 2);
        db.insert(new long[] {a, b, a, b}, ts, v, 0, 4);
        db.insert(a, new long[0], new float[0]);
        db.insertAsync(a, new long[] {T0 + 9}, new float[] {9});
        db.await();
        assertEquals(0, db.pending());
        assertThrows(IndexOutOfBoundsException.class, () -> db.insert(a, ts, v, 3, 2));
        assertArrayEquals(new long[] {T0, T0 + 1, T0 + 2, T0 + 9}, db.queryPoints(a, T0, T0 + 10).timestamps());
        assertArrayEquals(new float[] {1, 2, 3, 9}, db.queryPoints(a, T0, T0 + 10).values());
        assertArrayEquals(new float[] {2, 4}, db.queryPoints(b, T0, T0 + 10).values());
    }

    @Test
    void lastWriteWinsAndCorrections() throws TsdbException {
        long s = db.series("x");
        db.insert(s, new long[] {T0, T0 + 1, T0}, new float[] {1, 2, 3}); /* later row of a batch wins */
        db.append(s, T0 + 1, 20);
        db.flush();
        db.insert(s, new long[] {T0 + 1}, new float[] {200});
        assertArrayEquals(new float[] {3, 200}, db.queryPoints(s, T0, T0 + 2).values());
        db.optimize();
        assertArrayEquals(new float[] {3, 200}, db.queryPoints(s, T0, T0 + 2).values());
        db.commit();
    }

    @Test
    void descendingWithLimit() throws TsdbException {
        long s = db.series("x");
        long[] ts = new long[50];
        float[] v = new float[50];
        for (int i = 0; i < 50; i++) {
            ts[i] = T0 + i;
            v[i] = i;
        }
        db.insert(s, ts, v);
        assertArrayEquals(new float[] {49, 48, 47}, db.queryPoints(s, T0, T0 + 50, Order.DESC, 3).values());
        assertArrayEquals(new float[] {0, 1}, db.queryPoints(s, T0, T0 + 50, Order.ASC, 2).values());
        List<Float> got = new ArrayList<>();
        db.queryRaw(s, T0, T0 + 50, Order.DESC, 4, (t, x, k) -> {
            for (int i = 0; i < k; i++)
                got.add(x[i]);
            return true;
        });
        assertEquals(List.of(49f, 48f, 47f, 46f), got);
        List<Bucket> b = db.queryAgg(s, T0, T0 + 50, 10, Order.DESC, 2);
        assertEquals(2, b.size());
        assertEquals(T0 + 40, b.get(0).start());
        assertEquals(T0 + 30, b.get(1).start());
        assertThrows(IllegalArgumentException.class, () -> db.queryPoints(s, T0, T0 + 1, Order.ASC, -1));
    }

    @Test
    void everyTypeRoundTripsWithNulls() throws TsdbException {
        BitSet nulls = new BitSet();
        nulls.set(1);
        nulls.set(3);
        long[] ts = {T0, T0 + 1, T0 + 2, T0 + 3, T0 + 4};
        Column[] cols = {
            Column.ofFloats(1.5f, 0, Float.NaN, 4, Float.POSITIVE_INFINITY).withNulls(nulls),
            Column.ofDoubles(1.5, 0, -0.0, 4, Double.NEGATIVE_INFINITY).withNulls(nulls),
            Column.ofLongs(Long.MAX_VALUE, 0, -7, 4, Long.MIN_VALUE + 1).withNulls(nulls),
            Column.ofBools(true, false, false, true, true).withNulls(nulls),
            null,
        };
        long st = db.seriesCreate("state", ValueType.STATE);
        int on = db.state(st, "on");
        int off = db.state(st, "off");
        assertEquals(on, db.state(st, "on"));
        assertEquals(off, db.stateLookup(st, "off"));
        assertEquals("on", db.stateName(st, on));
        assertEquals(Status.ERR_NOTFOUND, status(() -> db.stateLookup(st, "nope")));
        assertEquals(Status.ERR_NOTFOUND, status(() -> db.stateName(st, 999)));
        cols[4] = Column.ofStates(on, off, off, on, off).withNulls(nulls);
        for (Column c : cols) {
            long s = c.type() == ValueType.STATE ? st : db.seriesCreate("t" + c.type(), c.type());
            assertEquals(c.type(), db.seriesType(s));
            db.insertSeriesValues(s, ts, c);
            ValuePoints p = db.queryValues(s, T0, T0 + 10);
            assertEquals(5, p.size());
            assertArrayEquals(ts, p.timestamps());
            assertEquals(c.type(), p.type());
            for (int i = 0; i < 5; i++) {
                assertEquals(c.isNull(i), p.values().isNull(i), c.type() + " null " + i);
                if (!c.isNull(i))
                    assertEquals(Double.doubleToRawLongBits(c.getDouble(i)),
                            Double.doubleToRawLongBits(p.values().getDouble(i)), c.type() + " value " + i);
            }
            /* streamed, in DESC with a limit */
            List<Long> seen = new ArrayList<>();
            assertTrue(db.queryValues(s, T0, T0 + 10, Order.DESC, 3, (t, col, k) -> {
                assertEquals(k, col.size());
                for (int i = 0; i < k; i++) {
                    seen.add(t[i]);
                    assertEquals(c.isNull((int) (t[i] - T0)), col.isNull(i));
                }
                return true;
            }));
            assertEquals(List.of(T0 + 4, T0 + 3, T0 + 2), seen);
        }
        long f = db.series("f32");
        assertEquals(Status.ERR_INVALID, status(() -> db.insertSeriesValues(f, ts, cols[1])), "type mismatch");
        assertEquals(Status.ERR_INVALID, status(() -> db.seriesCreate("f32", ValueType.INT64)));
        long i64 = db.seriesCreate("tINT64", ValueType.INT64);
        assertEquals(Status.ERR_INVALID, status(() -> db.queryPoints(i64, T0, T0 + 10)));
    }

    @Test
    void manySeriesTypedInsertAndOffsets() throws TsdbException {
        long a = db.seriesCreate("a", ValueType.FLOAT64);
        long b = db.seriesCreate("b", ValueType.FLOAT64);
        BitSet nulls = new BitSet();
        nulls.set(2);
        Column c = Column.ofDoubles(0, 1, 2, 3, 4, 5).withNulls(nulls);
        long[] ts = {T0, T0, T0 + 1, T0 + 1, T0 + 2, T0 + 2};
        db.insertValues(new long[] {a, b, a, b, a, b}, ts, c, 1, 5); /* the bitmap shifts with the offset */
        ValuePoints pa = db.queryValues(a, T0, T0 + 10);
        assertArrayEquals(new long[] {T0 + 1, T0 + 2}, pa.timestamps());
        assertTrue(pa.values().isNull(0));
        assertEquals(4.0, pa.values().getDouble(1));
        ValuePoints pb = db.queryValues(b, T0, T0 + 10);
        assertArrayEquals(new double[] {1, 3, 5}, pb.values().doubles());
        assertFalse(pb.values().hasNulls());
    }

    @Test
    void reservedValuesReadBackAsNull() throws TsdbException {
        long f = db.seriesCreate("f", ValueType.FLOAT32);
        long i = db.seriesCreate("i", ValueType.INT64);
        db.insertSeriesValues(f, new long[] {T0, T0 + 1}, Column.ofFloats(Float.intBitsToFloat(0x7fc01001), 1));
        db.insertSeriesValues(i, new long[] {T0}, Column.ofLongs(Long.MIN_VALUE));
        assertTrue(db.queryValues(f, T0, T0 + 2).values().isNull(0));
        assertTrue(Float.isNaN(db.queryPoints(f, T0, T0 + 2).value(0)), "a null reads as NaN through the float API");
        assertTrue(db.queryValues(i, T0, T0 + 2).values().isNull(0));
    }

    @Test
    void aggregates() throws TsdbException {
        long s = db.series("x");
        long[] ts = new long[100];
        float[] v = new float[100];
        for (int i = 0; i < 100; i++) {
            ts[i] = T0 + i * 1000L;
            v[i] = i;
        }
        v[50] = Float.NaN;
        db.insert(s, ts, v);
        List<Bucket> one = db.queryAgg(s, T0, T0 + 100_000, 0);
        assertEquals(1, one.size());
        assertEquals(100, one.get(0).count());
        assertTrue(Double.isNaN(one.get(0).sum()), "NaN propagates into sum");
        assertEquals(0, one.get(0).min());
        assertEquals(99, one.get(0).max(), "NaN is ignored by max");
        List<Bucket> tens = db.queryAgg(s, T0, T0 + 100_000, 10_000);
        assertEquals(10, tens.size());
        assertEquals(45, tens.get(0).sum());
        assertEquals(4.5, tens.get(0).avg());
        long n = db.seriesCreate("n", ValueType.INT64);
        db.insertSeriesValues(n, new long[] {T0, T0 + 1, T0 + 2}, Column.ofLongs(Long.MAX_VALUE, Long.MAX_VALUE, -1));
        Bucket b = db.queryAgg(n, T0, T0 + 3, 0).get(0);
        assertEquals(Long.MAX_VALUE, b.isum(), "clamped exact sum");
        assertEquals(-1, b.imin());
        assertEquals(Long.MAX_VALUE, b.imax());
        assertEquals(Status.ERR_INVALID, status(() -> db.queryBounds(s, T0, T0 + 1, new long[] {5, 5})));
        assertThrows(IllegalArgumentException.class, () -> db.queryBounds(s, T0, T0 + 1, new long[0]));
        List<Bucket> bb = db.queryBounds(s, T0, T0 + 100_000, new long[] {T0 + 10_000, T0 + 95_000});
        assertEquals(2, bb.size());
        assertEquals(85, bb.get(0).count());
        assertEquals(T0 + 95_000, bb.get(1).start());
    }

    @Test
    void spreadTwaAndLttb() throws TsdbException {
        long s = db.seriesCreate("sp", ValueType.FLOAT64);
        db.insertSeriesValues(s, new long[] {T0, T0 + 1000, T0 + 2000, T0 + 3000}, Column.ofDoubles(1, 2, 3, 4));
        SpreadBucket all = db.querySpread(s, T0, T0 + 4000, 0).get(0);
        assertEquals(4, all.bucket().count());
        assertEquals(3, all.range());
        assertEquals(1.25, all.varPop(), 1e-12);
        assertEquals(5.0 / 3, all.varSamp(), 1e-12);
        assertEquals(Math.sqrt(1.25), all.stddevPop(), 1e-12);
        List<SpreadBucket> last = db.querySpread(s, T0, T0 + 4000, 2000, Order.DESC, 1);
        assertEquals(1, last.size());
        assertEquals(T0 + 2000, last.get(0).bucket().start());
        assertEquals(0.25, last.get(0).varPop(), 1e-12);
        assertEquals(db.querySpread(s, T0, T0 + 4000, 2000), db.querySpreadBounds(s, T0, T0 + 4000,
                new long[] {T0, T0 + 2000}, Order.ASC, 0));
        assertEquals(db.querySpread(s, T0, T0 + 4000, 0).get(0).bucket(), db.queryAgg(s, T0, T0 + 4000, 0).get(0));
        assertEquals(1, db.querySpreadGroup(s, Long.MIN_VALUE, Long.MAX_VALUE, Group.of(Unit.DAY).in("Europe/Oslo"),
                Order.ASC, 0).size());
        assertEquals(Status.ERR_INVALID, status(() -> db.querySpreadBounds(s, T0, T0 + 1, new long[] {5, 5},
                Order.ASC, 0)));
        assertThrows(IllegalArgumentException.class, () -> db.querySpreadBounds(s, T0, T0 + 1, new long[0],
                Order.ASC, 0));
        assertThrows(IllegalArgumentException.class, () -> db.querySpread(s, T0, T0 + 1, 0, Order.ASC, -1));

        long t = db.seriesCreate("tw", ValueType.FLOAT64);
        db.insertSeriesValues(t, new long[] {T0, T0 + 1000, T0 + 3000}, Column.ofDoubles(0, 10, 10));
        TwaBucket step = db.queryTwa(t, T0, T0 + 3000, 0, TwaOptions.step()).get(0);
        assertEquals(20_000, step.area(), 1e-9);
        assertEquals(3000, step.coveredMs());
        assertEquals(2, step.count());
        assertEquals(20.0 / 3, step.twa(), 1e-12);
        assertEquals(25_000, db.queryTwa(t, T0, T0 + 3000, 0, TwaOptions.linear()).get(0).area(), 1e-9);
        TwaBucket gap = db.queryTwa(t, T0, T0 + 3000, 0, TwaOptions.step().maxGap(1500)).get(0);
        assertEquals(1000, gap.coveredMs(), "the 2 s segment is stale");
        List<TwaBucket> desc = db.queryTwa(t, T0, T0 + 3000, 1000, TwaOptions.step().order(Order.DESC).limit(2));
        assertEquals(List.of(T0 + 2000, T0 + 1000), desc.stream().map(TwaBucket::start).toList());
        assertEquals(db.queryTwa(t, T0, T0 + 3000, 1000, TwaOptions.linear()),
                db.queryTwaBounds(t, T0, T0 + 3000, new long[] {T0, T0 + 1000, T0 + 2000}, TwaOptions.linear()));
        assertEquals(db.queryTwa(t, T0, T0 + 3000, 0, TwaOptions.linear()),
                db.queryTwaGroup(t, T0, T0 + 3000, null, TwaOptions.linear()));
        assertEquals(1, db.queryTwaGroup(t, Long.MIN_VALUE, Long.MAX_VALUE, Group.of(Unit.DAY).in("Asia/Tokyo"),
                TwaOptions.step()).size());
        long st = db.seriesCreate("st", ValueType.STATE);
        assertEquals(Status.ERR_INVALID, status(() -> db.queryTwa(st, T0, T0 + 1, 0, TwaOptions.step())));
        assertEquals(Status.ERR_INVALID, status(() -> db.queryTwa(t, T0, T0 + 1, -1, TwaOptions.step())));

        long l = db.series("lt");
        long[] ts = new long[1000];
        float[] v = new float[1000];
        for (int i = 0; i < ts.length; i++) {
            ts[i] = T0 + i * 1000L;
            v[i] = (float) Math.sin(i / 30.0);
        }
        db.insert(l, ts, v);
        ValuePoints p = db.queryLttb(l, Long.MIN_VALUE, Long.MAX_VALUE, 50);
        assertEquals(50, p.size());
        assertEquals(ts[0], p.timestamps()[0]);
        assertEquals(ts[999], p.timestamps()[49]);
        ValuePoints newest = db.queryLttb(l, Long.MIN_VALUE, Long.MAX_VALUE, 50, Order.DESC, 10);
        assertEquals(10, newest.size());
        for (int i = 0; i < 10; i++) {
            assertEquals(p.timestamps()[49 - i], newest.timestamps()[i]);
            assertEquals(p.values().getDouble(49 - i), newest.values().getDouble(i));
        }
        assertEquals(1000, db.queryLttb(l, Long.MIN_VALUE, Long.MAX_VALUE, 5000).size(), "fewer points than asked");
        assertEquals(1000, db.queryLttb(l, Long.MIN_VALUE, Long.MAX_VALUE, -1).size(), "as tsdbd: a huge size_t");
        assertEquals(Status.ERR_INVALID, status(() -> db.queryLttb(l, T0, T0 + 1, 2)));
        assertEquals(Status.ERR_INVALID, status(() -> db.queryLttb(st, T0, T0 + 1, 10)));
        assertEquals(0, db.queryLttb(l, 0, 1, 10).size());
        /* as tsdbd and the Rust Db: the threshold first, then an unknown series has no points */
        assertEquals(Status.ERR_INVALID, status(() -> db.queryLttb(987_654, T0, T0 + 1, 2)));
        ValuePoints none = db.queryLttb((1L << 32) | 987_654, Long.MIN_VALUE, Long.MAX_VALUE, 10, Order.DESC, 3);
        assertEquals(0, none.size());
        assertEquals(ValueType.FLOAT32, none.values().type());

        List<Bucket> asc = db.queryAgg(l, T0, T0 + 1_000_000, 60_000);
        List<Bucket> top = db.queryAgg(l, T0, T0 + 1_000_000, 60_000, Order.DESC, 3);
        assertEquals(List.of(asc.get(asc.size() - 1), asc.get(asc.size() - 2), asc.get(asc.size() - 3)), top);
        assertEquals(asc.subList(0, 2), db.queryAgg(l, T0, T0 + 1_000_000, 60_000, Order.ASC, 2));
    }

    @Test
    void deleteReplaceAndRetention() throws TsdbException {
        long s = db.seriesCreate("d", ValueType.INT64);
        long[] ts = new long[10];
        long[] v = new long[10];
        for (int i = 0; i < 10; i++) {
            ts[i] = T0 + i * 1000L;
            v[i] = i;
        }
        db.insertSeriesValues(s, ts, Column.ofLongs(v));
        db.deleteRange(s, T0 + 2000, T0 + 5000);
        ValuePoints left = db.queryValues(s, Long.MIN_VALUE, Long.MAX_VALUE);
        assertArrayEquals(new long[] {ts[0], ts[1], ts[5], ts[6], ts[7], ts[8], ts[9]}, left.timestamps());
        assertEquals(7, db.queryAgg(s, Long.MIN_VALUE, Long.MAX_VALUE, 0).get(0).count());
        db.deleteRange(s, T0 + 9000, T0 + 1000); /* start >= end deletes nothing */
        assertEquals(7, db.queryValues(s, Long.MIN_VALUE, Long.MAX_VALUE).size());
        db.insertSeriesValues(s, new long[] {T0 + 3000}, Column.ofLongs(33)); /* a write after the delete is seen */
        assertEquals(8, db.queryValues(s, Long.MIN_VALUE, Long.MAX_VALUE).size());

        db.replaceRange(s, T0, T0 + 6000, new long[] {T0 + 500, T0 + 5999}, Column.ofLongs(-1, -2)
                .withNulls(BitSet.valueOf(new long[] {2})));
        ValuePoints r = db.queryValues(s, Long.MIN_VALUE, Long.MAX_VALUE);
        assertArrayEquals(new long[] {T0 + 500, T0 + 5999, ts[6], ts[7], ts[8], ts[9]}, r.timestamps());
        assertEquals(-1, r.values().longs()[0]);
        assertTrue(r.values().isNull(1), "a null replaces like a value");
        db.replaceRange(s, T0 + 9000, T0 + 10_000, new long[0], Column.ofLongs()); /* no rows: a delete */
        assertEquals(5, db.queryValues(s, Long.MIN_VALUE, Long.MAX_VALUE).size());
        assertEquals(Status.ERR_INVALID, status(() -> db.replaceRange(s, T0, T0 + 10, new long[] {T0 + 10},
                Column.ofLongs(1))));
        assertEquals(Status.ERR_INVALID, status(() -> db.replaceRange(s, T0, T0 + 10, new long[] {T0},
                Column.ofDoubles(1))));
        assertThrows(IllegalArgumentException.class, () -> db.replaceRange(s, T0, T0 + 10, new long[] {T0, T0 + 1},
                Column.ofLongs(1)));
        assertEquals(5, db.queryValues(s, Long.MIN_VALUE, Long.MAX_VALUE).size(), "a refused replace changes nothing");

        assertEquals(0, db.retention(s));
        db.setRetention(s, 1000); /* the rows are from 2024: all expired */
        assertEquals(1000, db.retention(s));
        assertEquals(0, db.queryValues(s, Long.MIN_VALUE, Long.MAX_VALUE).size());
        db.setRetention(s, 0); /* merges have not removed them yet */
        assertEquals(5, db.queryValues(s, Long.MIN_VALUE, Long.MAX_VALUE).size());
        assertEquals(Status.ERR_INVALID, status(() -> db.setRetention(s, -1)));
        assertEquals(Status.ERR_NOTFOUND, status(() -> db.retention(12345)));
        db.close();
        db = EmbeddedDb.open(tmp.resolve("db").toString(), new EmbeddedOptions().readOnly(true));
        assertEquals(Status.ERR_READONLY, status(() -> db.deleteRange(s, T0, T0 + 1)));
        assertEquals(5, db.queryValues(s, Long.MIN_VALUE, Long.MAX_VALUE).size(), "durable");
    }

    @Test
    void groupInUtcAndInZones() throws TsdbException {
        long s = db.series("x");
        int n = 24 * 400; /* hourly for 400 days: DST changes both ways, month and year ends */
        long[] ts = new long[n];
        float[] v = new float[n];
        for (int i = 0; i < n; i++) {
            ts[i] = T0 + i * 3_600_000L;
            v[i] = i % 17;
        }
        db.insert(s, ts, v);
        List<Bucket> days = db.queryGroup(s, Long.MIN_VALUE, Long.MAX_VALUE, Group.of(Unit.DAY));
        assertEquals(400, days.size());
        assertEquals(24, days.get(0).count());
        assertEquals(days, db.queryAgg(s, Long.MIN_VALUE, Long.MAX_VALUE, 86_400_000L));
        assertEquals(1, db.queryGroup(s, T0, T0 + 86_400_000L, null).size());
        String[] zones = {"Europe/Oslo", "America/St_Johns", "Asia/Kolkata", "Australia/Lord_Howe", "+05:30"};
        Unit[] units = {Unit.HOUR, Unit.DAY, Unit.WEEK, Unit.MONTH, Unit.QUARTER, Unit.YEAR};
        for (String z : zones) {
            for (Unit u : units) {
                for (int count : new int[] {1, 3}) {
                    Group g = Group.of(u, count).in(z);
                    for (long[] r : new long[][] {{Long.MIN_VALUE, Long.MAX_VALUE}, {T0 + 7_777_777, T0 + 99 * 86_400_000L}}) {
                        List<Bucket> java = db.queryGroup(s, r[0], r[1], g);
                        assertEquals(db.queryGroupNative(s, r[0], r[1], g, Order.ASC, 0), java, z + " " + u + count);
                        assertEquals(db.queryGroupNative(s, r[0], r[1], g, Order.DESC, 2),
                                db.queryGroup(s, r[0], r[1], g, Order.DESC, 2));
                        long sum = 0;
                        for (Bucket b : java)
                            sum += b.count();
                        if (r[0] == Long.MIN_VALUE)
                            assertEquals(n, sum);
                    }
                }
                assertArrayEquals(EmbeddedDb.calendarBoundsNative(z, u.ordinal() + 1, 1, T0, T0 + 400 * 86_400_000L),
                        Group.of(u).in(z).bounds(T0, T0 + 400 * 86_400_000L), z + " " + u);
            }
        }
        /* more local periods than the calendar computes (64 Mi seconds is 777 days): TOO_MANY_BUCKETS from every
         * grouped call, as from the TCP clients */
        long wide = db.series("wide");
        db.insert(wide, new long[] {T0, T0 + 800 * 86_400_000L}, new float[] {1, 2});
        Group secs = Group.of(Unit.SECOND).in("Europe/Oslo");
        assertEquals(Status.ERR_TOO_MANY_BUCKETS, status(() -> db.queryGroup(wide, Long.MIN_VALUE, Long.MAX_VALUE,
                secs)));
        assertEquals(Status.ERR_TOO_MANY_BUCKETS, status(() -> db.querySpreadGroup(wide, Long.MIN_VALUE,
                Long.MAX_VALUE, secs, Order.ASC, 0)));
        assertEquals(Status.ERR_TOO_MANY_BUCKETS, status(() -> db.queryTwaGroup(wide, Long.MIN_VALUE, Long.MAX_VALUE,
                secs, TwaOptions.step())));
        long empty = db.series("empty");
        assertEquals(0, db.queryGroup(empty, Long.MIN_VALUE, Long.MAX_VALUE, Group.of(Unit.DAY).in("Europe/Oslo"))
                .size());
    }

    @Test
    void errorsAndClosedDatabase() throws Exception {
        assertEquals(Status.ERR_NOTFOUND, status(() -> db.queryValues((1L << 32) | 4242, 0, 1)));
        assertEquals(Status.ERR_INVALID, status(() -> db.queryValues(4242, 0, 1))); /* no volume field: malformed */
        assertEquals(Status.ERR_NOTFOUND, status(() -> db.seriesType(4242)));
        /* a second open of a directory being written fails cleanly */
        TsdbException locked = assertThrows(TsdbException.class, () -> EmbeddedDb.open(db.directory()));
        assertEquals(Status.ERR_LOCKED, locked.status());
        assertTrue(locked.getMessage().contains(db.directory()));
        try (EmbeddedDb ro = EmbeddedDb.open(db.directory(), new EmbeddedOptions().readOnly(true))) {
            assertEquals(Status.ERR_READONLY, status(() -> ro.series("new")));
        }
        assertNull(db.failed());
        EmbeddedDb other = EmbeddedDb.open(tmp.resolve("other").toString());
        other.close();
        other.close(); /* twice is fine */
        assertFalse(other.isOpen());
        assertEquals(Status.ERR_IO, status(() -> other.series("x")));
        assertEquals(Status.ERR_IO, status(() -> other.queryAgg(0, 0, 1, 0)));
    }

    @Test
    void exceptionInAConsumerStopsTheQuery() throws TsdbException {
        long s = db.series("x");
        int n = 3 * EmbeddedDb.CHUNK;
        long[] ts = new long[n];
        float[] v = new float[n];
        for (int i = 0; i < n; i++)
            ts[i] = T0 + i;
        db.insert(s, ts, v);
        AtomicInteger calls = new AtomicInteger();
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> db.queryRaw(s, Long.MIN_VALUE, Long.MAX_VALUE, (t, x, k) -> {
                    calls.incrementAndGet();
                    throw new IllegalStateException("boom");
                }));
        assertEquals("boom", e.getMessage());
        assertEquals(1, calls.get(), "no chunk after the exception");
        StackOverflowError so = assertThrows(StackOverflowError.class,
                () -> db.queryValues(s, Long.MIN_VALUE, Long.MAX_VALUE, (t, c, k) -> {
                    throw new StackOverflowError();
                }));
        assertTrue(so != null);
        /* the database and this thread's buffers still work */
        assertEquals(n, db.queryPoints(s, Long.MIN_VALUE, Long.MAX_VALUE).size());
        long[] count = {0};
        db.queryRaw(s, Long.MIN_VALUE, Long.MAX_VALUE, (t, x, k) -> {
            count[0] += k;
            return true;
        });
        assertEquals(n, count[0]);
        /* closing from a consumer is refused rather than deadlocking */
        assertThrows(IllegalStateException.class, () -> db.queryRaw(s, T0, T0 + 1, (t, x, k) -> {
            try {
                db.close();
            } catch (TsdbException ex) {
                throw new AssertionError(ex);
            }
            return true;
        }));
        /* a nested query from a consumer gets its own buffers */
        long[] nested = {0};
        db.queryRaw(s, T0, T0 + 10, (t, x, k) -> {
            try {
                nested[0] += db.queryPoints(s, T0, T0 + 5).size();
                db.queryRaw(s, T0, T0 + 3, (t2, x2, k2) -> {
                    nested[0] += k2;
                    return true;
                });
            } catch (TsdbException ex) {
                throw new AssertionError(ex);
            }
            return true;
        });
        assertEquals(8, nested[0]);
    }

    @Test
    void maintenanceStatsAndParts() throws TsdbException {
        long s = db.series("x");
        for (int k = 0; k < 3; k++)
            db.insert(s, new long[] {T0 + k * 86_400_000L}, new float[] {k});
        db.flush();
        Stats st = db.stats();
        assertEquals(3, st.rows());
        assertTrue(st.parts() >= 1);
        List<PartInfo> parts = db.parts();
        assertEquals(st.parts(), parts.size());
        long rows = 0;
        for (PartInfo p : parts) {
            rows += p.rows();
            assertTrue(p.name().length() > 0);
            assertTrue(p.minTs() <= p.maxTs());
            /* a row a day: class 0 (too few rows to measure a rate), the day the row lies in */
            assertEquals(0, p.cls());
            assertEquals(p.partition() * 86_400_000L, p.startMs());
            assertEquals(p.startMs() + 86_400_000L, p.endMs());
            assertTrue(p.startMs() <= p.minTs() && p.maxTs() < p.endMs(), p.toString());
        }
        assertEquals(3, rows);
        assertEquals(0, db.seriesClass(s));
        db.dropBefore(T0 + 86_400_000L);
        assertEquals(2, db.queryPoints(s, Long.MIN_VALUE, Long.MAX_VALUE).size());
        db.queryAgg(s, Long.MIN_VALUE, Long.MAX_VALUE, 0);
        QueryStats q = db.lastQueryStats();
        assertEquals(2, q.rows());
        assertEquals(1, q.resultRows());
        db.queryPoints(s, T0 + 86_400_000L, T0 + 86_400_001L);
        assertEquals(1, db.lastQueryStats().resultRows());
    }

    @Test
    void lastQueryStatsBelongToTheVirtualThread() throws Exception {
        long s = db.series("x");
        long[] ts = new long[1000];
        float[] v = new float[1000];
        for (int i = 0; i < 1000; i++)
            ts[i] = T0 + i;
        db.insert(s, ts, v);
        try (ExecutorService ex = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Boolean>> fs = new ArrayList<>();
            for (int k = 1; k <= 200; k++) {
                int want = k;
                fs.add(ex.submit(() -> {
                    db.queryPoints(s, T0, T0 + want);
                    Thread.yield();
                    return db.lastQueryStats().resultRows() == want;
                }));
            }
            for (Future<Boolean> f : fs)
                assertTrue(f.get());
        }
    }

    @Test
    void manyThreadsOnOneDatabase() throws Exception {
        int threads = 16;
        long[] ids = new long[threads];
        for (int t = 0; t < threads; t++)
            ids[t] = db.series("s" + t);
        ExecutorService ex = Executors.newFixedThreadPool(threads);
        try {
            List<Future<Long>> fs = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                long id = ids[t];
                long other = ids[(t + 1) % threads];
                fs.add(ex.submit(() -> {
                    long checked = 0;
                    for (int round = 0; round < 50; round++) {
                        long[] ts = new long[200];
                        float[] v = new float[200];
                        for (int i = 0; i < 200; i++) {
                            ts[i] = T0 + round * 200L + i;
                            v[i] = round;
                        }
                        db.insert(id, ts, v);
                        Points p = db.queryPoints(id, T0 + round * 200L, T0 + round * 200L + 200);
                        assertEquals(200, p.size());
                        assertEquals(round, p.value(199));
                        db.queryAgg(other, Long.MIN_VALUE, Long.MAX_VALUE, 1000);
                        db.queryRaw(other, Long.MIN_VALUE, Long.MAX_VALUE, (a, b, k) -> true);
                        checked += p.size();
                    }
                    return checked;
                }));
            }
            long total = 0;
            for (Future<Long> f : fs)
                total += f.get();
            assertEquals(threads * 50L * 200, total);
            for (long id : ids)
                assertEquals(10_000, db.queryPoints(id, Long.MIN_VALUE, Long.MAX_VALUE).size());
        } finally {
            ex.shutdown();
        }
    }

    @Test
    void closeWaitsForRunningCalls() throws Exception {
        long s = db.series("x");
        long[] ts = new long[50_000];
        float[] v = new float[50_000];
        for (int i = 0; i < ts.length; i++)
            ts[i] = T0 + i;
        db.insert(s, ts, v);
        ExecutorService ex = Executors.newFixedThreadPool(4);
        try {
            List<Future<Integer>> fs = new ArrayList<>();
            for (int t = 0; t < 4; t++)
                fs.add(ex.submit(() -> {
                    int ok = 0;
                    try {
                        while (true) {
                            db.queryRaw(s, Long.MIN_VALUE, Long.MAX_VALUE, (a, b, k) -> true);
                            ok++;
                        }
                    } catch (TsdbException e) {
                        assertEquals(Status.ERR_IO, e.status()); /* closed: never a crash */
                    }
                    return ok;
                }));
            Thread.sleep(200);
            db.close();
            for (Future<Integer> f : fs)
                assertTrue(f.get() > 0);
        } finally {
            ex.shutdown();
        }
        assertFalse(db.isOpen());
    }

    @Test
    void arraysTheLibraryReturnsAreFreed() throws TsdbException {
        long s = db.series("x");
        db.insert(s, new long[] {T0, T0 + 1000}, new float[] {1, 2});
        long before = Native.OUTSTANDING.get();
        for (int i = 0; i < 10_000; i++) {
            db.queryAgg(s, T0, T0 + 2000, 1000);
            db.queryGroup(s, T0, T0 + 2000, Group.of(Unit.SECOND));
            db.queryBounds(s, T0, T0 + 2000, new long[] {T0});
        }
        assertEquals(before, Native.OUTSTANDING.get());
    }

    @Test
    void volumesOnlyWhereAllowed() throws Exception {
        /* create_volumes is the last field of tsdb_options: off, as in tsdbd, an unknown directory is not made */
        Path other = tmp.resolve("db2");
        Path vol = tmp.resolve("vol2b");
        try (EmbeddedDb d = EmbeddedDb.open(other.toString(), new EmbeddedOptions().fsync(false).createVolumes(false))) {
            assertEquals(Status.ERR_NOTFOUND, status(() -> d.seriesAt("x", vol.toString())));
            assertFalse(Files.exists(vol), "no directory for a refused volume");
            assertEquals(1, d.volumeCount());
        }
        /* on (the default), the same path checks as tsdb volume add still apply */
        assertEquals(Status.ERR_INVALID, status(() -> db.seriesAt("x", "relative/vol")));
        assertEquals(Status.ERR_INVALID, status(() -> db.seriesAt("x", vol + "/../vol3")));
        assertEquals(Status.ERR_INVALID, status(() -> db.seriesAt("x", vol + "\nx")));
        assertEquals(Status.ERR_INVALID, status(() -> db.seriesAt("x", tmp.resolve("db").resolve("in").toString())));
        assertFalse(Files.exists(tmp.resolve("vol3")));
        assertEquals(1, db.volumeCount());
        db.seriesAt("x", vol.toString());
        assertEquals(2, db.volumeCount());
    }

    @Test
    void aNulInsideAStringIsRefused() throws Exception {
        /* C would read "a" and use that series instead; tsdbd refuses the same names */
        long a = db.series("a");
        long st = db.seriesCreate("st", ValueType.STATE);
        db.state(st, "run");
        long count = db.seriesCount();
        String vol = tmp.resolve("vol").toString();
        assertEquals(Status.ERR_INVALID, status(() -> db.series("a\0b")));
        assertEquals(Status.ERR_INVALID, status(() -> db.series("a\0b", false)));
        assertEquals(Status.ERR_INVALID, status(() -> db.seriesAt("a\0b", vol)));
        assertEquals(Status.ERR_INVALID, status(() -> db.seriesAt("c", vol + "\0x")));
        assertEquals(Status.ERR_INVALID, status(() -> db.seriesCreate("a\0b", ValueType.INT64)));
        assertEquals(Status.ERR_INVALID, status(() -> db.seriesCreate("c", ValueType.INT64, vol + "\0x")));
        assertEquals(Status.ERR_INVALID, status(() -> db.state(st, "run\0x")));
        assertEquals(Status.ERR_INVALID, status(() -> db.stateLookup(st, "run\0x")));
        assertEquals(Status.ERR_INVALID, status(() -> EmbeddedDb.open(tmp.resolve("db").toString() + "\0x")));
        assertEquals(count, db.seriesCount());
        assertEquals(a, db.series("a", false));
        assertEquals(Status.ERR_NOTFOUND, status(() -> db.series("c", false)));
    }

    @Test
    void aCollectingQueryOverTheArrayLimitFails() throws TsdbException {
        long s = db.series("x");
        long[] ts = new long[1000];
        for (int i = 0; i < ts.length; i++)
            ts[i] = T0 + i;
        db.insert(s, ts, new float[ts.length]);
        int old = EmbeddedDb.collectLimit;
        EmbeddedDb.collectLimit = 100;
        try {
            /* used to loop on 0 rows of room with the read lock held */
            int st = assertTimeoutPreemptively(Duration.ofSeconds(20),
                    () -> status(() -> db.queryValues(s, Long.MIN_VALUE, Long.MAX_VALUE)));
            assertEquals(Status.ERR_NOMEM, st);
        } finally {
            EmbeddedDb.collectLimit = old;
        }
        assertEquals(1000, db.queryValues(s, Long.MIN_VALUE, Long.MAX_VALUE).size());
    }

    /* rows in a part and in the memtable reach the stub in several engine callbacks */
    private long rowsInSeveralCallbacks(ValueType type) throws TsdbException {
        long s = type == ValueType.FLOAT32 ? db.series("x") : db.seriesCreate("x", type);
        int n = EmbeddedDb.CHUNK + 100;
        long[] ts = new long[n];
        for (int i = 0; i < n; i++)
            ts[i] = T0 + i;
        db.insertSeriesValues(s, ts, type == ValueType.FLOAT32 ? Column.ofFloats(new float[n])
                : Column.ofLongs(new long[n]));
        db.flush();
        for (int i = 0; i < n; i++)
            ts[i] = T0 + n + i;
        db.insertSeriesValues(s, ts, type == ValueType.FLOAT32 ? Column.ofFloats(new float[n])
                : Column.ofLongs(new long[n]));
        return s;
    }

    @Test
    void anExceptionStopsAQueryThatSpansSeveralEngineCallbacks() throws TsdbException {
        long s = rowsInSeveralCallbacks(ValueType.FLOAT32);
        AtomicInteger calls = new AtomicInteger();
        assertThrows(IllegalStateException.class, () -> db.queryRaw(s, Long.MIN_VALUE, Long.MAX_VALUE, (t, x, k) -> {
            calls.incrementAndGet();
            throw new IllegalStateException("boom");
        }));
        assertEquals(1, calls.get(), "no chunk after the exception");
        AtomicInteger seen = new AtomicInteger();
        db.queryRaw(s, Long.MIN_VALUE, Long.MAX_VALUE, (t, x, k) -> seen.addAndGet(k) > 0);
        assertEquals(2 * (EmbeddedDb.CHUNK + 100), seen.get(), "both runs reach the consumer");
    }

    @Test
    void anExceptionStopsATypedQueryThatSpansSeveralEngineCallbacks() throws TsdbException {
        long s = rowsInSeveralCallbacks(ValueType.INT64);
        AtomicInteger calls = new AtomicInteger();
        assertThrows(IllegalStateException.class,
                () -> db.queryValues(s, Long.MIN_VALUE, Long.MAX_VALUE, (t, c, k) -> {
                    calls.incrementAndGet();
                    throw new IllegalStateException("boom");
                }));
        assertEquals(1, calls.get(), "no chunk after the exception");
    }

    @Test
    void closeWaitsForACallInsideAConsumer() throws Exception {
        long s = db.series("x");
        db.insert(s, new long[] {T0, T0 + 1}, new float[] {1, 2});
        CountDownLatch inside = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture<Boolean> query = CompletableFuture.supplyAsync(() -> {
            try {
                return db.queryRaw(s, Long.MIN_VALUE, Long.MAX_VALUE, (t, x, k) -> {
                    inside.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    return true;
                });
            } catch (TsdbException e) {
                throw new AssertionError(e);
            }
        });
        assertTrue(inside.await(20, TimeUnit.SECONDS));
        CompletableFuture<Void> closing = CompletableFuture.runAsync(() -> {
            try {
                db.close();
            } catch (TsdbException e) {
                throw new AssertionError(e);
            }
        });
        Thread.sleep(300);
        assertFalse(closing.isDone(), "close returned while a query was inside the library");
        release.countDown();
        assertTrue(query.get(20, TimeUnit.SECONDS));
        closing.get(20, TimeUnit.SECONDS);
        assertFalse(db.isOpen());
    }

    @Test
    void insertOffsetsApplyToTheSeriesArrayAndToBools() throws TsdbException {
        long a = db.series("a");
        long b = db.series("b");
        long[] ts = {T0, T0 + 1, T0 + 2, T0 + 3};
        db.insert(new long[] {b, a, b, a}, ts, new float[] {1, 2, 3, 4}, 1, 2);
        assertArrayEquals(new long[] {T0 + 1}, db.queryPoints(a, T0, T0 + 10).timestamps());
        assertArrayEquals(new float[] {2}, db.queryPoints(a, T0, T0 + 10).values());
        assertArrayEquals(new long[] {T0 + 2}, db.queryPoints(b, T0, T0 + 10).timestamps());
        long c = db.seriesCreate("c", ValueType.BOOL);
        long d = db.seriesCreate("d", ValueType.BOOL);
        db.insertSeriesValues(c, ts, Column.ofBools(true, false, true, true), 1, 2);
        assertArrayEquals(new boolean[] {false, true}, db.queryValues(c, T0, T0 + 10).values().bools());
        db.insertValues(new long[] {c, d, d, c}, ts, Column.ofBools(true, true, false, true), 1, 2);
        ValuePoints vd = db.queryValues(d, T0, T0 + 10);
        assertArrayEquals(new long[] {T0 + 1, T0 + 2}, vd.timestamps());
        assertArrayEquals(new boolean[] {true, false}, vd.values().bools());
    }

    @Test
    void lastQueryStatsFollowStreamingAndCollectingQueries() throws TsdbException {
        long s = db.series("x");
        long[] ts = new long[100];
        for (int i = 0; i < ts.length; i++)
            ts[i] = T0 + i;
        db.insert(s, ts, new float[ts.length]);
        db.queryPoints(s, T0, T0 + 50);
        assertEquals(50, db.lastQueryStats().resultRows());
        db.queryRaw(s, T0, T0 + 7, (t, x, k) -> true);
        assertEquals(7, db.lastQueryStats().resultRows());
        db.queryValues(s, T0, T0 + 11);
        assertEquals(11, db.lastQueryStats().resultRows());
        db.queryValues(s, T0, T0 + 13, (t, c, k) -> true);
        assertEquals(13, db.lastQueryStats().resultRows());
    }

    @Test
    void optimizeRaisesThePartLevel() throws TsdbException {
        long s = db.series("x");
        for (int k = 0; k < 3; k++) {
            db.insert(s, new long[] {T0 + k}, new float[] {k});
            db.flush();
        }
        db.optimize();
        List<PartInfo> parts = db.parts();
        assertEquals(1, parts.size());
        assertTrue(parts.get(0).level() >= 1, "a merged part has a level: " + parts.get(0));
        assertEquals(3, parts.get(0).rows());
    }
}
