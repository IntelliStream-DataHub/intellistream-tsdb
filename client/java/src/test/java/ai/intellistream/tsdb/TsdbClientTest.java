package ai.intellistream.tsdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/** Integration tests: every operation against a live build/tsdbd. */
class TsdbClientTest {
    static final long T0 = 1699999980000L; /* 2023-11-14T22:13:00Z, on a minute boundary: buckets below are whole */
    static final int N = 200_000;          /* rows of the big series: several streamed chunks */
    static final long HOUR = 3_600_000L, DAY = 86_400_000L;

    static Path scratch;
    static TsdbServer server;
    static TsdbClient c;

    @BeforeAll
    static void start() throws Exception {
        scratch = TsdbServer.scratch("tsdbd-java-");
        server = TsdbServer.start(scratch.resolve("db"));
        c = server.client();
    }

    @AfterAll
    static void stop() throws Exception {
        try {
            if (c != null)
                c.close();
        } finally {
            if (server != null) {
                int exit = server.stop();
                assertEquals(0, exit, () -> "tsdbd exit status; log:\n" + logOf(server));
            }
            TsdbServer.delete(scratch);
        }
    }

    static String logOf(TsdbServer s) {
        try {
            return s.log();
        } catch (IOException e) {
            return e.toString();
        }
    }

    static long[] seq(long start, long step, int n) {
        long[] t = new long[n];
        for (int i = 0; i < n; i++)
            t[i] = start + i * step;
        return t;
    }

    static float[] bigValues() {
        float[] v = new float[N];
        for (int i = 0; i < N; i++)
            v[i] = (i % 1000) / 10.0f;
        return v;
    }

    static int status(TsdbException e) {
        return e.status();
    }

    @Test
    void hello() throws Exception {
        assertEquals(Protocol.VERSION, c.hello());
    }

    @Test
    void series() throws Exception {
        long a = c.series("tank.level_bar");
        long b = c.series("pump.flow_m3h", true);
        assertNotEquals(a, b);
        assertEquals(a, c.series("tank.level_bar", false));
        assertEquals(Status.ERR_NOTFOUND, assertThrows(TsdbException.class, () -> c.series("missing", false)).status());
        assertEquals("pump.flow_m3h", c.seriesName(b));
        assertEquals(Status.ERR_NOTFOUND, assertThrows(TsdbException.class, () -> c.seriesName(0x7fff0000)).status());
        String unicode = "målestasjon.Ø.温度";
        assertEquals(unicode, c.seriesName(c.series(unicode)));
        assertThrows(IllegalArgumentException.class, () -> c.series(""));
        assertThrows(IllegalArgumentException.class, () -> c.series("x".repeat(65536)));
        assertTrue(c.isOpen());
    }

    @Test
    void seriesAtVolume() throws Exception {
        String vol = scratch.resolve("volume").toString();
        /* tsdbd never creates a volume for a client: a path that is none is NOTFOUND and makes no directory */
        assertEquals(Status.ERR_NOTFOUND,
                assertThrows(TsdbException.class, () -> c.seriesAt("remote.on_volume", vol)).status());
        assertFalse(java.nio.file.Files.exists(scratch.resolve("volume")));
        server.addVolume(scratch.resolve("volume"));
        long id = c.seriesAt("remote.on_volume", vol);
        assertEquals(id, c.seriesAt("remote.on_volume", null)); /* the path only matters at creation */
        c.insert(id, new long[] {T0}, new float[] {3.5f});
        Points p = c.queryPoints(id, Long.MIN_VALUE, Long.MAX_VALUE);
        assertEquals(1, p.size());
        assertEquals(3.5f, p.value(0));
    }

    @Test
    void largeInsertStreamsBack() throws Exception {
        long id = c.series("big.stream");
        long[] ts = seq(T0, 1000, N);
        float[] v = bigValues();
        c.insert(id, ts, v);

        Points p = c.queryPoints(id, Long.MIN_VALUE, Long.MAX_VALUE);
        assertArrayEquals(ts, p.timestamps());
        assertArrayEquals(v, p.values());

        int[] chunks = {0};
        long[] rows = {0}, last = {Long.MIN_VALUE};
        boolean[] ordered = {true};
        assertTrue(c.queryRaw(id, Long.MIN_VALUE, Long.MAX_VALUE, (t, x, n) -> {
            for (int i = 0; i < n; i++) {
                ordered[0] &= t[i] > last[0] && x[i] == v[(int) rows[0] + i];
                last[0] = t[i];
            }
            rows[0] += n;
            chunks[0]++;
            return true;
        }));
        assertEquals(N, rows[0]);
        assertTrue(ordered[0]);
        assertTrue(chunks[0] > 1, "a large result arrives in several chunks");

        /* a sub-range: half open */
        Points sub = c.queryPoints(id, T0 + 5000, T0 + 15000);
        assertArrayEquals(seq(T0 + 5000, 1000, 10), sub.timestamps());
    }

    @Test
    void stoppingAStreamKeepsTheConnection() throws Exception {
        long id = c.series("stop.early");
        c.insert(id, seq(T0, 1000, 100_000), new float[100_000]);
        int[] calls = {0};
        assertFalse(c.queryRaw(id, Long.MIN_VALUE, Long.MAX_VALUE, (t, x, n) -> ++calls[0] < 1));
        assertEquals(1, calls[0]);

        RuntimeException boom = new IllegalStateException("boom");
        assertSame(boom, assertThrows(IllegalStateException.class,
                () -> c.queryRaw(id, Long.MIN_VALUE, Long.MAX_VALUE, (t, x, n) -> {
                    throw boom;
                })));
        assertEquals(Protocol.VERSION, c.hello());
        assertEquals(100_000, c.queryPoints(id, Long.MIN_VALUE, Long.MAX_VALUE).size());
    }

    @Test
    void lastWriteWins() throws Exception {
        long a = c.series("lww.a"), b = c.series("lww.b");

        /* within one batch, the later row */
        c.insert(a, new long[] {T0, T0, T0 + 1000}, new float[] {1, 2, 3});
        Points p = c.queryPoints(a, Long.MIN_VALUE, Long.MAX_VALUE);
        assertArrayEquals(new long[] {T0, T0 + 1000}, p.timestamps());
        assertArrayEquals(new float[] {2, 3}, p.values());

        /* rows of several series in one batch, then a pipelined rewrite of the same keys */
        long[] s = {b, b, a};
        long[] t = {T0, T0 + 1000, T0};
        float[] v = {1.5f, 2.5f, -7.0f};
        c.insertAsync(s, t, v);
        v[1] = 9.0f; /* the arrays may be reused once insertAsync returns */
        c.insertAsync(s, t, v);
        assertEquals(2, c.pending());
        c.await();
        assertEquals(0, c.pending());
        assertArrayEquals(new float[] {1.5f, 9.0f}, c.queryPoints(b, Long.MIN_VALUE, Long.MAX_VALUE).values());
        assertArrayEquals(new float[] {-7.0f, 3}, c.queryPoints(a, Long.MIN_VALUE, Long.MAX_VALUE).values());

        /* a later synchronous insert replaces a pipelined one, and also after a flush into parts */
        c.insertAsync(a, new long[] {T0}, new float[] {10});
        c.insert(a, new long[] {T0}, new float[] {11});
        c.flush();
        c.insert(a, new long[] {T0}, new float[] {12});
        assertEquals(12, c.queryPoints(a, T0, T0 + 1).value(0));
        c.optimize();
        assertEquals(12, c.queryPoints(a, T0, T0 + 1).value(0));
    }

    @Test
    void offsetsAndEmptyInserts() throws Exception {
        long id = c.series("offsets");
        long[] ts = seq(T0, 1000, 20);
        float[] v = new float[20];
        for (int i = 0; i < 20; i++)
            v[i] = i;
        c.insert(id, ts, v, 10, 5);
        c.insert(new long[] {id, id, id}, ts, v, 0, 2);
        c.insert(id, new long[0], new float[0]);
        Points p = c.queryPoints(id, Long.MIN_VALUE, Long.MAX_VALUE);
        assertArrayEquals(new float[] {0, 1, 10, 11, 12, 13, 14}, p.values());
        assertThrows(IllegalArgumentException.class, () -> c.insert(id, ts, new float[3]));
        assertThrows(IndexOutOfBoundsException.class, () -> c.insert(id, ts, v, 18, 5));
        assertThrows(IllegalArgumentException.class, () -> c.insert(0xffffffffL, ts, v)); /* the many-series marker */
        assertEquals(0, c.pending());
    }

    /**
     * Protocol 10 carries 32-bit ids: a larger one is refused before anything is sent, never cut to the series of
     * its low bits (here a real one, which must not change).
     */
    @Test
    void idsBeyond32BitsAreRefusedNotTruncated() throws Exception {
        long id = c.series("wide.id");
        c.insert(id, new long[] {T0}, new float[] {1});
        long[] ts = {T0, T0 + 1000};
        float[] v = {7, 8};
        c.queryTwa(id, T0, T0 + 1, 0, TwaOptions.step()); /* HELLO asked here, not by a refused call below */
        for (long big : new long[] {id | 1L << 32, -1L, Long.MIN_VALUE | id}) {
            long sent = c.bytesSent();
            List<Executable> calls = List.of(() -> c.insert(big, ts, v), () -> c.insertAsync(big, ts, v),
                    () -> c.insert(new long[] {id, big}, ts, v),
                    () -> c.insertSeriesValues(big, ts, Column.ofFloats(v)),
                    () -> c.insertValues(new long[] {big, id}, ts, Column.ofFloats(v)),
                    () -> c.replaceRange(big, T0, T0 + 2000, ts, Column.ofFloats(v)),
                    () -> c.deleteRange(big, Long.MIN_VALUE, Long.MAX_VALUE), () -> c.setRetention(big, 1),
                    () -> c.retention(big), () -> c.seriesName(big), () -> c.seriesType(big),
                    () -> c.state(big, "on"), () -> c.stateName(big, 1),
                    () -> c.queryPoints(big, Long.MIN_VALUE, Long.MAX_VALUE),
                    () -> c.queryValues(big, Long.MIN_VALUE, Long.MAX_VALUE),
                    () -> c.queryAgg(big, Long.MIN_VALUE, Long.MAX_VALUE, 0),
                    () -> c.queryGroup(big, Long.MIN_VALUE, Long.MAX_VALUE, Group.of(Unit.DAY)),
                    () -> c.queryBounds(big, Long.MIN_VALUE, Long.MAX_VALUE, new long[] {T0}),
                    () -> c.queryLttb(big, Long.MIN_VALUE, Long.MAX_VALUE, 10),
                    () -> c.queryTwa(big, Long.MIN_VALUE, Long.MAX_VALUE, 0, TwaOptions.step()),
                    () -> c.queryValuesMulti(new long[] {id, big}, Long.MIN_VALUE, Long.MAX_VALUE));
            for (Executable call : calls)
                assertEquals(Status.ERR_INVALID, assertThrows(TsdbException.class, call).status(),
                        Long.toUnsignedString(big));
            assertEquals(sent, c.bytesSent(), "nothing was sent");
            assertEquals(0, c.pending());
        }
        assertArrayEquals(new float[] {1}, c.queryPoints(id, Long.MIN_VALUE, Long.MAX_VALUE).values());
        assertEquals("wide.id", c.seriesName(id));
    }

    @Test
    void descendingWithLimit() throws Exception {
        long id = c.series("desc.limit");
        long[] ts = seq(T0, 1000, N);
        c.insert(id, ts, bigValues());
        Points p = c.queryPoints(id, Long.MIN_VALUE, Long.MAX_VALUE, Order.DESC, 5);
        assertArrayEquals(new long[] {ts[N - 1], ts[N - 2], ts[N - 3], ts[N - 4], ts[N - 5]}, p.timestamps());
        p = c.queryPoints(id, Long.MIN_VALUE, Long.MAX_VALUE, Order.ASC, 3);
        assertArrayEquals(new long[] {ts[0], ts[1], ts[2]}, p.timestamps());
        Points all = c.queryPoints(id, Long.MIN_VALUE, Long.MAX_VALUE, Order.DESC, 0);
        assertEquals(N, all.size());
        assertEquals(ts[N - 1], all.timestamp(0));
        assertEquals(ts[0], all.timestamp(N - 1));
        long[] n = {0};
        assertTrue(c.queryRaw(id, Long.MIN_VALUE, Long.MAX_VALUE, Order.DESC, 40_000, (t, x, k) -> {
            n[0] += k;
            return true;
        }));
        assertEquals(40_000, n[0]);
        assertThrows(IllegalArgumentException.class, () -> c.queryPoints(id, 0, 1, Order.DESC, -1));
    }

    @Test
    void aggregates() throws Exception {
        long id = c.series("agg.minutes");
        long[] ts = seq(T0, 1000, N);
        float[] v = bigValues();
        v[0] = -7.0f;
        c.insert(id, ts, v);
        List<Bucket> bk = c.queryAgg(id, T0, T0 + HOUR, 60_000);
        assertEquals(60, bk.size());
        for (int k = 0; k < bk.size(); k++) {
            double sum = 0;
            float mn = Float.POSITIVE_INFINITY, mx = Float.NEGATIVE_INFINITY;
            for (int i = k * 60; i < k * 60 + 60; i++) {
                sum += v[i];
                mn = Math.min(mn, v[i]);
                mx = Math.max(mx, v[i]);
            }
            Bucket b = bk.get(k);
            assertEquals(T0 + k * 60_000L, b.start());
            assertEquals(60, b.count());
            assertEquals(sum, b.sum(), 1e-6);
            assertEquals(sum / 60, b.avg(), 1e-9);
            assertEquals(mn, b.min());
            assertEquals(mx, b.max());
        }

        /* interval 0: one bucket starting at the query start */
        List<Bucket> one = c.queryAgg(id, Long.MIN_VALUE, Long.MAX_VALUE, 0);
        assertEquals(1, one.size());
        assertEquals(N, one.get(0).count());
        assertEquals(Long.MIN_VALUE, one.get(0).start());

        /* a reply far larger than the receive buffer: N buckets of 32 bytes */
        assertEquals(N, c.queryAgg(id, Long.MIN_VALUE, Long.MAX_VALUE, 1000).size());
        assertEquals(Protocol.VERSION, c.hello());
        assertTrue(c.queryAgg(id, 0, 1000, 1000).isEmpty());
    }

    @Test
    void groupByTimezone() throws Exception {
        long id = c.series("group.days");
        long[] ts = seq(T0, 1000, N); /* 2023-11-14 to 2023-11-17, no DST change in Oslo */
        c.insert(id, ts, bigValues());

        /* the last two local days in Oslo (UTC+1 in November), newest first */
        List<Bucket> days = c.queryGroup(id, Long.MIN_VALUE, Long.MAX_VALUE, Group.of(Unit.DAY).in("Europe/Oslo"),
                Order.DESC, 2);
        assertEquals(2, days.size());
        assertEquals(DAY, days.get(0).start() - days.get(1).start());
        assertEquals(23 * HOUR, Math.floorMod(days.get(0).start(), DAY), "local midnight is 23:00 UTC");

        /* all local days hold every row once */
        List<Bucket> all = c.queryGroup(id, Long.MIN_VALUE, Long.MAX_VALUE, Group.of(Unit.DAY).in("Europe/Oslo"));
        assertEquals(N, all.stream().mapToLong(Bucket::count).sum());
        for (int i = 1; i < all.size(); i++)
            assertTrue(all.get(i).start() > all.get(i - 1).start());

        /* 6-hour buckets in a half-hour zone align to its standard time */
        for (Bucket b : c.queryGroup(id, Long.MIN_VALUE, Long.MAX_VALUE, Group.of(Unit.HOUR, 6).in("+05:30")))
            assertEquals(0, Math.floorMod(b.start() + 5 * HOUR + HOUR / 2, 6 * HOUR));

        /* a month in UTC, and no grouping at all: one bucket each */
        List<Bucket> month = c.queryGroup(id, Long.MIN_VALUE, Long.MAX_VALUE, Group.of(Unit.MONTH));
        assertEquals(1, month.size());
        assertEquals(1698796800000L, month.get(0).start()); /* 2023-11-01T00:00Z */
        List<Bucket> whole = c.queryGroup(id, T0, T0 + 10_000, null);
        assertEquals(1, whole.size());
        assertEquals(10, whole.get(0).count());
    }

    @Test
    void errors() throws Exception {
        long id = c.series("errors");
        c.insert(id, new long[] {T0}, new float[] {1});
        assertEquals(Status.ERR_INVALID,
                assertThrows(TsdbException.class, () -> c.queryAgg(id, T0, T0 + 1, -5)).status());
        assertThrows(IllegalArgumentException.class, () -> Group.of(Unit.DAY).in("Not/A_Zone"));
        TsdbException e = assertThrows(TsdbException.class, () -> c.seriesName(0x7ffffff0));
        assertEquals(Status.ERR_NOTFOUND, e.status());
        assertTrue(e.getMessage().contains("not found"), e.getMessage());
        c.insert(id, new long[] {0, 4_000_000_000_000L}, new float[] {0, 0});
        assertEquals(Status.ERR_TOO_MANY_BUCKETS, assertThrows(TsdbException.class,
                () -> c.queryAgg(id, Long.MIN_VALUE, Long.MAX_VALUE, 1)).status());
        assertEquals("too many buckets", Status.message(Status.ERR_TOO_MANY_BUCKETS));

        /* a failed pipelined insert: await reports it, the inserts around it still apply */
        int unknown = 0x7ffffff0;
        c.insertAsync(id, new long[] {T0 + 1}, new float[] {2});
        c.insertAsync(unknown, new long[] {T0}, new float[] {1});
        c.insertAsync(id, new long[] {T0 + 2}, new float[] {3});
        assertEquals(Status.ERR_INVALID, assertThrows(TsdbException.class, c::await).status());
        c.await(); /* reported once */
        /* ... and the next call that waits reports it instead of running */
        c.insertAsync(unknown, new long[] {T0}, new float[] {1});
        assertEquals(Status.ERR_INVALID, assertThrows(TsdbException.class, c::hello).status());
        assertEquals(Status.ERR_INVALID, assertThrows(TsdbException.class,
                () -> c.insert(unknown, new long[] {T0}, new float[] {1})).status());
        /* server errors leave the connection usable */
        assertTrue(c.isOpen());
        assertEquals(5, c.queryPoints(id, Long.MIN_VALUE, Long.MAX_VALUE).size());

        /* nothing listens on a fresh port */
        int port;
        try (java.net.ServerSocket s = new java.net.ServerSocket(0)) {
            port = s.getLocalPort();
        }
        int p = port;
        assertEquals(Status.ERR_IO,
                assertThrows(TsdbException.class, () -> TsdbClient.connect("127.0.0.1", p)).status());
    }

    @Test
    void maintenanceAndStats() throws Exception {
        long id = c.series("maint");
        c.insert(id, seq(T0, 1000, 1000), new float[1000]);
        Stats before = c.stats();
        assertTrue(before.rows() + before.memtableRows() >= 1000, before.toString());
        c.commit();
        c.flush();
        c.optimize();
        Stats after = c.stats();
        assertEquals(0, after.memtableRows(), after.toString());
        assertTrue(after.rows() >= 1000 && after.parts() >= 1 && after.partitions() >= 1 && after.bytes() > 0,
                after.toString());
    }

    @Test
    void lastQueryStats() throws Exception {
        try (TsdbClient own = server.client()) {
            assertEquals(new QueryStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), own.lastQueryStats(), "before any query");
            long id = own.series("query.stats");
            own.insert(id, seq(T0, 1000, N), bigValues());
            own.flush();

            /* a whole-range aggregate is answered from granule statistics */
            List<Bucket> one = own.queryAgg(id, Long.MIN_VALUE, Long.MAX_VALUE, 0);
            assertEquals(N, one.get(0).count());
            QueryStats s = own.lastQueryStats();
            assertEquals(N, s.rows(), s.toString());
            assertEquals(N, s.rowsFromStats(), s.toString());
            assertEquals(0, s.rowsDecoded(), s.toString());
            assertEquals(0, s.memtableRows(), s.toString());
            assertEquals(0, s.bytesRead(), s.toString());
            assertEquals(s.granules(), s.granulesFromStats(), s.toString());
            assertTrue(s.parts() >= 1 && s.granules() >= N / 8192 && s.bytes() > 0, s.toString());
            assertEquals(one.size(), s.resultRows());
            assertTrue(s.elapsedS() >= 0 && s.elapsedS() < 60, s.toString());
            assertEquals(s, own.lastQueryStats(), "LAST_STATS is not a query itself");

            /* buckets of 10 s split every granule: the rows are decoded */
            List<Bucket> small = own.queryAgg(id, Long.MIN_VALUE, Long.MAX_VALUE, 10_000);
            assertEquals(N / 10, small.size());
            s = own.lastQueryStats();
            assertTrue(s.rowsDecoded() > 0 && s.bytesRead() > 0, s.toString());
            assertEquals(s.rows(), s.rowsDecoded() + s.rowsFromStats() + s.memtableRows(), s.toString());
            assertEquals(small.size(), s.resultRows());

            /* raw queries count the rows they return */
            Points p = own.queryPoints(id, Long.MIN_VALUE, Long.MAX_VALUE, Order.DESC, 5);
            s = own.lastQueryStats();
            assertEquals(p.size(), s.resultRows(), s.toString());
            assertTrue(s.rowsDecoded() >= 5, s.toString());

            /* another connection's queries do not change this one's */
            c.queryAgg(id, Long.MIN_VALUE, Long.MAX_VALUE, 0);
            assertEquals(s, own.lastQueryStats());
        }
    }

    @Test
    void concurrentPipelinedClients() throws Exception {
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<Bucket>> results = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                long id = c.series("vib." + i);
                float value = i;
                results.add(pool.submit(() -> {
                    try (TsdbClient own = server.client()) { /* one client per thread */
                        float[] v = new float[1000];
                        java.util.Arrays.fill(v, value);
                        for (int round = 0; round < 50; round++)
                            own.insertAsync(id, seq(T0 + round * 1_000_000L, 1000, 1000), v);
                        own.await();
                        return own.queryAgg(id, Long.MIN_VALUE, Long.MAX_VALUE, 0).get(0);
                    }
                }));
            }
            for (int i = 0; i < threads; i++) {
                Bucket b = results.get(i).get();
                assertEquals(50_000, b.count());
                assertEquals(i, b.min());
                assertEquals(i, b.max());
            }
        } finally {
            pool.shutdown();
        }
    }

    @Test
    void closeWaitsForPipelinedInserts() throws Exception {
        long id = c.series("close.pipelined");
        TsdbClient own = server.client();
        for (int round = 0; round < 100; round++)
            own.insertAsync(id, seq(T0 + round * 100_000L, 1000, 100), new float[100]);
        own.close();
        own.close(); /* idempotent */
        assertFalse(own.isOpen());
        assertThrows(IllegalStateException.class, own::hello);
        assertEquals(10_000, c.queryAgg(id, Long.MIN_VALUE, Long.MAX_VALUE, 0).get(0).count());
    }

    @Test
    void manyPipelinedInsertsStayBounded() throws Exception {
        long id = c.series("bounded");
        try (TsdbClient own = server.client()) {
            long[] t = new long[1];
            float[] v = new float[1];
            for (int i = 0; i < 3 * TsdbClient.MAX_IN_FLIGHT; i++) {
                t[0] = T0 + i;
                own.insertAsync(id, t, v);
                assertTrue(own.pending() <= TsdbClient.MAX_IN_FLIGHT);
            }
            own.await();
            assertEquals(3 * TsdbClient.MAX_IN_FLIGHT, own.queryPoints(id, Long.MIN_VALUE, Long.MAX_VALUE).size());
        }
    }
}
