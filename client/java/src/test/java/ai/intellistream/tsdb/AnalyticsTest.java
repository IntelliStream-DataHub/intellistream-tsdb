package ai.intellistream.tsdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/** Protocol 9 against a live build/tsdbd: the spread, time-weighted averages, LTTB; and older servers. */
class AnalyticsTest {
    static final long T0 = TsdbClientTest.T0;
    static final long MIN = Long.MIN_VALUE, MAX = Long.MAX_VALUE;

    static Path scratch;
    static TsdbServer server;
    static TsdbClient c;
    static long calc, wave, state;

    @BeforeAll
    static void start() throws Exception {
        scratch = TsdbServer.scratch("tsdbd-java-analytics-");
        server = TsdbServer.start(scratch.resolve("db"));
        c = server.client();
        calc = c.seriesCreate("calc", ValueType.FLOAT64);
        c.insertSeriesValues(calc, new long[] {T0, T0 + 1000, T0 + 3000, T0 + 4000},
                Column.ofDoubles(10.0, 20.0, 30.0, 40.0));
        wave = c.series("wave");
        long[] ts = new long[1000];
        float[] v = new float[1000];
        for (int i = 0; i < 1000; i++) {
            ts[i] = T0 + i * 1000L;
            v[i] = (float) (Math.sin(i / 30.0) * 100);
        }
        c.insert(wave, ts, v);
        state = c.seriesCreate("calc.state", ValueType.STATE);
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

    static void invalid(Executable e) {
        TsdbException x = assertThrows(TsdbException.class, e);
        assertEquals(Status.ERR_INVALID, x.status(), x.getMessage());
    }

    @Test
    void aggregatesWithOrderAndLimit() throws Exception {
        List<Bucket> sec = c.queryAgg(calc, MIN, MAX, 1000, Order.DESC, 2);
        assertEquals(List.of(T0 + 4000, T0 + 3000), sec.stream().map(Bucket::start).toList());
        assertEquals(List.of(40.0, 30.0), sec.stream().map(Bucket::sum).toList());
        assertEquals(c.queryAgg(calc, MIN, MAX, 0), c.queryAgg(calc, MIN, MAX, 0, Order.ASC, 0));
        invalid(() -> c.queryAgg(calc, MIN, MAX, -1, Order.ASC, 0));
    }

    @Test
    void spread() throws Exception {
        List<SpreadBucket> s = c.querySpread(calc, MIN, MAX, 0);
        assertEquals(1, s.size());
        SpreadBucket b = s.get(0);
        assertEquals(4, b.bucket().count());
        assertEquals(100.0, b.bucket().sum());
        assertEquals(30.0, b.range());
        assertEquals(125.0, b.varPop());
        assertEquals(Math.sqrt(125), b.stddevPop());
        assertEquals(500.0 / 3, b.varSamp(), 1e-9);
        assertEquals(Math.sqrt(500.0 / 3), b.stddevSamp(), 1e-9);

        List<SpreadBucket> per2s = c.querySpread(calc, MIN, MAX, 2000, Order.DESC, 0);
        assertEquals(List.of(T0 + 4000, T0 + 2000, T0), per2s.stream().map(x -> x.bucket().start()).toList());
        assertTrue(Double.isNaN(per2s.get(0).varSamp()), "one value has no sample variance");
        assertEquals(per2s, c.querySpreadGroup(calc, MIN, MAX, Group.of(Unit.SECOND, 2), Order.DESC, 0));
        assertEquals(per2s.subList(0, 2), c.querySpread(calc, MIN, MAX, 2000, Order.DESC, 2));
        List<SpreadBucket> day = c.querySpreadGroup(calc, MIN, MAX, Group.of(Unit.DAY).in("Europe/Oslo"), Order.ASC, 0);
        assertEquals(125.0, day.get(0).varPop());
        assertEquals(c.querySpread(calc, MIN, MAX, 0), c.querySpreadGroup(calc, MIN, MAX, null, Order.ASC, 0));
        List<SpreadBucket> bounds = c.querySpreadBounds(calc, MIN, MAX, new long[] {T0, T0 + 3000}, Order.ASC, 1);
        assertEquals(1, bounds.size());
        assertEquals(2, bounds.get(0).bucket().count());
        assertEquals(10.0, bounds.get(0).range());
        assertEquals(25.0, bounds.get(0).varPop());
        assertThrows(IllegalArgumentException.class, () -> c.querySpreadBounds(calc, MIN, MAX, new long[0], Order.ASC, 0));
        invalid(() -> c.querySpreadBounds(calc, MIN, MAX, new long[] {5, 5}, Order.ASC, 0));
        invalid(() -> c.querySpread(calc, MIN, MAX, -1));
    }

    @Test
    void aNegativeSpreadIntervalIsRefusedWithoutAskingTheServer() throws Exception {
        try (CompressionTest.FakeServer f = new CompressionTest.FakeServer(9)) {
            try (TsdbClient o = TsdbClient.connect("127.0.0.1", f.port())) {
                invalid(() -> o.querySpread(1, MIN, MAX, -1));
                invalid(() -> o.querySpread(1, MIN, MAX, -5, Order.DESC, 3));
                o.commit();
            }
            /* HELLO may go out or not (the client asks for the version when it needs it); no query does */
            assertEquals(List.of((int) Protocol.OP_COMMIT),
                    f.ops().stream().filter(op -> op != Protocol.OP_HELLO).toList());
        }
    }

    /* More local periods than one request carries (32 Mi bounds; the calendar itself stops at 64 Mi): every grouped
     * call fails with TOO_MANY_BUCKETS, as in the C and Rust clients, before it sends the bounds. */
    @Test
    void overWideLocalRangesAreTooManyBuckets() throws Exception {
        long wide = c.seriesCreate("wide", ValueType.FLOAT64);
        c.insertSeriesValues(wide, new long[] {T0, T0 + 450 * 86_400_000L}, Column.ofDoubles(1, 2)); /* 38.9 M s */
        long wider = c.seriesCreate("wider", ValueType.FLOAT64);
        c.insertSeriesValues(wider, new long[] {T0, T0 + 800 * 86_400_000L}, Column.ofDoubles(1, 2)); /* 69.1 M s */
        Group secs = Group.of(Unit.SECOND).in("Europe/Oslo");
        for (long s : new long[] {wide, wider}) {
            tooMany(() -> c.queryGroup(s, MIN, MAX, secs, Order.ASC, 0));
            tooMany(() -> c.querySpreadGroup(s, MIN, MAX, secs, Order.ASC, 0));
            tooMany(() -> c.queryTwaGroup(s, MIN, MAX, secs, TwaOptions.step()));
        }
        c.commit(); /* the connection is still good */
        /* bounded on both sides, TWA covers the range: 33,554,418 seconds is one past QUERY_TWA's (MAX_FRAME - 120) / 8
         * (and below QUERY_BOUNDS' cap). The client refuses it without sending it: tsdbd would refuse it too. */
        try (CompressionTest.FakeServer f = new CompressionTest.FakeServer(9)) {
            try (TsdbClient o = TsdbClient.connect("127.0.0.1", f.port())) {
                tooMany(() -> o.queryTwaGroup(wide, T0, T0 + 33_554_418_000L, secs, TwaOptions.step()));
            }
            assertTrue(!f.ops().contains((int) Protocol.OP_QUERY_TWA), "" + f.ops());
        }
    }

    static void tooMany(Executable e) {
        TsdbException x = assertThrows(TsdbException.class, e);
        assertEquals(Status.ERR_TOO_MANY_BUCKETS, x.status(), x.getMessage());
    }

    /* QUERY_TWA carries 56 bytes more than QUERY_BOUNDS before its bounds, so it takes fewer: as the C client. */
    @Test
    void twaBoundsCapIsTheCClients() throws Exception {
        long[] b = new long[(Protocol.MAX_FRAME - 120) / 8 + 1];
        assertThrows(IllegalArgumentException.class, () -> c.queryTwaBounds(calc, MIN, MAX, b, TwaOptions.step()));
        c.commit();
    }

    @Test
    void timeWeightedAverages() throws Exception {
        long end = T0 + 5000;
        /* 10 for 1 s, 20 for 2 s, 30 for 1 s; 40 has no segment */
        TwaBucket step = c.queryTwa(calc, T0, end, 0, TwaOptions.step()).get(0);
        assertEquals(new TwaBucket(T0, 20.0, 80_000.0, 4000, 4), step);
        TwaBucket lin = c.queryTwa(calc, T0, end, 0, TwaOptions.linear()).get(0);
        assertEquals(15_000.0 + 50_000.0 + 35_000.0, lin.area());
        assertEquals(4000, lin.coveredMs());
        TwaBucket gap = c.queryTwa(calc, T0, end, 0, TwaOptions.step().maxGap(1500)).get(0);
        assertEquals(40_000.0, gap.area());
        assertEquals(2000, gap.coveredMs());

        List<TwaBucket> secs = c.queryTwa(calc, T0, end, 1000, TwaOptions.step().order(Order.DESC).limit(2));
        assertEquals(2, secs.size());
        /* the last reading holds nothing (no later reading ends its segment) but is counted */
        assertEquals(T0 + 4000, secs.get(0).start());
        assertEquals(1, secs.get(0).count());
        assertEquals(0, secs.get(0).coveredMs());
        assertTrue(Double.isNaN(secs.get(0).twa()));
        assertEquals(30.0, secs.get(1).twa());
        assertEquals(c.queryTwa(calc, T0, end, 1000, TwaOptions.step()),
                c.queryTwaGroup(calc, T0, end, Group.of(Unit.SECOND), TwaOptions.step()));
        assertEquals(List.of(step), c.queryTwaGroup(calc, T0, end, null, TwaOptions.step()));
        List<TwaBucket> local = c.queryTwaGroup(calc, T0, end, Group.of(Unit.DAY).in("Europe/Oslo"), TwaOptions.step());
        assertEquals(80_000.0, local.stream().mapToDouble(TwaBucket::area).sum());
        List<TwaBucket> b = c.queryTwaBounds(calc, T0, end, new long[] {T0 + 500, T0 + 2000}, TwaOptions.step());
        assertEquals(List.of(T0 + 500, T0 + 2000), b.stream().map(TwaBucket::start).toList());
        assertEquals(List.of(25_000.0, 50_000.0), b.stream().map(TwaBucket::area).toList());

        invalid(() -> c.queryTwa(calc, T0, end, -1, TwaOptions.step()));
        invalid(() -> c.queryTwa(state, MIN, MAX, 0, TwaOptions.step()));
        assertThrows(IllegalArgumentException.class, () -> TwaOptions.step().limit(-1));
        assertThrows(IllegalArgumentException.class, () -> TwaOptions.step().maxGap(-1));
    }

    @Test
    void lttb() throws Exception {
        ValuePoints all = c.queryLttb(wave, MIN, MAX, 50);
        assertEquals(50, all.size());
        assertEquals(T0, all.timestamps()[0]);
        assertEquals(T0 + 999_000, all.timestamps()[49]);
        assertEquals(ValueType.FLOAT32, all.type());
        ValuePoints desc = c.queryLttb(wave, MIN, MAX, 50, Order.DESC, 0);
        long[] rev = new long[50];
        for (int i = 0; i < 50; i++)
            rev[i] = all.timestamps()[49 - i];
        assertArrayEquals(rev, desc.timestamps());
        assertArrayEquals(Arrays.copyOf(rev, 3), c.queryLttb(wave, MIN, MAX, 50, Order.DESC, 3).timestamps());
        assertArrayEquals(Arrays.copyOf(all.timestamps(), 3), c.queryLttb(wave, MIN, MAX, 50, Order.ASC, 3).timestamps());
        assertEquals(10, c.queryLttb(wave, T0, T0 + 10_000, 50).size(), "fewer points than the threshold: all");
        invalid(() -> c.queryLttb(wave, MIN, MAX, 2));
        invalid(() -> c.queryLttb(state, MIN, MAX, 10));
    }

    @Test
    void anOlderServerIsNotAsked() throws Exception {
        Group oslo = Group.of(Unit.DAY).in("Europe/Oslo");
        for (int version = 4; version <= 8; version++) {
            try (CompressionTest.FakeServer f = new CompressionTest.FakeServer(version)) {
                try (TsdbClient o = TsdbClient.connect("127.0.0.1", f.port())) {
                    if (version < 5)
                        invalid(() -> o.queryLttb(1, MIN, MAX, 100));
                    if (version < 6) {
                        invalid(() -> o.queryTwa(1, MIN, MAX, 0, TwaOptions.step()));
                        invalid(() -> o.queryTwaGroup(1, MIN, MAX, Group.of(Unit.HOUR), TwaOptions.linear()));
                        invalid(() -> o.queryTwaGroup(1, 0, 1000, oslo, TwaOptions.step()));
                        invalid(() -> o.queryTwaBounds(1, MIN, MAX, new long[] {0, 10}, TwaOptions.step()));
                    }
                    if (version < 8) {
                        invalid(() -> o.querySpread(1, MIN, MAX, 0));
                        invalid(() -> o.querySpreadGroup(1, MIN, MAX, Group.of(Unit.HOUR), Order.DESC, 0));
                        invalid(() -> o.querySpreadGroup(1, MIN, MAX, oslo, Order.ASC, 0));
                        invalid(() -> o.querySpreadBounds(1, MIN, MAX, new long[] {0, 10}, Order.ASC, 0));
                    }
                    invalid(() -> o.queryLttb(1, MIN, MAX, 100, Order.DESC, 0));
                    invalid(() -> o.queryLttb(1, MIN, MAX, 100, Order.ASC, 10));
                    o.commit(); /* the connection is still good */
                }
                assertArrayEquals(new Object[] {(int) Protocol.OP_HELLO, (int) Protocol.OP_COMMIT},
                        f.ops().toArray(), "version " + version + ": HELLO and COMMIT only");
            }
        }
    }

    /* A server of version 5 to 8 knows only the 28-byte QUERY_LTTB: this one answers it with no points and the
     * 37-byte form of version 9 with INVALID. A plain queryLttb must still work there. */
    @Test
    void plainLttbIsTheShortRequest() throws Exception {
        for (int version = 5; version <= 9; version++) {
            List<Integer> lengths = new java.util.ArrayList<>();
            try (CompressionTest.FakeServer f = new CompressionTest.FakeServer(version,
                    fr -> new byte[] {1, 0, 0, 0, 0, 0, 0, 0}, /* the last chunk: FLOAT32, no points */
                    fr -> fr.op() == Protocol.OP_QUERY_LTTB && fr.payload().remaining() != 28 ? Status.ERR_INVALID : 0);
                 TsdbClient o = TsdbClient.connect("127.0.0.1", f.port())) {
                ValuePoints p = o.queryLttb(1, MIN, MAX, 100);
                assertEquals(0, p.size(), "version " + version);
                assertEquals(0, o.queryLttb(1, MIN, MAX, 100, Order.ASC, 0).size(), "version " + version);
                synchronized (f.frames) {
                    for (CompressionTest.Frame fr : f.frames)
                        if (fr.op() == Protocol.OP_QUERY_LTTB)
                            lengths.add(fr.payload().remaining());
                }
            }
            assertEquals(List.of(28, 28), lengths, "version " + version);
        }
    }
}
