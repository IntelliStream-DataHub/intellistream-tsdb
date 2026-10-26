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
    static int calc, wave, state;

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
}
