package ai.intellistream.tsdb.pulsar;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.intellistream.tsdb.Column;
import ai.intellistream.tsdb.TsdbClient;
import ai.intellistream.tsdb.ValuePoints;
import ai.intellistream.tsdb.ValueType;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The sink against a real build/tsdbd in its default durable mode: the value types, nulls and corrections arrive as
 * written, the server's refusals become dead letters, and a kill -9 of tsdbd mid-stream loses nothing that was acked
 * and nothing at all once it is back.
 */
class TsdbdSinkTest {
    private Path dir;
    private Tsdbd server;
    private TsdbSink sink;
    private final CapturingDeadLetter dlq = new CapturingDeadLetter();

    @BeforeEach
    void start() throws Exception {
        dir = Tsdbd.scratch("tsdb-pulsar-");
        server = Tsdbd.start(dir);
    }

    @AfterEach
    void stop() throws Exception {
        if (sink != null)
            sink.close();
        if (server != null)
            server.close();
        Tsdbd.delete(dir);
    }

    private TsdbSinkConfig config() {
        TsdbSinkConfig c = new TsdbSinkConfig();
        c.port = server.port;
        c.lingerMs = 5;
        c.retryInitialMs = 20;
        c.retryMaxMs = 200;
        c.connectTimeoutMs = 1000;
        c.closeTimeoutMs = 5000;
        return c;
    }

    private void open(TsdbSinkConfig c) {
        c.validate();
        sink = new TsdbSink(TsdbConnection.client(c));
        sink.open(c, null, dlq);
    }

    private static void eventually(BooleanSupplier ok, long ms, String what) throws InterruptedException {
        long end = System.nanoTime() + ms * 1_000_000;
        while (!ok.getAsBoolean()) {
            if (System.nanoTime() > end)
                throw new AssertionError("timed out waiting for " + what);
            Thread.sleep(10);
        }
    }

    private static FakeRecord rec(int seq, String series, long ts, String value, String type) {
        return new FakeRecord(seq, "{\"series\":\"" + series + "\",\"ts\":" + ts + ",\"value\":" + value
                + (type == null ? "" : ",\"type\":\"" + type + "\"") + "}");
    }

    private static ValuePoints values(TsdbClient c, String series) throws Exception {
        return c.queryValues(c.series(series, false), Long.MIN_VALUE, Long.MAX_VALUE);
    }

    @Test
    void typesNullsAndCorrectionsArriveAsWritten() throws Exception {
        open(config());
        List<FakeRecord> rs = new ArrayList<>();
        int seq = 0;
        for (int i = 0; i < 10; i++)
            rs.add(rec(seq++, "f64", 1000 + i, Double.toString(i * 0.5), null)); /* default type: float64 */
        for (int i = 0; i < 5; i++)
            rs.add(rec(seq++, "f64", 1000 + i, Double.toString(100 + i), null)); /* corrections */
        rs.add(rec(seq++, "f64", 1009, "null", null)); /* a null replaces a value like any other write */
        for (int i = 0; i < 4; i++)
            rs.add(rec(seq++, "i64", 1000 + i, Long.toString((1L << 60) + i), "int64")); /* beyond 2^53: exact */
        rs.add(rec(seq++, "i64", 1001, "-7", null)); /* correction; the type comes from the existing series */
        rs.add(rec(seq++, "f32", 1000, "1.25", "float32"));
        rs.add(rec(seq++, "on", 1000, "true", "bool"));
        rs.add(rec(seq++, "on", 1001, "false", "bool"));
        rs.add(rec(seq++, "on", 1000, "false", null));
        rs.add(rec(seq++, "mode", 1000, "\"run\"", "state"));
        rs.add(rec(seq++, "mode", 1001, "\"stop\"", null));
        rs.add(rec(seq++, "mode", 1002, "\"run\"", null));
        rs.add(rec(seq++, "mode", 1001, "null", null));
        for (FakeRecord r : rs)
            sink.write(r);
        eventually(() -> rs.stream().allMatch(FakeRecord::settled), 10_000, "acks");
        assertTrue(rs.stream().allMatch(r -> r.acks.get() == 1 && r.fails.get() == 0), "a record failed");
        assertTrue(dlq.records.isEmpty(), String.valueOf(dlq.reasons));

        try (TsdbClient c = server.client()) {
            ValuePoints f = values(c, "f64");
            assertEquals(ValueType.FLOAT64, f.type());
            assertEquals(10, f.size());
            double[] want = {100, 101, 102, 103, 104, 2.5, 3, 3.5, 4, 0};
            for (int i = 0; i < 9; i++)
                assertEquals(want[i], f.values().doubles()[i], "f64@" + (1000 + i));
            assertTrue(f.values().isNull(9));

            ValuePoints l = values(c, "i64");
            assertEquals(ValueType.INT64, l.type());
            assertArrayEquals(new long[] {1L << 60, -7, (1L << 60) + 2, (1L << 60) + 3}, l.values().longs());

            ValuePoints g = values(c, "f32");
            assertEquals(ValueType.FLOAT32, g.type());
            assertEquals(1.25f, g.values().floats()[0]);

            ValuePoints b = values(c, "on");
            assertEquals(ValueType.BOOL, b.type());
            assertArrayEquals(new boolean[] {false, false}, b.values().bools());

            ValuePoints m = values(c, "mode");
            assertEquals(ValueType.STATE, m.type());
            assertEquals(3, m.size());
            int id = c.series("mode", false);
            Column mc = m.values();
            assertEquals("run", c.stateName(id, mc.states()[0]));
            assertTrue(mc.isNull(1));
            assertEquals("run", c.stateName(id, mc.states()[2]));
        }
    }

    @Test
    void serverRefusalsBecomeDeadLettersAndTheRestIsWritten() throws Exception {
        try (TsdbClient c = server.client()) {
            c.seriesCreate("made.elsewhere", ValueType.INT64); /* not in the sink's cache: tsdbd refuses the type */
        }
        open(config());
        List<FakeRecord> good = new ArrayList<>();
        List<FakeRecord> bad = new ArrayList<>();
        good.add(rec(0, "flow", 1, "2.5", null));
        bad.add(rec(1, "n".repeat(40_000), 1, "1", null)); /* longer than tsdbd's 32767-byte names */
        bad.add(rec(2, "made.elsewhere", 1, "1.5", "float64"));
        bad.add(rec(3, "flow", 2, "\"high\"", null));
        good.add(rec(4, "flow", 3, "4", null));
        good.add(rec(5, "made.elsewhere", 1, "9", null));
        List<FakeRecord> all = new ArrayList<>(good);
        all.addAll(bad);
        all.sort((a, b) -> a.seq - b.seq);
        for (FakeRecord r : all)
            sink.write(r);
        eventually(() -> all.stream().allMatch(FakeRecord::settled), 10_000, "acks");
        assertTrue(all.stream().allMatch(r -> r.acks.get() == 1 && r.fails.get() == 0));
        assertEquals(bad.size(), dlq.records.size(), String.valueOf(dlq.reasons));
        assertTrue(dlq.records.containsAll(bad));
        assertTrue(dlq.reasons.stream().allMatch(r -> r.length() < 1000), "a 40 kB name in a log line");
        try (TsdbClient c = server.client()) {
            assertArrayEquals(new long[] {1, 3}, values(c, "flow").timestamps());
            assertArrayEquals(new long[] {9}, values(c, "made.elsewhere").values().longs());
        }
    }

    /**
     * After a failover the old writer may serve as a read-only follower and still accept connections. Listed first in
     * hosts, it answers every insert with READONLY: the sink must move on to the next endpoint, not ask it forever.
     */
    @Test
    void aReadOnlyFirstEndpointIsPassedOverForTheWriter() throws Exception {
        Path roDir = Tsdbd.scratch("tsdb-pulsar-ro-");
        try {
            Tsdbd.start(roDir).stop(); /* a database for the read-only server to open */
            try (Tsdbd ro = Tsdbd.start(roDir, "--read-only")) {
                TsdbSinkConfig c = config();
                c.hosts = "127.0.0.1:" + ro.port + ",127.0.0.1:" + server.port;
                c.connections = 2;
                open(c);
                List<FakeRecord> rs = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    rs.add(rec(i, "s" + (i % 10), i, Integer.toString(i), null));
                    sink.write(rs.get(i));
                }
                eventually(() -> rs.stream().allMatch(FakeRecord::settled), 20_000, "acks through the writer");
                assertTrue(rs.stream().allMatch(r -> r.acks.get() == 1), "dead letters: " + dlq.reasons);
                try (TsdbClient cl = server.client()) {
                    for (int s = 0; s < 10; s++)
                        assertEquals(10, values(cl, "s" + s).timestamps().length);
                }
            }
        } finally {
            Tsdbd.delete(roDir);
        }
    }

    /**
     * kill -9 of tsdbd while the sink streams: every record acked before the kill is on disk after it (ack means
     * durable), and once tsdbd is back on the same address every record is written, last value per key.
     */
    @Test
    void killDashNineMidStreamLosesNothing() throws Exception {
        TsdbSinkConfig cfg = config();
        cfg.connections = 2;
        cfg.batchRows = 500;
        cfg.maxInFlightRecords = 4000;
        open(cfg);
        final int series = 200;
        final int n = 40_000;
        List<FakeRecord> rs = new ArrayList<>(n);
        String[] key = new String[n];
        for (int i = 0; i < n; i++) {
            /* the last eighth corrects keys written in the first eighth, with new values */
            int k = i < n - n / 8 ? i : i - (n - n / 8);
            key[i] = "pump" + (k % series) + "@" + (1000 + k / series);
            rs.add(rec(i, "pump" + (k % series), 1000 + k / series, Integer.toString(i), null));
        }
        AtomicReference<Throwable> writerError = new AtomicReference<>();
        Thread writer = new Thread(() -> {
            try {
                for (FakeRecord r : rs)
                    sink.write(r);
            } catch (Throwable t) {
                writerError.set(t);
            }
        });
        writer.start();
        eventually(() -> rs.stream().filter(r -> r.acks.get() > 0).count() >= n / 4, 60_000, "a quarter acked");
        server.kill();
        int port = server.port;
        List<FakeRecord> acked = new ArrayList<>();
        for (FakeRecord r : rs)
            if (r.acks.get() > 0)
                acked.add(r);
        assertTrue(acked.size() < n, "everything was acked before the kill: the test proves nothing");
        long retriesBefore = sink.metrics().retries();

        /* Look at the disk before the sink can reach tsdbd again: on another port. */
        Thread.sleep(300); /* the sink notices and backs off meanwhile */
        Map<String, Double> durable = new HashMap<>();
        Tsdbd peek = Tsdbd.start(dir);
        try (TsdbClient c = peek.client()) {
            for (int s = 0; s < series; s++) {
                ValuePoints v;
                try {
                    v = values(c, "pump" + s);
                } catch (Exception e) {
                    continue; /* not created yet */
                }
                for (int i = 0; i < v.size(); i++)
                    durable.put("pump" + s + "@" + v.timestamps()[i], v.values().doubles()[i]);
            }
        } finally {
            peek.stop();
        }
        for (FakeRecord r : acked) {
            Double v = durable.get(key[r.seq]);
            assertTrue(v != null && v >= r.seq, "acked record " + r.seq + " (" + key[r.seq] + ") lost: " + v);
        }

        server = Tsdbd.start(dir, port);
        writer.join(120_000);
        assertEquals(null, writerError.get());
        eventually(() -> rs.stream().allMatch(FakeRecord::settled), 120_000, "all acked after the restart");
        assertTrue(rs.stream().allMatch(r -> r.acks.get() == 1 && r.fails.get() == 0), "failed or acked twice");
        assertTrue(dlq.records.isEmpty(), String.valueOf(dlq.reasons));
        assertTrue(sink.metrics().retries() > retriesBefore, "the sink never retried");

        Map<String, Double> want = new HashMap<>();
        for (int i = 0; i < n; i++)
            want.put(key[i], (double) i);
        Map<String, Double> got = new HashMap<>();
        long rows = 0;
        try (TsdbClient c = server.client()) {
            for (int s = 0; s < series; s++) {
                ValuePoints v = values(c, "pump" + s);
                rows += v.size();
                for (int i = 0; i < v.size(); i++)
                    got.put("pump" + s + "@" + v.timestamps()[i], v.values().doubles()[i]);
            }
        }
        assertEquals(want.size(), rows, "duplicate or missing rows");
        assertEquals(want, got);
    }
}
