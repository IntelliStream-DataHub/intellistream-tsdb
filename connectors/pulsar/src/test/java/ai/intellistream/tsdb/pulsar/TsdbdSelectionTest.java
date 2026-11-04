package ai.intellistream.tsdb.pulsar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.intellistream.tsdb.TsdbClient;
import ai.intellistream.tsdb.TsdbException;
import ai.intellistream.tsdb.ValuePoints;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Two tenants into two volumes, and a sink that takes only some series, against a real build/tsdbd: the operator adds
 * the volumes with the CLI first, the sink creates each new series in the volume its name maps to, series outside the
 * selection never reach tsdbd, and a volume nobody added dead-letters the message (until it is added, which tsdbd
 * accepts while it runs).
 */
class TsdbdSelectionTest {
    private Path dir;
    private Path acme;
    private Path globex;
    private Tsdbd server;
    private TsdbSink sink;
    private final CapturingDeadLetter dlq = new CapturingDeadLetter();

    @BeforeEach
    void start() throws Exception {
        dir = Tsdbd.scratch("tsdb-pulsar-sel-");
        acme = Tsdbd.scratch("tsdb-pulsar-vol-acme-").toRealPath();
        globex = Tsdbd.scratch("tsdb-pulsar-vol-globex-").toRealPath();
        dir = dir.toRealPath();
        /* the first start makes the database; the operator adds the volumes (the CLI works while tsdbd runs) before
         * the sink names them */
        server = Tsdbd.start(dir);
        Tsdbd.cli(dir, "volume", "add", acme.toString());
        Tsdbd.cli(dir, "volume", "add", globex.toString());
    }

    @AfterEach
    void stop() throws Exception {
        if (sink != null)
            sink.close();
        if (server != null)
            server.close();
        Tsdbd.delete(dir);
        Tsdbd.delete(acme);
        Tsdbd.delete(globex);
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

    private static void eventually(BooleanSupplier ok, String what) throws InterruptedException {
        long end = System.nanoTime() + 10_000L * 1_000_000;
        while (!ok.getAsBoolean()) {
            if (System.nanoTime() > end)
                throw new AssertionError("timed out waiting for " + what);
            Thread.sleep(10);
        }
    }

    /** Stops the sink and tsdbd and returns series name to volume path, as the CLI lists them. */
    private Map<String, String> volumesOfSeries() throws Exception {
        sink.close();
        server.close();
        Map<String, String> m = new HashMap<>();
        for (String line : Tsdbd.cli(dir, "series").split("\n")) {
            String[] f = line.split("\t", -1);
            if (f.length >= 3) /* name, type, volume path, class */
                m.put(f[0], f[2]);
        }
        return m;
    }

    @Test
    void twoTenantsIntoTwoVolumes() throws Exception {
        TsdbSinkConfig c = config();
        c.connections = 2;
        c.volumes = Map.of("acme.", acme.toString(), "globex.", globex.toString());
        open(c);
        List<FakeRecord> rs = new ArrayList<>();
        String[] names = {"acme.pump.flow", "acme.pump.temp", "globex.tank.bar", "initech.misc", "globex.pump.flow"};
        for (int i = 0; i < 20; i++) {
            FakeRecord r = FakeRecord.point(i, names[i % names.length], 1000 + i, i);
            rs.add(r);
            sink.write(r);
        }
        eventually(() -> rs.stream().allMatch(FakeRecord::settled), "acks");
        assertTrue(rs.stream().allMatch(r -> r.acks.get() == 1), "a record was not acknowledged once");
        assertTrue(dlq.records.isEmpty(), String.valueOf(dlq.reasons));
        try (TsdbClient cl = server.client()) {
            ValuePoints p = cl.queryValues(cl.series("acme.pump.flow", false), Long.MIN_VALUE, Long.MAX_VALUE);
            assertEquals(4, p.size());
        }
        Map<String, String> where = volumesOfSeries();
        assertEquals(acme.toString(), where.get("acme.pump.flow"));
        assertEquals(acme.toString(), where.get("acme.pump.temp"));
        assertEquals(globex.toString(), where.get("globex.tank.bar"));
        assertEquals(globex.toString(), where.get("globex.pump.flow"));
        String root = where.get("initech.misc");
        assertTrue(root != null && !root.equals(acme.toString()) && !root.equals(globex.toString()),
                "an unmapped name must stay in the database directory, was " + root);
    }

    @Test
    void onlyTheSelectedSeriesReachTsdbd() throws Exception {
        TsdbSinkConfig c = config();
        c.series = List.of("pump7.flow", "pump7.temp", "tank1.bar");
        open(c);
        List<FakeRecord> rs = new ArrayList<>();
        String[] names = {"pump7.flow", "pump7.temp", "tank1.bar", "pump7.other", "pump8.flow", "tank1.bar2"};
        for (int i = 0; i < 18; i++) {
            FakeRecord r = FakeRecord.point(i, names[i % names.length], 1000 + i, i);
            rs.add(r);
            sink.write(r);
        }
        eventually(() -> rs.stream().allMatch(FakeRecord::settled), "acks");
        assertTrue(rs.stream().allMatch(r -> r.acks.get() == 1));
        assertEquals(9, sink.metrics().written());
        assertEquals(9, sink.metrics().skippedRecords());
        try (TsdbClient cl = server.client()) {
            for (String n : new String[] {"pump7.flow", "pump7.temp", "tank1.bar"})
                assertEquals(3, cl.queryValues(cl.series(n, false), Long.MIN_VALUE, Long.MAX_VALUE).size(), n);
            for (String n : new String[] {"pump7.other", "pump8.flow", "tank1.bar2"}) {
                TsdbException e = org.junit.jupiter.api.Assertions.assertThrows(TsdbException.class,
                        () -> cl.series(n, false), n + " must not exist");
                assertEquals(ai.intellistream.tsdb.Status.ERR_NOTFOUND, e.status());
            }
        }
    }

    @Test
    void aVolumeNobodyAddedIsDeadLetteredUntilTheOperatorAddsIt() throws Exception {
        Path initech = Tsdbd.scratch("tsdb-pulsar-vol-initech-").toRealPath();
        try {
            TsdbSinkConfig c = config();
            c.volumes = Map.of("acme.", acme.toString(), "initech.", initech.toString());
            open(c);
            FakeRecord bad = FakeRecord.point(1, "initech.a", 1001, 1);
            FakeRecord good = FakeRecord.point(2, "acme.a", 1002, 2);
            sink.write(bad);
            sink.write(good);
            eventually(() -> bad.settled() && good.settled(), "acks");
            assertEquals(1, dlq.records.size(), String.valueOf(dlq.reasons));
            assertTrue(dlq.reasons.get(0).contains(initech.toString()), dlq.reasons.get(0));
            assertTrue(dlq.reasons.get(0).contains("volume add"), dlq.reasons.get(0));
            assertEquals(0, sink.metrics().retries());
            try (TsdbClient cl = server.client()) {
                assertEquals(1, cl.queryValues(cl.series("acme.a", false), Long.MIN_VALUE, Long.MAX_VALUE).size());
                assertTrue(org.junit.jupiter.api.Assertions.assertThrows(TsdbException.class,
                        () -> cl.series("initech.a", false)).status() < 0);
            }
            /* tsdbd opens a volume added while it runs when a request first names its path */
            Tsdbd.cli(dir, "volume", "add", initech.toString());
            FakeRecord again = FakeRecord.point(3, "initech.a", 1003, 3);
            sink.write(again);
            eventually(again::settled, "ack after the volume was added");
            assertEquals(1, dlq.records.size(), String.valueOf(dlq.reasons));
            Map<String, String> where = volumesOfSeries();
            assertEquals(initech.toString(), where.get("initech.a"));
            assertFalse(where.containsKey("initech.b"));
        } finally {
            Tsdbd.delete(initech);
        }
    }
}
