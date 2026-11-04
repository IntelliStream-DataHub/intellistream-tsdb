package ai.intellistream.tsdb.pulsar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.intellistream.tsdb.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Series selection and target volumes in the sink, against an in-memory tsdb: messages for series another instance
 * takes are acknowledged and not written, new series go to the volume their name maps to, and a volume the operator
 * has not added dead-letters the message with a reason that names the path.
 */
class SelectionSinkTest {
    private final FakeTsdb tsdb = new FakeTsdb();
    private final CapturingDeadLetter dlq = new CapturingDeadLetter();
    private TsdbSink sink;

    private TsdbSinkConfig config() {
        TsdbSinkConfig c = new TsdbSinkConfig();
        c.lingerMs = 5;
        c.retryInitialMs = 5;
        c.retryMaxMs = 40;
        c.closeTimeoutMs = 2000;
        return c;
    }

    private void open(TsdbSinkConfig c) {
        c.validate();
        sink = new TsdbSink(tsdb);
        sink.open(c, null, dlq);
    }

    @AfterEach
    void close() throws Exception {
        if (sink != null)
            sink.close();
    }

    private static void eventually(BooleanSupplier ok, String what) throws InterruptedException {
        for (int i = 0; i < 1000; i++) {
            if (ok.getAsBoolean())
                return;
            Thread.sleep(10);
        }
        throw new AssertionError("timed out waiting for " + what);
    }

    private static boolean allSettled(List<FakeRecord> rs) {
        return rs.stream().allMatch(FakeRecord::settled);
    }

    private static FakeRecord points(int seq, String... seriesNames) {
        StringBuilder b = new StringBuilder("{\"points\":[");
        for (int i = 0; i < seriesNames.length; i++)
            b.append(i > 0 ? "," : "").append("{\"series\":\"").append(seriesNames[i]).append("\",\"ts\":").append(1000 + seq)
                    .append(",\"value\":").append(seq + 0.5).append("}");
        return new FakeRecord(seq, b.append("]}").toString());
    }

    @Test
    void otherSeriesAreAcknowledgedAndNotWritten() throws Exception {
        TsdbSinkConfig c = config();
        c.series = List.of("solo");
        c.seriesPrefixes = List.of("acme.");
        open(c);
        List<FakeRecord> rs = new ArrayList<>();
        String[] names = {"solo", "acme.a", "globex.a", "acme", "solo2", "acme.b"};
        for (int i = 0; i < names.length; i++) {
            rs.add(FakeRecord.point(i, names[i], 1000 + i, i));
            sink.write(rs.get(i));
        }
        eventually(() -> allSettled(rs), "acks");
        for (FakeRecord r : rs)
            assertEquals(1, r.acks.get(), "record " + r.seq + " must be acknowledged once");
        assertEquals(List.of("acme.a", "acme.b", "solo"), tsdb.rows().stream().map(r -> r.series).sorted().toList());
        assertEquals(3, sink.metrics().skippedRecords());
        assertEquals(3, sink.metrics().skippedPoints());
        assertEquals(3, sink.metrics().written());
        assertTrue(dlq.records.isEmpty());
        assertTrue(!tsdb.series.containsKey("globex.a") && !tsdb.series.containsKey("solo2"), "skipped series were created");
        assertEquals(0, sink.inFlight());
    }

    @Test
    void aMessageWithSelectedAndOtherPointsWritesTheSelectedOnes() throws Exception {
        TsdbSinkConfig c = config();
        c.seriesPattern = "acme\\..*";
        c.connections = 3;
        open(c);
        FakeRecord mixed = points(1, "acme.a", "globex.a", "acme.b", "initech.z");
        FakeRecord none = points(2, "globex.a", "initech.z");
        sink.write(mixed);
        sink.write(none);
        eventually(() -> allSettled(List.of(mixed, none)), "acks");
        assertEquals(1, mixed.acks.get());
        assertEquals(1, none.acks.get());
        assertEquals(List.of("acme.a", "acme.b"), tsdb.rows().stream().map(r -> r.series).sorted().toList());
        assertEquals(1, sink.metrics().skippedRecords());
        assertEquals(4, sink.metrics().skippedPoints());
    }

    @Test
    void byDefaultAllSeriesAreWritten() throws Exception {
        open(config());
        FakeRecord r = points(1, "a", "b", "c");
        sink.write(r);
        eventually(r::settled, "ack");
        assertEquals(3, tsdb.rows().size());
        assertEquals(0, sink.metrics().skippedPoints());
    }

    @Test
    void newSeriesAreCreatedInTheVolumeTheirNameMapsTo() throws Exception {
        tsdb.volumes.addAll(List.of("/vol/acme", "/vol/globex", "/vol/acme-hot"));
        TsdbSinkConfig c = config();
        c.connections = 2;
        c.volumes = Map.of("acme.", "/vol/acme", "acme.hot.", "/vol/acme-hot", "globex.", "/vol/globex");
        open(c);
        FakeRecord r = points(1, "acme.cold.t", "acme.hot.t", "globex.t", "initech.t");
        sink.write(r);
        eventually(r::settled, "ack");
        assertEquals(1, r.acks.get());
        assertEquals("/vol/acme", tsdb.volumeOf.get("acme.cold.t"));
        assertEquals("/vol/acme-hot", tsdb.volumeOf.get("acme.hot.t"));
        assertEquals("/vol/globex", tsdb.volumeOf.get("globex.t"));
        assertTrue(tsdb.series.containsKey("initech.t"));
        assertNull(tsdb.volumeOf.get("initech.t"), "unmapped names go to the database directory");
        assertTrue(dlq.records.isEmpty(), String.valueOf(dlq.reasons));
    }

    @Test
    void aVolumeTheOperatorHasNotAddedDeadLettersTheMessageAndNamesThePath() throws Exception {
        tsdb.volumes.add("/vol/globex");
        TsdbSinkConfig c = config();
        c.volumes = Map.of("acme.", "/vol/acme", "globex.", "/vol/globex");
        open(c);
        FakeRecord bad1 = points(1, "acme.a");
        FakeRecord good = points(2, "globex.a");
        FakeRecord bad2 = points(3, "acme.b");
        for (FakeRecord r : List.of(bad1, good, bad2))
            sink.write(r);
        eventually(() -> allSettled(List.of(bad1, good, bad2)), "settled");
        assertEquals(2, dlq.records.size());
        for (String reason : dlq.reasons) {
            assertTrue(reason.contains("/vol/acme"), reason);
            assertTrue(reason.contains("volume add"), reason);
        }
        assertEquals(1, bad1.acks.get());
        assertEquals(1, good.acks.get());
        assertEquals(1, bad2.acks.get());
        assertEquals(List.of("globex.a"), tsdb.rows().stream().map(r -> r.series).toList());
        assertTrue(!tsdb.series.containsKey("acme.a"), "created without its volume");
        assertEquals(0, sink.metrics().retries(), "a missing volume is permanent, not retried");
    }

    @Test
    void onceTheOperatorAddedTheVolumeTheNextMessagesAreWritten() throws Exception {
        TsdbSinkConfig c = config();
        c.volumes = Map.of("acme.", "/vol/acme");
        open(c);
        FakeRecord first = points(1, "acme.a");
        sink.write(first);
        eventually(first::settled, "first");
        assertEquals(1, dlq.records.size());
        synchronized (tsdb) {
            tsdb.volumes.add("/vol/acme");
        }
        FakeRecord second = points(2, "acme.a");
        sink.write(second);
        eventually(second::settled, "second");
        assertEquals(1, dlq.records.size());
        assertEquals("/vol/acme", tsdb.volumeOf.get("acme.a"));
        assertEquals(List.of(1002L), tsdb.rows().stream().map(r -> r.ts).toList());
    }

    @Test
    void permanentCodesOverrideApplies() throws Exception {
        /* the operator may declare NOTFOUND retryable (a volume that is added later is awaited): then it is retried */
        TsdbSinkConfig c = config();
        c.volumes = Map.of("acme.", "/vol/acme");
        c.retryableCodes = Integer.toString(Status.ERR_NOTFOUND);
        open(c);
        FakeRecord r = points(1, "acme.a");
        sink.write(r);
        eventually(() -> sink.metrics().retries() >= 2, "retries");
        assertTrue(!r.settled());
        synchronized (tsdb) {
            tsdb.volumes.add("/vol/acme");
        }
        eventually(r::settled, "ack");
        assertEquals("/vol/acme", tsdb.volumeOf.get("acme.a"));
        assertTrue(dlq.records.isEmpty());
    }
}
