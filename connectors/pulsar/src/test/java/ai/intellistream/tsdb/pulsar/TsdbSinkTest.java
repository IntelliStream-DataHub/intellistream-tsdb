package ai.intellistream.tsdb.pulsar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.intellistream.tsdb.Status;
import ai.intellistream.tsdb.ValueType;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.function.BooleanSupplier;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.io.core.SinkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The sink's contract against an in-memory tsdb: ack after durable, order, retries, dead letters, backpressure. */
class TsdbSinkTest {
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

    private TsdbSink open(TsdbSinkConfig c) {
        c.validate();
        sink = new TsdbSink(tsdb);
        sink.open(c, null, dlq);
        return sink;
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

    @Test
    void acksOnlyAfterTsdbAcknowledged() throws Exception {
        tsdb.gate = new Semaphore(0);
        open(config());
        List<FakeRecord> rs = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            rs.add(FakeRecord.point(i, "s" + (i % 5), 1000 + i, i * 1.5));
            sink.write(rs.get(i));
        }
        Thread.sleep(200);
        assertTrue(rs.stream().noneMatch(FakeRecord::settled), "acked before tsdb acknowledged");
        assertTrue(tsdb.rows().isEmpty());
        tsdb.gate.release(1000);
        eventually(() -> allSettled(rs), "acks");
        for (FakeRecord r : rs) {
            assertEquals(1, r.acks.get());
            assertEquals(0, r.fails.get());
        }
        assertEquals(50, tsdb.rows().size());
        assertEquals(50, sink.metrics().written());
    }

    @Test
    void retryableFailuresRetryTheSameBatchInOrder() throws Exception {
        for (int i = 0; i < 3; i++)
            tsdb.awaitFailures.add(i == 1 ? Status.ERR_READONLY : Status.ERR_IO);
        TsdbSinkConfig c = config();
        c.batchRows = 7;
        open(c);
        List<FakeRecord> rs = new ArrayList<>();
        /* 300 corrections of 10 keys: the last write of each key must win */
        for (int i = 0; i < 300; i++) {
            rs.add(FakeRecord.point(i, "pump" + (i % 3), 1000 + i % 10, i));
            sink.write(rs.get(i));
        }
        eventually(() -> allSettled(rs), "acks");
        assertTrue(rs.stream().allMatch(r -> r.acks.get() == 1 && r.fails.get() == 0), "a record failed or acked twice");
        assertTrue(sink.metrics().retries() >= 3);
        Map<String, Double> want = new HashMap<>();
        for (int i = 0; i < 300; i++)
            want.put("pump" + (i % 3) + "@" + (1000 + i % 10), (double) i);
        assertEquals(want, tsdb.last());
        /* per series, rows reach tsdb in the order they were written to the sink */
        Map<String, Double> prev = new HashMap<>();
        for (FakeTsdb.Row r : tsdb.rows()) {
            Double p = prev.put(r.series, r.value);
            assertTrue(p == null || p < r.value, "series " + r.series + " out of order: " + p + " then " + r.value);
        }
    }

    @Test
    void perSeriesOrderAcrossConnections() throws Exception {
        TsdbSinkConfig c = config();
        c.connections = 4;
        c.batchRows = 13;
        open(c);
        tsdb.awaitFailures.add(Status.ERR_IO);
        List<FakeRecord> rs = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            rs.add(FakeRecord.point(i, "s" + (i % 37), i / 37, i));
            sink.write(rs.get(i));
        }
        eventually(() -> allSettled(rs), "acks");
        Map<String, Double> prev = new HashMap<>();
        int n = 0;
        for (FakeTsdb.Row r : tsdb.rows()) {
            Double p = prev.put(r.series, r.value);
            assertTrue(p == null || p < r.value, "series " + r.series + " out of order");
            n++;
        }
        assertTrue(n >= 2000);
        assertEquals(2000, tsdb.last().size());
    }

    @Test
    void backpressureBlocksWriteWhileTsdbIsDown() throws Exception {
        tsdb.down = true;
        TsdbSinkConfig c = config();
        c.maxInFlightRecords = 10;
        open(c);
        List<FakeRecord> rs = new ArrayList<>();
        for (int i = 0; i < 100; i++)
            rs.add(FakeRecord.point(i, "s", i, i));
        int[] written = {0};
        Thread producer = new Thread(() -> {
            try {
                for (FakeRecord r : rs) {
                    sink.write(r);
                    written[0]++;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        producer.start();
        Thread.sleep(300);
        assertTrue(producer.isAlive(), "write() did not block");
        assertEquals(10, sink.inFlight());
        assertTrue(written[0] <= 10);
        assertTrue(rs.stream().noneMatch(FakeRecord::settled));
        assertTrue(sink.metrics().retries() > 0);
        assertTrue(sink.metrics().backoffMs() > 0);
        tsdb.down = false;
        producer.join(10_000);
        eventually(() -> allSettled(rs), "acks after tsdb came back");
        assertEquals(100, tsdb.last().size());
        assertTrue(rs.stream().allMatch(r -> r.acks.get() == 1));
        assertEquals(0, sink.metrics().backoffMs());
    }

    @Test
    void byteLimitBlocksToo() throws Exception {
        tsdb.down = true;
        TsdbSinkConfig c = config();
        c.maxInFlightBytes = 1000; /* about 15 points of a short series name */
        open(c);
        Thread producer = new Thread(() -> {
            try {
                for (int i = 0; i < 100; i++)
                    sink.write(FakeRecord.point(i, "s", i, i));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        producer.start();
        Thread.sleep(300);
        assertTrue(producer.isAlive());
        assertTrue(sink.inFlight() < 30 && sink.inFlight() > 0, "in flight " + sink.inFlight());
        tsdb.down = false;
        producer.join(10_000);
        eventually(() -> sink.inFlight() == 0, "drain");
    }

    @Test
    void poisonRecordsGoToTheDeadLetterTopicAndTheRestProceeds() throws Exception {
        open(config());
        List<FakeRecord> good = new ArrayList<>();
        List<FakeRecord> bad = new ArrayList<>();
        good.add(FakeRecord.point(0, "flow", 1, 2.5));
        good.add(new FakeRecord(1, "{\"series\":\"count\",\"ts\":1,\"value\":7,\"type\":\"int64\"}"));
        bad.add(FakeRecord.point(2, "flow", 2, "\"high\"".replace("\"", ""))); /* a string into float64 */
        bad.add(new FakeRecord(3, "{not json"));
        bad.add(new FakeRecord(4, "{\"ts\":1,\"value\":1}")); /* no series */
        bad.add(new FakeRecord(5, "{\"series\":\"count\",\"ts\":2,\"value\":1.5}")); /* fraction into int64 */
        bad.add(new FakeRecord(6, "{\"series\":\"count\",\"ts\":3,\"value\":1,\"type\":\"float64\"}")); /* type */
        bad.add(new FakeRecord(7, "{\"series\":\"\",\"ts\":3,\"value\":1}")); /* empty name */
        bad.add(new FakeRecord(8, "{\"series\":\"x\",\"ts\":\"yesterday\",\"value\":1}"));
        bad.add(new FakeRecord(9, "{\"series\":\"x\",\"ts\":1,\"value\":1,\"type\":\"complex\"}"));
        bad.add(new FakeRecord(10, "{\"series\":\"" + "n".repeat(70_000) + "\",\"ts\":1,\"value\":1}"));
        good.add(FakeRecord.point(11, "flow", 3, 4.0));
        good.add(new FakeRecord(12, "{\"series\":\"on\",\"ts\":1,\"value\":true,\"type\":\"bool\"}"));
        good.add(new FakeRecord(13, "{\"series\":\"mode\",\"ts\":1,\"value\":\"auto\",\"type\":\"state\"}"));
        good.add(new FakeRecord(14, "{\"series\":\"flow\",\"ts\":4,\"value\":null}"));
        List<FakeRecord> all = new ArrayList<>(good);
        all.addAll(bad);
        all.sort((a, b) -> a.seq - b.seq);
        for (FakeRecord r : all)
            sink.write(r);
        eventually(() -> allSettled(all), "all settled");
        assertTrue(good.stream().allMatch(r -> r.acks.get() == 1 && r.fails.get() == 0));
        assertTrue(bad.stream().allMatch(r -> r.acks.get() == 1 && r.fails.get() == 0), "dead letters are acked");
        assertEquals(bad.size(), dlq.records.size(), String.valueOf(dlq.reasons));
        assertTrue(dlq.records.containsAll(bad));
        assertEquals(bad.size(), sink.metrics().deadLetteredCount());
        Map<String, Double> last = tsdb.last();
        assertEquals(2.5, last.get("flow@1"));
        assertEquals(4.0, last.get("flow@3"));
        assertTrue(Double.isNaN(last.get("flow@4")));
        assertEquals(7.0, last.get("count@1"));
        assertEquals(ValueType.INT64, tsdb.series.get("count").type);
        assertEquals(ValueType.FLOAT64, tsdb.series.get("flow").type);
        assertEquals(ValueType.STATE, tsdb.series.get("mode").type);
        assertFalse(last.containsKey("count@2"));
    }

    @Test
    void recordRefusedByTheServerIsFoundByHalvingTheBatch() throws Exception {
        tsdb.refused.add("evil");
        TsdbSinkConfig c = config();
        c.lingerMs = 200;
        open(c);
        List<FakeRecord> rs = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            rs.add(FakeRecord.point(i, i == 17 ? "evil" : "s" + (i % 4), i, i));
            sink.write(rs.get(i));
        }
        eventually(() -> allSettled(rs), "all settled");
        assertEquals(1, dlq.records.size());
        assertEquals(17, ((FakeRecord) dlq.records.get(0)).seq);
        assertEquals(39, tsdb.last().size());
    }

    @Test
    void stateNamesFromTheTopicStayWithinTheCacheBounds() throws Exception {
        TsdbSinkConfig c = config();
        c.stateCacheSize = 3;
        c.seriesCacheBytes = 256 << 10;
        open(c);
        List<FakeRecord> rs = new ArrayList<>();
        for (int i = 0; i < 60; i++) { /* 60 KB state names, and the first three again at the end */
            String v = (i < 57 ? "v" + i : "v" + (i - 57)) + "x".repeat(60_000);
            rs.add(new FakeRecord(i, "{\"series\":\"mode\",\"ts\":" + i + ",\"value\":\"" + v
                    + "\",\"type\":\"state\"}"));
            sink.write(rs.get(i));
        }
        eventually(() -> allSettled(rs), "all settled");
        assertEquals(0, dlq.records.size(), String.valueOf(dlq.reasons));
        assertEquals(60, tsdb.last().size());
        assertEquals(60, tsdb.stateCalls.get(), "v0 to v2 were evicted and asked again");
        for (int i = 60; i < 63; i++) { /* the latest name stays cached (two of 120 KB fit): no request */
            rs.add(new FakeRecord(i, "{\"series\":\"mode\",\"ts\":" + i + ",\"value\":\"v2"
                    + "x".repeat(60_000) + "\",\"type\":\"state\"}"));
            sink.write(rs.get(i));
        }
        eventually(() -> allSettled(rs), "all settled");
        assertEquals(63, tsdb.last().size());
        assertEquals(60, tsdb.stateCalls.get(), "cached state names were asked again");
    }

    @Test
    void autoCreateOffSendsUnknownSeriesToTheDeadLetterTopic() throws Exception {
        TsdbSinkConfig c = config();
        c.autoCreate = false;
        tsdb.series.put("known", new SeriesCache.Entry(1, ValueType.FLOAT64));
        tsdb.names.put(1L, "known");
        open(c);
        FakeRecord a = FakeRecord.point(0, "known", 1, 1.0);
        FakeRecord b = FakeRecord.point(1, "unknown", 1, 1.0);
        sink.write(a);
        sink.write(b);
        eventually(() -> a.settled() && b.settled(), "settled");
        assertEquals(List.of(b), dlq.records);
        assertEquals(1, tsdb.rows().size());
    }

    @Test
    void deadLetterTopicDownLeavesRecordUnackedUntilItWorks() throws Exception {
        dlq.failNext = 3;
        open(config());
        FakeRecord bad = new FakeRecord(0, "nonsense");
        FakeRecord good = FakeRecord.point(1, "s", 1, 1);
        Thread t = new Thread(() -> {
            try {
                sink.write(bad);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        t.start();
        t.join(5000);
        sink.write(good);
        eventually(() -> bad.settled() && good.settled(), "settled");
        assertEquals(List.of(bad), dlq.records);
        assertEquals(3, sink.metrics().deadLetterFailures());
    }

    @Test
    void batchMessageOverSeveralConnectionsIsAckedOnceWhenAllPartsAreWritten() throws Exception {
        TsdbSinkConfig c = config();
        c.connections = 3;
        open(c);
        StringBuilder b = new StringBuilder("{\"points\":[");
        for (int i = 0; i < 30; i++)
            b.append(i > 0 ? "," : "").append("{\"series\":\"s").append(i).append("\",\"ts\":5,\"value\":").append(i)
                    .append('}');
        FakeRecord r = new FakeRecord(0, b.append("]}").toString());
        FakeRecord col = new FakeRecord(1, "{\"series\":\"c\",\"ts\":[1,2,3],\"value\":[1.5,null,3]}");
        FakeRecord arr = new FakeRecord(2, "[{\"series\":\"a\",\"ts\":1,\"value\":1},{\"series\":\"b\",\"ts\":1,\"value\":2}]");
        /* hold one shard's part: the record must wait for it although the other shards are done */
        tsdb.held.add("s0");
        int s0 = TsdbSink.shardOf("s0", 3);
        long others = java.util.stream.IntStream.range(0, 30).filter(i -> TsdbSink.shardOf("s" + i, 3) != s0).count();
        sink.write(r);
        sink.write(col);
        sink.write(arr);
        eventually(() -> col.settled() && arr.settled() && tsdb.rows().size() >= others + 5, "the other parts");
        Thread.sleep(100);
        assertFalse(r.settled(), "acked while a part of it was not written");
        tsdb.heldGate.release(1000);
        eventually(r::settled, "settled");
        Thread.sleep(50);
        assertEquals(1, r.acks.get());
        assertEquals(0, r.fails.get());
        assertEquals(35, tsdb.last().size());
    }

    @Test
    void closeLeavesUnwrittenRecordsUnacknowledged() throws Exception {
        tsdb.down = true;
        TsdbSinkConfig c = config();
        c.closeTimeoutMs = 300;
        c.batchRows = 2; /* most records are still queued, not in the batch being retried, when close gives up */
        open(c);
        List<FakeRecord> rs = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            rs.add(FakeRecord.point(i, "s", i, i));
            sink.write(rs.get(i));
        }
        long t0 = System.nanoTime();
        sink.close();
        assertTrue(System.nanoTime() - t0 < 5_000_000_000L, "close took too long");
        assertTrue(rs.stream().noneMatch(FakeRecord::settled), "close acked or failed unwritten records");
        FakeRecord late = FakeRecord.point(9, "s", 9, 9);
        sink.write(late);
        assertFalse(late.settled());
        sink = null;
    }

    @Test
    void closeFlushesWhatTsdbTakes() throws Exception {
        TsdbSinkConfig c = config();
        c.lingerMs = 10_000;
        c.batchRows = 1_000_000;
        open(c);
        List<FakeRecord> rs = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            rs.add(FakeRecord.point(i, "s", i, i));
            sink.write(rs.get(i));
        }
        sink.close();
        sink = null;
        assertTrue(rs.stream().allMatch(r -> r.acks.get() == 1));
        assertEquals(20, tsdb.rows().size());
    }

    @Test
    void closeWhileAnInsertHangsGivesUpAtTheDeadline() throws Exception {
        tsdb.gate = new Semaphore(0);
        TsdbSinkConfig c = config();
        c.closeTimeoutMs = 200;
        open(c);
        FakeRecord r = FakeRecord.point(0, "s", 1, 1);
        sink.write(r);
        Thread.sleep(100);
        sink.close();
        sink = null;
        assertFalse(r.settled());
        assertTrue(tsdb.rows().isEmpty());
    }

    @Test
    void staleCacheIsRefreshedBeforeARecordIsPoison() throws Exception {
        open(config());
        FakeRecord first = FakeRecord.point(0, "s", 1, 1.5);
        sink.write(first);
        eventually(first::settled, "first");
        /* the series changes type behind the sink's cache, as with another server behind the same endpoint */
        synchronized (tsdb) {
            tsdb.series.put("s", new SeriesCache.Entry(1000, ValueType.INT64));
            tsdb.names.put(1000L, "s");
        }
        FakeRecord second = FakeRecord.point(1, "s", 2, 7);
        sink.write(second);
        eventually(second::settled, "second");
        assertEquals(1, second.acks.get());
        assertTrue(dlq.records.isEmpty(), String.valueOf(dlq.reasons));
        assertEquals(ValueType.INT64, tsdb.rows().get(1).type);
    }

    @Test
    void batchBytesEndsABatchBeforeTheLinger() throws Exception {
        TsdbSinkConfig c = config();
        c.lingerMs = 10_000;
        c.batchRows = 1_000_000;
        c.batchBytes = 1; /* every record is a batch of its own; none waits for the linger */
        open(c);
        List<FakeRecord> rs = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            rs.add(FakeRecord.point(i, "s", i, i));
            sink.write(rs.get(i));
        }
        long end = System.nanoTime() + 3_000_000_000L;
        while (!allSettled(rs) && System.nanoTime() < end)
            Thread.sleep(10);
        assertTrue(allSettled(rs), "records waited for the linger instead of ending at batchBytes");
        synchronized (tsdb) {
            assertEquals(10, tsdb.awaits);
        }
    }

    @Test
    void lingerGathersRecordsIntoOneBatch() throws Exception {
        TsdbSinkConfig c = config();
        c.lingerMs = 1000;
        open(c);
        List<FakeRecord> rs = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            rs.add(FakeRecord.point(i, "s", i, i));
            sink.write(rs.get(i));
            Thread.sleep(20);
        }
        eventually(() -> allSettled(rs), "acks");
        synchronized (tsdb) {
            assertEquals(1, tsdb.awaits, "records that came within the linger were written in separate batches");
        }
    }

    @Test
    void nullEncodingsAndOutOfRangeValuesAreDeadLettered() throws Exception {
        open(config());
        List<FakeRecord> rs = List.of(
                new FakeRecord(0, "{\"series\":\"i\",\"ts\":1,\"value\":-9223372036854775808,\"type\":\"int64\"}"),
                new FakeRecord(1, "{\"series\":\"f\",\"ts\":1,\"value\":1e39,\"type\":\"float32\"}"),
                new FakeRecord(2, PartsTest.record(Map.of("series", "d", "ts", 1L, "value",
                        Double.longBitsToDouble(0x7ff8000000001001L)), SchemaType.AVRO)),
                new FakeRecord(3, PartsTest.record(Map.of("series", "g", "ts", 1L, "value",
                        Float.intBitsToFloat(0x7fc01001), "type", "float32"), SchemaType.AVRO)),
                new FakeRecord(4, "{\"series\":\"i\",\"ts\":2,\"value\":-9223372036854775807}"),
                new FakeRecord(5, "{\"series\":\"f\",\"ts\":2,\"value\":3.5}"),
                new FakeRecord(6, PartsTest.record(Map.of("series", "d", "ts", 2L, "value", Double.NaN), SchemaType.AVRO)));
        for (FakeRecord r : rs)
            sink.write(r);
        eventually(() -> allSettled(rs), "settled");
        assertEquals(rs.subList(0, 4), dlq.records, String.valueOf(dlq.reasons));
        assertTrue(rs.stream().allMatch(r -> r.acks.get() == 1));
        Map<String, Double> last = tsdb.last();
        assertEquals(Set.of("i@2", "f@2", "d@2"), last.keySet());
        assertEquals(-9223372036854775807.0, last.get("i@2"));
        assertEquals(3.5, last.get("f@2"));
        assertTrue(Double.isNaN(last.get("d@2")));
    }

    @Test
    void backoffStartsOverAfterASuccess() throws Exception {
        TsdbSinkConfig c = config();
        c.retryInitialMs = 50;
        c.retryMaxMs = 800;
        open(c);
        synchronized (tsdb) {
            for (int i = 0; i < 5; i++)
                tsdb.awaitFailures.add(Status.ERR_NOMEM);
        }
        FakeRecord a = FakeRecord.point(0, "s", 1, 1);
        sink.write(a);
        eventually(a::settled, "the first outage");
        synchronized (tsdb) {
            tsdb.awaitFailures.add(Status.ERR_NOMEM);
        }
        FakeRecord b = FakeRecord.point(1, "s", 2, 2);
        long t0 = System.nanoTime();
        sink.write(b);
        eventually(b::settled, "the second outage");
        long ms = (System.nanoTime() - t0) / 1_000_000;
        assertTrue(ms < 500, "the first retry after a success waited " + ms + " ms: the backoff did not start over");
        assertEquals(0, sink.metrics().backoffMs());
    }

    @Test
    void untrustedFieldsAreCutInDeadLetterReasons() throws Exception {
        open(config());
        String big = "x".repeat(3_000_000);
        List<FakeRecord> rs = List.of(
                new FakeRecord(0, "{\"series\":\"s\",\"ts\":1,\"value\":1,\"type\":\"" + big + "\"}"),
                new FakeRecord(1, "{\"series\":\"s\",\"ts\":\"" + big + "\",\"value\":1}"),
                new FakeRecord(2, "{\"series\":\"s\",\"ts\":1,\"value\":\"" + big + "\"}"),
                new FakeRecord(3, "{\"series\":\"s\",\"ts\":1,\"value\":1e39,\"type\":\"" + big + "\"}"));
        for (FakeRecord r : rs)
            sink.write(r);
        eventually(() -> allSettled(rs), "settled");
        assertEquals(4, dlq.records.size());
        for (String reason : dlq.reasons)
            assertTrue(reason.length() <= Pending.MAX_REASON + 3, "a reason of " + reason.length() + " chars");
    }

    @Test
    void anAckThatThrowsNeitherRewritesTheBatchNorStopsTheShard() throws Exception {
        TsdbSinkConfig c = config();
        c.lingerMs = 200;
        open(c);
        FakeRecord a = FakeRecord.point(0, "s", 1, 1);
        a.ackThrows = 1;
        FakeRecord b = FakeRecord.point(1, "s", 2, 2);
        sink.write(a);
        sink.write(b);
        eventually(b::settled, "b");
        FakeRecord later = FakeRecord.point(2, "s", 3, 3);
        sink.write(later);
        eventually(later::settled, "the next record");
        assertEquals(0, a.acks.get(), "a record whose ack failed is left to be delivered again");
        assertEquals(1, b.acks.get());
        assertEquals(3, tsdb.rows().size(), "the batch was written twice");
        eventually(() -> sink.inFlight() == 0, "room released");
    }

    private static SinkContext context(SinkConfig rc) {
        return (SinkContext) Proxy.newProxyInstance(SinkContext.class.getClassLoader(), new Class<?>[] {SinkContext.class},
                (proxy, m, args) -> m.getName().equals("getSinkConfig") ? rc : null);
    }

    @Test
    void openRefusesAnAckTimeoutAndAnOrderedInputWithoutDeadLetterTopic() throws Exception {
        TsdbSinkConfig c = config();
        c.validate();
        SinkConfig timeout = new SinkConfig();
        timeout.setTimeoutMs(30_000L);
        assertThrows(IllegalArgumentException.class, () -> new TsdbSink(tsdb).open(c, context(timeout), null));
        SinkConfig ordered = new SinkConfig();
        ordered.setRetainKeyOrdering(true);
        assertThrows(IllegalArgumentException.class, () -> new TsdbSink(tsdb).open(c, context(ordered), null));
        c.deadLetterTopic = "topic://public/default/tsdb-dead";
        sink = new TsdbSink(tsdb);
        sink.open(c, context(ordered), null);
        FakeRecord r = FakeRecord.point(0, "s", 1, 1);
        sink.write(r);
        eventually(r::settled, "ack");
    }

    @Test
    void deadLetterFailuresThatNeedAnOperatorAreRecognised() {
        assertTrue(DeadLetter.needsOperator(new PulsarClientException.AuthorizationException("not allowed")));
        assertTrue(DeadLetter.needsOperator(new RuntimeException(new PulsarClientException("message is too large"))));
        assertFalse(DeadLetter.needsOperator(new PulsarClientException.TimeoutException("slow broker")));
        assertFalse(DeadLetter.needsOperator(new Exception("dead-letter topic unavailable")));
    }

    @Test
    void shardOfIsStableAndSpreads() {
        int[] n = new int[4];
        for (int i = 0; i < 4000; i++)
            n[TsdbSink.shardOf("series" + i, 4)]++;
        for (int k : n)
            assertTrue(k > 800, "uneven " + java.util.Arrays.toString(n));
        assertEquals(TsdbSink.shardOf("pump7.flow", 7), TsdbSink.shardOf("pump7.flow", 7));
    }

    @Test
    void deniedIsPermanentAndARefusedLoginRetryable() {
        ErrorClassifier k = new ErrorClassifier(Set.of(), Set.of());
        assertEquals(ErrorClassifier.Kind.PERMANENT, k.classify(Status.ERR_DENIED));
        assertEquals(ErrorClassifier.Kind.RETRYABLE, k.classify(Status.ERR_AUTH));
        assertEquals(ErrorClassifier.Kind.RETRYABLE,
                new ErrorClassifier(Set.of(), Set.of(Status.ERR_DENIED)).classify(Status.ERR_DENIED));
    }

    /** One foreign series in a batch: tsdbd refuses the frame, the sink finds the records by halving. */
    @Test
    void deniedRecordsAreDeadLetteredAndTheRestIsWritten() throws Exception {
        tsdb.denied.add("globex/x");
        TsdbSinkConfig c = config();
        c.lingerMs = 200;
        open(c);
        List<FakeRecord> rs = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            rs.add(FakeRecord.point(i, i % 13 == 5 ? "globex/x" : "acme/s" + (i % 4), i, i));
            sink.write(rs.get(i));
        }
        eventually(() -> allSettled(rs), "all settled");
        assertEquals(3, dlq.records.size(), String.valueOf(dlq.reasons)); /* 5, 18, 31 */
        assertTrue(dlq.statuses.stream().allMatch(s -> s == Status.ERR_DENIED), String.valueOf(dlq.statuses));
        assertEquals(37, tsdb.last().size());
        assertTrue(rs.stream().allMatch(r -> r.acks.get() == 1 && r.fails.get() == 0));
    }

    /** A logger that keeps the format of every warning. */
    private static org.slf4j.Logger capturing(List<String> warnings) {
        return (org.slf4j.Logger) Proxy.newProxyInstance(org.slf4j.Logger.class.getClassLoader(),
                new Class<?>[] {org.slf4j.Logger.class}, (proxy, m, args) -> {
                    if (m.getName().equals("warn") && args != null && args[0] instanceof String)
                        warnings.add((String) args[0]);
                    if (m.getReturnType() == boolean.class)
                        return true;
                    return m.getReturnType() == String.class ? "test" : null;
                });
    }

    /**
     * A refused login (wrong password, dropped user) is the operator's to fix: the batch waits with backoff, nothing is
     * acknowledged or dead-lettered, and each backoff step logs one line that says it is the login.
     */
    @Test
    void aRefusedLoginIsRetriedWithBackoffAndLoggedOncePerStep() throws Exception {
        tsdb.loginRefused = true;
        List<String> warnings = new java.util.concurrent.CopyOnWriteArrayList<>();
        org.slf4j.Logger logger = capturing(warnings);
        SinkContext ctx = (SinkContext) Proxy.newProxyInstance(SinkContext.class.getClassLoader(),
                new Class<?>[] {SinkContext.class}, (proxy, m, args) -> m.getName().equals("getLogger") ? logger : null);
        TsdbSinkConfig c = config();
        c.validate();
        sink = new TsdbSink(tsdb);
        sink.open(c, ctx, dlq);
        List<FakeRecord> rs = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            rs.add(FakeRecord.point(i, "s" + i, i, i));
            sink.write(rs.get(i));
        }
        eventually(() -> sink.metrics().retries() >= 4, "retries of the login");
        assertTrue(rs.stream().noneMatch(FakeRecord::settled), "acknowledged without a login");
        assertEquals(0, dlq.records.size());
        tsdb.loginRefused = false;
        eventually(() -> allSettled(rs), "acks once the login works");
        assertTrue(rs.stream().allMatch(r -> r.acks.get() == 1 && r.fails.get() == 0));
        assertEquals(0, dlq.records.size());
        int refused;
        synchronized (tsdb) {
            refused = tsdb.loginsRefused;
        }
        long login = warnings.stream().filter(w -> w.contains("refused the login")).count();
        assertEquals(refused, login, "one line per refused login: " + warnings);
        assertEquals(refused, sink.metrics().retries(), "every refused login is one backoff step");
    }
}
