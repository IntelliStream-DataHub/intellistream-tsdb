package ai.intellistream.tsdb.pulsar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.intellistream.tsdb.Status;
import ai.intellistream.tsdb.TsdbException;
import ai.intellistream.tsdb.ValueType;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.Test;

/** Classification, backoff, LRU, configuration and schema mapping. */
class PartsTest {
    @Test
    void classification() {
        ErrorClassifier c = new ErrorClassifier(Set.of(), Set.of());
        for (int s : new int[] {Status.ERR_IO, Status.ERR_NOMEM, Status.ERR_CORRUPT, Status.ERR_LOCKED,
                Status.ERR_STOPPED, Status.ERR_READONLY, -10, -42})
            assertEquals(ErrorClassifier.Kind.RETRYABLE, c.classify(s), "status " + s);
        for (int s : new int[] {Status.ERR_INVALID, Status.ERR_NOTFOUND, Status.ERR_TOO_MANY_BUCKETS})
            assertEquals(ErrorClassifier.Kind.PERMANENT, c.classify(s), "status " + s);
        assertEquals(ErrorClassifier.Kind.PERMANENT, c.classify(new IllegalArgumentException("name too long")));
        assertEquals(ErrorClassifier.Kind.RETRYABLE, c.classify(new TsdbException(Status.ERR_IO)));
        assertEquals(ErrorClassifier.Kind.RETRYABLE, c.classify(new java.net.SocketTimeoutException()));
        /* overrides: e.g. a protocol 5 "moved" code made explicit, or NOTFOUND made retryable */
        ErrorClassifier o = new ErrorClassifier(TsdbSinkConfig.codes("-12"), TsdbSinkConfig.codes("-6"));
        assertEquals(ErrorClassifier.Kind.PERMANENT, o.classify(-12));
        assertEquals(ErrorClassifier.Kind.RETRYABLE, o.classify(Status.ERR_NOTFOUND));
    }

    @Test
    void backoffDoublesUpToTheCapWithJitter() {
        Backoff b = new Backoff(100, 1000, 0.2);
        long[] want = {100, 200, 400, 800, 1000, 1000};
        for (long w : want) {
            long d = b.next();
            assertTrue(d >= w * 0.8 - 1 && d <= Math.min(1000, w * 1.2 + 1), "delay " + d + " for " + w);
        }
        assertTrue(b.current() > 0);
        b.reset();
        assertEquals(0, b.current());
        Backoff exact = new Backoff(10, 80, 0);
        assertEquals(10, exact.next());
        assertEquals(20, exact.next());
    }

    @Test
    void lruEvictsTheLeastRecentlyUsed() {
        SeriesCache c = new SeriesCache(2);
        c.put("a", new SeriesCache.Entry(1, ValueType.FLOAT64));
        c.put("b", new SeriesCache.Entry(2, ValueType.FLOAT64));
        c.get("a");
        c.put("c", new SeriesCache.Entry(3, ValueType.FLOAT64));
        assertEquals(2, c.size());
        assertNull(c.get("b"));
        assertEquals(1L, c.get("a").id);
    }

    @Test
    void limiterLetsAnOversizedRecordThroughAlone() throws Exception {
        InFlightLimiter l = new InFlightLimiter(10, 100);
        assertTrue(l.acquire(1000));
        Thread t = new Thread(() -> {
            try {
                l.acquire(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        t.start();
        t.join(200);
        assertTrue(t.isAlive());
        l.release(1000);
        t.join(2000);
        assertEquals(1, l.count());
        l.shutdown();
        assertTrue(!l.acquire(1));
    }

    @Test
    void configDefaultsAndErrors() {
        TsdbSinkConfig c = TsdbSinkConfig.load(Map.of("hosts", "a:7071, b", "connections", 2, "defaultType", "int64"));
        assertEquals(List.of(Map.entry("a", 7071), Map.entry("b", 7070)), c.endpoints());
        assertEquals(ValueType.INT64, c.defaultValueType());
        assertThrows(IllegalArgumentException.class, () -> TsdbSinkConfig.load(Map.of("hostz", "x")));
        assertThrows(IllegalArgumentException.class, () -> TsdbSinkConfig.load(Map.of("connections", 0)));
        assertThrows(IllegalArgumentException.class, () -> TsdbSinkConfig.load(Map.of("defaultType", "text")));
        assertThrows(IllegalArgumentException.class, () -> TsdbSinkConfig.load(Map.of("permanentCodes", "4")));
        assertEquals(10_000, TsdbSinkConfig.load(null).batchRows);
    }

    static GenericRecord record(Map<String, Object> fields, SchemaType type) {
        List<Field> fs = new java.util.ArrayList<>();
        int i = 0;
        for (String k : fields.keySet())
            fs.add(new Field(k, i++));
        return new GenericRecord() {
            @Override
            public byte[] getSchemaVersion() {
                return null;
            }

            @Override
            public List<Field> getFields() {
                return fs;
            }

            @Override
            public Object getField(String name) {
                if (!fields.containsKey(name))
                    throw new RuntimeException("Not a valid schema field: " + name); /* as AVRO does */
                return fields.get(name);
            }

            @Override
            public SchemaType getSchemaType() {
                return type;
            }
        };
    }

    @Test
    void schemaMapping() throws Exception {
        TsdbSinkConfig c = new TsdbSinkConfig();
        PointParser p = new PointParser(c);
        /* AVRO record: strings arrive as Utf8 (a CharSequence), longs as Long */
        CharSequence utf8 = new StringBuilder("pump7.flow");
        Map<String, Object> f = new java.util.LinkedHashMap<>();
        f.put("series", utf8);
        f.put("ts", 1709251200000L);
        f.put("value", 41.5);
        f.put("type", null);
        List<Point> pts = p.parse(record(f, SchemaType.AVRO));
        assertEquals(1, pts.size());
        assertEquals("pump7.flow", pts.get(0).series);
        assertEquals(1709251200000L, pts.get(0).ts);
        assertEquals(41.5, pts.get(0).value);
        assertNull(pts.get(0).type);
        /* AVRO batch: points array of records */
        Map<String, Object> batch = Map.of("points", List.of(record(f, SchemaType.AVRO), record(f, SchemaType.AVRO)));
        assertEquals(2, p.parse(record(batch, SchemaType.AVRO)).size());
        /* a missing field is an error, not a null */
        Map<String, Object> noValue = Map.of("series", "x", "ts", 1L);
        assertThrows(PoisonException.class, () -> p.parse(record(noValue, SchemaType.JSON)));
        /* STRING topic with JSON text; columnar form */
        GenericObject str = new GenericObject() {
            @Override
            public SchemaType getSchemaType() {
                return SchemaType.STRING;
            }

            @Override
            public Object getNativeObject() {
                return "{\"series\":\"s\",\"ts\":[1,2],\"value\":[true,false],\"type\":\"bool\"}";
            }
        };
        List<Point> col = p.parse(str);
        assertEquals(2, col.size());
        assertEquals(Boolean.FALSE, col.get(1).value);
        assertEquals(ValueType.BOOL, col.get(1).type);
        /* configurable field names */
        TsdbSinkConfig named = new TsdbSinkConfig();
        named.seriesField = "name";
        named.timestampField = "time";
        named.valueField = "v";
        List<Point> n = new PointParser(named).parse(FakeRecord.bytes("{\"name\":\"a\",\"time\":5,\"v\":-3}"));
        assertEquals(-3L, n.get(0).value);
        assertThrows(PoisonException.class, () -> p.parse(FakeRecord.bytes("{\"series\":\"s\",\"ts\":1.5,\"value\":1}")));
        /* Pulsar 5's runtime delivers a schema-less message as a GenericRecord without fields around the bytes */
        GenericRecord wrapped = new GenericRecord() {
            @Override
            public byte[] getSchemaVersion() {
                return null;
            }

            @Override
            public List<Field> getFields() {
                return List.of();
            }

            @Override
            public Object getField(String name) {
                throw new UnsupportedOperationException();
            }

            @Override
            public SchemaType getSchemaType() {
                return SchemaType.BYTES;
            }

            @Override
            public Object getNativeObject() {
                return "{\"series\":\"w\",\"ts\":7,\"value\":2.5}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
        };
        List<Point> w = p.parse(wrapped);
        assertEquals("w", w.get(0).series);
        assertEquals(7, w.get(0).ts);
        assertThrows(PoisonException.class, () -> p.parse(FakeRecord.bytes("[]")));
        assertThrows(PoisonException.class, () -> p.parse(FakeRecord.bytes("{\"series\":\"s\",\"ts\":[1],\"value\":[]}")));
        assertThrows(PoisonException.class, () -> p.parse(FakeRecord.bytes("{\"series\":\"s\",\"ts\":1,\"value\":{}}")));
    }
}
