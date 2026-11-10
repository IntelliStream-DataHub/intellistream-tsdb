// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb.pulsar;

import ai.intellistream.tsdb.ValueType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Turns the value of a Pulsar record into points.
 *
 * <p>Accepted forms, with the field names of {@link TsdbSinkConfig}:
 * <ul>
 * <li>a point: {@code {"series": "pump7.flow", "ts": 1709251200000, "value": 41.5, "type": "float64"}}; the type is
 *     optional, the value may be a number, a bool, a string (a state's name) or null;</li>
 * <li>a columnar batch of one series: the same with {@code ts} and {@code value} as arrays of equal length;</li>
 * <li>a batch message: {@code {"points": [point, ...]}}, or a JSON array of points.</li>
 * </ul>
 * The value can be a JSON or AVRO record (a topic with a schema; the sink reads {@link GenericRecord} fields), or
 * JSON in a BYTES or STRING message (a topic without a schema). Anything else is a {@link PoisonException}.
 */
final class PointParser {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final TsdbSinkConfig c;

    PointParser(TsdbSinkConfig c) {
        this.c = c;
    }

    /** A record-like object: a JSON object or a Pulsar generic record. */
    private interface Obj {
        boolean has(String field);

        Object get(String field);
    }

    List<Point> parse(Object value) throws PoisonException {
        Object v = normalize(value);
        List<Point> out = new ArrayList<>();
        if (v instanceof List)
            for (Object o : (List<?>) v)
                points(asObj(o), out);
        else {
            Obj o = asObj(v);
            if (o.has(c.pointsField) && !o.has(c.seriesField)) {
                Object p = o.get(c.pointsField);
                if (!(p instanceof List))
                    throw new PoisonException("field " + c.pointsField + " is not an array");
                for (Object e : (List<?>) p)
                    points(asObj(e), out);
            } else
                points(o, out);
        }
        if (out.isEmpty())
            throw new PoisonException("no points in record");
        return out;
    }

    private void points(Obj o, List<Point> out) throws PoisonException {
        if (!o.has(c.seriesField))
            throw new PoisonException("missing field " + c.seriesField);
        Object s = o.get(c.seriesField);
        if (!(s instanceof String) || ((String) s).isEmpty())
            throw new PoisonException("field " + c.seriesField + " is not a non-empty string");
        String series = (String) s;
        ValueType type = null;
        if (o.has(c.typeField) && o.get(c.typeField) != null) {
            Object t = o.get(c.typeField);
            type = t instanceof String ? TsdbSinkConfig.parseType((String) t) : null;
            if (type == null)
                throw new PoisonException("unknown type " + Point.clip(t));
        }
        if (!o.has(c.timestampField))
            throw new PoisonException("missing field " + c.timestampField);
        if (!o.has(c.valueField))
            throw new PoisonException("missing field " + c.valueField);
        Object ts = o.get(c.timestampField);
        Object val = o.get(c.valueField);
        if (ts instanceof List) {
            List<?> tl = (List<?>) ts;
            if (!(val instanceof List) || ((List<?>) val).size() != tl.size())
                throw new PoisonException("columnar point: " + c.valueField + " must be an array as long as "
                        + c.timestampField);
            Iterator<?> vi = ((List<?>) val).iterator();
            for (Object t : tl)
                out.add(new Point(series, timestamp(t), scalar(vi.next()), type));
        } else
            out.add(new Point(series, timestamp(ts), scalar(val), type));
    }

    private long timestamp(Object t) throws PoisonException {
        if (t instanceof Long || t instanceof Integer || t instanceof Short)
            return ((Number) t).longValue();
        if (t instanceof Double || t instanceof Float || t instanceof BigDecimal) {
            double d = ((Number) t).doubleValue();
            if (d == Math.rint(d) && Math.abs(d) < 9.0e15)
                return (long) d;
        }
        throw new PoisonException("timestamp is not an integer of ms: " + Point.clip(t));
    }

    private static Object scalar(Object v) throws PoisonException {
        if (v == null || v instanceof Number || v instanceof Boolean || v instanceof String)
            return v;
        throw new PoisonException("value is not a number, bool, string or null: " + v.getClass().getSimpleName());
    }

    private Obj asObj(Object v) throws PoisonException {
        if (v instanceof Obj)
            return (Obj) v;
        throw new PoisonException("not a record: " + (v == null ? "null" : v.getClass().getSimpleName()));
    }

    /** Converts payloads to Obj, List, String, Number, Boolean or null. */
    private Object normalize(Object v) throws PoisonException {
        if (v == null)
            return null;
        if (v instanceof GenericObject) {
            GenericObject g = (GenericObject) v;
            SchemaType t = g.getSchemaType();
            /* The runtime wraps BYTES and STRING messages in a GenericRecord without fields too (with
             * AUTO_CONSUME): only a struct schema has fields to read; the rest carries JSON in its native object. */
            if (v instanceof GenericRecord && (t == null || t.isStruct()))
                return record((GenericRecord) v);
            Object n = g.getNativeObject();
            if (n == v)
                throw new PoisonException("unsupported payload of schema " + t);
            return normalize(n);
        }
        if (v instanceof byte[])
            return json(new String((byte[]) v, StandardCharsets.UTF_8));
        if (v instanceof ByteBuffer) {
            ByteBuffer b = ((ByteBuffer) v).duplicate();
            byte[] a = new byte[b.remaining()];
            b.get(a);
            return json(new String(a, StandardCharsets.UTF_8));
        }
        if (v instanceof JsonNode)
            return node((JsonNode) v);
        if (v instanceof CharSequence)
            return json(v.toString()); /* a STRING topic carries JSON text */
        if (v instanceof Collection) {
            List<Object> l = new ArrayList<>();
            for (Object e : (Collection<?>) v)
                l.add(e instanceof GenericRecord ? record((GenericRecord) e) : e instanceof JsonNode ? node((JsonNode) e) : e);
            return l;
        }
        throw new PoisonException("unsupported payload " + v.getClass().getSimpleName());
    }

    /** The top level of a BYTES or STRING message must be JSON; its strings are values, not payloads. */
    private Object json(String s) throws PoisonException {
        try {
            return node(JSON.readTree(s));
        } catch (IOException e) {
            throw new PoisonException("not JSON: " + e.getMessage().lines().findFirst().orElse(""));
        }
    }

    private Object node(JsonNode n) {
        if (n == null || n.isNull() || n.isMissingNode())
            return null;
        if (n.isObject())
            return new Obj() {
                @Override
                public boolean has(String f) {
                    return n.has(f);
                }

                @Override
                public Object get(String f) {
                    return node(n.get(f));
                }
            };
        if (n.isArray()) {
            List<Object> l = new ArrayList<>(n.size());
            for (JsonNode e : n)
                l.add(node(e));
            return l;
        }
        if (n.isBoolean())
            return n.booleanValue();
        if (n.isIntegralNumber())
            return n.canConvertToLong() ? (Object) n.longValue() : n.bigIntegerValue();
        if (n.isNumber())
            return n.doubleValue();
        if (n.isTextual())
            return n.textValue();
        return n.toString();
    }

    private Obj record(GenericRecord r) {
        return new Obj() {
            @Override
            public boolean has(String f) {
                for (Field x : r.getFields())
                    if (x.getName().equals(f))
                        return true;
                return false;
            }

            @Override
            public Object get(String f) {
                Object v;
                try {
                    v = r.getField(f);
                } catch (RuntimeException e) {
                    return null; /* AVRO throws for fields the schema lacks; has() said so already */
                }
                if (v instanceof GenericRecord)
                    return record((GenericRecord) v);
                if (v instanceof JsonNode)
                    return node((JsonNode) v);
                if (v instanceof CharSequence)
                    return v.toString(); /* AVRO's Utf8 */
                if (v instanceof Collection) {
                    List<Object> l = new ArrayList<>();
                    for (Object e : (Collection<?>) v)
                        l.add(e instanceof GenericRecord ? record((GenericRecord) e)
                                : e instanceof CharSequence ? e.toString() : e);
                    return l;
                }
                return v;
            }
        };
    }
}
