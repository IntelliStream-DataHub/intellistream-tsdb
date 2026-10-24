package ai.intellistream.tsdb.pulsar;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;

/** A record of a BYTES topic carrying JSON, counting acks and fails. */
final class FakeRecord implements Record<GenericObject> {
    final int seq;
    final GenericObject value;
    final AtomicInteger acks = new AtomicInteger();
    final AtomicInteger fails = new AtomicInteger();
    /** ack() throws this many times first, as a runtime whose consumer is closing might. */
    volatile int ackThrows;

    FakeRecord(int seq, String json) {
        this(seq, bytes(json));
    }

    FakeRecord(int seq, GenericObject value) {
        this.seq = seq;
        this.value = value;
    }

    static GenericObject bytes(String json) {
        byte[] b = json.getBytes(StandardCharsets.UTF_8);
        return new GenericObject() {
            @Override
            public SchemaType getSchemaType() {
                return SchemaType.BYTES;
            }

            @Override
            public Object getNativeObject() {
                return b;
            }
        };
    }

    static FakeRecord point(int seq, String series, long ts, Object value) {
        String v = value instanceof String ? "\"" + value + "\"" : String.valueOf(value);
        return new FakeRecord(seq, "{\"series\":\"" + series + "\",\"ts\":" + ts + ",\"value\":" + v + "}");
    }

    @Override
    public GenericObject getValue() {
        return value;
    }

    @Override
    public Optional<String> getKey() {
        return Optional.of("k" + seq);
    }

    @Override
    public Map<String, String> getProperties() {
        return Map.of("seq", Integer.toString(seq));
    }

    @Override
    public void ack() {
        if (ackThrows > 0) {
            ackThrows--;
            throw new IllegalStateException("consumer closed");
        }
        acks.incrementAndGet();
    }

    @Override
    public void fail() {
        fails.incrementAndGet();
    }

    boolean settled() {
        return acks.get() + fails.get() > 0;
    }
}
