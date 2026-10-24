package ai.intellistream.tsdb.pulsar;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.functions.api.Record;

/**
 * Where records go that tsdb will never take.
 *
 * <p>{@link #send} returns once the record is safely elsewhere; the sink acknowledges it only then. If it throws,
 * the record stays unacknowledged and the sink tries again later.
 */
interface DeadLetter extends AutoCloseable {
    /** Property naming why the record was refused. */
    String REASON = "tsdb-error";
    /** Property with the tsdb status code (0 when the sink refused the record itself). */
    String STATUS = "tsdb-status";
    /** Property with the topic the record came from. */
    String SOURCE_TOPIC = "tsdb-source-topic";
    /** Property with the message id the record had there. */
    String SOURCE_ID = "tsdb-source-message-id";

    /** True if {@link #send} settles the record itself (fail), so the sink must not acknowledge it. */
    boolean settles();

    void send(Record<?> r, String reason, int status) throws Exception;

    @Override
    void close();

    /**
     * True for a failure of the dead-letter topic that retrying does not end: no permission, no such topic, a closed
     * or terminated topic, a schema or configuration conflict, or a message over the broker's size limit.
     */
    static boolean needsOperator(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof PulsarClientException.AuthorizationException
                    || t instanceof PulsarClientException.AuthenticationException
                    || t instanceof PulsarClientException.NotFoundException
                    || t instanceof PulsarClientException.TopicTerminatedException
                    || t instanceof PulsarClientException.IncompatibleSchemaException
                    || t instanceof PulsarClientException.InvalidConfigurationException)
                return true;
            String m = t.getMessage();
            if (m != null && m.toLowerCase(Locale.ROOT).contains("too large"))
                return true;
        }
        return false;
    }

    /**
     * {@code record.fail()}. On a Shared subscription the runtime redelivers it and, with {@code --max-redeliver-count}
     * and {@code --dead-letter-topic}, eventually moves it to that topic. On an ordered input ({@code retainOrdering} or
     * {@code retainKeyOrdering}: a stream consumer) Pulsar 5 has no negative acknowledgement and fail() stops the
     * instance, so the sink requires {@code deadLetterTopic} there.
     */
    static DeadLetter fail() {
        return new DeadLetter() {
            @Override
            public boolean settles() {
                return true;
            }

            @Override
            public void send(Record<?> r, String reason, int status) {
                r.fail();
            }

            @Override
            public void close() {
            }
        };
    }

    /**
     * Publishes the original message bytes, key and properties, plus the reason, to {@code topic} with the runtime's
     * Pulsar 5 (v5 API) client. Works on any subscription type, scalable topics included.
     */
    static DeadLetter topic(PulsarClient client, String topic) {
        return new DeadLetter() {
            private Producer<byte[]> producer;

            @Override
            public boolean settles() {
                return false;
            }

            @Override
            public synchronized void send(Record<?> r, String reason, int status) throws Exception {
                if (producer == null) /* lazily: a topic:// dead-letter topic must exist, and may be created late */
                    producer = client.newProducer(Schema.bytes()).topic(topic).create();
                Map<String, String> props = new HashMap<>(r.getProperties());
                props.put(REASON, reason);
                props.put(STATUS, Integer.toString(status));
                r.getTopicName().ifPresent(t -> props.put(SOURCE_TOPIC, t));
                byte[] data;
                if (r.getMessage().isPresent()) {
                    data = r.getMessage().get().getData();
                    props.put(SOURCE_ID, r.getMessage().get().getMessageId().toString());
                } else
                    data = String.valueOf(r.getValue()).getBytes(StandardCharsets.UTF_8);
                var b = producer.newMessage().value(data).properties(props);
                if (r.getKey().isPresent())
                    b = b.key(r.getKey().get());
                b.send();
            }

            @Override
            public synchronized void close() {
                if (producer != null) {
                    try {
                        producer.close();
                    } catch (Exception e) {
                        /* sent messages are acknowledged by the broker already; nothing pending to lose */
                    }
                }
            }
        };
    }
}
