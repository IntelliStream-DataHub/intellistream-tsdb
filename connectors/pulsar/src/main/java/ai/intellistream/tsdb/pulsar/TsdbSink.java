package ai.intellistream.tsdb.pulsar;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Pulsar IO sink (Pulsar 5.0) that writes points into tsdbd.
 *
 * <p><b>Contract.</b> A record is acknowledged only after tsdbd acknowledged the insert that holds all of its points
 * (in tsdbd's default sync mode that means durable), or after it was handed to the dead-letter path. Nothing is
 * acknowledged early, so a crash of the sink, tsdbd or Pulsar loses nothing: unacknowledged records are delivered
 * again (at least once), and writing a point twice is harmless because tsdb keeps the last write of a (series,
 * timestamp).
 *
 * <p><b>Order.</b> The points of one series are written in the order the sink received them: each series belongs to
 * one connection ({@link Shard}), which writes one batch at a time and retries a failed batch in place rather than
 * failing records (a negative acknowledgement would redeliver them after newer ones). Pulsar must deliver the
 * records of a series in order too: producers key messages by series, and the sink runs with an ordered
 * subscription ({@code --retain-key-ordering}, best on a Pulsar 5 scalable {@code topic://} topic).
 *
 * <p><b>Backpressure.</b> {@link #write} blocks while {@code maxInFlightRecords} or {@code maxInFlightBytes} are in
 * flight, so while tsdb is down Pulsar keeps the backlog and the sink holds a bounded amount.
 *
 * <p>The input is a {@link GenericObject}: JSON or AVRO records, or JSON in BYTES or STRING messages; see
 * {@link PointParser} and the README.
 */
@Connector(name = "tsdb", type = IOType.SINK, help = "Writes points into tsdbd; acks only what tsdb made durable",
        configClass = TsdbSinkConfig.class)
public class TsdbSink implements Sink<GenericObject> {
    private TsdbSinkConfig config;
    private Shard[] shards;
    private PointParser parser;
    private InFlightLimiter limiter;
    private DeadLetter deadLetter;
    private Metrics metrics;
    private Logger log = LoggerFactory.getLogger(TsdbSink.class);
    private volatile boolean closed;
    private TsdbConnection.Factory factory;

    /** Creates a sink; Pulsar calls {@link #open} next. */
    public TsdbSink() {
    }

    /** For tests: a sink that writes through {@code factory} instead of TCP connections. */
    TsdbSink(TsdbConnection.Factory factory) {
        this.factory = factory;
    }

    /**
     * Reads the configuration and starts one writer thread per connection. Connections open lazily, so the sink
     * starts while tsdbd is down and writes once it is up.
     *
     * @param map the sink configuration ({@link TsdbSinkConfig})
     * @param ctx the sink context; its logger, metrics and Pulsar 5 client are used
     * @throws IllegalArgumentException for an invalid configuration, an ack timeout on the input, or an ordered input
     *     ({@code retainOrdering}, {@code retainKeyOrdering}) without {@code deadLetterTopic}
     */
    @Override
    public void open(Map<String, Object> map, SinkContext ctx) {
        open(TsdbSinkConfig.load(map, ctx), ctx, null);
    }

    void open(TsdbSinkConfig c, SinkContext ctx, DeadLetter dl) {
        config = c;
        if (ctx != null && ctx.getLogger() != null)
            log = ctx.getLogger();
        SinkConfig rc = ctx != null ? ctx.getSinkConfig() : null;
        /* An ack timeout redelivers a record that waits here for tsdb (hours, while a volume is away) and queues the
         * copy behind newer records of its key: the old value is then written last and overwrites a correction. */
        if (rc != null && rc.getTimeoutMs() != null && rc.getTimeoutMs() > 0)
            throw new IllegalArgumentException("the tsdb sink must run without an ack timeout (timeoutMs / --timeout-ms "
                    + rc.getTimeoutMs() + "): a redelivered copy would overwrite newer values of its key");
        boolean ordered = rc != null && (Boolean.TRUE.equals(rc.getRetainOrdering())
                || Boolean.TRUE.equals(rc.getRetainKeyOrdering()));
        boolean runtimeDeadLetter = rc != null && rc.getDeadLetterTopic() != null && !rc.getDeadLetterTopic().isEmpty();
        /* On an ordered input record.fail() stops the instance (Pulsar 5 streams have no negative ack): the first
         * invalid record would make it crash and restart forever. */
        if (ordered && c.deadLetterTopic.isEmpty())
            throw new IllegalArgumentException("deadLetterTopic is required with retainOrdering / retainKeyOrdering: "
                    + "there an invalid record could only fail the instance");
        metrics = new Metrics(ctx);
        parser = new PointParser(c);
        limiter = new InFlightLimiter(c.maxInFlightRecords, c.maxInFlightBytes);
        if (dl != null)
            deadLetter = dl;
        else if (!c.deadLetterTopic.isEmpty())
            deadLetter = DeadLetter.topic(ctx.getPulsarClientV5(), c.deadLetterTopic);
        else {
            deadLetter = DeadLetter.fail();
            if (ctx != null && !runtimeDeadLetter)
                log.warn("tsdb sink: neither deadLetterTopic nor the runtime's --dead-letter-topic is set: an invalid "
                        + "record is negatively acknowledged and delivered again and again; set deadLetterTopic");
        }
        if (factory == null)
            factory = TsdbConnection.client(c);
        ErrorClassifier classifier = new ErrorClassifier(TsdbSinkConfig.codes(c.permanentCodes),
                TsdbSinkConfig.codes(c.retryableCodes));
        shards = new Shard[c.connections];
        for (int i = 0; i < shards.length; i++)
            shards[i] = new Shard(i, c, factory, classifier, metrics, this::settled, log);
        for (Shard s : shards)
            s.start();
        log.info("tsdb sink: {} connection(s) to {} {}, batches of {} rows / {} ms, in flight {} records / {} bytes",
                c.connections, c.hosts, c.user.isEmpty() ? "without login" : "as user " + c.user, c.batchRows,
                c.lingerMs, c.maxInFlightRecords, c.maxInFlightBytes);
    }

    /**
     * Queues a record for writing; blocks while the in-flight limits are reached. Returns without acknowledging: the
     * record is acknowledged by a writer thread once tsdb acknowledged it. A record that cannot be parsed goes to the
     * dead-letter path at once. After {@link #close} it returns without doing anything, so the record stays
     * unacknowledged and is delivered again.
     *
     * @param record a Pulsar record
     * @throws InterruptedException if interrupted while waiting for room; the record is left unacknowledged
     */
    @Override
    public void write(Record<GenericObject> record) throws InterruptedException {
        if (closed)
            return;
        List<Point> points;
        try {
            points = parser.parse(record.getValue());
        } catch (PoisonException e) {
            Pending p = new Pending(record, 64);
            if (!limiter.acquire(p.size))
                return;
            p.poison(e.getMessage(), e.status());
            settle(p);
            return;
        } catch (RuntimeException e) {
            /* the runtime failed to decode the payload against its schema */
            Pending p = new Pending(record, 64);
            if (!limiter.acquire(p.size))
                return;
            p.poison("cannot decode record: " + e, 0);
            settle(p);
            return;
        }
        long size = 0;
        for (Point pt : points)
            size += pt.size();
        Pending p = new Pending(record, size);
        if (!limiter.acquire(size))
            return;
        int n = shards.length;
        if (n == 1) {
            p.parts(1);
            shards[0].add(new Shard.Part(p, points, size));
            return;
        }
        List<List<Point>> per = new ArrayList<>(n);
        for (int i = 0; i < n; i++)
            per.add(null);
        int parts = 0;
        for (Point pt : points) {
            int s = shardOf(pt.series, n);
            if (per.get(s) == null) {
                per.set(s, new ArrayList<>());
                parts++;
            }
            per.get(s).add(pt);
        }
        p.parts(parts);
        for (int i = 0; i < n; i++)
            if (per.get(i) != null) {
                long b = 0;
                for (Point pt : per.get(i))
                    b += pt.size();
                shards[i].add(new Shard.Part(p, per.get(i), b));
            }
    }

    /** The shard of a series: a fixed function of the name, so all its points go through one connection. */
    static int shardOf(String series, int n) {
        int h = series.hashCode() * 0x9E3779B9;
        return Math.floorMod(h ^ (h >>> 16), n);
    }

    private void settled(Shard.Part part) throws InterruptedException {
        if (part.pending.partDone())
            settle(part.pending);
    }

    /** Acknowledges or dead-letters a record whose parts are all done; releases its room. */
    private void settle(Pending p) throws InterruptedException {
        try {
            if (p.isPoison()) {
                Backoff b = new Backoff(config.retryInitialMs, config.retryMaxMs, config.retryJitter);
                while (true) {
                    try {
                        deadLetter.send(p.record, p.reason(), p.status());
                        break;
                    } catch (Exception e) {
                        if (closed) /* leave it unacknowledged: it is redelivered and refused again */
                            return;
                        long d = b.next();
                        metrics.deadLetterFailure();
                        /* Never dropped: the record stays unacknowledged and the stream behind it waits. A failure
                         * that only an operator can end (permissions, a missing topic, a size limit) says so loudly. */
                        if (DeadLetter.needsOperator(e))
                            log.error("tsdb sink: cannot write to the dead-letter topic, the stream waits until this is "
                                    + "fixed; retrying in {} ms: {}", d, e.toString());
                        else
                            log.warn("tsdb sink: dead-lettering failed, retrying in {} ms: {}", d, e.toString());
                        Thread.sleep(d);
                    }
                }
                metrics.deadLettered();
                log.warn("tsdb sink: record dead-lettered: {}", p.reason());
                if (!deadLetter.settles())
                    p.record.ack();
            } else
                p.record.ack();
        } finally {
            limiter.release(p.size);
        }
    }

    /**
     * Writes what is queued, waiting up to {@code closeTimeoutMs}, then stops. Records not written by then are not
     * acknowledged; Pulsar delivers them again to the next instance.
     *
     * @throws InterruptedException if interrupted while waiting
     */
    @Override
    public void close() throws InterruptedException {
        if (closed || shards == null)
            return;
        closed = true;
        limiter.shutdown();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.closeTimeoutMs);
        for (Shard s : shards)
            s.close(deadline);
        for (Shard s : shards)
            s.join();
        deadLetter.close();
        log.info("tsdb sink closed: {} records written, {} retries, {} dead-lettered, {} left unacknowledged",
                metrics.written(), metrics.retries(), metrics.deadLetteredCount(), limiter.count());
    }

    /**
     * The sink's counters.
     *
     * @return the metrics since {@link #open}
     */
    public Metrics metrics() {
        return metrics;
    }

    /** Records received and not yet settled (tests). */
    long inFlight() {
        return limiter.count();
    }
}
