package ai.intellistream.tsdb.pulsar;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.io.core.SinkContext;

/**
 * Counters of the sink, reported through {@link SinkContext#recordMetric} (they appear as user metrics of the sink
 * instance, with the {@code tsdb_} prefix) and kept here for tests and logs.
 */
public final class Metrics {
    private final SinkContext ctx;
    final AtomicLong written = new AtomicLong();
    final AtomicLong retries = new AtomicLong();
    final AtomicLong deadLettered = new AtomicLong();
    final AtomicLong batches = new AtomicLong();
    final AtomicLong backoffMs = new AtomicLong();
    final AtomicLong deadLetterFailures = new AtomicLong();
    final AtomicLong skippedRecords = new AtomicLong();
    final AtomicLong skippedPoints = new AtomicLong();

    Metrics(SinkContext ctx) {
        this.ctx = ctx;
    }

    private void record(String name, double v) {
        if (ctx != null)
            ctx.recordMetric("tsdb_" + name, v);
    }

    void batch(long records, long rows, long latencyMs) {
        written.addAndGet(records);
        batches.incrementAndGet();
        record("records_written", records);
        record("rows_written", rows);
        record("batch_latency_ms", latencyMs);
        if (backoffMs.getAndSet(0) != 0)
            record("backoff_ms", 0);
    }

    void retry(long delayMs) {
        retries.incrementAndGet();
        backoffMs.set(delayMs);
        record("retries", 1);
        record("backoff_ms", delayMs);
    }

    void deadLettered() {
        deadLettered.incrementAndGet();
        record("dead_lettered", 1);
    }

    /** A record none of whose points is selected, or one point of a message that has some that are. */
    void skipped(boolean wholeRecord, long points) {
        if (wholeRecord) {
            skippedRecords.incrementAndGet();
            record("skipped_records", 1);
        }
        skippedPoints.addAndGet(points);
        record("skipped_points", points);
    }

    void deadLetterFailure() {
        deadLetterFailures.incrementAndGet();
        record("dead_letter_failures", 1);
    }

    /**
     * Failed attempts to write a record to the dead-letter path (it stays unacknowledged and is tried again).
     *
     * @return the count since open
     */
    public long deadLetterFailures() {
        return deadLetterFailures.get();
    }

    /**
     * Records acknowledged without being written because none of their series is selected.
     *
     * @return the count since open
     */
    public long skippedRecords() {
        return skippedRecords.get();
    }

    /**
     * Points left out because their series is not selected (also those of messages that were written in part).
     *
     * @return the count since open
     */
    public long skippedPoints() {
        return skippedPoints.get();
    }

    /**
     * Records acknowledged after tsdb made them durable.
     *
     * @return the count since open
     */
    public long written() {
        return written.get();
    }

    /**
     * Failed attempts that were retried.
     *
     * @return the count since open
     */
    public long retries() {
        return retries.get();
    }

    /**
     * Records sent to the dead-letter path.
     *
     * @return the count since open
     */
    public long deadLetteredCount() {
        return deadLettered.get();
    }

    /**
     * The delay of the latest retry, 0 once a batch succeeded since.
     *
     * @return ms
     */
    public long backoffMs() {
        return backoffMs.get();
    }
}
