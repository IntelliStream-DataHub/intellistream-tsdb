package ai.intellistream.tsdb.pulsar;

import ai.intellistream.tsdb.Column;
import ai.intellistream.tsdb.Status;
import ai.intellistream.tsdb.TsdbException;
import ai.intellistream.tsdb.ValueType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;

/**
 * One connection to tsdbd and the thread that writes through it.
 *
 * <p>Every series is hashed to one shard, and a shard writes its batches one after another, each only after the
 * previous one was acknowledged. Together with the FIFO queue that keeps the points of a series in the order
 * {@code write} received them, and with retries of the same batch in place, this keeps per-series order: a newer
 * value of a key is never written before an older one, so "last write wins" in tsdb picks the newest.
 *
 * <p>Retrying a batch writes again rows that may already be in tsdb; that is harmless because tsdb keeps the last
 * write of a (series, timestamp), and nothing newer of those keys has been sent yet.
 */
final class Shard implements Runnable {
    /** The points of one record that hash to this shard. */
    static final class Part {
        final Pending pending;
        final List<Point> points;
        final long bytes;

        Part(Pending pending, List<Point> points, long bytes) {
            this.pending = pending;
            this.points = points;
            this.bytes = bytes;
        }
    }

    /** Called when a part is settled; the sink acknowledges or dead-letters the record when all parts are. */
    interface Settler {
        void settled(Part p) throws InterruptedException;
    }

    private final int index;
    private final TsdbSinkConfig c;
    private final TsdbConnection.Factory factory;
    private final ErrorClassifier classifier;
    private final Metrics metrics;
    private final Settler settler;
    private final Logger log;
    private final SeriesCache cache;
    private final Backoff backoff;
    private final ValueType defaultType;
    private final LinkedBlockingQueue<Part> queue = new LinkedBlockingQueue<>();
    private TsdbConnection conn;
    private volatile boolean closing;
    private volatile long deadline; /* System.nanoTime() after which a closing shard gives up */
    private final Thread thread;

    Shard(int index, TsdbSinkConfig c, TsdbConnection.Factory factory, ErrorClassifier classifier, Metrics metrics,
            Settler settler, Logger log) {
        this.index = index;
        this.c = c;
        this.factory = factory;
        this.classifier = classifier;
        this.metrics = metrics;
        this.settler = settler;
        this.log = log;
        this.cache = new SeriesCache(c.seriesCacheSize, c.seriesCacheBytes, c.stateCacheSize);
        this.backoff = new Backoff(c.retryInitialMs, c.retryMaxMs, c.retryJitter);
        this.defaultType = c.defaultValueType();
        this.thread = new Thread(this, "tsdb-sink-shard-" + index);
        this.thread.setDaemon(true);
    }

    void start() {
        thread.start();
    }

    void add(Part p) {
        queue.add(p);
    }

    /** Stops taking new batches once the queue is empty, or at {@code deadlineNanos}; see {@link #join}. */
    void close(long deadlineNanos) {
        deadline = deadlineNanos;
        closing = true;
    }

    /** Waits for the thread until the deadline, then interrupts it: what is not written stays unacknowledged. */
    void join() throws InterruptedException {
        long left = deadline - System.nanoTime();
        thread.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(left)));
        if (thread.isAlive()) {
            thread.interrupt();
            thread.join(10_000);
        }
    }

    private boolean pastDeadline() {
        return closing && System.nanoTime() - deadline >= 0;
    }

    @Override
    public void run() {
        try {
            while (true) {
                List<Part> batch = collect();
                if (batch == null)
                    break;
                if (!batch.isEmpty())
                    process(batch);
                if (pastDeadline())
                    break;
            }
        } catch (InterruptedException e) {
            /* close() gave up waiting: leave the rest unacknowledged, Pulsar redelivers it */
        } finally {
            if (conn != null)
                conn.close();
        }
    }

    /** The next batch; empty if nothing came within a poll; null when closing and drained. */
    private List<Part> collect() throws InterruptedException {
        Part first = queue.poll(100, TimeUnit.MILLISECONDS);
        if (first == null)
            return closing ? null : List.of();
        List<Part> batch = new ArrayList<>();
        batch.add(first);
        long rows = first.points.size(), bytes = first.bytes;
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(c.lingerMs);
        while (rows < c.batchRows && bytes < c.batchBytes) {
            Part p = queue.peek() != null ? queue.poll() : null;
            if (p == null) {
                long left = end - System.nanoTime();
                if (left <= 0 || closing)
                    break;
                /* in slices, so that close() does not wait out a long linger before flushing */
                p = queue.poll(Math.min(left, TimeUnit.MILLISECONDS.toNanos(50)), TimeUnit.NANOSECONDS);
                if (p == null)
                    continue;
            }
            batch.add(p);
            rows += p.points.size();
            bytes += p.bytes;
        }
        return batch;
    }

    /** Writes a batch, retrying it in place until it is written or the sink closes past its deadline. */
    private void process(List<Part> batch) throws InterruptedException {
        long start = System.nanoTime();
        long rows;
        while (true) {
            try {
                rows = write(batch);
                break;
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                if (conn != null && refusesWrites(e)) {
                    /* a follower or a server on its way down answers on an open connection: try the next endpoint */
                    conn.closeAndRotate();
                    conn = null;
                } else if (conn != null && !conn.isOpen()) {
                    conn.close();
                    conn = null;
                }
                if (Thread.currentThread().isInterrupted() || pastDeadline())
                    throw new InterruptedException("closing");
                long d = backoff.next();
                metrics.retry(d);
                log.warn("tsdb sink shard {}: batch of {} records failed, retrying in {} ms: {}", index, batch.size(), d,
                        e.toString());
                if (closing)
                    d = Math.min(d, Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())));
                Thread.sleep(d);
            }
        }
        /* outside the retry: a failure from here on must not write and settle the batch a second time, which would
         * count a part of a record split over shards twice and acknowledge it while another shard still holds it */
        backoff.reset();
        metrics.batch(batch.size(), rows, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        for (Part p : batch) {
            try {
                settler.settled(p);
            } catch (RuntimeException e) {
                /* e.g. ack() of a consumer that is closing: the record stays unacknowledged and comes again */
                log.error("tsdb sink shard {}: settling a written record failed: {}", index, e.toString());
            }
        }
    }

    /** Errors of a server that is up but will not write now, where another endpoint may. */
    static boolean refusesWrites(Exception e) {
        if (!(e instanceof TsdbException))
            return false;
        int s = ((TsdbException) e).status();
        return s == Status.ERR_READONLY || s == Status.ERR_LOCKED || s == Status.ERR_STOPPED;
    }

    /**
     * Writes the parts that are not poison and waits for tsdb's acknowledgement. Parts found to be poison are marked
     * so; the caller settles every part after this returns. Throws for retryable failures.
     *
     * @return rows written
     */
    long write(List<Part> parts) throws Exception {
        if (conn == null)
            conn = factory.connect();
        Map<ValueType, Rows> byType = new HashMap<>();
        List<Part> live = new ArrayList<>();
        long n = 0;
        for (Part p : parts) {
            if (p.pending.isPoison())
                continue;
            List<Resolved> rs = new ArrayList<>(p.points.size());
            try {
                for (Point pt : p.points)
                    rs.add(resolve(pt));
            } catch (PoisonException e) {
                p.pending.poison(e.getMessage(), e.status());
                continue;
            }
            for (Resolved r : rs)
                byType.computeIfAbsent(r.entry.type, t -> new Rows(t)).add(r);
            live.add(p);
            n += rs.size();
        }
        if (live.isEmpty())
            return 0;
        try {
            for (Rows r : byType.values())
                r.send(conn);
            conn.await();
            return n;
        } catch (TsdbException | IllegalArgumentException e) {
            if (classifier.classify(e) != ErrorClassifier.Kind.PERMANENT)
                throw e;
            /* Validated rows refused: a stale cache (the series is gone, or changed type) or a check the sink does not
             * know. Resolve again, and find the record at fault by halving, in order. */
            cache.clear();
            int status = e instanceof TsdbException ? ((TsdbException) e).status() : Status.ERR_INVALID;
            if (live.size() == 1) {
                if (retriedFresh) {
                    retriedFresh = false;
                    live.get(0).pending.poison("tsdb refused the record: " + e.getMessage(), status);
                    return 0;
                }
                retriedFresh = true;
                try {
                    return write(live);
                } finally {
                    retriedFresh = false;
                }
            }
            int half = live.size() / 2;
            return write(new ArrayList<>(live.subList(0, half))) + write(new ArrayList<>(live.subList(half, live.size())));
        }
    }

    /** Set while a single record is retried once with a fresh cache before it is declared poison. */
    private boolean retriedFresh;

    /** A point with its series resolved and value converted. */
    private static final class Resolved {
        final SeriesCache.Entry entry;
        final long ts;
        final Object value; /* Double, Float, Long, Boolean, Integer (state code), or null */

        Resolved(SeriesCache.Entry entry, long ts, Object value) {
            this.entry = entry;
            this.ts = ts;
            this.value = value;
        }
    }

    private Resolved resolve(Point p) throws Exception {
        SeriesCache.Entry e = cache.get(p.series);
        if (e == null) {
            try {
                e = conn.lookup(p.series);
                if (e == null) {
                    if (!c.autoCreate)
                        throw new PoisonException("series " + Point.brief(p.series) + " does not exist", Status.ERR_NOTFOUND);
                    e = conn.create(p.series, p.type != null ? p.type : defaultType);
                }
            } catch (TsdbException | IllegalArgumentException x) {
                if (classifier.classify(x) == ErrorClassifier.Kind.PERMANENT)
                    throw new PoisonException("series " + Point.brief(p.series) + ": " + x.getMessage(),
                            x instanceof TsdbException ? ((TsdbException) x).status() : Status.ERR_INVALID);
                throw x;
            }
            cache.put(p.series, e);
        }
        if (p.type != null && p.type != e.type)
            throw new PoisonException("series " + Point.brief(p.series) + " is " + e.type + ", the record says " + p.type,
                    Status.ERR_INVALID);
        return new Resolved(e, p.ts, convert(p, e));
    }

    private Object convert(Point p, SeriesCache.Entry e) throws Exception {
        Object v = p.value;
        if (v == null)
            return null;
        switch (e.type) {
        case FLOAT64:
            if (v instanceof Number) {
                double d = ((Number) v).doubleValue();
                if (Double.doubleToRawLongBits(d) == NULL_F64_BITS)
                    throw reservedNull(p, e);
                return d;
            }
            break;
        case FLOAT32:
            if (v instanceof Number) {
                double d = ((Number) v).doubleValue();
                float f = (float) d;
                if (Float.isInfinite(f) && !Double.isInfinite(d))
                    throw new PoisonException("value " + Point.clip(v) + " out of float32 range for " + Point.brief(p.series),
                            Status.ERR_INVALID);
                if (Float.floatToRawIntBits(f) == NULL_F32_BITS)
                    throw reservedNull(p, e);
                return f;
            }
            break;
        case INT64:
            Long l = null;
            if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte)
                l = ((Number) v).longValue();
            else if (v instanceof BigInteger && ((BigInteger) v).bitLength() < 64)
                l = ((BigInteger) v).longValue();
            else if (v instanceof Double || v instanceof Float || v instanceof BigDecimal) {
                double d = ((Number) v).doubleValue();
                if (d == Math.rint(d) && Math.abs(d) < 9.2e18)
                    l = (long) d;
            }
            if (l != null && l == Long.MIN_VALUE)
                throw reservedNull(p, e);
            if (l != null)
                return l;
            break;
        case BOOL:
            if (v instanceof Boolean)
                return v;
            break;
        case STATE:
            if (v instanceof String) {
                Integer code = cache.state(e, (String) v);
                if (code == null) {
                    try {
                        code = conn.state(e.id, (String) v);
                    } catch (TsdbException | IllegalArgumentException x) {
                        if (classifier.classify(x) == ErrorClassifier.Kind.PERMANENT)
                            throw new PoisonException("state " + Point.clip(v) + " of " + Point.brief(p.series) + ": " + x.getMessage(),
                                    Status.ERR_INVALID);
                        throw x;
                    }
                    cache.putState(e, (String) v, code);
                }
                return code;
            }
            break;
        default:
            break;
        }
        throw new PoisonException("value " + Point.clip(v) + " (" + v.getClass().getSimpleName() + ") does not fit " + Point.brief(p.series)
                + " of type " + e.type, Status.ERR_INVALID);
    }

    /* tsdb stores null as one reserved value per type (TSDB_NULL_* in tsdb.h, Column in client/java) */
    private static final long NULL_F64_BITS = 0x7ff8000000001001L;
    private static final int NULL_F32_BITS = 0x7fc01001;

    /**
     * A value that is tsdb's encoding of null. Written, it would read back as null and, last write wins, replace an
     * older real value without anyone noticing; a producer that means null sends null.
     */
    private static PoisonException reservedNull(Point p, SeriesCache.Entry e) {
        return new PoisonException("value of " + Point.brief(p.series) + " is the bit pattern tsdb reserves for null in "
                + e.type + " (send null instead)", Status.ERR_INVALID);
    }

    /** The rows of one type of a batch, in arrival order. */
    private static final class Rows {
        final ValueType type;
        final List<Resolved> rows = new ArrayList<>();

        Rows(ValueType type) {
            this.type = type;
        }

        void add(Resolved r) {
            rows.add(r);
        }

        void send(TsdbConnection conn) throws TsdbException {
            int n = rows.size();
            int[] ids = new int[n];
            long[] ts = new long[n];
            BitSet nulls = new BitSet();
            for (int i = 0; i < n; i++) {
                ids[i] = rows.get(i).entry.id;
                ts[i] = rows.get(i).ts;
                if (rows.get(i).value == null)
                    nulls.set(i);
            }
            Column col;
            switch (type) {
            case FLOAT64: {
                double[] a = new double[n];
                for (int i = 0; i < n; i++)
                    a[i] = nulls.get(i) ? 0 : (Double) rows.get(i).value;
                col = Column.ofDoubles(a);
                break;
            }
            case FLOAT32: {
                float[] a = new float[n];
                for (int i = 0; i < n; i++)
                    a[i] = nulls.get(i) ? 0 : (Float) rows.get(i).value;
                col = Column.ofFloats(a);
                break;
            }
            case INT64: {
                long[] a = new long[n];
                for (int i = 0; i < n; i++)
                    a[i] = nulls.get(i) ? 0 : (Long) rows.get(i).value;
                col = Column.ofLongs(a);
                break;
            }
            case BOOL: {
                boolean[] a = new boolean[n];
                for (int i = 0; i < n; i++)
                    a[i] = !nulls.get(i) && (Boolean) rows.get(i).value;
                col = Column.ofBools(a);
                break;
            }
            default: {
                int[] a = new int[n];
                for (int i = 0; i < n; i++)
                    a[i] = nulls.get(i) ? 0 : (Integer) rows.get(i).value;
                col = Column.ofStates(a);
                break;
            }
            }
            if (!nulls.isEmpty())
                col = col.withNulls(nulls);
            conn.insertAsync(ids, ts, col, n);
        }
    }
}
