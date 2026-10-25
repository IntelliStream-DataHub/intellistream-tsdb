package ai.intellistream.tsdb.pulsar;

import ai.intellistream.tsdb.Column;
import ai.intellistream.tsdb.Status;
import ai.intellistream.tsdb.TsdbException;
import ai.intellistream.tsdb.ValueType;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * An in-memory tsdbd for the sink's unit tests. Rows of an insert become visible ("durable") only when await()
 * succeeds, as with the real server's acknowledgement; injected failures drop the pending rows of that connection.
 */
final class FakeTsdb implements TsdbConnection.Factory {
    /** A committed row. */
    static final class Row {
        final String series;
        final long ts;
        final double value; /* NaN for null */
        final ValueType type;

        Row(String series, long ts, double value, ValueType type) {
            this.series = series;
            this.ts = ts;
            this.value = value;
            this.type = type;
        }
    }

    final Map<String, SeriesCache.Entry> series = new HashMap<>();
    final Map<Integer, String> names = new HashMap<>();
    /** STATE_NAME requests, to tell cache hits from lookups. */
    final java.util.concurrent.atomic.AtomicInteger stateCalls = new java.util.concurrent.atomic.AtomicInteger();
    final List<Row> rows = new ArrayList<>();
    /** Failures the next await() calls throw, one each. */
    final Deque<Integer> awaitFailures = new ArrayDeque<>();
    /** Series whose rows the server refuses with ERR_INVALID (a check the sink does not make). */
    final Set<String> refused = new HashSet<>();
    /** While true, connect() and every call fail with ERR_IO. */
    volatile boolean down;
    /** When set, await() takes a permit first: lets tests hold acknowledgements. */
    volatile Semaphore gate;
    /** Series whose inserts are held: await() of an insert holding one of them first takes a permit of heldGate. */
    final Set<String> held = ConcurrentHashMap.newKeySet();
    final Semaphore heldGate = new Semaphore(0);
    int connects;
    int awaits;

    synchronized List<Row> rows() {
        return new ArrayList<>(rows);
    }

    synchronized Map<String, Double> last() {
        Map<String, Double> m = new HashMap<>();
        for (Row r : rows)
            m.put(r.series + "@" + r.ts, r.value);
        return m;
    }

    @Override
    public synchronized TsdbConnection connect() throws TsdbException {
        if (down)
            throw new TsdbException(Status.ERR_IO, "connection refused", null);
        connects++;
        return new Conn();
    }

    private final class Conn implements TsdbConnection {
        private final List<Row> pending = new ArrayList<>();
        private TsdbException failed;
        private boolean open = true;
        private boolean holding;

        private void check() throws TsdbException {
            if (down || !open) {
                open = false;
                throw new TsdbException(Status.ERR_IO, "connection reset", null);
            }
        }

        @Override
        public SeriesCache.Entry lookup(String name) throws TsdbException {
            check();
            if (name.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 65535)
                throw new IllegalArgumentException("name too long"); /* as TsdbClient does */
            synchronized (FakeTsdb.this) {
                return series.get(name);
            }
        }

        @Override
        public SeriesCache.Entry create(String name, ValueType type) throws TsdbException {
            check();
            synchronized (FakeTsdb.this) {
                SeriesCache.Entry e = series.get(name);
                if (e != null && e.type != type)
                    throw new TsdbException(Status.ERR_INVALID);
                if (e == null) {
                    e = new SeriesCache.Entry(series.size() + 1, type);
                    series.put(name, e);
                    names.put(e.id, name);
                }
                return new SeriesCache.Entry(e.id, e.type);
            }
        }

        @Override
        public int state(int s, String name) throws TsdbException {
            check();
            stateCalls.incrementAndGet();
            return Math.abs(name.hashCode() % 1000) + 1;
        }

        @Override
        public void insertAsync(int[] ids, long[] ts, Column values, int n) throws TsdbException {
            check();
            synchronized (FakeTsdb.this) {
                for (int i = 0; i < n; i++) {
                    String name = names.get(ids[i]);
                    holding |= held.contains(name);
                    if (name == null || series.get(name).type != values.type() || refused.contains(name))
                        failed = failed != null ? failed : new TsdbException(Status.ERR_INVALID);
                    if (failed == null)
                        pending.add(new Row(name, ts[i], values.getDouble(i), values.type()));
                }
            }
            if (failed != null)
                pending.clear();
        }

        @Override
        public void await() throws TsdbException {
            if (holding) {
                try {
                    heldGate.acquire();
                } catch (InterruptedException e) {
                    open = false;
                    Thread.currentThread().interrupt();
                    throw new TsdbException(Status.ERR_IO, "interrupted", e);
                } finally {
                    holding = false;
                }
            }
            Semaphore g = gate;
            if (g != null) {
                try {
                    g.acquire();
                } catch (InterruptedException e) {
                    open = false;
                    Thread.currentThread().interrupt();
                    throw new TsdbException(Status.ERR_IO, "interrupted", e);
                }
            }
            try {
                check();
                synchronized (FakeTsdb.this) {
                    awaits++;
                    Integer f = awaitFailures.poll();
                    if (f != null) {
                        if (f == Status.ERR_IO)
                            open = false;
                        throw new TsdbException(f);
                    }
                    if (failed != null)
                        throw failed;
                    rows.addAll(pending);
                }
            } finally {
                pending.clear();
                failed = null;
            }
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() {
            open = false;
        }
    }
}
