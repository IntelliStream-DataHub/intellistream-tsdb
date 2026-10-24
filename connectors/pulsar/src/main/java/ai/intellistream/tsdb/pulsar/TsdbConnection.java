package ai.intellistream.tsdb.pulsar;

import ai.intellistream.tsdb.Column;
import ai.intellistream.tsdb.Status;
import ai.intellistream.tsdb.TsdbClient;
import ai.intellistream.tsdb.TsdbException;
import ai.intellistream.tsdb.ValueType;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The part of a tsdbd connection the sink uses. An interface so that tests can put a fake behind a shard; the real
 * one ({@link #client}) is a {@link TsdbClient}. Used by one thread at a time.
 */
interface TsdbConnection extends AutoCloseable {
    /** Opens connections; called again after a failure closed the previous one. */
    interface Factory {
        TsdbConnection connect() throws TsdbException;
    }

    /** The id and type of a series, or {@code null} if it does not exist. */
    SeriesCache.Entry lookup(String name) throws TsdbException;

    /** Creates a series (or returns the existing one if it has this type). */
    SeriesCache.Entry create(String name, ValueType type) throws TsdbException;

    /** The code of a state name of a state series, created if new. */
    int state(int series, String name) throws TsdbException;

    /** Sends an insert of rows of one type without waiting. */
    void insertAsync(int[] series, long[] ts, Column values, int n) throws TsdbException;

    /** Waits for every insert sent; throws the first error among them. Returns once they are durable. */
    void await() throws TsdbException;

    boolean isOpen();

    @Override
    void close();

    /**
     * Closes a connection whose server answers but will not write (read only, stopping), and makes the next
     * {@link Factory#connect} start at the endpoint after this one, so that a sink whose first endpoint is a follower
     * after a failover moves on to the new writer instead of asking the follower forever.
     */
    default void closeAndRotate() {
        close();
    }

    /**
     * Connections to the endpoints of {@code c}: the first that accepts one, in order, starting at the endpoint
     * after the last one that refused writes ({@link #closeAndRotate}). The start is shared by the shards.
     */
    static Factory client(TsdbSinkConfig c) {
        List<Map.Entry<String, Integer>> endpoints = c.endpoints();
        Duration timeout = Duration.ofMillis(c.connectTimeoutMs);
        Duration requestTimeout = Duration.ofMillis(c.requestTimeoutMs);
        AtomicInteger first = new AtomicInteger();
        return () -> {
            TsdbException last = null;
            int n = endpoints.size(), from = first.get();
            for (int i = 0; i < n; i++) {
                int k = (from + i) % n;
                Map.Entry<String, Integer> e = endpoints.get(k);
                try {
                    TsdbClient cl = TsdbClient.connect(e.getKey(), e.getValue(), timeout, requestTimeout);
                    cl.setCompression(c.compressionLevel);
                    return new ClientConnection(cl, () -> first.compareAndSet(k, (k + 1) % n));
                } catch (TsdbException x) {
                    last = x;
                }
            }
            throw last != null ? last : new TsdbException(Status.ERR_IO, "no endpoints", null);
        };
    }

    /** A {@link TsdbClient}. */
    final class ClientConnection implements TsdbConnection {
        private final TsdbClient c;
        private final Runnable rotate;

        ClientConnection(TsdbClient c, Runnable rotate) {
            this.c = c;
            this.rotate = rotate;
        }

        @Override
        public void closeAndRotate() {
            close();
            rotate.run();
        }

        @Override
        public SeriesCache.Entry lookup(String name) throws TsdbException {
            int id;
            try {
                id = c.series(name, false);
            } catch (TsdbException e) {
                if (e.status() == Status.ERR_NOTFOUND)
                    return null;
                throw e;
            }
            return new SeriesCache.Entry(id, c.seriesType(id));
        }

        @Override
        public SeriesCache.Entry create(String name, ValueType type) throws TsdbException {
            return new SeriesCache.Entry(c.seriesCreate(name, type), type);
        }

        @Override
        public int state(int series, String name) throws TsdbException {
            return c.state(series, name);
        }

        @Override
        public void insertAsync(int[] series, long[] ts, Column values, int n) throws TsdbException {
            c.insertValuesAsync(series, ts, values, 0, n);
        }

        @Override
        public void await() throws TsdbException {
            c.await();
        }

        @Override
        public boolean isOpen() {
            return c.isOpen();
        }

        @Override
        public void close() {
            try {
                c.close();
            } catch (TsdbException e) {
                /* already broken: nothing to flush, the sink acknowledges nothing it has not seen acknowledged */
            }
        }
    }
}
