package ai.intellistream.tsdb;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A connection to {@code tsdbd}.
 *
 * <pre>{@code
 * try (TsdbClient c = TsdbClient.connect("127.0.0.1", 7070)) {
 *     int id = c.series("pump7.flow");
 *     c.insert(id, timestamps, values);
 *     List<Bucket> perMinute = c.queryAgg(id, start, end, 60_000);
 * }
 * }</pre>
 *
 * <p><b>Threads.</b> A client is one TCP connection and is <em>not</em> thread safe: use it from one thread at a
 * time, and open one client per thread for concurrency. The server runs the requests of different connections in
 * parallel, and those of one connection in order.
 *
 * <p><b>Pipelining.</b> {@link #insertAsync(int, long[], float[])} sends an insert without waiting for its
 * acknowledgement; {@link #await()} collects the acknowledgements and throws the first error among them. Because
 * the server applies a connection's requests in order, pipelined inserts keep "last write wins". Every other call
 * first collects outstanding acknowledgements, and throws the first error among them instead of sending its own
 * request. At most {@value #MAX_IN_FLIGHT} insert frames are outstanding: past that, {@code insertAsync} reads the
 * oldest acknowledgement before sending, which bounds the memory the server spends on queued requests.
 *
 * <p><b>Compression.</b> Inserts travel packed and zstd compressed (level 1 by default) when the server speaks
 * protocol version 3, as with the C client; see {@link #setCompression(int)}. {@link #bytesSent()} counts what
 * goes on the wire.
 *
 * <p><b>Errors.</b> Every request either succeeds or throws {@link TsdbException}. A status from the server (such
 * as {@link Status#ERR_NOTFOUND}) leaves the connection usable. A connection failure, a malformed reply, or an
 * interrupt of the calling thread during I/O closes the connection: later calls throw {@link Status#ERR_IO}.
 * Arguments the protocol cannot carry (mismatched array lengths, names over 65535 UTF-8 bytes, negative limits)
 * throw {@link IllegalArgumentException} without contacting the server.
 *
 * <p><b>Value types.</b> A series has one {@link ValueType}, chosen when it is created: float32 (the type of
 * {@link #series(String)}), float64, int64, bool or state, and any value may be null. The float32 methods
 * ({@link #insert(int, long[], float[])}, {@link #queryPoints(int, long, long)}, {@link #queryRaw}) take and return
 * {@code float[]} and work on float32 series only; {@link #insertSeriesValues}, {@link #insertValues} and
 * {@link #queryValues} carry a {@link Column} of any type with its nulls (protocol version 2).
 * {@link #queryAgg} and {@link #queryGroup} work on every type and need a version 2 server.
 *
 * <p><b>Numbers.</b> Series ids and state codes are unsigned 32-bit integers carried in an {@code int}.
 * Timestamps are milliseconds since the Unix epoch. Time ranges are half open, {@code [start, end)}; use
 * {@link Long#MIN_VALUE} and {@link Long#MAX_VALUE} for "everything".
 */
public final class TsdbClient implements AutoCloseable {
    /** The port {@code tsdbd} listens on by default. */
    public static final int DEFAULT_PORT = Protocol.DEFAULT_PORT;

    /** The most insert frames {@code insertAsync} leaves unacknowledged. */
    public static final int MAX_IN_FLIGHT = 4096;

    private static final int BUF = 1 << 20;               /* send buffer; receive buffer between large replies */
    private static final int INSERT_FRAME_ROWS = 8 << 20; /* as the C client: under 256 MiB for any value type */
    private static final int MAX_STRING = 0xffff;         /* u16 length prefix */
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);

    private final SocketChannel ch;
    private final ByteBuffer out = direct(BUF);  /* write mode; empty between requests */
    private ByteBuffer in = direct(BUF).flip();  /* read mode: [position, limit) is received, unread data */
    private int bodyLen, bodyEnd;                /* of the reply frame being read */
    private int nextId;                          /* id of the next request */
    private int ackId;                           /* id of the oldest request whose (last) reply is unread */
    private int pending;                         /* insert acknowledgements not yet read */
    private int pendingStatus;                   /* the first error among them */
    private boolean broken, closed;
    private long[] chunkTs = new long[0];        /* reused for RawConsumer */
    private float[] chunkValues = Values.alloc(0);
    private Column chunkColumn;                  /* reused for ValuesConsumer */
    private final BitSet chunkNulls = new BitSet();
    private int zlevel = 1;                      /* zstd level of packed inserts; 0: plain arrays */
    private int serverVersion;                   /* from HELLO; 0 until asked */
    private Compressor zstd;                     /* created at the first packed insert */
    private boolean noZstd;                      /* zstd-jni could not be loaded: send plain arrays */
    private byte[] raw = new byte[0];            /* the reshaped body of a packed frame */
    private long bytesSent;

    private TsdbClient(SocketChannel ch) {
        this.ch = ch;
    }

    /**
     * Connects to a server, trying every address of {@code host}, with a 10 second timeout per address.
     *
     * @param host a host name or address, such as "127.0.0.1"
     * @param port the server's port, usually {@link #DEFAULT_PORT}
     * @return the connected client
     * @throws TsdbException with {@link Status#ERR_IO} if no address accepts the connection
     */
    public static TsdbClient connect(String host, int port) throws TsdbException {
        return connect(host, port, CONNECT_TIMEOUT);
    }

    /**
     * Connects to a server, trying every address of {@code host}.
     *
     * @param host a host name or address
     * @param port the server's port
     * @param timeout how long to wait for each address; zero waits as long as the operating system does
     * @return the connected client
     * @throws TsdbException with {@link Status#ERR_IO} if no address accepts the connection
     */
    public static TsdbClient connect(String host, int port, Duration timeout) throws TsdbException {
        Objects.requireNonNull(host, "host");
        int ms = (int) Math.min(Integer.MAX_VALUE, Math.max(0, timeout.toMillis()));
        InetAddress[] addrs;
        try {
            addrs = InetAddress.getAllByName(host);
        } catch (UnknownHostException e) {
            throw new TsdbException(Status.ERR_IO, "unknown host " + host, e);
        }
        IOException last = null;
        for (InetAddress a : addrs) {
            SocketChannel ch = null;
            try {
                ch = SocketChannel.open();
                ch.setOption(StandardSocketOptions.TCP_NODELAY, true);
                ch.socket().connect(new InetSocketAddress(a, port), ms);
                return new TsdbClient(ch);
            } catch (IOException e) {
                last = e;
                closeQuietly(ch);
            }
        }
        throw new TsdbException(Status.ERR_IO, "cannot connect to " + host + ":" + port, last);
    }

    /**
     * Closes the connection. Pipelined inserts are acknowledged first: the server drops the requests of a closed
     * connection that it has not run yet, so closing without waiting could lose them.
     *
     * @throws TsdbException the first error among the pipelined inserts; the connection is closed regardless
     */
    @Override
    public void close() throws TsdbException {
        if (closed)
            return;
        TsdbException err = null;
        if (!broken && pending > 0) {
            try {
                await();
            } catch (TsdbException e) {
                err = e;
            }
        }
        closed = true;
        closeQuietly(ch);
        if (zstd != null)
            zstd.close();
        if (err != null)
            throw err;
    }

    /**
     * Whether the client can still send requests: not closed, and not broken by a connection failure.
     *
     * @return {@code true} if the connection is usable
     */
    public boolean isOpen() {
        return !closed && !broken;
    }

    /**
     * Exchanges protocol versions with the server.
     *
     * @return the server's protocol version (3 for the server this client is written for)
     * @throws TsdbException on a connection failure
     */
    public int hello() throws TsdbException {
        request();
        begin(Protocol.OP_HELLO, 4);
        out.putInt(Protocol.VERSION);
        send();
        int st = frame();
        if (st == 0 && bodyLen != 4)
            throw lost(Status.ERR_CORRUPT, "HELLO reply of " + bodyLen + " bytes");
        int v = st == 0 ? in.getInt() : 0;
        endReply();
        check(st);
        serverVersion = v;
        return v;
    }

    /**
     * Sets how inserts travel. With a level other than 0 (the default is 1), inserts of every type, float32
     * included, are packed (delta varints and byte planes) and compressed as one zstd frame each, which takes a few
     * bytes a row on the wire instead of 16 or more. The client asks the server's version (HELLO) before the first
     * insert and packs only for a server that speaks protocol version 3; older servers get plain arrays. Level 0
     * sends plain arrays, which costs no CPU; negative levels are faster than 1 and compress less.
     *
     * <p>Packing needs zstd-jni at run time; without it the client sends plain arrays.
     *
     * @param level a zstd level from -131072 to 22, or 0 for plain arrays
     * @throws IllegalArgumentException for a level zstd does not have
     */
    public void setCompression(int level) {
        if (level < Compressor.MIN_LEVEL || level > Compressor.MAX_LEVEL)
            throw new IllegalArgumentException("zstd level " + level + " is not in [" + Compressor.MIN_LEVEL + ", "
                    + Compressor.MAX_LEVEL + "]");
        zlevel = level;
    }

    /**
     * The zstd level of inserts.
     *
     * @return the level set by {@link #setCompression(int)}; 0 for plain arrays
     */
    public int compression() {
        return zlevel;
    }

    /**
     * Bytes written to the connection so far: every request frame, headers included.
     *
     * @return the bytes sent since the client connected
     */
    public long bytesSent() {
        return bytesSent;
    }

    // ------------------------------------------------------------------ series

    /**
     * Returns the id of a series, creating it in the server's database directory if it does not exist.
     *
     * @param name the series name, 1 to 65535 bytes of UTF-8
     * @return the series id
     * @throws TsdbException if the server fails, e.g. {@link Status#ERR_READONLY}
     */
    public int series(String name) throws TsdbException {
        return seriesRequest(name, true, null);
    }

    /**
     * Returns the id of a series, optionally creating it.
     *
     * @param name the series name, 1 to 65535 bytes of UTF-8
     * @param create whether to create a missing series
     * @return the series id
     * @throws TsdbException with {@link Status#ERR_NOTFOUND} if the series is missing and {@code create} is false
     */
    public int series(String name, boolean create) throws TsdbException {
        return seriesRequest(name, create, null);
    }

    /**
     * Returns the id of a series, creating it in the volume {@code path} on the server if it does not exist
     * ({@code tsdb_series_at}). Series created with the same path share that volume; the path only matters when the
     * series is created.
     *
     * @param name the series name, 1 to 65535 bytes of UTF-8
     * @param path a directory on the server, or {@code null} or "" for the database directory
     * @return the series id
     * @throws TsdbException if the server cannot create the series or its volume
     */
    public int seriesAt(String name, String path) throws TsdbException {
        return seriesRequest(name, true, path);
    }

    private int seriesRequest(String name, boolean create, String path) throws TsdbException {
        byte[] n = utf8(Objects.requireNonNull(name, "name"), "name");
        if (n.length == 0)
            throw new IllegalArgumentException("empty series name");
        byte[] p = path != null ? utf8(path, "path") : null;
        request();
        begin(Protocol.OP_SERIES, 3 + n.length + (p != null ? 2 + p.length : 0));
        out.put((byte) (create ? 1 : 0)).putShort((short) n.length).put(n);
        if (p != null)
            out.putShort((short) p.length).put(p);
        send();
        int st = frame();
        if (st == 0 && bodyLen != 4)
            throw lost(Status.ERR_CORRUPT, "SERIES reply of " + bodyLen + " bytes");
        int id = st == 0 ? in.getInt() : 0;
        endReply();
        check(st);
        return id;
    }

    /**
     * Returns the name of a series.
     *
     * @param id the series id
     * @return its name
     * @throws TsdbException with {@link Status#ERR_NOTFOUND} for an unknown id
     */
    public String seriesName(int id) throws TsdbException {
        request();
        begin(Protocol.OP_SERIES_NAME, 4);
        out.putInt(id);
        send();
        return stringReply("SERIES_NAME");
    }

    // ----------------------------------------------------------------- inserts

    /**
     * Inserts points into one series and waits until they are durable and visible.
     *
     * <p>Rows may come in any order. Writing an existing (series, timestamp) replaces its value; within one batch the
     * later row wins. A batch is applied all or nothing, except that batches over 8M rows are sent as several
     * frames, each applied on its own.
     *
     * @param series the series id
     * @param timestamps ms since the epoch
     * @param values the values; as long as {@code timestamps}
     * @throws TsdbException the first error among this insert and earlier pipelined ones
     */
    public void insert(int series, long[] timestamps, float[] values) throws TsdbException {
        insert(series, timestamps, values, 0, sameLength(timestamps, values));
    }

    /**
     * Inserts {@code length} points from position {@code offset} of the arrays into one series, and waits.
     *
     * @param series the series id
     * @param timestamps ms since the epoch
     * @param values the values
     * @param offset the first row to send
     * @param length the number of rows
     * @throws TsdbException the first error among this insert and earlier pipelined ones
     * @see #insert(int, long[], float[])
     */
    public void insert(int series, long[] timestamps, float[] values, int offset, int length) throws TsdbException {
        insertFrames(series, null, timestamps, values, offset, length);
        await();
    }

    /**
     * Inserts points of several series, one series id per row, and waits until they are durable and visible.
     *
     * @param series the series id of each row
     * @param timestamps ms since the epoch
     * @param values the values
     * @throws TsdbException the first error among this insert and earlier pipelined ones
     * @see #insert(int, long[], float[])
     */
    public void insert(int[] series, long[] timestamps, float[] values) throws TsdbException {
        insert(series, timestamps, values, 0, sameLength(series, timestamps, values));
    }

    /**
     * Inserts {@code length} rows from position {@code offset} of the arrays, one series id per row, and waits.
     *
     * @param series the series id of each row
     * @param timestamps ms since the epoch
     * @param values the values
     * @param offset the first row to send
     * @param length the number of rows
     * @throws TsdbException the first error among this insert and earlier pipelined ones
     */
    public void insert(int[] series, long[] timestamps, float[] values, int offset, int length)
            throws TsdbException {
        insertFrames(0, Objects.requireNonNull(series, "series"), timestamps, values, offset, length);
        await();
    }

    /**
     * Sends an insert into one series without waiting for its acknowledgement. The arrays may be reused as soon as
     * this returns. Errors are reported by {@link #await()}, by the next call that waits, or by {@link #close()}.
     *
     * @param series the series id
     * @param timestamps ms since the epoch
     * @param values the values
     * @throws TsdbException on a connection failure
     */
    public void insertAsync(int series, long[] timestamps, float[] values) throws TsdbException {
        insertAsync(series, timestamps, values, 0, sameLength(timestamps, values));
    }

    /**
     * Sends {@code length} rows from position {@code offset} into one series without waiting.
     *
     * @param series the series id
     * @param timestamps ms since the epoch
     * @param values the values
     * @param offset the first row to send
     * @param length the number of rows
     * @throws TsdbException on a connection failure
     * @see #insertAsync(int, long[], float[])
     */
    public void insertAsync(int series, long[] timestamps, float[] values, int offset, int length)
            throws TsdbException {
        insertFrames(series, null, timestamps, values, offset, length);
    }

    /**
     * Sends an insert of several series, one series id per row, without waiting.
     *
     * @param series the series id of each row
     * @param timestamps ms since the epoch
     * @param values the values
     * @throws TsdbException on a connection failure
     * @see #insertAsync(int, long[], float[])
     */
    public void insertAsync(int[] series, long[] timestamps, float[] values) throws TsdbException {
        insertAsync(series, timestamps, values, 0, sameLength(series, timestamps, values));
    }

    /**
     * Sends {@code length} rows from position {@code offset}, one series id per row, without waiting.
     *
     * @param series the series id of each row
     * @param timestamps ms since the epoch
     * @param values the values
     * @param offset the first row to send
     * @param length the number of rows
     * @throws TsdbException on a connection failure
     * @see #insertAsync(int, long[], float[])
     */
    public void insertAsync(int[] series, long[] timestamps, float[] values, int offset, int length)
            throws TsdbException {
        insertFrames(0, Objects.requireNonNull(series, "series"), timestamps, values, offset, length);
    }

    /**
     * Waits for the acknowledgements of all pipelined inserts.
     *
     * @throws TsdbException the first error among them (the rest were still applied, each on its own)
     */
    public void await() throws TsdbException {
        ready();
        while (pending > 0)
            collectOne();
        int st = pendingStatus;
        pendingStatus = 0;
        check(st);
    }

    /**
     * The number of insert frames sent with {@code insertAsync} whose acknowledgement has not been read.
     *
     * @return outstanding acknowledgements
     */
    public int pending() {
        return pending;
    }

    private void insertFrames(int series, int[] many, long[] ts, float[] values, int off, int n)
            throws TsdbException {
        Objects.requireNonNull(ts, "timestamps");
        Objects.requireNonNull(values, "values");
        Objects.checkFromIndexSize(off, n, ts.length);
        Objects.checkFromIndexSize(off, n, values.length);
        if (many != null)
            Objects.checkFromIndexSize(off, n, many.length);
        else if (series == Protocol.MANY_SERIES)
            throw new IllegalArgumentException("series id 0xffffffff is reserved");
        ready();
        if (n > 0 && packing()) { /* float32 too: the packed form is the smaller one */
            insertValuesFrames(series, many, ts, Column.ofFloats(values), off, n);
            return;
        }
        int rowBytes = Long.BYTES + Values.BYTES + (many != null ? Integer.BYTES : 0);
        int sent = 0;
        do { /* one frame even for zero rows, as the C client */
            int m = Math.min(n - sent, INSERT_FRAME_ROWS);
            while (pending >= MAX_IN_FLIGHT)
                collectOne();
            begin(Protocol.OP_INSERT, 8 + (long) m * rowBytes);
            out.putInt(m).putInt(many != null ? Protocol.MANY_SERIES : series);
            putLongs(ts, off + sent, m);
            putValues(values, off + sent, m);
            if (many != null)
                putInts(many, off + sent, m);
            send();
            pending++;
            sent += m;
        } while (sent < n);
    }

    private void collectOne() throws TsdbException {
        int st = frame();
        endReply();
        pending--;
        if (st != 0 && pendingStatus == 0)
            pendingStatus = st;
    }

    // ----------------------------------------------------------------- queries

    /**
     * Returns the points of a series in {@code [start, end)}, oldest first.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @return the points
     * @throws TsdbException if the query fails, or {@link Status#ERR_NOMEM} if the result does not fit in one array
     */
    public Points queryPoints(int series, long start, long end) throws TsdbException {
        return queryPoints(series, start, end, Order.ASC, 0);
    }

    /**
     * Returns the points of a series in {@code [start, end)}, in the given order, at most {@code limit} of them.
     * With {@link Order#DESC} and a limit, this is "the newest N points".
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param order oldest or newest first
     * @param limit the most points to return; 0 for all
     * @return the points
     * @throws TsdbException if the query fails, or {@link Status#ERR_NOMEM} if the result does not fit in one array
     */
    public Points queryPoints(int series, long start, long end, Order order, long limit) throws TsdbException {
        Collector c = new Collector();
        raw(series, start, end, order, limit, c);
        if (c.overflow)
            throw new TsdbException(Status.ERR_NOMEM, "more points than one array holds; stream them with queryRaw",
                    null);
        return c.result();
    }

    /**
     * Streams the points of a series in {@code [start, end)}, oldest first, chunk by chunk.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param consumer receives the chunks
     * @return {@code true} if every point was delivered, {@code false} if the consumer stopped early
     * @throws TsdbException if the query fails
     */
    public boolean queryRaw(int series, long start, long end, RawConsumer consumer) throws TsdbException {
        return queryRaw(series, start, end, Order.ASC, 0, consumer);
    }

    /**
     * Streams the points of a series in {@code [start, end)}, in the given order, at most {@code limit} of them.
     * The server sends chunks of up to 16384 points, and memory stays bounded however large the result.
     *
     * <p>The protocol cannot cancel a query: when the consumer stops (or throws), the client still reads the rest of
     * the stream, so the connection stays usable. Use a limit to bound the work.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param order oldest or newest first
     * @param limit the most points to return; 0 for all
     * @param consumer receives the chunks
     * @return {@code true} if every point was delivered, {@code false} if the consumer stopped early
     * @throws TsdbException if the query fails
     */
    public boolean queryRaw(int series, long start, long end, Order order, long limit, RawConsumer consumer)
            throws TsdbException {
        Objects.requireNonNull(consumer, "consumer");
        return raw(series, start, end, order, limit, (b, n) -> {
            if (chunkTs.length < n) {
                chunkTs = new long[n];
                chunkValues = Values.alloc(n);
            }
            getLongs(b, chunkTs, 0, n);
            Values.get(b, chunkValues, 0, n);
            return consumer.accept(chunkTs, chunkValues, n);
        });
    }

    /** Takes one chunk of a raw reply: n timestamps at the buffer's position, then n values. */
    private interface Sink {
        boolean chunk(ByteBuffer b, int n);
    }

    /** Collects a raw reply into growing arrays, decoded straight from the receive buffer. */
    private static final class Collector implements Sink {
        long[] ts = new long[0];
        float[] values = Values.alloc(0);
        int n;
        boolean overflow;

        @Override
        public boolean chunk(ByteBuffer b, int k) {
            if (k > ts.length - n) {
                long cap = Math.max((long) ts.length * 2, (long) n + k);
                if ((long) n + k > Integer.MAX_VALUE - 8) {
                    overflow = true;
                    return false;
                }
                cap = Math.min(cap, Integer.MAX_VALUE - 8);
                ts = Arrays.copyOf(ts, (int) cap);
                values = Arrays.copyOf(values, (int) cap);
            }
            getLongs(b, ts, n, k);
            Values.get(b, values, n, k);
            n += k;
            return true;
        }

        Points result() {
            return n == ts.length ? new Points(ts, values) : new Points(Arrays.copyOf(ts, n), Arrays.copyOf(values, n));
        }
    }

    private boolean raw(int series, long start, long end, Order order, long limit, Sink sink) throws TsdbException {
        Objects.requireNonNull(order, "order");
        if (limit < 0)
            throw new IllegalArgumentException("negative limit");
        request();
        boolean opts = order != Order.ASC || limit != 0;
        begin(Protocol.OP_QUERY_RAW, opts ? 29 : 20);
        out.putInt(series).putLong(start).putLong(end);
        if (opts)
            out.put((byte) (order == Order.DESC ? 1 : 0)).putLong(limit);
        send();
        boolean stopped = false;
        RuntimeException thrown = null;
        for (;;) {
            int st = frame();
            if (bodyLen == 0 && st != 0) { /* refused before streaming: a bare status */
                endReply();
                throw new TsdbException(st);
            }
            int n = bodyLen >= 5 ? in.getInt(in.position() + 1) : -1;
            if (n < 0 || bodyLen != 5 + (long) n * (Long.BYTES + Values.BYTES))
                throw lost(Status.ERR_CORRUPT, "QUERY_RAW chunk of " + bodyLen + " bytes");
            boolean last = in.get() != 0;
            in.position(in.position() + 4);
            if (n > 0 && !stopped) {
                try {
                    stopped = !sink.chunk(in, n);
                } catch (RuntimeException e) {
                    thrown = e;
                    stopped = true;
                } catch (Error e) {
                    lost(Status.ERR_IO, "consumer failed");
                    throw e;
                }
            }
            if (!last) {
                in.position(bodyEnd);
                continue;
            }
            endReply();
            if (thrown != null)
                throw thrown;
            check(st);
            return !stopped;
        }
    }

    /**
     * Aggregates a series over {@code [start, end)} in fixed intervals aligned to the epoch, like ClickHouse's
     * {@code toStartOfInterval}. Only buckets with rows are returned, oldest first.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param intervalMs the bucket length in ms; 0 for a single bucket starting at {@code start}
     * @return the buckets
     * @throws TsdbException with {@link Status#ERR_INVALID} for a negative interval,
     *     {@link Status#ERR_TOO_MANY_BUCKETS} if the range holds too many, and so on
     */
    public List<Bucket> queryAgg(int series, long start, long end, long intervalMs) throws TsdbException {
        return buckets(series, start, end, intervalMs, null, Order.ASC, 0);
    }

    /**
     * Aggregates a series over {@code [start, end)} in fixed intervals, in the given order, at most {@code limit}
     * buckets.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param intervalMs the bucket length in ms; 0 for a single bucket starting at {@code start}
     * @param order oldest or newest bucket first
     * @param limit the most buckets to return; 0 for all
     * @return the buckets
     * @throws TsdbException as {@link #queryAgg(int, long, long, long)}
     */
    public List<Bucket> queryAgg(int series, long start, long end, long intervalMs, Order order, long limit)
            throws TsdbException {
        return buckets(series, start, end, intervalMs, null, order, limit);
    }

    /**
     * Aggregates a series over {@code [start, end)} in calendar buckets, oldest first.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param group the calendar grouping, or {@code null} for one bucket over the whole range
     * @return the buckets with rows
     * @throws TsdbException with {@link Status#ERR_INVALID} for an unknown time zone, and so on
     */
    public List<Bucket> queryGroup(int series, long start, long end, Group group) throws TsdbException {
        return queryGroup(series, start, end, group, Order.ASC, 0);
    }

    /**
     * Aggregates a series over {@code [start, end)} in calendar buckets, in the given order, at most {@code limit}
     * of them. With {@link Order#DESC} and a limit, this is "the last N days" (or months, ...) that have data.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param group the calendar grouping, or {@code null} for one bucket over the whole range
     * @param order oldest or newest bucket first
     * @param limit the most buckets to return; 0 for all
     * @return the buckets with rows
     * @throws TsdbException with {@link Status#ERR_INVALID} for an unknown time zone, and so on
     */
    public List<Bucket> queryGroup(int series, long start, long end, Group group, Order order, long limit)
            throws TsdbException {
        return buckets(series, start, end, 0, group, order, limit);
    }

    /** QUERY_BUCKETS: fixed intervals (group null) or calendar groups, with f64 min/max and exact integers. */
    private List<Bucket> buckets(int series, long start, long end, long interval, Group group, Order order,
                                 long limit) throws TsdbException {
        Objects.requireNonNull(order, "order");
        if (limit < 0)
            throw new IllegalArgumentException("negative limit");
        byte[] zone = group != null && group.timezone() != null ? utf8(group.timezone(), "timezone") : new byte[0];
        request();
        begin(Protocol.OP_QUERY_BUCKETS, 44 + zone.length);
        out.putInt(series).putLong(start).putLong(end).putLong(interval);
        out.put((byte) (group != null ? group.unit().code : 0)).putInt(group != null ? group.count() : 0);
        out.put((byte) (order == Order.DESC ? 1 : 0)).putLong(limit);
        out.putShort((short) zone.length).put(zone);
        send();
        int st = frame();
        if (st != 0) {
            endReply();
            throw new TsdbException(st);
        }
        int n = bodyLen >= 4 ? in.getInt() : -1;
        if (n < 0 || bodyLen != 4 + (long) n * Protocol.BUCKET_ROW)
            throw lost(Status.ERR_CORRUPT, "QUERY_BUCKETS reply of " + bodyLen + " bytes");
        Bucket[] b = new Bucket[n];
        for (int i = 0; i < n; i++)
            b[i] = new Bucket(in.getLong(), in.getLong(), in.getDouble(), in.getDouble(), in.getDouble(),
                    in.getLong(), in.getLong(), in.getLong());
        endReply();
        return Collections.unmodifiableList(Arrays.asList(b));
    }

    // ------------------------------------------------------------- value types

    /**
     * Returns the id of a series of {@code type}, creating it in the database directory if it does not exist.
     *
     * @param name the series name, 1 to 65535 bytes of UTF-8
     * @param type the value type
     * @return the series id
     * @throws TsdbException with {@link Status#ERR_INVALID} if the series exists with another type
     */
    public int seriesCreate(String name, ValueType type) throws TsdbException {
        return seriesCreate(name, type, null);
    }

    /**
     * Returns the id of a series of {@code type}, creating it in the volume {@code path} on the server if it does
     * not exist. An existing series keeps its type and volume.
     *
     * @param name the series name, 1 to 65535 bytes of UTF-8
     * @param type the value type
     * @param path a directory on the server, or {@code null} or "" for the database directory
     * @return the series id
     * @throws TsdbException with {@link Status#ERR_INVALID} if the series exists with another type
     */
    public int seriesCreate(String name, ValueType type, String path) throws TsdbException {
        byte[] n = utf8(Objects.requireNonNull(name, "name"), "name");
        if (n.length == 0)
            throw new IllegalArgumentException("empty series name");
        Objects.requireNonNull(type, "type");
        byte[] p = path != null ? utf8(path, "path") : new byte[0];
        request();
        begin(Protocol.OP_SERIES_CREATE, 5 + n.length + p.length);
        out.put((byte) type.code).putShort((short) n.length).put(n).putShort((short) p.length).put(p);
        send();
        return u32Reply("SERIES_CREATE");
    }

    /**
     * Returns the value type of a series.
     *
     * @param id the series id
     * @return its type
     * @throws TsdbException with {@link Status#ERR_NOTFOUND} for an unknown id
     */
    public ValueType seriesType(int id) throws TsdbException {
        request();
        begin(Protocol.OP_SERIES_TYPE, 4);
        out.putInt(id);
        send();
        int st = frame();
        ValueType t = null;
        if (st == 0 && (bodyLen != 1 || (t = ValueType.of(in.get())) == null))
            throw lost(Status.ERR_CORRUPT, "SERIES_TYPE reply");
        endReply();
        check(st);
        return t;
    }

    /**
     * Returns the code of a state name in the dictionary of a state series' volume, adding the name if it is new.
     * Codes start at 1 and never change.
     *
     * <p>The series only selects the volume whose dictionary is used: tsdbd accepts any series of that volume, of
     * any type.
     *
     * @param series a state series
     * @param name the state name, 1 to 65535 bytes of UTF-8
     * @return its code
     * @throws TsdbException with {@link Status#ERR_NOTFOUND} for an unknown series
     */
    public int state(int series, String name) throws TsdbException {
        return stateRequest(series, name, true);
    }

    /**
     * Returns the code of an existing state name.
     *
     * @param series a state series
     * @param name the state name
     * @return its code
     * @throws TsdbException with {@link Status#ERR_NOTFOUND} if the name has no code
     */
    public int stateLookup(int series, String name) throws TsdbException {
        return stateRequest(series, name, false);
    }

    /**
     * Returns the name of a state code.
     *
     * @param series a state series
     * @param code the code
     * @return its name
     * @throws TsdbException with {@link Status#ERR_NOTFOUND} for an unknown code
     */
    public String stateName(int series, int code) throws TsdbException {
        request();
        begin(Protocol.OP_STATE_NAME, 8);
        out.putInt(series).putInt(code);
        send();
        return stringReply("STATE_NAME");
    }

    private int stateRequest(int series, String name, boolean create) throws TsdbException {
        byte[] n = utf8(Objects.requireNonNull(name, "name"), "name");
        if (n.length == 0)
            throw new IllegalArgumentException("empty state name");
        request();
        begin(Protocol.OP_STATE, 7 + n.length);
        out.putInt(series).put((byte) (create ? 1 : 0)).putShort((short) n.length).put(n);
        send();
        return u32Reply("STATE");
    }

    private int u32Reply(String what) throws TsdbException {
        int st = frame();
        if (st == 0 && bodyLen != 4)
            throw lost(Status.ERR_CORRUPT, what + " reply of " + bodyLen + " bytes");
        int v = st == 0 ? in.getInt() : 0;
        endReply();
        check(st);
        return v;
    }

    private String stringReply(String what) throws TsdbException {
        int st = frame();
        String s = null;
        if (st == 0) {
            int len = bodyLen >= 2 ? in.getShort() & 0xffff : -1;
            if (bodyLen != 2 + len)
                throw lost(Status.ERR_CORRUPT, what + " reply of " + bodyLen + " bytes");
            byte[] b = new byte[len];
            in.get(b);
            s = new String(b, StandardCharsets.UTF_8);
        }
        endReply();
        check(st);
        return s;
    }

    /**
     * Inserts values of any type, with nulls, into one series and waits until they are durable and visible. The
     * column's type must be the series' type.
     *
     * @param series the series id
     * @param timestamps ms since the epoch
     * @param values the values; as long as {@code timestamps}
     * @throws TsdbException with {@link Status#ERR_INVALID} for a type mismatch; the first error among this insert
     *     and earlier pipelined ones
     * @see #insert(int, long[], float[])
     */
    public void insertSeriesValues(int series, long[] timestamps, Column values) throws TsdbException {
        insertSeriesValues(series, timestamps, values, 0, sameLength(timestamps, values));
    }

    /**
     * Inserts {@code length} rows from position {@code offset} into one series, and waits.
     *
     * @param series the series id
     * @param timestamps ms since the epoch
     * @param values the values
     * @param offset the first row to send
     * @param length the number of rows
     * @throws TsdbException the first error among this insert and earlier pipelined ones
     */
    public void insertSeriesValues(int series, long[] timestamps, Column values, int offset, int length)
            throws TsdbException {
        insertValuesFrames(series, null, timestamps, values, offset, length);
        await();
    }

    /**
     * Inserts values of several series, one series id per row, and waits. Every series must have the column's
     * type.
     *
     * @param series the series id of each row
     * @param timestamps ms since the epoch
     * @param values the values
     * @throws TsdbException with {@link Status#ERR_INVALID} for a type mismatch; the first error among this insert
     *     and earlier pipelined ones
     */
    public void insertValues(int[] series, long[] timestamps, Column values) throws TsdbException {
        insertValues(series, timestamps, values, 0, sameLength(series, timestamps, values));
    }

    /**
     * Inserts {@code length} rows from position {@code offset}, one series id per row, and waits.
     *
     * @param series the series id of each row
     * @param timestamps ms since the epoch
     * @param values the values
     * @param offset the first row to send
     * @param length the number of rows
     * @throws TsdbException the first error among this insert and earlier pipelined ones
     */
    public void insertValues(int[] series, long[] timestamps, Column values, int offset, int length)
            throws TsdbException {
        insertValuesFrames(0, Objects.requireNonNull(series, "series"), timestamps, values, offset, length);
        await();
    }

    /**
     * Sends an insert of typed values into one series without waiting; see
     * {@link #insertAsync(int, long[], float[])}. The arrays may be reused as soon as this returns.
     *
     * @param series the series id
     * @param timestamps ms since the epoch
     * @param values the values
     * @throws TsdbException on a connection failure
     */
    public void insertSeriesValuesAsync(int series, long[] timestamps, Column values) throws TsdbException {
        insertSeriesValuesAsync(series, timestamps, values, 0, sameLength(timestamps, values));
    }

    /**
     * Sends {@code length} typed rows from position {@code offset} into one series without waiting.
     *
     * @param series the series id
     * @param timestamps ms since the epoch
     * @param values the values
     * @param offset the first row to send
     * @param length the number of rows
     * @throws TsdbException on a connection failure
     */
    public void insertSeriesValuesAsync(int series, long[] timestamps, Column values, int offset, int length)
            throws TsdbException {
        insertValuesFrames(series, null, timestamps, values, offset, length);
    }

    /**
     * Sends an insert of typed values of several series, one series id per row, without waiting.
     *
     * @param series the series id of each row
     * @param timestamps ms since the epoch
     * @param values the values
     * @throws TsdbException on a connection failure
     */
    public void insertValuesAsync(int[] series, long[] timestamps, Column values) throws TsdbException {
        insertValuesAsync(series, timestamps, values, 0, sameLength(series, timestamps, values));
    }

    /**
     * Sends {@code length} typed rows from position {@code offset}, one series id per row, without waiting.
     *
     * @param series the series id of each row
     * @param timestamps ms since the epoch
     * @param values the values
     * @param offset the first row to send
     * @param length the number of rows
     * @throws TsdbException on a connection failure
     */
    public void insertValuesAsync(int[] series, long[] timestamps, Column values, int offset, int length)
            throws TsdbException {
        insertValuesFrames(0, Objects.requireNonNull(series, "series"), timestamps, values, offset, length);
    }

    private void insertValuesFrames(int series, int[] many, long[] ts, Column col, int off, int n)
            throws TsdbException {
        Objects.requireNonNull(ts, "timestamps");
        Objects.requireNonNull(col, "values");
        Objects.checkFromIndexSize(off, n, ts.length);
        Objects.checkFromIndexSize(off, n, col.size());
        if (many != null)
            Objects.checkFromIndexSize(off, n, many.length);
        else if (series == Protocol.MANY_SERIES)
            throw new IllegalArgumentException("series id 0xffffffff is reserved");
        ready();
        if (n > 0 && packing()) {
            for (int sent = 0; sent < n;) {
                int m = Math.min(n - sent, INSERT_FRAME_ROWS);
                while (pending >= MAX_IN_FLIGHT)
                    collectOne();
                sendPacked(series, many, ts, col, off + sent, m);
                pending++;
                sent += m;
            }
            return;
        }
        ValueType type = col.type();
        int sent = 0;
        do { /* one frame even for zero rows, as the C client */
            int m = Math.min(n - sent, INSERT_FRAME_ROWS);
            byte[] valid = Values.validity(col, off + sent, m);
            long end = Protocol.VALUES_HEADER + (long) m * (Long.BYTES + type.width)
                    + (valid != null ? valid.length : 0);
            int pad = many != null ? (int) (((end + 3) & ~3L) - end) : 0;
            while (pending >= MAX_IN_FLIGHT)
                collectOne();
            begin(Protocol.OP_INSERT_VALUES, end + pad + (many != null ? (long) m * Integer.BYTES : 0));
            out.putInt(m).putInt(many != null ? Protocol.MANY_SERIES : series);
            out.put((byte) type.code).put((byte) (valid != null ? Protocol.VALID : 0)).putShort((short) 0).putInt(0);
            putLongs(ts, off + sent, m);
            putColumn(col, off + sent, m);
            if (valid != null)
                putBytes(valid);
            if (many != null) {
                if (out.remaining() < pad)
                    send();
                for (int i = 0; i < pad; i++)
                    out.put((byte) 0);
                putInts(many, off + sent, m);
            }
            send();
            pending++;
            sent += m;
        } while (sent < n);
    }

    /**
     * Whether inserts go packed: compression on, zstd available, and a server that reads them (HELLO, asked once).
     * Outstanding acknowledgements are collected first; their errors stay for {@link #await()}.
     */
    private boolean packing() throws TsdbException {
        if (zlevel == 0 || noZstd)
            return false;
        if (serverVersion == 0) {
            while (pending > 0)
                collectOne();
            begin(Protocol.OP_HELLO, 4);
            out.putInt(Protocol.VERSION);
            send();
            int st = frame();
            int v = st == 0 && bodyLen == 4 ? in.getInt() : 1; /* as the C client: anything else is version 1 */
            endReply();
            serverVersion = v;
        }
        if (serverVersion < 3)
            return false;
        if (zstd == null && (zstd = Compressor.create()) == null) {
            noZstd = true;
            return false;
        }
        return true;
    }

    /** One packed frame of rows {@code [off, off + m)}: reshape, compress, send. */
    private void sendPacked(int series, int[] many, long[] ts, Column col, int off, int m) throws TsdbException {
        byte[] valid = Values.validity(col, off, m);
        long bound = Pack.rawBound(m, col.type(), valid != null, many != null);
        if (raw.length < bound)
            raw = new byte[(int) bound];
        int len = Pack.reshape(ts, col, many, off, m, valid, raw);
        int z;
        try {
            z = zstd.compress(raw, len, zlevel);
        } catch (RuntimeException e) { /* ZstdException: only when out of memory */
            throw new TsdbException(Status.ERR_NOMEM, "zstd", e);
        }
        begin(Protocol.OP_INSERT_VALUES, Protocol.VALUES_HEADER + 4 + (long) z);
        out.putInt(m).putInt(many != null ? Protocol.MANY_SERIES : series);
        out.put((byte) col.type().code).put((byte) (Protocol.PACKED | (valid != null ? Protocol.VALID : 0)));
        out.putShort((short) 0).putInt(0).putInt(len);
        putBytes(zstd.out(), z);
        send();
    }

    /**
     * Returns the points of a series of any type in {@code [start, end)}, oldest first, with nulls.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @return the points
     * @throws TsdbException if the query fails, or {@link Status#ERR_NOMEM} if the result does not fit in one array
     */
    public ValuePoints queryValues(int series, long start, long end) throws TsdbException {
        return queryValues(series, start, end, Order.ASC, 0);
    }

    /**
     * Returns the points of a series of any type in {@code [start, end)}, in the given order, at most {@code limit}
     * of them, with nulls. An unknown series gives no points (as a float32 column).
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param order oldest or newest first
     * @param limit the most points to return; 0 for all
     * @return the points
     * @throws TsdbException if the query fails, or {@link Status#ERR_NOMEM} if the result does not fit in one array
     */
    public ValuePoints queryValues(int series, long start, long end, Order order, long limit) throws TsdbException {
        ValueCollector c = new ValueCollector();
        values(series, start, end, order, limit, c);
        if (c.overflow)
            throw new TsdbException(Status.ERR_NOMEM, "more points than one array holds; stream them with queryValues",
                    null);
        if (c.mixed)
            throw new TsdbException(Status.ERR_CORRUPT, "chunks of different value types", null);
        return c.result();
    }

    /**
     * Streams the points of a series of any type in {@code [start, end)}, oldest first, chunk by chunk.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param consumer receives the chunks
     * @return {@code true} if every point was delivered, {@code false} if the consumer stopped early
     * @throws TsdbException if the query fails
     */
    public boolean queryValues(int series, long start, long end, ValuesConsumer consumer) throws TsdbException {
        return queryValues(series, start, end, Order.ASC, 0, consumer);
    }

    /**
     * Streams the points of a series of any type in {@code [start, end)}, in the given order, at most {@code limit}
     * of them, in chunks of up to 16384 points. As with {@link #queryRaw(int, long, long, Order, long, RawConsumer)},
     * stopping early still reads the rest of the stream.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param order oldest or newest first
     * @param limit the most points to return; 0 for all
     * @param consumer receives the chunks
     * @return {@code true} if every point was delivered, {@code false} if the consumer stopped early
     * @throws TsdbException if the query fails
     */
    public boolean queryValues(int series, long start, long end, Order order, long limit, ValuesConsumer consumer)
            throws TsdbException {
        Objects.requireNonNull(consumer, "consumer");
        return values(series, start, end, order, limit, (b, type, n, valid) -> {
            if (n == 0)
                return true;
            if (chunkTs.length < n)
                chunkTs = new long[n];
            if (chunkColumn == null || chunkColumn.type() != type || chunkColumn.capacity() < n)
                chunkColumn = Column.allocate(type, Math.max(n, chunkTs.length));
            getLongs(b, chunkTs, 0, n);
            Values.get(b, chunkColumn, 0, n);
            chunkNulls.clear();
            chunkColumn.nulls = chunkNulls;
            if (valid)
                Values.getValidity(b, chunkColumn, 0, n);
            if (chunkNulls.isEmpty())
                chunkColumn.nulls = null;
            chunkColumn.size = n;
            return consumer.accept(chunkTs, chunkColumn, n);
        });
    }

    /** Takes one chunk of a QUERY_VALUES reply: n timestamps at the buffer's position, n values, the bitmap. */
    private interface ValueSink {
        boolean chunk(ByteBuffer b, ValueType type, int n, boolean valid);
    }

    /** Collects a QUERY_VALUES reply into growing arrays. */
    private static final class ValueCollector implements ValueSink {
        long[] ts = new long[0];
        Column col;
        int n;
        boolean overflow, mixed;

        @Override
        public boolean chunk(ByteBuffer b, ValueType type, int k, boolean valid) {
            if (col == null) {
                col = Column.allocate(type, 0);
            } else if (col.type() != type) {
                mixed = true;
                return false;
            }
            if (k > ts.length - n) {
                if ((long) n + k > Integer.MAX_VALUE - 8) {
                    overflow = true;
                    return false;
                }
                int cap = (int) Math.min(Math.max((long) ts.length * 2, (long) n + k), Integer.MAX_VALUE - 8);
                ts = Arrays.copyOf(ts, cap);
                col.grow(cap);
            }
            getLongs(b, ts, n, k);
            Values.get(b, col, n, k);
            if (valid)
                Values.getValidity(b, col, n, k);
            n += k;
            col.size = n;
            return true;
        }

        ValuePoints result() {
            if (col == null)
                col = Column.allocate(ValueType.FLOAT32, 0);
            if (col.capacity() != n)
                col.grow(n);
            return new ValuePoints(n == ts.length ? ts : Arrays.copyOf(ts, n), col);
        }
    }

    private boolean values(int series, long start, long end, Order order, long limit, ValueSink sink)
            throws TsdbException {
        Objects.requireNonNull(order, "order");
        if (limit < 0)
            throw new IllegalArgumentException("negative limit");
        request();
        boolean opts = order != Order.ASC || limit != 0;
        begin(Protocol.OP_QUERY_VALUES, opts ? 29 : 20);
        out.putInt(series).putLong(start).putLong(end);
        if (opts)
            out.put((byte) (order == Order.DESC ? 1 : 0)).putLong(limit);
        send();
        boolean stopped = false;
        RuntimeException thrown = null;
        for (;;) {
            int st = frame();
            if (bodyLen == 0 && st != 0) { /* not produced by tsdbd today, but cheap to accept */
                endReply();
                throw new TsdbException(st);
            }
            int p = in.position();
            ValueType type = bodyLen >= 8 ? ValueType.of(in.get(p + 1)) : null;
            int n = bodyLen >= 8 ? in.getInt(p + 4) : -1;
            boolean valid = bodyLen >= 8 && (in.get(p + 2) & Protocol.VALID) != 0;
            if (type == null || n < 0
                    || bodyLen != 8 + (long) n * (Long.BYTES + type.width) + (valid ? (n + 7) / 8 : 0))
                throw lost(Status.ERR_CORRUPT, "QUERY_VALUES chunk of " + bodyLen + " bytes");
            boolean last = in.get(p) != 0;
            in.position(p + 8);
            if (!stopped) {
                try {
                    stopped = !sink.chunk(in, type, n, valid);
                } catch (RuntimeException e) {
                    thrown = e;
                    stopped = true;
                } catch (Error e) {
                    lost(Status.ERR_IO, "consumer failed");
                    throw e;
                }
            }
            if (!last) {
                in.position(bodyEnd);
                continue;
            }
            endReply();
            if (thrown != null)
                throw thrown;
            check(st);
            return !stopped;
        }
    }

    // ------------------------------------------------------------- maintenance

    /**
     * Makes everything written so far durable, without creating parts.
     *
     * @throws TsdbException if the server fails
     */
    public void commit() throws TsdbException {
        simple(Protocol.OP_COMMIT);
    }

    /**
     * Commits, then turns the server's memtable into parts.
     *
     * @throws TsdbException if the server fails
     */
    public void flush() throws TsdbException {
        simple(Protocol.OP_FLUSH);
    }

    /**
     * Merges every partition down to one part. This can take a long time on a large database.
     *
     * @throws TsdbException if the server fails
     */
    public void optimize() throws TsdbException {
        simple(Protocol.OP_OPTIMIZE);
    }

    /**
     * Returns the storage statistics of the server's database.
     *
     * @return the statistics
     * @throws TsdbException on a connection failure
     */
    public Stats stats() throws TsdbException {
        request();
        begin(Protocol.OP_STATS, 0);
        send();
        int st = frame();
        if (st == 0 && bodyLen != 64)
            throw lost(Status.ERR_CORRUPT, "STATS reply of " + bodyLen + " bytes");
        Stats s = st == 0
                ? new Stats(in.getLong(), in.getLong(), in.getLong(), in.getLong(), in.getLong(), in.getLong(),
                        in.getLong(), in.getLong())
                : null;
        endReply();
        check(st);
        return s;
    }

    /**
     * Returns what the server did for this connection's previous query: {@code queryPoints}, {@code queryRaw},
     * {@code queryValues}, {@code queryAgg} or {@code queryGroup}. Queries on other connections do not change it;
     * before the first query every field is 0.
     *
     * @return the statistics of the previous query
     * @throws TsdbException on a connection failure
     */
    public QueryStats lastQueryStats() throws TsdbException {
        request();
        begin(Protocol.OP_LAST_STATS, 0);
        send();
        int st = frame();
        if (st == 0 && bodyLen != Protocol.STATS_REPLY)
            throw lost(Status.ERR_CORRUPT, "LAST_STATS reply of " + bodyLen + " bytes");
        QueryStats q = st == 0
                ? new QueryStats(in.getDouble(), in.getLong(), in.getLong(), in.getLong(), in.getLong(), in.getLong(),
                        in.getLong(), in.getLong(), in.getLong(), in.getLong(), in.getLong())
                : null;
        endReply();
        check(st);
        return q;
    }

    private void simple(byte op) throws TsdbException {
        request();
        begin(op, 0);
        send();
        int st = frame();
        endReply();
        check(st);
    }

    // --------------------------------------------------------------------- I/O

    private static ByteBuffer direct(int n) {
        return ByteBuffer.allocateDirect(n).order(ByteOrder.LITTLE_ENDIAN);
    }

    private static void check(int status) throws TsdbException {
        if (status != 0)
            throw new TsdbException(status);
    }

    private static void closeQuietly(SocketChannel ch) {
        if (ch != null) {
            try {
                ch.close();
            } catch (IOException e) {
                /* nothing left to do with it */
            }
        }
    }

    private void ready() throws TsdbException {
        if (closed)
            throw new IllegalStateException("client is closed");
        if (broken)
            throw new TsdbException(Status.ERR_IO, "connection lost by an earlier error", null);
    }

    /** Before a request that reads its reply at once: collects pipelined acknowledgements first. */
    private void request() throws TsdbException {
        await();
    }

    /** The connection is out of step with the server (or gone): close it for good. */
    private TsdbException lost(int status, String detail) {
        return lost(status, detail, null);
    }

    private TsdbException lost(int status, String detail, Throwable cause) {
        broken = true;
        closeQuietly(ch);
        return new TsdbException(status, detail, cause);
    }

    /** Starts a request frame in the (empty) send buffer. */
    private void begin(byte op, long payload) {
        if (payload > Protocol.MAX_FRAME - 5)
            throw new IllegalArgumentException("request of " + payload + " bytes exceeds the frame limit");
        out.putInt((int) (5 + payload)).putInt(nextId++).put(op);
    }

    /** Writes the send buffer out, completely. */
    private void send() throws TsdbException {
        out.flip();
        try {
            while (out.hasRemaining())
                bytesSent += ch.write(out);
        } catch (IOException e) {
            throw lost(Status.ERR_IO, "send failed", e);
        } finally {
            out.clear();
        }
    }

    private void putLongs(long[] a, int off, int n) throws TsdbException {
        while (n > 0) {
            if (out.remaining() < Long.BYTES)
                send();
            int k = Math.min(n, out.remaining() / Long.BYTES);
            out.asLongBuffer().put(a, off, k);
            out.position(out.position() + k * Long.BYTES);
            off += k;
            n -= k;
        }
    }

    private void putInts(int[] a, int off, int n) throws TsdbException {
        while (n > 0) {
            if (out.remaining() < Integer.BYTES)
                send();
            int k = Math.min(n, out.remaining() / Integer.BYTES);
            out.asIntBuffer().put(a, off, k);
            out.position(out.position() + k * Integer.BYTES);
            off += k;
            n -= k;
        }
    }

    private void putValues(float[] a, int off, int n) throws TsdbException {
        while (n > 0) {
            if (out.remaining() < Values.BYTES)
                send();
            int k = Math.min(n, out.remaining() / Values.BYTES);
            Values.put(out, a, off, k);
            off += k;
            n -= k;
        }
    }

    private void putColumn(Column c, int off, int n) throws TsdbException {
        int w = c.type().width;
        while (n > 0) {
            if (out.remaining() < w)
                send();
            int k = Math.min(n, out.remaining() / w);
            Values.put(out, c, off, k);
            off += k;
            n -= k;
        }
    }

    private void putBytes(byte[] a) throws TsdbException {
        putBytes(a, a.length);
    }

    private void putBytes(byte[] a, int len) throws TsdbException {
        for (int off = 0; off < len;) {
            if (!out.hasRemaining())
                send();
            int k = Math.min(len - off, out.remaining());
            out.put(a, off, k);
            off += k;
        }
    }

    private static void getLongs(ByteBuffer b, long[] a, int off, int n) {
        b.asLongBuffer().get(a, off, n);
        b.position(b.position() + n * Long.BYTES);
    }

    /** Makes at least {@code n} received bytes available in {@code in}, growing it for large frames. */
    private void fill(int n) throws TsdbException {
        if (in.remaining() >= n)
            return;
        if (in.capacity() < n) {
            ByteBuffer b = direct(Math.max(n, Math.min(in.capacity() * 2, Protocol.MAX_FRAME)));
            b.put(in);
            in = b;
        } else {
            in.compact();
        }
        while (in.position() < n) {
            int r;
            try {
                r = ch.read(in);
            } catch (IOException e) {
                throw lost(Status.ERR_IO, "receive failed", e);
            }
            if (r < 0)
                throw lost(Status.ERR_IO, "connection closed by the server");
        }
        in.flip();
    }

    /**
     * Reads the header of the next reply frame, which must answer request {@code ackId}, and makes its whole body
     * available in {@code in}.
     *
     * @return the status
     */
    private int frame() throws TsdbException {
        fill(Protocol.REPLY_HEADER);
        int len = in.getInt(), id = in.getInt();
        byte op = in.get();
        int status = in.getInt();
        if (len < 9 || len > Protocol.MAX_FRAME || op != Protocol.OP_REPLY)
            throw lost(Status.ERR_CORRUPT, "malformed reply frame");
        if (id != ackId)
            throw lost(Status.ERR_CORRUPT, "reply to request " + Integer.toUnsignedString(id) + ", expected "
                    + Integer.toUnsignedString(ackId));
        bodyLen = len - 9;
        fill(bodyLen);
        bodyEnd = in.position() + bodyLen;
        return status;
    }

    /** Finishes the last reply frame of a request. */
    private void endReply() {
        in.position(bodyEnd);
        ackId++;
        if (in.capacity() > BUF && !in.hasRemaining())
            in = direct(BUF).flip(); /* give a large reply's buffer back */
    }

    private static byte[] utf8(String s, String what) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        if (b.length > MAX_STRING)
            throw new IllegalArgumentException(what + " longer than 65535 bytes of UTF-8");
        return b;
    }

    private static int sameLength(long[] ts, float[] values) {
        if (ts.length != values.length)
            throw new IllegalArgumentException("timestamps and values differ in length");
        return ts.length;
    }

    private static int sameLength(long[] ts, Column values) {
        if (ts.length != values.size())
            throw new IllegalArgumentException("timestamps and values differ in length");
        return ts.length;
    }

    private static int sameLength(int[] series, long[] ts, Column values) {
        if (series.length != ts.length)
            throw new IllegalArgumentException("series and timestamps differ in length");
        return sameLength(ts, values);
    }

    private static int sameLength(int[] series, long[] ts, float[] values) {
        if (series.length != ts.length)
            throw new IllegalArgumentException("series and timestamps differ in length");
        return sameLength(ts, values);
    }
}
