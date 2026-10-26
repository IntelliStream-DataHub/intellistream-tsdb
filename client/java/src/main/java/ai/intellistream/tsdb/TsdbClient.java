package ai.intellistream.tsdb;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.StandardSocketOptions;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import jdk.net.ExtendedSocketOptions;

/**
 * A connection to {@code tsdbd}.
 *
 * <pre>{@code
 * try (TsdbClient c = TsdbClient.connect("127.0.0.1", 7070)) {
 *     int id = c.series("pump7.flow");                              // get or create
 *     c.insert(id, timestamps, values);                             // long[] ms, float[] values
 *     Points newest = c.queryPoints(id, Long.MIN_VALUE, Long.MAX_VALUE, Order.DESC, 10);
 *     List<Bucket> perMinute = c.queryAgg(id, start, end, 60_000);
 *     List<Bucket> perDay = c.queryGroup(id, start, end, Group.of(Unit.DAY).in("Europe/Oslo"));
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
 * <p><b>Time zones.</b> The server groups by UTC only (protocol version 4). For local days, weeks, months, quarters
 * and years, {@link #queryGroup} computes the UTC instant each local period begins, with the JDK's time zone data,
 * and asks for buckets that begin there ({@link #queryBounds}); a server older than version 4 gets the zone's name
 * instead and groups by itself.
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
    private static final int MAX_BOUNDS = (Protocol.MAX_FRAME - 64) / Long.BYTES; /* QUERY_BOUNDS, as the C client */
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
    /** TCP keepalive of a new connection: first probe after 30 s of silence, then every 10 s, 3 probes. */
    private static final Duration KEEPALIVE_IDLE = Duration.ofSeconds(30);
    private static final Duration KEEPALIVE_INTERVAL = Duration.ofSeconds(10);
    private static final int KEEPALIVE_COUNT = 3;

    private final SocketChannel ch;
    private final Selector selector;             /* waits for the socket with a timeout; the channel is non-blocking */
    private final SelectionKey key;
    private long timeoutNanos;                   /* of each wait for the server; 0: forever */
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

    private TsdbClient(SocketChannel ch, Duration requestTimeout) throws IOException {
        this.ch = ch;
        this.selector = Selector.open();
        try {
            ch.configureBlocking(false);
            this.key = ch.register(selector, 0);
        } catch (IOException | RuntimeException e) {
            closeQuietly(ch);
            closeQuietly(selector);
            throw e;
        }
        this.timeoutNanos = nanos(requestTimeout);
    }

    /** A connect timeout in whole milliseconds, at least 1 when positive (0 would mean "the operating system's"). */
    private static int millis(Duration d) {
        Objects.requireNonNull(d, "connectTimeout");
        if (d.isNegative())
            throw new IllegalArgumentException("negative timeout " + d);
        if (d.isZero())
            return 0;
        try {
            return (int) Math.max(1, Math.min(Integer.MAX_VALUE, (d.toNanos() + 999_999) / 1_000_000));
        } catch (ArithmeticException e) {
            return Integer.MAX_VALUE;
        }
    }

    private static long nanos(Duration d) {
        Objects.requireNonNull(d, "timeout");
        if (d.isNegative())
            throw new IllegalArgumentException("negative timeout " + d);
        try {
            return d.toNanos();
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE / 2; /* centuries: effectively forever, without overflowing the deadline */
        }
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
     * Connects to a server, trying every address of {@code host}. Requests wait for the server as long as it takes;
     * see {@link #connect(String, int, Duration, Duration)} to bound them.
     *
     * @param host a host name or address
     * @param port the server's port
     * @param timeout how long to wait for each address; zero waits as long as the operating system does
     * @return the connected client
     * @throws TsdbException with {@link Status#ERR_IO} if no address accepts the connection
     */
    public static TsdbClient connect(String host, int port, Duration timeout) throws TsdbException {
        return connect(host, port, timeout, Duration.ZERO);
    }

    /**
     * Connects to a server, trying every address of {@code host}, and bounds every later wait for the server.
     *
     * <p>A server that stalls (a hung disk, a dead peer that never sent a reset) would otherwise block the caller
     * for good. With a request timeout, any wait of {@code requestTimeout} on the socket, sending or receiving,
     * fails the call with a {@link TsdbException} of {@link Status#ERR_IO} ({@link TsdbException#isTimeout()} is
     * {@code true}) and closes the connection: the reply may still arrive, so the stream cannot be trusted any more.
     * Later calls fail with {@link Status#ERR_IO} at once; connect again. The bound applies to each wait, not to a
     * whole call: a large reply that keeps arriving is not cut off.
     *
     * <p>TCP keepalive is on for the connection (first probe after 30 s of silence, then every 10 s, dropped after
     * 3 unanswered probes, where the platform allows); see {@link #setKeepAlive(Duration, Duration, int)}.
     *
     * @param host a host name or address
     * @param port the server's port
     * @param connectTimeout how long to wait for each address, rounded up to whole milliseconds; zero waits as long as
     *        the operating system does
     * @param requestTimeout how long any one wait for the server may take; zero, the default of the other
     *        {@code connect} methods, waits forever
     * @return the connected client
     * @throws TsdbException with {@link Status#ERR_IO} if no address accepts the connection
     * @throws IllegalArgumentException if a timeout is negative
     */
    public static TsdbClient connect(String host, int port, Duration connectTimeout, Duration requestTimeout)
            throws TsdbException {
        Objects.requireNonNull(host, "host");
        nanos(requestTimeout);
        int ms = millis(connectTimeout);
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
                /* before the client exists: its selector is opened last, and only a built client owns it */
                setKeepAlive(ch, KEEPALIVE_IDLE, KEEPALIVE_INTERVAL, KEEPALIVE_COUNT);
                return new TsdbClient(ch, requestTimeout);
            } catch (IOException | RuntimeException e) {
                closeQuietly(ch);
                if (e instanceof RuntimeException re)
                    throw re;
                last = (IOException) e;
            }
        }
        throw new TsdbException(Status.ERR_IO, "cannot connect to " + host + ":" + port, last);
    }

    /**
     * Changes how long any one wait for the server may take; see {@link #connect(String, int, Duration, Duration)}.
     * Takes effect for the next wait. Like the rest of the client it is not thread safe.
     *
     * @param timeout the bound; zero waits forever
     * @throws IllegalArgumentException if {@code timeout} is negative
     */
    public void setRequestTimeout(Duration timeout) {
        timeoutNanos = nanos(timeout);
    }

    /**
     * The bound on each wait for the server.
     *
     * @return the request timeout; zero waits forever (the default)
     */
    public Duration requestTimeout() {
        return Duration.ofNanos(timeoutNanos);
    }

    /**
     * Sets the TCP keepalive probes of the connection, which notice a peer that vanished without a reset (power
     * loss, a cut cable, a NAT that forgot the connection) while the connection is idle. They do not bound a
     * request: {@link #setRequestTimeout} does. New connections use 30 s, 10 s and 3 probes.
     *
     * <p>Whether the idle time, interval and count can be set depends on the platform (Linux, macOS and recent
     * Windows can); where one cannot, the system default stays.
     *
     * @param idle silence before the first probe, rounded up to whole seconds
     * @param interval time between probes, rounded up to whole seconds
     * @param count unanswered probes before the connection is dropped
     * @throws IOException if the socket refuses keepalive altogether
     */
    public void setKeepAlive(Duration idle, Duration interval, int count) throws IOException {
        setKeepAlive(ch, idle, interval, count);
    }

    private static void setKeepAlive(SocketChannel ch, Duration idle, Duration interval, int count)
            throws IOException {
        ch.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        try {
            setExtended(ch, ExtendedSocketOptions.TCP_KEEPIDLE, seconds(idle));
            setExtended(ch, ExtendedSocketOptions.TCP_KEEPINTERVAL, seconds(interval));
            setExtended(ch, ExtendedSocketOptions.TCP_KEEPCOUNT, Math.max(1, count));
        } catch (LinkageError e) {
            /* a runtime image without the jdk.net module: the system defaults stay */
        }
    }

    /**
     * Turns TCP keepalive off, or back on with the settings of a new connection.
     *
     * @param on whether the connection sends keepalive probes
     * @throws IOException if the socket refuses the option
     */
    public void setKeepAlive(boolean on) throws IOException {
        if (on)
            setKeepAlive(KEEPALIVE_IDLE, KEEPALIVE_INTERVAL, KEEPALIVE_COUNT);
        else
            ch.setOption(StandardSocketOptions.SO_KEEPALIVE, false);
    }

    /** A socket option of the connection, or {@code null} where the platform has none (for the tests). */
    <T> T socketOption(java.net.SocketOption<T> opt) throws IOException {
        return ch.supportedOptions().contains(opt) ? ch.getOption(opt) : null;
    }

    private static int seconds(Duration d) {
        long s = d.getSeconds() + (d.getNano() > 0 ? 1 : 0);
        return (int) Math.max(1, Math.min(Integer.MAX_VALUE, s));
    }

    /** A refinement of keepalive: a platform without the option keeps its default rather than failing the connect. */
    private static void setExtended(SocketChannel ch, java.net.SocketOption<Integer> opt, int value) {
        try {
            if (ch.supportedOptions().contains(opt))
                ch.setOption(opt, value);
        } catch (IOException | UnsupportedOperationException | IllegalArgumentException e) {
            /* keeps the system default */
        }
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
        closeQuietly(selector);
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
     * @return the server's protocol version (4 for the server this client is written for)
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
     * series is created. The volume must be one the operator added ({@code tsdb <dir> volume add}): tsdbd never adds
     * one, and answers {@code ERR_NOTFOUND} for any other path.
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
        return buckets(series, start, end, intervalMs, null, "", Order.ASC, 0);
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
        return buckets(series, start, end, intervalMs, null, "", order, limit);
    }

    /**
     * Aggregates a series over {@code [start, end)} in calendar buckets, oldest first.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param group the calendar grouping, or {@code null} for one bucket over the whole range
     * @return the buckets with rows
     * @throws TsdbException with {@link Status#ERR_TOO_MANY_BUCKETS} if the range holds too many buckets, and so on
     * @see #queryGroup(int, long, long, Group, Order, long)
     */
    public List<Bucket> queryGroup(int series, long start, long end, Group group) throws TsdbException {
        return queryGroup(series, start, end, group, Order.ASC, 0);
    }

    /**
     * Aggregates a series over {@code [start, end)} in calendar buckets, in the given order, at most {@code limit}
     * of them. With {@link Order#DESC} and a limit, this is "the last N days" (or months, ...) that have data.
     *
     * <p>The server groups in UTC only. For another time zone the client asks the series' first and last row in the
     * range, computes the UTC instants at which the local periods between them begin ({@link Group#bounds}), and
     * asks for buckets that begin there ({@link #queryBounds}): three requests instead of one. A server older than
     * protocol version 4 still groups by the zone itself, given by its name.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param group the calendar grouping, or {@code null} for one bucket over the whole range
     * @param order oldest or newest bucket first
     * @param limit the most buckets to return; 0 for all
     * @return the buckets with rows
     * @throws TsdbException with {@link Status#ERR_TOO_MANY_BUCKETS} if the range holds too many buckets, and so on
     * @throws IllegalArgumentException for a negative limit
     */
    public List<Bucket> queryGroup(int series, long start, long end, Group group, Order order, long limit)
            throws TsdbException {
        Objects.requireNonNull(order, "order");
        if (limit < 0)
            throw new IllegalArgumentException("negative limit");
        request();
        if (group == null || group.utc())
            return buckets(series, start, end, 0, group, "", order, limit);
        if (serverVersion() < 4) /* an older server still takes the zone itself */
            return buckets(series, start, end, 0, group, group.zone().normalized().getId(), order, limit);
        long[] bounds = localBounds(series, start, end, group, false);
        return bounds == null ? Collections.emptyList() : queryBounds(series, start, end, bounds, order, limit);
    }

    /**
     * The UTC instants at which the local periods of {@code group} begin, for a query over {@code [start, end)};
     * {@code null} if there is nothing to ask for. The aggregates cut an open range to the first and last row there
     * is; the time-weighted averages ({@code bounded}) cover a bounded side to its end, since a reading before the
     * range carries into it, and cut only an open side.
     */
    private long[] localBounds(int series, long start, long end, Group group, boolean bounded)
            throws TsdbException {
        long[] edge = {start, end == Long.MAX_VALUE ? Long.MIN_VALUE : end - 1};
        boolean[] found = {true, true};
        for (int i = 0; i < 2; i++) {
            if (bounded && (i == 0 ? start != Long.MIN_VALUE : end != Long.MAX_VALUE))
                continue;
            int j = i;
            found[j] = false;
            values(series, start, end, i == 0 ? Order.ASC : Order.DESC, 1, (b, type, n, valid) -> {
                if (n > 0) {
                    edge[j] = b.getLong(b.position());
                    found[j] = true;
                }
                return true;
            });
        }
        if (!found[0] || !found[1] || edge[0] > edge[1])
            return null;
        /* the newest row may sit at Long.MAX_VALUE: then the range is beyond what bounds cover anyway */
        long[] bounds = group.bounds(edge[0], edge[1] == Long.MAX_VALUE ? Long.MAX_VALUE : edge[1] + 1);
        if (bounds.length > MAX_BOUNDS)
            throw new TsdbException(Status.ERR_TOO_MANY_BUCKETS, bounds.length + " bounds exceed one request", null);
        return bounds;
    }

    /**
     * Aggregates a series over {@code [start, end)} in buckets that begin at the given UTC instants, oldest first:
     * bucket {@code i} holds the rows in {@code [bounds[i], bounds[i + 1])}, the last one every row from
     * {@code bounds[n - 1]} on. Rows before {@code bounds[0]} belong to no bucket. Only buckets with rows are
     * returned, each with its bound as start. Needs a server of protocol version 4.
     *
     * <p>This is how calendar periods in a time zone reach the server: {@link Group#bounds} computes them.
     *
     * <pre>{@code
     * long[] days = Group.of(Unit.DAY).in("America/St_Johns").bounds(start, end);
     * List<Bucket> perDay = client.queryBounds(id, start, end, days);
     * }</pre>
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param bounds the bucket starts in ms since the epoch, strictly increasing; at least one
     * @return the buckets with rows
     * @throws TsdbException with {@link Status#ERR_INVALID} if the bounds do not strictly increase, or the server is
     *     older than version 4
     * @throws IllegalArgumentException if there are no bounds, or more than one request can carry (about 33 million)
     */
    public List<Bucket> queryBounds(int series, long start, long end, long[] bounds) throws TsdbException {
        return queryBounds(series, start, end, bounds, Order.ASC, 0);
    }

    /**
     * Aggregates a series over {@code [start, end)} in buckets that begin at the given UTC instants, in the given
     * order, at most {@code limit} of them.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param bounds the bucket starts in ms since the epoch, strictly increasing; at least one
     * @param order oldest or newest bucket first
     * @param limit the most buckets to return; 0 for all
     * @return the buckets with rows
     * @throws TsdbException as {@link #queryBounds(int, long, long, long[])}
     * @throws IllegalArgumentException if there are no bounds or too many, or for a negative limit
     */
    public List<Bucket> queryBounds(int series, long start, long end, long[] bounds, Order order, long limit)
            throws TsdbException {
        Objects.requireNonNull(bounds, "bounds");
        Objects.requireNonNull(order, "order");
        if (limit < 0)
            throw new IllegalArgumentException("negative limit");
        if (bounds.length == 0 || bounds.length > MAX_BOUNDS)
            throw new IllegalArgumentException(bounds.length + " bounds: 1 to " + MAX_BOUNDS + " fit in a request");
        request();
        boundsSend(series, start, end, bounds, order, limit, 0);
        return bucketsReply("QUERY_BOUNDS");
    }

    /** Sends QUERY_BOUNDS, with the {@code fields} byte when it is not 0. */
    private void boundsSend(int series, long start, long end, long[] bounds, Order order, long limit, int fields)
            throws TsdbException {
        begin(Protocol.OP_QUERY_BOUNDS, 33 + (long) bounds.length * Long.BYTES + (fields != 0 ? 1 : 0));
        out.putInt(series).putLong(start).putLong(end);
        out.put((byte) (order == Order.DESC ? 1 : 0)).putLong(limit).putInt(bounds.length);
        putLongs(bounds, 0, bounds.length);
        if (fields != 0)
            out.put((byte) fields);
        send();
    }

    /** QUERY_BUCKETS: fixed intervals (group null) or calendar groups, with f64 min/max and exact integers. */
    private List<Bucket> buckets(int series, long start, long end, long interval, Group group, String zone,
                                 Order order, long limit) throws TsdbException {
        Objects.requireNonNull(order, "order");
        if (limit < 0)
            throw new IllegalArgumentException("negative limit");
        byte[] z = utf8(zone, "timezone");
        request();
        bucketsSend(series, start, end, interval, group, z, order, limit, 0);
        return bucketsReply("QUERY_BUCKETS");
    }

    /** Sends QUERY_BUCKETS, with the {@code fields} byte when it is not 0. */
    private void bucketsSend(int series, long start, long end, long interval, Group group, byte[] z, Order order,
                             long limit, int fields) throws TsdbException {
        begin(Protocol.OP_QUERY_BUCKETS, 44 + z.length + (fields != 0 ? 1 : 0));
        out.putInt(series).putLong(start).putLong(end).putLong(interval);
        out.put((byte) (group != null ? group.unit().code : 0)).putInt(group != null ? group.count() : 0);
        out.put((byte) (order == Order.DESC ? 1 : 0)).putLong(limit);
        out.putShort((short) z.length).put(z);
        if (fields != 0)
            out.put((byte) fields);
        send();
    }

    /** The reply of QUERY_BUCKETS or QUERY_BOUNDS: u32 n, n buckets of 64 bytes. */
    private List<Bucket> bucketsReply(String what) throws TsdbException {
        int st = frame();
        if (st != 0) {
            endReply();
            throw new TsdbException(st);
        }
        int n = bodyLen >= 4 ? in.getInt() : -1;
        if (n < 0 || bodyLen != 4 + (long) n * Protocol.BUCKET_ROW)
            throw lost(Status.ERR_CORRUPT, what + " reply of " + bodyLen + " bytes");
        Bucket[] b = new Bucket[n];
        for (int i = 0; i < n; i++)
            b[i] = readBucket();
        endReply();
        return Collections.unmodifiableList(Arrays.asList(b));
    }

    private Bucket readBucket() {
        return new Bucket(in.getLong(), in.getLong(), in.getDouble(), in.getDouble(), in.getDouble(), in.getLong(),
                in.getLong(), in.getLong());
    }

    /** The reply of QUERY_BUCKETS or QUERY_BOUNDS with {@link Protocol#SPREAD}: u32 n, n buckets of 104 bytes. */
    private List<SpreadBucket> spreadReply(String what) throws TsdbException {
        int st = frame();
        if (st != 0) {
            endReply();
            throw new TsdbException(st);
        }
        int n = bodyLen >= 4 ? in.getInt() : -1;
        if (n < 0 || bodyLen != 4 + (long) n * Protocol.SPREAD_ROW)
            throw lost(Status.ERR_CORRUPT, what + " reply of " + bodyLen + " bytes");
        SpreadBucket[] b = new SpreadBucket[n];
        for (int i = 0; i < n; i++)
            b[i] = new SpreadBucket(readBucket(), in.getDouble(), in.getDouble(), in.getDouble(), in.getDouble(),
                    in.getDouble());
        endReply();
        return Collections.unmodifiableList(Arrays.asList(b));
    }

    // ------------------------------------------------------------------ spread

    /**
     * Aggregates a series over {@code [start, end)} in fixed intervals, oldest first, with the spread of each bucket:
     * range, population and sample variance and standard deviation (protocol version 8).
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param intervalMs the bucket length in ms; 0 for a single bucket starting at {@code start}
     * @return the buckets with rows
     * @throws TsdbException with {@link Status#ERR_INVALID} for a negative interval or from a server older than
     *     version 8 (asked with HELLO; the request is not sent)
     */
    public List<SpreadBucket> querySpread(int series, long start, long end, long intervalMs) throws TsdbException {
        return querySpread(series, start, end, intervalMs, Order.ASC, 0);
    }

    /**
     * As {@link #querySpread(int, long, long, long)}, in the given order, at most {@code limit} buckets.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param intervalMs the bucket length in ms; 0 for a single bucket starting at {@code start}
     * @param order oldest or newest bucket first
     * @param limit the most buckets to return; 0 for all
     * @return the buckets with rows
     * @throws TsdbException as {@link #querySpread(int, long, long, long)}
     * @throws IllegalArgumentException for a negative limit
     */
    public List<SpreadBucket> querySpread(int series, long start, long end, long intervalMs, Order order, long limit)
            throws TsdbException {
        checkOpts(order, limit);
        needVersion(8, "the spread");
        bucketsSend(series, start, end, intervalMs, null, new byte[0], order, limit, Protocol.SPREAD);
        return spreadReply("QUERY_BUCKETS");
    }

    /**
     * The spread in calendar buckets, as {@link #queryGroup(int, long, long, Group, Order, long)} groups (local
     * periods become bounds here) (protocol version 8).
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param group the calendar grouping, or {@code null} for one bucket over the whole range
     * @param order oldest or newest bucket first
     * @param limit the most buckets to return; 0 for all
     * @return the buckets with rows
     * @throws TsdbException with {@link Status#ERR_INVALID} from a server older than version 8
     * @throws IllegalArgumentException for a negative limit
     */
    public List<SpreadBucket> querySpreadGroup(int series, long start, long end, Group group, Order order,
                                               long limit) throws TsdbException {
        checkOpts(order, limit);
        needVersion(8, "the spread");
        if (group == null || group.utc()) {
            bucketsSend(series, start, end, 0, group, new byte[0], order, limit, Protocol.SPREAD);
            return spreadReply("QUERY_BUCKETS");
        }
        long[] bounds = localBounds(series, start, end, group, false);
        return bounds == null ? Collections.emptyList() : querySpreadBounds(series, start, end, bounds, order, limit);
    }

    /**
     * The spread in buckets that begin at the given UTC instants, as {@link #queryBounds(int, long, long, long[],
     * Order, long)} (protocol version 8).
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param bounds the bucket starts in ms since the epoch, strictly increasing; at least one
     * @param order oldest or newest bucket first
     * @param limit the most buckets to return; 0 for all
     * @return the buckets with rows
     * @throws TsdbException with {@link Status#ERR_INVALID} if the bounds do not increase or the server is older
     *     than version 8
     * @throws IllegalArgumentException if there are no bounds or too many, or for a negative limit
     */
    public List<SpreadBucket> querySpreadBounds(int series, long start, long end, long[] bounds, Order order,
                                                long limit) throws TsdbException {
        checkBounds(bounds);
        checkOpts(order, limit);
        needVersion(8, "the spread");
        boundsSend(series, start, end, bounds, order, limit, Protocol.SPREAD);
        return spreadReply("QUERY_BOUNDS");
    }

    // ------------------------------------------------- time-weighted averages

    /**
     * Time-weighted averages of a series over {@code [start, end)} in fixed intervals (protocol version 6): each
     * reading weighted by how long it held, so a reading made once an hour counts as much as an hour of readings
     * made every second. A reading before the range carries into it.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param intervalMs the bucket length in ms; 0 for a single bucket starting at {@code start}
     * @param opts method, gap limit, order and limit
     * @return the buckets with readings or coverage
     * @throws TsdbException with {@link Status#ERR_INVALID} for a negative interval, a state series or a server
     *     older than version 6 (asked with HELLO; the request is not sent)
     */
    public List<TwaBucket> queryTwa(int series, long start, long end, long intervalMs, TwaOptions opts)
            throws TsdbException {
        Objects.requireNonNull(opts, "opts");
        if (intervalMs < 0)
            throw new TsdbException(Status.ERR_INVALID, "negative interval", null);
        return twa(series, start, end, intervalMs, 0, 0, new long[0], opts);
    }

    /**
     * Time-weighted averages in calendar buckets (protocol version 6). For a time zone other than UTC the client
     * computes the local periods' bounds: over a bounded range to its ends, over an open side to the first or last
     * row there is.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param group the calendar grouping, or {@code null} for one bucket over the whole range
     * @param opts method, gap limit, order and limit
     * @return the buckets with readings or coverage
     * @throws TsdbException with {@link Status#ERR_INVALID} for a state series or a server older than version 6
     */
    public List<TwaBucket> queryTwaGroup(int series, long start, long end, Group group, TwaOptions opts)
            throws TsdbException {
        Objects.requireNonNull(opts, "opts");
        if (group == null)
            return twa(series, start, end, 0, 0, 0, new long[0], opts);
        if (group.utc())
            return twa(series, start, end, 0, group.unit().code, group.count(), new long[0], opts);
        needVersion(6, "time-weighted averages");
        long[] bounds = localBounds(series, start, end, group, true);
        return bounds == null ? Collections.emptyList() : twa(series, start, end, 0, 0, 0, bounds, opts);
    }

    /**
     * Time-weighted averages in buckets that begin at the given UTC instants (protocol version 6).
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param bounds the bucket starts in ms since the epoch, strictly increasing; at least one
     * @param opts method, gap limit, order and limit
     * @return the buckets with readings or coverage
     * @throws TsdbException with {@link Status#ERR_INVALID} if the bounds do not increase, for a state series or a
     *     server older than version 6
     * @throws IllegalArgumentException if there are no bounds or too many
     */
    public List<TwaBucket> queryTwaBounds(int series, long start, long end, long[] bounds, TwaOptions opts)
            throws TsdbException {
        Objects.requireNonNull(opts, "opts");
        checkBounds(bounds);
        return twa(series, start, end, 0, 0, 0, bounds, opts);
    }

    /** QUERY_TWA and its reply: u32 n, n buckets of 40 bytes. */
    private List<TwaBucket> twa(int series, long start, long end, long interval, int unit, int count, long[] bounds,
                                TwaOptions o) throws TsdbException {
        needVersion(6, "time-weighted averages");
        begin(Protocol.OP_QUERY_TWA, Protocol.TWA_HEADER + (long) bounds.length * Long.BYTES);
        out.putInt(series).putLong(start).putLong(end);
        out.put((byte) (o.method() == TwaMethod.LINEAR ? 1 : 0)).put((byte) (o.order() == Order.DESC ? 1 : 0));
        out.put((byte) unit).put((byte) 0).putLong(o.maxGapMs()).putLong(o.limit()).putLong(interval);
        out.putInt(count).putInt(bounds.length);
        putLongs(bounds, 0, bounds.length);
        send();
        int st = frame();
        if (st != 0) {
            endReply();
            throw new TsdbException(st);
        }
        int n = bodyLen >= 4 ? in.getInt() : -1;
        if (n < 0 || bodyLen != 4 + (long) n * Protocol.TWA_ROW)
            throw lost(Status.ERR_CORRUPT, "QUERY_TWA reply of " + bodyLen + " bytes");
        TwaBucket[] b = new TwaBucket[n];
        for (int i = 0; i < n; i++)
            b[i] = new TwaBucket(in.getLong(), in.getDouble(), in.getDouble(), in.getLong(), in.getLong());
        endReply();
        return Collections.unmodifiableList(Arrays.asList(b));
    }

    private static void checkOpts(Order order, long limit) {
        Objects.requireNonNull(order, "order");
        if (limit < 0)
            throw new IllegalArgumentException("negative limit");
    }

    private static void checkBounds(long[] bounds) {
        Objects.requireNonNull(bounds, "bounds");
        if (bounds.length == 0 || bounds.length > MAX_BOUNDS)
            throw new IllegalArgumentException(bounds.length + " bounds: 1 to " + MAX_BOUNDS + " fit in a request");
    }

    // -------------------------------------------------------------------- LTTB

    /**
     * Downsamples a series for a chart (protocol version 5): at most {@code threshold} points of {@code [start,
     * end)} that keep the shape of the line (Largest-Triangle-Three-Buckets), oldest first, with their stored
     * values. The first and last points are always kept; a range of {@code threshold} points or fewer comes back
     * whole.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param threshold the most points to keep; 0 keeps all of them
     * @return the chosen points
     * @throws TsdbException with {@link Status#ERR_INVALID} for a threshold of 1 or 2, a state series or a server
     *     older than version 5
     */
    public ValuePoints queryLttb(int series, long start, long end, long threshold) throws TsdbException {
        return queryLttb(series, start, end, threshold, Order.ASC, 0);
    }

    /**
     * As {@link #queryLttb(int, long, long, long)}, newest first with {@link Order#DESC} and only the first
     * {@code limit} of the chosen points in that order; the choice does not change. Needs protocol version 9 with
     * {@link Order#DESC} or a limit.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param threshold the most points to keep; 0 keeps all of them
     * @param order oldest or newest first
     * @param limit the most points to return; 0 for all
     * @return the chosen points
     * @throws TsdbException as {@link #queryLttb(int, long, long, long)}, and from a server older than version 9
     *     with an order or a limit
     */
    public ValuePoints queryLttb(int series, long start, long end, long threshold, Order order, long limit)
            throws TsdbException {
        checkOpts(order, limit);
        needVersion(5, "LTTB");
        if (order == Order.DESC || limit != 0)
            needVersion(9, "LTTB with an order or a limit");
        ValueCollector c = new ValueCollector();
        valuesStream(Protocol.OP_QUERY_LTTB, series, start, end, order, limit, threshold, c);
        if (c.overflow || c.mixed)
            throw new TsdbException(c.mixed ? Status.ERR_CORRUPT : Status.ERR_NOMEM, "QUERY_LTTB reply", null);
        return c.result();
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
            if (bodyLen < 2 || bodyLen != 2 + len) /* bodyLen 1 would pass the second test with len -1 */
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
        if (serverVersion() < 3)
            return false;
        if (zstd == null && (zstd = Compressor.create()) == null) {
            noZstd = true;
            return false;
        }
        return true;
    }

    /**
     * The server's protocol version, asked once (HELLO). Outstanding acknowledgements are collected first; their
     * errors stay for {@link #await()}.
     */
    private int serverVersion() throws TsdbException {
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
        return serverVersion;
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
        checkOpts(order, limit);
        request();
        return valuesStream(Protocol.OP_QUERY_VALUES, series, start, end, order, limit, 0, sink);
    }

    /** QUERY_VALUES, or QUERY_LTTB with {@code threshold}, and its chunks. */
    private boolean valuesStream(byte op, int series, long start, long end, Order order, long limit, long threshold,
                                 ValueSink sink) throws TsdbException {
        boolean opts = order != Order.ASC || limit != 0;
        boolean lttb = op == Protocol.OP_QUERY_LTTB;
        begin(op, (opts ? 29 : 20) + (lttb ? 8 : 0));
        out.putInt(series).putLong(start).putLong(end);
        if (lttb)
            out.putLong(threshold);
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

    // ---------------------------------------------------- deletes and retention

    /**
     * Deletes every row of a series with {@code start <= ts < end} (protocol version 7); {@code start >= end}
     * deletes nothing. It returns once the delete is durable and visible: queries stop seeing the rows at once,
     * merges remove them from disk later. {@link Long#MIN_VALUE} and {@link Long#MAX_VALUE} leave a side unbounded.
     * Pending inserts are collected first.
     *
     * @param series the series id
     * @param start the first timestamp deleted, ms since the epoch
     * @param end the first timestamp kept after the range
     * @throws TsdbException with {@link Status#ERR_INVALID} from a server older than version 7 (asked with HELLO;
     *     the request is not sent); the first error of earlier pipelined inserts
     */
    public void deleteRange(int series, long start, long end) throws TsdbException {
        needDeletes();
        begin(Protocol.OP_DELETE_RANGE, 24);
        out.putInt(series).putInt(0).putLong(start).putLong(end);
        send();
        int st = frame();
        endReply();
        check(st);
    }

    /**
     * Replaces the rows of a series in {@code [start, end)} by {@code timestamps} and {@code values}, atomically
     * (protocol version 7): a reader sees either the old rows or the new ones. Every timestamp must lie in the
     * range; no rows make it a delete. The column's type must be the series' type; its nulls are written as nulls.
     * The rows travel in one plain frame of at most 256 MiB.
     *
     * @param series the series id
     * @param start the first timestamp replaced, ms since the epoch
     * @param end the first timestamp kept after the range
     * @param timestamps ms since the epoch, each in {@code [start, end)}
     * @param values the values; as long as {@code timestamps}
     * @throws IllegalArgumentException if the lengths differ or the rows do not fit in one frame
     * @throws TsdbException with {@link Status#ERR_INVALID} for a timestamp outside the range, a column of another
     *     type than the series or a server older than version 7; the first error of earlier pipelined inserts
     */
    public void replaceRange(int series, long start, long end, long[] timestamps, Column values)
            throws TsdbException {
        int n = sameLength(Objects.requireNonNull(timestamps, "timestamps"), Objects.requireNonNull(values, "values"));
        ValueType type = values.type();
        byte[] valid = Values.validity(values, 0, n);
        long payload = Protocol.REPLACE_HEADER + (long) n * (Long.BYTES + type.width)
                + (valid != null ? valid.length : 0);
        if (payload > Protocol.MAX_FRAME - 5)
            throw new IllegalArgumentException(n + " rows do not fit in one request");
        needDeletes();
        begin(Protocol.OP_REPLACE_RANGE, payload);
        out.putInt(n).putInt(series);
        out.put((byte) type.code).put((byte) (valid != null ? Protocol.VALID : 0)).putShort((short) 0).putInt(0);
        out.putLong(start).putLong(end);
        putLongs(timestamps, 0, n);
        putColumn(values, 0, n);
        if (valid != null)
            putBytes(valid);
        send();
        int st = frame();
        endReply();
        check(st);
    }

    /**
     * Sets how long a series keeps its rows, in ms (protocol version 7): rows older than the server's clock minus
     * {@code limitMs} disappear from queries at once and from disk as merges get to them; 0 keeps them forever.
     * Set it right after creating a series to never show older rows. The limit is durable when this returns.
     *
     * @param series the series id
     * @param limitMs the age limit in ms, 0 for none
     * @throws TsdbException with {@link Status#ERR_INVALID} for a negative limit or a server older than version 7,
     *     {@link Status#ERR_NOTFOUND} for an unknown series
     */
    public void setRetention(int series, long limitMs) throws TsdbException {
        needDeletes();
        begin(Protocol.OP_SET_RETENTION, 16);
        out.putInt(series).putInt(0).putLong(limitMs);
        send();
        int st = frame();
        endReply();
        check(st);
    }

    /**
     * Returns the retention limit of a series (protocol version 7).
     *
     * @param series the series id
     * @return the age limit in ms, 0 for none
     * @throws TsdbException as {@link #setRetention(int, long)}
     */
    public long retention(int series) throws TsdbException {
        needDeletes();
        begin(Protocol.OP_GET_RETENTION, 4);
        out.putInt(series);
        send();
        int st = frame();
        if (st == 0 && bodyLen != 8)
            throw lost(Status.ERR_CORRUPT, "GET_RETENTION reply");
        long v = st == 0 ? in.getLong() : 0;
        endReply();
        check(st);
        return v;
    }

    /**
     * Deletes and retention need a server of protocol version 7. An older one answers {@link Status#ERR_INVALID}
     * for an op it does not know; asking HELLO first gives the same answer without sending it. Pending inserts are
     * collected first, so that their errors are not taken for an old server.
     */
    private void needDeletes() throws TsdbException {
        needVersion(7, "deletes");
    }

    /** As {@link #needDeletes}, for any op newer than version 1: a server older than {@code v} is not asked. */
    private void needVersion(int v, String what) throws TsdbException {
        request();
        int have = serverVersion();
        if (have < v)
            throw new TsdbException(Status.ERR_INVALID,
                    "the server speaks protocol " + have + "; " + what + ": protocol " + v, null);
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
     * {@code queryValues}, {@code queryAgg}, {@code queryGroup} or {@code queryBounds} (for a {@code queryGroup} in a
     * time zone, its last request: the buckets). Queries on other connections do not change it;
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

    private static void closeQuietly(java.io.Closeable ch) {
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
        closeQuietly(selector);
        return new TsdbException(status, detail, cause);
    }

    /** Starts a request frame in the (empty) send buffer. */
    private void begin(byte op, long payload) {
        if (payload > Protocol.MAX_FRAME - 5)
            throw new IllegalArgumentException("request of " + payload + " bytes exceeds the frame limit");
        out.putInt((int) (5 + payload)).putInt(nextId++).put(op);
    }

    /**
     * Waits until the (non-blocking) socket is ready for {@code op}, for at most the request timeout.
     *
     * @throws SocketTimeoutException if it is not
     */
    private void awaitSocket(int op) throws IOException {
        long deadline = timeoutNanos > 0 ? System.nanoTime() + timeoutNanos : 0;
        key.interestOps(op);
        while (true) {
            long left = Long.MAX_VALUE;
            int n;
            if (timeoutNanos > 0) {
                left = deadline - System.nanoTime();
                if (left <= 0)
                    throw new SocketTimeoutException("no progress for " + Duration.ofNanos(timeoutNanos));
                /* rounded up: select(ms) with 0 would wait forever */
                n = selector.select(Math.max(1, (left + 999_999) / 1_000_000));
            } else {
                n = selector.select();
            }
            if (n > 0) {
                selector.selectedKeys().clear();
                return;
            }
            if (Thread.currentThread().isInterrupted())
                throw new java.nio.channels.ClosedByInterruptException();
        }
    }

    /** Writes the send buffer out, completely. */
    private void send() throws TsdbException {
        out.flip();
        try {
            while (out.hasRemaining()) {
                int w = ch.write(out);
                bytesSent += w;
                if (w == 0)
                    awaitSocket(SelectionKey.OP_WRITE);
            }
        } catch (IOException e) {
            throw lost(Status.ERR_IO, e instanceof SocketTimeoutException ? "timed out sending to the server"
                    : "send failed", e);
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
                if (r == 0)
                    awaitSocket(SelectionKey.OP_READ);
            } catch (IOException e) {
                throw lost(Status.ERR_IO, e instanceof SocketTimeoutException ? "timed out waiting for the server"
                        : "receive failed", e);
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

    /**
     * The UTF-8 bytes of a name, path or zone. getBytes would turn an unpaired surrogate into '?' and so map
     * different malformed names onto one series; such names, and names with NUL (which the server refuses too), fail
     * here with ERR_INVALID before anything is sent. One pass over the string, once per request.
     */
    private static byte[] utf8(String s, String what) throws TsdbException {
        for (int i = 0, n = s.length(); i < n; i++) {
            char ch = s.charAt(i);
            if (ch == 0)
                throw new TsdbException(Status.ERR_INVALID, what + " contains NUL", null);
            if (Character.isSurrogate(ch)) {
                if (Character.isHighSurrogate(ch) && i + 1 < n && Character.isLowSurrogate(s.charAt(i + 1))) {
                    i++;
                    continue;
                }
                throw new TsdbException(Status.ERR_INVALID, what + " contains an unpaired UTF-16 surrogate at index "
                        + i, null);
            }
        }
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
