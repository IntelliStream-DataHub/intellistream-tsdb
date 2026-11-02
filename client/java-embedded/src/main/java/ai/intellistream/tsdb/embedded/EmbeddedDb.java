package ai.intellistream.tsdb.embedded;

import ai.intellistream.tsdb.Bucket;
import ai.intellistream.tsdb.Buckets;
import ai.intellistream.tsdb.Column;
import ai.intellistream.tsdb.Group;
import ai.intellistream.tsdb.MultiOptions;
import ai.intellistream.tsdb.Order;
import ai.intellistream.tsdb.Points;
import ai.intellistream.tsdb.QueryStats;
import ai.intellistream.tsdb.RawConsumer;
import ai.intellistream.tsdb.SeriesResult;
import ai.intellistream.tsdb.Stats;
import ai.intellistream.tsdb.SpreadBucket;
import ai.intellistream.tsdb.Status;
import ai.intellistream.tsdb.TsdbClient;
import ai.intellistream.tsdb.TsdbException;
import ai.intellistream.tsdb.TwaBucket;
import ai.intellistream.tsdb.TwaMethod;
import ai.intellistream.tsdb.TwaOptions;
import ai.intellistream.tsdb.ValuePoints;
import ai.intellistream.tsdb.ValueType;
import ai.intellistream.tsdb.ValuesConsumer;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static ai.intellistream.tsdb.embedded.Native.BUCKET;
import static ai.intellistream.tsdb.embedded.Native.PTR;
import static ai.intellistream.tsdb.embedded.Native.QUERY_STATS;
import static ai.intellistream.tsdb.embedded.Native.SIZE_T;
import static ai.intellistream.tsdb.embedded.Native.rethrow;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

/**
 * A tsdb database in this process: the engine of {@code libtsdb.so} called through the Java FFM API, with the
 * methods and types of {@link TsdbClient}, the client of tsdbd. Code written against one moves to the other by
 * changing how it is opened:
 *
 * <pre>{@code
 * try (EmbeddedDb db = EmbeddedDb.open("/var/lib/tsdb")) {
 *     long id = db.series("cpu.temp");
 *     db.insert(id, new long[] {t0, t0 + 1000}, new float[] {21.5f, 21.7f});
 *     List<Bucket> perHour = db.queryGroup(id, t0, t1, Group.of(Unit.HOUR).in("Europe/Oslo"));
 * }
 * }</pre>
 *
 * <p>Every method is thread safe, like the C API, and one instance is meant to be shared by all threads. Closing
 * waits for the calls in progress. Only one process can open a directory for writing at a time; another gets
 * {@link Status#ERR_LOCKED}. Errors are the TCP client's: {@link TsdbException} with the engine's status code.
 *
 * <p>Memory: arguments are copied into native memory for the duration of a call (a confined arena per call, so
 * freeing it never synchronizes with other threads); arrays the engine returns are copied into Java objects and
 * freed with {@code tsdb_free} at once. Streamed rows are copied from the engine's buffers into Java arrays that
 * are reused chunk after chunk, so a consumer sees exactly what {@link TsdbClient} gives it.
 *
 * <p>Unlike the client, an insert here returns once its rows are durable and visible (as the options say): the
 * {@code insertAsync} methods do the same and {@link #await()} has nothing to wait for. {@link #lastQueryStats()}
 * describes the calling thread's last query (a virtual thread's own, too).
 */
public final class EmbeddedDb implements AutoCloseable {
    /** Rows per chunk handed to a {@link RawConsumer} or {@link ValuesConsumer}, as the TCP client's chunks. */
    public static final int CHUNK = 16384;

    private final String dir;
    /* Read: a call is using the handle; write: close. Close cannot free the engine under a running call. */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private MemorySegment db; /* NULL once closed */
    private final Scratch.Pool scratch; /* native copies of inserted arrays */

    private EmbeddedDb(String dir, MemorySegment db, long scratchBytes) {
        this.dir = dir;
        this.db = db;
        this.scratch = new Scratch.Pool(scratchBytes);
    }

    // ------------------------------------------------------------------------------------------------ opening

    /**
     * Opens (and creates if needed) the database in {@code dir} with the default options.
     *
     * @param dir the database directory
     * @return the open database
     * @throws TsdbException e.g. {@link Status#ERR_LOCKED} if another process has it open for writing,
     *     {@link Status#ERR_IO} if the directory cannot be created
     */
    public static EmbeddedDb open(String dir) throws TsdbException {
        return open(dir, null);
    }

    /**
     * Opens (and creates if needed) the database in {@code dir} with the default options.
     *
     * @param dir the database directory
     * @return the open database
     * @throws TsdbException e.g. {@link Status#ERR_LOCKED} if another process has it open for writing
     */
    public static EmbeddedDb open(Path dir) throws TsdbException {
        return open(dir.toString(), null);
    }

    /**
     * Opens (and creates if needed) the database in {@code dir}.
     *
     * @param dir the database directory
     * @param options the options, or {@code null} for the defaults
     * @return the open database
     * @throws TsdbException e.g. {@link Status#ERR_LOCKED} if another process has it open for writing,
     *     {@link Status#ERR_INVALID} for options the engine refuses
     */
    public static EmbeddedDb open(String dir, EmbeddedOptions options) throws TsdbException {
        Objects.requireNonNull(dir, "dir");
        checkCString(dir, "directory");
        MemorySegment handle;
        int rc;
        try (Arena a = Arena.ofConfined()) {
            MemorySegment opts = options == null ? MemorySegment.NULL : options.toNative(a);
            MemorySegment out = a.allocate(PTR);
            try {
                rc = (int) Native.OPEN.invokeExact(a.allocateFrom(dir), opts, out);
            } catch (Throwable t) {
                throw rethrow(t);
            }
            handle = out.get(PTR, 0);
        }
        check(rc, dir);
        return new EmbeddedDb(dir, handle,
                options == null ? EmbeddedOptions.DEFAULT_SCRATCH_BYTES : options.scratchBytes());
    }

    /**
     * Closes the database: waits for the calls in progress, flushes the memtable and stops the background work.
     * Closing again does nothing.
     *
     * @throws TsdbException if the final flush fails (the database is closed anyway)
     * @throws IllegalStateException when called from a query's consumer
     */
    @Override
    public void close() throws TsdbException {
        if (lock.getReadHoldCount() > 0)
            throw new IllegalStateException("close from inside a call on this database");
        int rc;
        lock.writeLock().lock();
        try {
            if (db == null)
                return;
            MemorySegment d = db;
            db = null;
            scratch.clear();
            try {
                rc = (int) Native.CLOSE.invokeExact(d);
            } catch (Throwable t) {
                throw rethrow(t);
            }
        } finally {
            lock.writeLock().unlock();
        }
        check(rc, "close");
    }

    /**
     * Whether the database is open.
     *
     * @return false once {@link #close()} was called
     */
    public boolean isOpen() {
        lock.readLock().lock();
        try {
            return db != null;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * The directory this database was opened on.
     *
     * @return the path given to {@code open}
     */
    public String directory() {
        return dir;
    }

    /**
     * Where libtsdb.so was loaded from: a path from {@code tsdb.library}, {@code TSDB_LIBRARY} or
     * {@code java.library.path}, the copy bundled in the jar, or the dynamic linker's search.
     *
     * @return a description of the library's origin
     */
    public static String librarySource() {
        return Native.SOURCE;
    }

    /**
     * The SIMD kernels the engine picked for this CPU.
     *
     * @return "avx512", "avx2" or "scalar", plus the codecs' CPU level in a portable build
     */
    public static String simdLevel() {
        try {
            return Native.string((MemorySegment) Native.SIMD_LEVEL.invokeExact());
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    /**
     * The engine's message for a status code.
     *
     * @param status a {@link Status} code
     * @return {@code tsdb_strerror}'s text
     */
    public static String strerror(int status) {
        return Native.strerror(status);
    }

    private MemorySegment enter() throws TsdbException {
        lock.readLock().lock();
        if (db == null) {
            lock.readLock().unlock();
            throw new TsdbException(Status.ERR_IO, "the database is closed", null);
        }
        return db;
    }

    private void exit() {
        lock.readLock().unlock();
    }

    private static void check(int rc, String what) throws TsdbException {
        if (rc != 0)
            throw new TsdbException(rc, what, null);
    }

    /** Errors of writes: an I/O error also says what failed for good (tsdb_failed). */
    private void checkWrite(MemorySegment d, int rc, String what) throws TsdbException {
        if (rc == Status.ERR_IO) {
            String why = failed(d);
            if (why != null)
                what = what + ": " + why;
        }
        check(rc, what);
    }

    // ------------------------------------------------------------------------------------------------ series

    /**
     * Gets or creates a float32 series in the database directory.
     *
     * @param name the series name
     * @return the series id
     * @throws TsdbException e.g. {@link Status#ERR_READONLY}
     */
    public long series(String name) throws TsdbException {
        return series(name, true);
    }

    /**
     * Gets a series, creating a float32 one if {@code create} is set.
     *
     * @param name the series name
     * @param create whether to create a missing series
     * @return the series id
     * @throws TsdbException with {@link Status#ERR_NOTFOUND} if the series is missing and {@code create} is false
     */
    public long series(String name, boolean create) throws TsdbException {
        checkName(name, "series");
        MemorySegment d = enter();
        int rc;
        try (Arena a = Arena.ofConfined()) {
            MemorySegment id = a.allocate(JAVA_LONG);
            rc = create ? (int) Native.SERIES.invokeExact(d, a.allocateFrom(name), id)
                    : (int) Native.SERIES_LOOKUP.invokeExact(d, a.allocateFrom(name), id);
            if (rc == 0)
                return id.get(JAVA_LONG, 0);
        } catch (Throwable t) {
            throw rethrow(t);
        } finally {
            exit();
        }
        throw new TsdbException(rc, name, null);
    }

    /**
     * Gets or creates a float32 series in volume {@code path} (a directory, for example on another disk or NFS
     * mount). The path only matters when the series is created.
     *
     * @param name the series name
     * @param path the volume's directory
     * @return the series id
     * @throws TsdbException if the series or its volume cannot be created
     */
    public long seriesAt(String name, String path) throws TsdbException {
        return seriesCreate(name, ValueType.FLOAT32, Objects.requireNonNull(path, "path"));
    }

    /**
     * Gets or creates a series of {@code type} in the database directory. An existing series keeps its type.
     *
     * @param name the series name
     * @param type its value type
     * @return the series id
     * @throws TsdbException with {@link Status#ERR_INVALID} if the series exists with another type
     */
    public long seriesCreate(String name, ValueType type) throws TsdbException {
        return seriesCreate(name, type, null);
    }

    /**
     * Gets or creates a series of {@code type} in volume {@code path}.
     *
     * @param name the series name
     * @param type its value type
     * @param path the volume's directory, or {@code null} for the database directory
     * @return the series id
     * @throws TsdbException with {@link Status#ERR_INVALID} if the series exists with another type
     */
    public long seriesCreate(String name, ValueType type, String path) throws TsdbException {
        checkName(name, "series");
        Objects.requireNonNull(type, "type");
        checkCString(path, "volume path");
        MemorySegment d = enter();
        int rc;
        try (Arena a = Arena.ofConfined()) {
            MemorySegment id = a.allocate(JAVA_LONG);
            MemorySegment p = path == null ? MemorySegment.NULL : a.allocateFrom(path);
            rc = (int) Native.SERIES_CREATE.invokeExact(d, a.allocateFrom(name), type.code(), p, id);
            if (rc == 0)
                return id.get(JAVA_LONG, 0);
            checkWrite(d, rc, name);
        } catch (TsdbException e) {
            throw e;
        } catch (Throwable t) {
            throw rethrow(t);
        } finally {
            exit();
        }
        throw new TsdbException(rc, name, null);
    }

    /**
     * The value type of a series.
     *
     * @param id the series id
     * @return its type
     * @throws TsdbException with {@link Status#ERR_NOTFOUND} for an unknown id
     */
    public ValueType seriesType(long id) throws TsdbException {
        MemorySegment d = enter();
        int rc;
        try (Arena a = Arena.ofConfined()) {
            MemorySegment t = a.allocate(JAVA_INT);
            rc = (int) Native.SERIES_TYPE.invokeExact(d, id, t);
            if (rc == 0)
                return typeOf(t.get(JAVA_INT, 0));
        } catch (Throwable t) {
            throw rethrow(t);
        } finally {
            exit();
        }
        throw new TsdbException(rc, "series " + Long.toUnsignedString(id), null);
    }

    /**
     * The partition class of a series' newest rows: 0 to 3 for partitions of 1, 7, 28 or 364 base partitions
     * ({@link EmbeddedOptions#partitionClasses(boolean)}, {@link PartInfo#cls()}).
     *
     * @param id the series id
     * @return its class
     * @throws TsdbException with {@link Status#ERR_NOTFOUND} for an unknown id
     */
    public int seriesClass(long id) throws TsdbException {
        MemorySegment d = enter();
        int rc;
        try (Arena a = Arena.ofConfined()) {
            MemorySegment c = a.allocate(JAVA_INT);
            rc = (int) Native.SERIES_CLASS.invokeExact(d, id, c);
            if (rc == 0)
                return c.get(JAVA_INT, 0);
        } catch (Throwable t) {
            throw rethrow(t);
        } finally {
            exit();
        }
        throw new TsdbException(rc, "series " + Long.toUnsignedString(id), null);
    }

    /**
     * The name of a series.
     *
     * @param id the series id
     * @return its name
     * @throws TsdbException with {@link Status#ERR_NOTFOUND} for an unknown id
     */
    public String seriesName(long id) throws TsdbException {
        String s = idString(Native.SERIES_NAME, id);
        if (s == null)
            throw new TsdbException(Status.ERR_NOTFOUND, "series " + Long.toUnsignedString(id), null);
        return s;
    }

    /**
     * The volume directory a series lives in.
     *
     * @param id the series id
     * @return the directory
     * @throws TsdbException with {@link Status#ERR_NOTFOUND} for an unknown id
     */
    public String seriesPath(long id) throws TsdbException {
        String s = idString(Native.SERIES_PATH, id);
        if (s == null)
            throw new TsdbException(Status.ERR_NOTFOUND, "series " + Long.toUnsignedString(id), null);
        return s;
    }

    /**
     * The number of series. Ids are not dense: list them with {@link #seriesIds()}, never as a range.
     *
     * @return the count
     * @throws TsdbException if the database is closed
     */
    public long seriesCount() throws TsdbException {
        MemorySegment d = enter();
        try {
            return (long) Native.SERIES_COUNT.invokeExact(d);
        } catch (Throwable t) {
            throw rethrow(t);
        } finally {
            exit();
        }
    }

    /**
     * The id of every series (those of one volume in creation order; no order across volumes is promised).
     *
     * @return the ids
     * @throws TsdbException if the database is closed
     */
    public long[] seriesIds() throws TsdbException {
        MemorySegment d = enter();
        IdList out = new IdList();
        long ctx = IDS.getAndIncrement();
        int rc;
        try {
            SINKS.put(ctx, out);
            rc = (int) Native.SERIES_FOREACH.invokeExact(d, SERIES_STUB, MemorySegment.ofAddress(ctx));
        } catch (Throwable t) {
            throw rethrow(t);
        } finally {
            SINKS.remove(ctx);
            exit();
        }
        if (out.error != null)
            throw rethrow(out.error);
        check(rc, "series ids");
        return Arrays.copyOf(out.ids, out.n);
    }

    /** The ids tsdb_series_foreach hands {@link #seriesId}. */
    private static final class IdList {
        long[] ids = new long[64];
        int n;
        Throwable error;
    }

    private static int seriesId(MemorySegment ctx, long id) {
        IdList l = null;
        try {
            l = (IdList) SINKS.get(ctx.address());
            if (l.n == l.ids.length)
                l.ids = Arrays.copyOf(l.ids, Math.addExact(l.n, l.n >> 1));
            l.ids[l.n++] = id;
            return 0;
        } catch (Throwable t) {
            if (l != null)
                l.error = t;
            return 1;
        }
    }

    /**
     * The number of volumes: 1 plus the extra directories series were created in.
     *
     * @return the count
     * @throws TsdbException if the database is closed
     */
    public int volumeCount() throws TsdbException {
        return intOf(Native.VOLUME_COUNT);
    }

    /**
     * The directory of a volume.
     *
     * @param index 0 for the database directory, up to {@link #volumeCount()} - 1
     * @return its directory
     * @throws TsdbException with {@link Status#ERR_NOTFOUND} for an index out of range
     */
    public String volumePath(int index) throws TsdbException {
        String s = stringOf(Native.VOLUME_PATH, index);
        if (s == null)
            throw new TsdbException(Status.ERR_NOTFOUND, "volume " + index, null);
        return s;
    }

    private String stringOf(java.lang.invoke.MethodHandle h, int arg) throws TsdbException {
        MemorySegment d = enter();
        try {
            return Native.string((MemorySegment) h.invokeExact(d, arg));
        } catch (Throwable t) {
            throw rethrow(t);
        } finally {
            exit();
        }
    }

    private String idString(java.lang.invoke.MethodHandle h, long id) throws TsdbException {
        MemorySegment d = enter();
        try {
            return Native.string((MemorySegment) h.invokeExact(d, id));
        } catch (Throwable t) {
            throw rethrow(t);
        } finally {
            exit();
        }
    }

    private int intOf(java.lang.invoke.MethodHandle h) throws TsdbException {
        MemorySegment d = enter();
        try {
            return (int) h.invokeExact(d);
        } catch (Throwable t) {
            throw rethrow(t);
        } finally {
            exit();
        }
    }

    private static void checkName(String name, String what) throws TsdbException {
        if (Objects.requireNonNull(name, "name").isEmpty())
            throw new IllegalArgumentException("empty " + what + " name");
        checkCString(name, what + " name");
    }

    /**
     * Refuses a string with a NUL in it, as tsdbd does: C reads a name up to its first NUL, so "cpu\0x" would be
     * series "cpu", and a name from an untrusted source could reach another series.
     */
    static void checkCString(String s, String what) throws TsdbException {
        if (s != null && s.indexOf('\0') >= 0)
            throw new TsdbException(Status.ERR_INVALID, what + " contains a NUL character", null);
    }

    /** The value type of a C {@code tsdb_type} code. */
    static ValueType typeOf(int code) {
        for (ValueType t : ValueType.values())
            if (t.code() == code)
                return t;
        throw new IllegalStateException("libtsdb.so returned value type " + code + ", which this binding does not know");
    }

    // ------------------------------------------------------------------------------------------------ states

    /**
     * The code of a state name in the dictionary of the series' volume, created if needed. Codes start at 1 and
     * never change.
     *
     * @param series a state series
     * @param name the state name
     * @return its code
     * @throws TsdbException e.g. {@link Status#ERR_INVALID} if the series is not a state series
     */
    public int state(long series, String name) throws TsdbException {
        return stateCode(series, name, true);
    }

    /**
     * The code of an existing state name.
     *
     * @param series a state series
     * @param name the state name
     * @return its code
     * @throws TsdbException with {@link Status#ERR_NOTFOUND} if the name has no code
     */
    public int stateLookup(long series, String name) throws TsdbException {
        return stateCode(series, name, false);
    }

    private int stateCode(long series, String name, boolean create) throws TsdbException {
        checkName(name, "state");
        MemorySegment d = enter();
        int rc;
        try (Arena a = Arena.ofConfined()) {
            MemorySegment code = a.allocate(JAVA_INT);
            rc = create ? (int) Native.STATE.invokeExact(d, series, a.allocateFrom(name), code)
                    : (int) Native.STATE_LOOKUP.invokeExact(d, series, a.allocateFrom(name), code);
            if (rc == 0)
                return code.get(JAVA_INT, 0);
            checkWrite(d, rc, name);
        } catch (TsdbException e) {
            throw e;
        } catch (Throwable t) {
            throw rethrow(t);
        } finally {
            exit();
        }
        throw new TsdbException(rc, name, null);
    }

    /**
     * The name of a state code.
     *
     * @param series a state series
     * @param code the code
     * @return its name
     * @throws TsdbException with {@link Status#ERR_NOTFOUND} for an unknown code
     */
    public String stateName(long series, int code) throws TsdbException {
        MemorySegment d = enter();
        String s;
        try {
            s = Native.string((MemorySegment) Native.STATE_NAME.invokeExact(d, series, code));
        } catch (Throwable t) {
            throw rethrow(t);
        } finally {
            exit();
        }
        if (s == null)
            throw new TsdbException(Status.ERR_NOTFOUND, "state " + Integer.toUnsignedString(code), null);
        return s;
    }

    // ------------------------------------------------------------------------------------------------ writing

    /**
     * Inserts points into one float32 series; returns once they are durable and visible. Writing an existing
     * (series, timestamp) replaces its value.
     *
     * @param series the series id
     * @param timestamps ms since the epoch, in any order
     * @param values the values, as many as timestamps
     * @throws TsdbException e.g. {@link Status#ERR_INVALID} if the series is not float32
     */
    public void insert(long series, long[] timestamps, float[] values) throws TsdbException {
        insert(series, timestamps, values, 0, timestamps.length);
    }

    /**
     * Inserts {@code length} points from {@code offset} into one float32 series.
     *
     * @param series the series id
     * @param timestamps ms since the epoch
     * @param values the values
     * @param offset the first point
     * @param length the number of points
     * @throws TsdbException e.g. {@link Status#ERR_INVALID} if the series is not float32
     */
    public void insert(long series, long[] timestamps, float[] values, int offset, int length) throws TsdbException {
        Objects.checkFromIndexSize(offset, length, timestamps.length);
        Objects.checkFromIndexSize(offset, length, values.length);
        MemorySegment d = enter();
        Scratch a = null;
        int rc;
        try {
            a = scratch.take();
            if (length == 0)
                return;
            MemorySegment ts = a.allocateFrom(JAVA_LONG, MemorySegment.ofArray(timestamps), JAVA_LONG, offset * 8L, length);
            MemorySegment v = a.allocateFrom(JAVA_FLOAT, MemorySegment.ofArray(values), JAVA_FLOAT, offset * 4L, length);
            rc = (int) Native.INSERT_SERIES.invokeExact(d, series, ts, v, (long) length);
            checkWrite(d, rc, "insert");
        } catch (TsdbException e) {
            throw e;
        } catch (Throwable t) {
            throw rethrow(t);
        } finally {
            try {
                if (a != null)
                    scratch.give(a);
            } finally {
                exit(); /* even if give failed: a held read lock would block close() forever */
            }
        }
    }

    /**
     * Inserts float32 points of many series: point {@code i} belongs to {@code series[i]}.
     *
     * @param series the series id of each point
     * @param timestamps ms since the epoch
     * @param values the values
     * @throws TsdbException e.g. {@link Status#ERR_INVALID} if a series is not float32
     */
    public void insert(long[] series, long[] timestamps, float[] values) throws TsdbException {
        insert(series, timestamps, values, 0, timestamps.length);
    }

    /**
     * Inserts {@code length} float32 points of many series from {@code offset}.
     *
     * @param series the series id of each point
     * @param timestamps ms since the epoch
     * @param values the values
     * @param offset the first point
     * @param length the number of points
     * @throws TsdbException e.g. {@link Status#ERR_INVALID} if a series is not float32
     */
    public void insert(long[] series, long[] timestamps, float[] values, int offset, int length)
            throws TsdbException {
        Objects.checkFromIndexSize(offset, length, series.length);
        Objects.checkFromIndexSize(offset, length, timestamps.length);
        Objects.checkFromIndexSize(offset, length, values.length);
        MemorySegment d = enter();
        Scratch a = null;
        int rc;
        try {
            a = scratch.take();
            if (length == 0)
                return;
            MemorySegment s = a.allocateFrom(JAVA_LONG, MemorySegment.ofArray(series), JAVA_LONG, offset * 8L, length);
            MemorySegment ts = a.allocateFrom(JAVA_LONG, MemorySegment.ofArray(timestamps), JAVA_LONG, offset * 8L, length);
            MemorySegment v = a.allocateFrom(JAVA_FLOAT, MemorySegment.ofArray(values), JAVA_FLOAT, offset * 4L, length);
            rc = (int) Native.INSERT.invokeExact(d, s, ts, v, (long) length);
            checkWrite(d, rc, "insert");
        } catch (TsdbException e) {
            throw e;
        } catch (Throwable t) {
            throw rethrow(t);
        } finally {
            try {
                if (a != null)
                    scratch.give(a);
            } finally {
                exit(); /* even if give failed: a held read lock would block close() forever */
            }
        }
    }

    /**
     * The same as {@link #insert(long, long[], float[])}: an embedded insert has no round trip to overlap.
     *
     * @param series the series id
     * @param timestamps ms since the epoch
     * @param values the values
     * @throws TsdbException as {@code insert}
     */
    public void insertAsync(long series, long[] timestamps, float[] values) throws TsdbException {
        insert(series, timestamps, values);
    }

    /**
     * The same as {@link #insert(long[], long[], float[])}.
     *
     * @param series the series id of each point
     * @param timestamps ms since the epoch
     * @param values the values
     * @throws TsdbException as {@code insert}
     */
    public void insertAsync(long[] series, long[] timestamps, float[] values) throws TsdbException {
        insert(series, timestamps, values);
    }

    /**
     * Nothing to wait for: every insert has completed when it returns. For code written against
     * {@link TsdbClient#await()}.
     *
     * @throws TsdbException if the database is closed
     */
    public void await() throws TsdbException {
        enter();
        exit();
    }

    /**
     * Inserts in flight: always 0 here.
     *
     * @return 0
     */
    public int pending() {
        return 0;
    }

    /**
     * Inserts points of any type, with nulls, into one series.
     *
     * @param series the series id
     * @param timestamps ms since the epoch
     * @param values the values: a column of the series' type
     * @throws TsdbException with {@link Status#ERR_INVALID} if the column's type is not the series'
     */
    public void insertSeriesValues(long series, long[] timestamps, Column values) throws TsdbException {
        insertSeriesValues(series, timestamps, values, 0, timestamps.length);
    }

    /**
     * Inserts {@code length} points of any type from {@code offset} into one series.
     *
     * @param series the series id
     * @param timestamps ms since the epoch
     * @param values the values: a column of the series' type
     * @param offset the first point
     * @param length the number of points
     * @throws TsdbException with {@link Status#ERR_INVALID} if the column's type is not the series'
     */
    public void insertSeriesValues(long series, long[] timestamps, Column values, int offset, int length)
            throws TsdbException {
        insertValues(null, series, timestamps, values, offset, length);
    }

    /**
     * Inserts points of any type, with nulls, of many series of the column's type.
     *
     * @param series the series id of each point
     * @param timestamps ms since the epoch
     * @param values the values
     * @throws TsdbException with {@link Status#ERR_INVALID} if a series has another type
     */
    public void insertValues(long[] series, long[] timestamps, Column values) throws TsdbException {
        insertValues(series, timestamps, values, 0, timestamps.length);
    }

    /**
     * Inserts {@code length} points of any type of many series from {@code offset}.
     *
     * @param series the series id of each point
     * @param timestamps ms since the epoch
     * @param values the values
     * @param offset the first point
     * @param length the number of points
     * @throws TsdbException with {@link Status#ERR_INVALID} if a series has another type
     */
    public void insertValues(long[] series, long[] timestamps, Column values, int offset, int length)
            throws TsdbException {
        insertValues(Objects.requireNonNull(series, "series"), 0, timestamps, values, offset, length);
    }

    /**
     * The same as {@link #insertSeriesValues(long, long[], Column)}.
     *
     * @param series the series id
     * @param timestamps ms since the epoch
     * @param values the values
     * @throws TsdbException as {@code insertSeriesValues}
     */
    public void insertSeriesValuesAsync(long series, long[] timestamps, Column values) throws TsdbException {
        insertSeriesValues(series, timestamps, values);
    }

    /**
     * The same as {@link #insertValues(long[], long[], Column)}.
     *
     * @param series the series id of each point
     * @param timestamps ms since the epoch
     * @param values the values
     * @throws TsdbException as {@code insertValues}
     */
    public void insertValuesAsync(long[] series, long[] timestamps, Column values) throws TsdbException {
        insertValues(series, timestamps, values);
    }

    private void insertValues(long[] many, long one, long[] timestamps, Column c, int off, int n)
            throws TsdbException {
        Objects.requireNonNull(c, "values");
        Objects.checkFromIndexSize(off, n, timestamps.length);
        Objects.checkFromIndexSize(off, n, c.size());
        if (many != null)
            Objects.checkFromIndexSize(off, n, many.length);
        MemorySegment d = enter();
        Scratch a = null;
        int rc;
        try {
            a = scratch.take();
            if (n == 0)
                return;
            MemorySegment ts = a.allocateFrom(JAVA_LONG, MemorySegment.ofArray(timestamps), JAVA_LONG, off * 8L, n);
            MemorySegment col = a.allocate(Native.VALUES).fill((byte) 0); /* scratch memory is not zeroed */
            col.set(JAVA_INT, 0, c.type().code());
            col.set(PTR, 8, columnValues(a, c, off, n));
            col.set(PTR, 16, validity(a, c, off, n));
            if (many != null) {
                MemorySegment s = a.allocateFrom(JAVA_LONG, MemorySegment.ofArray(many), JAVA_LONG, off * 8L, n);
                rc = (int) Native.INSERT_VALUES.invokeExact(d, s, ts, col, (long) n);
            } else {
                rc = (int) Native.INSERT_SERIES_VALUES.invokeExact(d, one, ts, col, (long) n);
            }
            checkWrite(d, rc, "insert");
        } catch (TsdbException e) {
            throw e;
        } catch (Throwable t) {
            throw rethrow(t);
        } finally {
            try {
                if (a != null)
                    scratch.give(a);
            } finally {
                exit(); /* even if give failed: a held read lock would block close() forever */
            }
        }
    }

    private static MemorySegment columnValues(SegmentAllocator a, Column c, int off, int n) {
        switch (c.type()) {
        case FLOAT32: return a.allocateFrom(JAVA_FLOAT, MemorySegment.ofArray(c.floats()), JAVA_FLOAT, off * 4L, n);
        case FLOAT64: return a.allocateFrom(JAVA_DOUBLE, MemorySegment.ofArray(c.doubles()), JAVA_DOUBLE, off * 8L, n);
        case INT64: return a.allocateFrom(JAVA_LONG, MemorySegment.ofArray(c.longs()), JAVA_LONG, off * 8L, n);
        case BOOL: {
            boolean[] b = c.bools();
            MemorySegment m = a.allocate(n);
            for (int i = 0; i < n; i++)
                m.set(JAVA_BYTE, i, (byte) (b[off + i] ? 1 : 0));
            return m;
        }
        default: return a.allocateFrom(JAVA_INT, MemorySegment.ofArray(c.states()), JAVA_INT, off * 4L, n);
        }
    }

    /** The validity bitmap of values {@code off .. off + n}: bit i set when known; NULL when all are. */
    private static MemorySegment validity(SegmentAllocator a, Column c, int off, int n) {
        if (!c.hasNulls())
            return MemorySegment.NULL;
        BitSet nulls = c.nulls();
        if (nulls.nextSetBit(off) < 0 || nulls.nextSetBit(off) >= off + n)
            return MemorySegment.NULL;
        byte[] bits = new byte[(n + 7) >>> 3];
        Arrays.fill(bits, (byte) 0xff);
        for (int i = nulls.nextSetBit(off); i >= 0 && i < off + n; i = nulls.nextSetBit(i + 1))
            bits[(i - off) >>> 3] &= (byte) ~(1 << ((i - off) & 7));
        return a.allocateFrom(JAVA_BYTE, bits);
    }

    /**
     * Appends one float32 point through the WAL and memtable: visible at once, durable per {@code walSyncMs}.
     *
     * @param series the series id
     * @param timestamp ms since the epoch
     * @param value the value
     * @throws TsdbException e.g. {@link Status#ERR_INVALID} if the series is not float32
     */
    public void append(long series, long timestamp, float value) throws TsdbException {
        MemorySegment d = enter();
        try {
            int rc;
            try {
                rc = (int) Native.APPEND.invokeExact(d, series, timestamp, value);
            } catch (Throwable t) {
                throw rethrow(t);
            }
            checkWrite(d, rc, "append");
        } finally {
            exit();
        }
    }

    // ------------------------------------------------------------------------------------------------ queries

    /*
     * Streaming: the engine calls one upcall stub per callback type, created once for the JVM, with ctx naming
     * the query's Sink in SINKS. The stub catches everything: an exception must never unwind into C, so it is
     * kept in the sink, the callback returns 1 (stop), and the query rethrows it once the engine has returned.
     */
    private static final ConcurrentHashMap<Long, Object> SINKS = new ConcurrentHashMap<>();
    private static final AtomicLong IDS = new AtomicLong(1);
    private static final MemorySegment RAW_STUB = Native.upcall(MethodHandles.lookup(), "rawChunk", Native.RAW_FN);
    private static final MemorySegment VALUES_STUB = Native.upcall(MethodHandles.lookup(), "valuesChunk",
            Native.VALUES_FN);
    private static final MemorySegment PART_STUB = Native.upcall(MethodHandles.lookup(), "partInfo", Native.PART_FN);
    private static final MemorySegment SERIES_STUB = Native.upcall(MethodHandles.lookup(), "seriesId",
            Native.SERIES_FN);

    /* the calling thread's last query statistics, copied right after each query: a virtual thread may run on
     * another carrier (with other C thread-locals) by the time it asks */
    private static final ThreadLocal<QueryStats> LAST = new ThreadLocal<>();
    /* per-thread buffers of a stream, lent to one query at a time (a nested query gets its own) */
    private static final ThreadLocal<Sink> SPARE = new ThreadLocal<>();
    /* the most rows a collecting query holds: the largest Java array; lowered by a test */
    static volatile int collectLimit = Integer.MAX_VALUE - 8;

    private static int rawChunk(MemorySegment ctx, MemorySegment ts, MemorySegment v, long n) {
        Sink s = null;
        try {
            s = (Sink) SINKS.get(ctx.address());
            return s.addRaw(ts, v, (int) n) ? 0 : 1;
        } catch (Throwable t) {
            if (s != null)
                s.error = t;
            return 1;
        }
    }

    private static int valuesChunk(MemorySegment ctx, MemorySegment ts, MemorySegment values, long n) {
        Sink s = null;
        try {
            s = (Sink) SINKS.get(ctx.address());
            return s.addValues(ts, values, (int) n) ? 0 : 1;
        } catch (Throwable t) {
            if (s != null)
                s.error = t;
            return 1;
        }
    }

    @SuppressWarnings("unchecked")
    private static void partInfo(MemorySegment ctx, MemorySegment info) {
        List<Object> out = (List<Object>) SINKS.get(ctx.address());
        try {
            MemorySegment p = info.reinterpret(Native.PART_INFO.byteSize());
            out.add(new PartInfo(Native.string(p.get(PTR, 0)), p.get(JAVA_LONG, 8), p.get(JAVA_LONG, 16),
                    p.get(JAVA_LONG, 24), p.get(JAVA_LONG, 32), p.get(JAVA_LONG, 40), p.get(JAVA_LONG, 48),
                    p.get(JAVA_INT, 56), p.get(JAVA_LONG, 64), p.get(JAVA_LONG, 72), p.get(JAVA_INT, 80),
                    p.get(JAVA_LONG, 88), p.get(JAVA_LONG, 96)));
        } catch (Throwable t) {
            out.add(t);
        }
    }

    private static void saveStats() {
        try (Arena a = Arena.ofConfined()) {
            MemorySegment s = a.allocate(QUERY_STATS);
            Native.LAST_QUERY_STATS.invokeExact(s);
            LAST.set(new QueryStats(s.get(JAVA_DOUBLE, 0), s.get(JAVA_LONG, 8), s.get(JAVA_LONG, 16),
                    s.get(JAVA_LONG, 24), s.get(JAVA_LONG, 32), s.get(JAVA_LONG, 40), s.get(JAVA_LONG, 48),
                    s.get(JAVA_LONG, 56), s.get(JAVA_LONG, 64), s.get(JAVA_LONG, 72), s.get(JAVA_LONG, 80)));
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private static MemorySegment opts(Arena a, Order order, long limit) {
        Objects.requireNonNull(order, "order");
        if (limit < 0)
            throw new IllegalArgumentException("negative limit");
        MemorySegment o = a.allocate(Native.QUERY_OPTS);
        o.set(JAVA_INT, 0, order == Order.DESC ? 1 : 0);
        o.set(SIZE_T, 8, limit);
        return o;
    }

    /**
     * Streams a query's rows: copies the engine's runs into reused arrays and hands them on in chunks of
     * {@link #CHUNK} rows (or collects them all for the methods that return points).
     */
    private static final class Sink {
        RawConsumer raw;
        ValuesConsumer values;
        boolean collect;
        Throwable error;
        boolean stopped;
        ValueType type;
        int fill;
        long[] ts = new long[0];
        float[] f;
        double[] dbl;
        long[] lng;
        boolean[] bool;
        int[] state;
        BitSet nulls = new BitSet();
        byte[] bits = new byte[0];

        static Sink take() {
            Sink s = SPARE.get();
            if (s == null)
                return new Sink();
            SPARE.remove();
            return s;
        }

        void release() {
            raw = null;
            values = null;
            collect = false;
            error = null;
            stopped = false;
            fill = 0;
            nulls.clear();
            if (ts.length > CHUNK) /* a collecting query's arrays went to its result */
                ts = new long[0];
            f = null;
            dbl = null;
            lng = null;
            bool = null;
            state = null;
            type = null;
            SPARE.set(this);
        }

        /**
         * Room for {@code k} more rows; returns how many fit before a chunk is full (at least one while k > 0).
         * A collecting query that has filled the largest Java array fails with ERR_NOMEM, as queryPoints does,
         * instead of looping on 0 rows of room with the read lock held.
         */
        private int room(int k) throws TsdbException {
            int want = fill + k;
            int limit = collect ? collectLimit : CHUNK;
            if (want > ts.length && ts.length < limit) {
                long cap = Math.max(want, collect ? (long) ts.length * 2 : Math.min(CHUNK, (long) ts.length * 2));
                int c = (int) Math.min(cap, limit);
                ts = Arrays.copyOf(ts, c);
                if (type != null) {
                    switch (type) {
                    case FLOAT32: f = f == null ? new float[c] : Arrays.copyOf(f, c); break;
                    case FLOAT64: dbl = dbl == null ? new double[c] : Arrays.copyOf(dbl, c); break;
                    case INT64: lng = lng == null ? new long[c] : Arrays.copyOf(lng, c); break;
                    case BOOL: bool = bool == null ? new boolean[c] : Arrays.copyOf(bool, c); break;
                    default: state = state == null ? new int[c] : Arrays.copyOf(state, c); break;
                    }
                }
            }
            int r = Math.min(k, ts.length - fill);
            if (r == 0 && k > 0 && collect)
                throw new TsdbException(Status.ERR_NOMEM, "more points than one array holds", null);
            return r;
        }

        boolean addRaw(MemorySegment tsp, MemorySegment vp, int n) throws TsdbException {
            MemorySegment t = tsp.reinterpret(n * 8L);
            MemorySegment v = vp.reinterpret(n * 4L);
            int off = 0;
            while (off < n) {
                if (f == null || f.length != ts.length) {
                    type = ValueType.FLOAT32;
                    f = f == null ? new float[ts.length] : Arrays.copyOf(f, ts.length);
                }
                int k = room(n - off);
                MemorySegment.copy(t, JAVA_LONG, off * 8L, ts, fill, k);
                MemorySegment.copy(v, JAVA_FLOAT, off * 4L, f, fill, k);
                fill += k;
                off += k;
                if (fill == CHUNK && !deliver())
                    return false;
            }
            return true;
        }

        boolean addValues(MemorySegment tsp, MemorySegment col, int n) throws TsdbException {
            MemorySegment c = col.reinterpret(Native.VALUES.byteSize());
            ValueType t = typeOf(c.get(JAVA_INT, 0));
            if (type == null) {
                type = t;
                int cap = ts.length;
                ts = new long[0]; /* the typed array is allocated along */
                room(Math.max(cap, Math.min(n, CHUNK)));
            } else if (t != type) {
                throw new TsdbException(Status.ERR_CORRUPT, "chunks of different value types", null);
            }
            MemorySegment tsm = tsp.reinterpret(n * 8L);
            MemorySegment vm = c.get(PTR, 8).reinterpret((long) n * (t == ValueType.BOOL ? 1 : t == ValueType.FLOAT64
                    || t == ValueType.INT64 ? 8 : 4));
            MemorySegment valid = c.get(PTR, 16);
            if (valid.address() != 0) {
                int nb = (n + 7) >>> 3;
                if (bits.length < nb)
                    bits = new byte[Math.max(nb, bits.length * 2)];
                MemorySegment.copy(valid.reinterpret(nb), JAVA_BYTE, 0, bits, 0, nb);
            }
            int off = 0;
            while (off < n) {
                int k = room(n - off);
                MemorySegment.copy(tsm, JAVA_LONG, off * 8L, ts, fill, k);
                switch (t) {
                case FLOAT32: MemorySegment.copy(vm, JAVA_FLOAT, off * 4L, f, fill, k); break;
                case FLOAT64: MemorySegment.copy(vm, JAVA_DOUBLE, off * 8L, dbl, fill, k); break;
                case INT64: MemorySegment.copy(vm, JAVA_LONG, off * 8L, lng, fill, k); break;
                case BOOL:
                    for (int i = 0; i < k; i++)
                        bool[fill + i] = vm.get(JAVA_BYTE, off + i) != 0;
                    break;
                default: MemorySegment.copy(vm, JAVA_INT, off * 4L, state, fill, k); break;
                }
                if (valid.address() != 0)
                    for (int i = 0; i < k; i++)
                        if ((bits[(off + i) >>> 3] >>> ((off + i) & 7) & 1) == 0)
                            nulls.set(fill + i);
                fill += k;
                off += k;
                if (!collect && fill == CHUNK && !deliver())
                    return false;
            }
            return true;
        }

        /** Hands the filled chunk to the consumer; false when it asked to stop. */
        boolean deliver() {
            int n = fill;
            fill = 0;
            boolean more;
            if (raw != null) {
                more = raw.accept(ts, f, n);
            } else {
                Column c = column(n, n != ts.length);
                more = values.accept(n == ts.length ? ts : Arrays.copyOf(ts, n), c, n);
                nulls.clear();
            }
            if (!more)
                stopped = true;
            return more;
        }

        /** The first {@code n} values as a column: over the arrays themselves unless {@code copy}. */
        Column column(int n, boolean copy) {
            Column c;
            switch (type) {
            case FLOAT32: c = Column.ofFloats(copy ? Arrays.copyOf(f, n) : f); break;
            case FLOAT64: c = Column.ofDoubles(copy ? Arrays.copyOf(dbl, n) : dbl); break;
            case INT64: c = Column.ofLongs(copy ? Arrays.copyOf(lng, n) : lng); break;
            case BOOL: c = Column.ofBools(copy ? Arrays.copyOf(bool, n) : bool); break;
            default: c = Column.ofStates(copy ? Arrays.copyOf(state, n) : state); break;
            }
            return nulls.isEmpty() ? c : c.withNulls(nulls);
        }
    }

    /** Runs a streaming query and returns true unless the consumer stopped it. */
    private boolean stream(Sink s, long series, long start, long end, Order order, long limit, boolean typed)
            throws TsdbException {
        return stream(s, series, start, end, order, limit, typed ? VALUES : RAW, 0);
    }

    private static final int RAW = 0, VALUES = 1, LTTB = 2;

    /** Runs a streaming query of {@code kind} ({@code threshold} for LTTB only). */
    private boolean stream(Sink s, long series, long start, long end, Order order, long limit, int kind,
                           long threshold) throws TsdbException {
        MemorySegment d = enter();
        long id = IDS.getAndIncrement();
        int rc;
        try (Arena a = Arena.ofConfined()) {
            MemorySegment o = opts(a, order, limit);
            SINKS.put(id, s);
            try {
                MemorySegment ctx = MemorySegment.ofAddress(id);
                if (kind == LTTB) /* a negative threshold is a huge size_t, as tsdbd reads it: every point */
                    rc = (int) Native.QUERY_LTTB_EX.invokeExact(d, series, start, end, threshold, o, VALUES_STUB, ctx);
                else if (kind == VALUES)
                    rc = (int) Native.QUERY_VALUES.invokeExact(d, series, start, end, o, VALUES_STUB, ctx);
                else
                    rc = (int) Native.QUERY_RAW_EX.invokeExact(d, series, start, end, o, RAW_STUB, ctx);
            } catch (Throwable t) {
                throw rethrow(t);
            } finally {
                SINKS.remove(id);
            }
            saveStats();
            if (s.error != null) {
                Throwable e = s.error;
                if (e instanceof TsdbException te)
                    throw te;
                throw rethrow(e);
            }
            if (rc == Status.ERR_STOPPED && s.stopped)
                return false;
            check(rc, "query");
            /* the last chunk too is delivered under the read lock: a consumer always runs inside the call */
            if (!s.collect && s.fill > 0)
                return s.deliver();
            return true;
        } finally {
            exit();
        }
    }

    /**
     * The points of a float32 series in {@code [start, end)}, oldest first. Nulls read as NaN.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @return the points
     * @throws TsdbException with {@link Status#ERR_INVALID} for a series of another type (use
     *     {@link #queryValues(long, long, long)})
     */
    public Points queryPoints(long series, long start, long end) throws TsdbException {
        return queryPoints(series, start, end, Order.ASC, 0);
    }

    /**
     * The points of a float32 series in {@code [start, end)} in the given order, at most {@code limit} of them.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param order by timestamp
     * @param limit at most this many points; 0 for all
     * @return the points
     * @throws TsdbException with {@link Status#ERR_INVALID} for a series of another type
     * @throws IllegalArgumentException for a negative limit
     */
    public Points queryPoints(long series, long start, long end, Order order, long limit) throws TsdbException {
        MemorySegment d = enter();
        int rc;
        try (Arena a = Arena.ofConfined()) {
            MemorySegment o = opts(a, order, limit);
            MemorySegment p = a.allocate(Native.POINTS);
            try {
                rc = (int) Native.QUERY_POINTS_EX.invokeExact(d, series, start, end, o, p);
            } catch (Throwable t) {
                throw rethrow(t);
            }
            saveStats();
            if (rc == 0)
                Native.OUTSTANDING.incrementAndGet();
            try {
                if (rc == 0) {
                    long n = p.get(SIZE_T, 16);
                    if (n > Integer.MAX_VALUE - 8)
                        throw new TsdbException(Status.ERR_NOMEM, "more points than one array holds", null);
                    long[] ts = new long[(int) n];
                    float[] v = new float[(int) n];
                    if (n > 0) {
                        MemorySegment.copy(p.get(PTR, 0).reinterpret(n * 8), JAVA_LONG, 0, ts, 0, (int) n);
                        MemorySegment.copy(p.get(PTR, 8).reinterpret(n * 4), JAVA_FLOAT, 0, v, 0, (int) n);
                    }
                    return new Points(ts, v);
                }
            } finally {
                try {
                    Native.POINTS_FREE.invokeExact(p);
                } catch (Throwable t) {
                    throw rethrow(t);
                }
                if (rc == 0)
                    Native.OUTSTANDING.decrementAndGet();
            }
        } finally {
            exit();
        }
        throw new TsdbException(rc, "query", null);
    }

    /**
     * Streams the points of a float32 series in {@code [start, end)}, oldest first, chunk by chunk.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param consumer takes each chunk; the arrays are reused for the next one
     * @return true if every point was delivered, false if the consumer stopped
     * @throws TsdbException with {@link Status#ERR_INVALID} for a series of another type
     */
    public boolean queryRaw(long series, long start, long end, RawConsumer consumer) throws TsdbException {
        return queryRaw(series, start, end, Order.ASC, 0, consumer);
    }

    /**
     * Streams the points of a float32 series in {@code [start, end)} in the given order, at most {@code limit} of
     * them, in chunks of up to {@link #CHUNK} points. Returning false from the consumer stops the query at once.
     * An exception it throws stops the query too and is rethrown here.
     *
     * <p>The consumer runs on the calling thread. It may query the database, but must not close it.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param order by timestamp
     * @param limit at most this many points; 0 for all
     * @param consumer takes each chunk; the arrays are reused for the next one
     * @return true if every point was delivered, false if the consumer stopped
     * @throws TsdbException with {@link Status#ERR_INVALID} for a series of another type
     * @throws IllegalArgumentException for a negative limit
     */
    public boolean queryRaw(long series, long start, long end, Order order, long limit, RawConsumer consumer)
            throws TsdbException {
        Objects.requireNonNull(consumer, "consumer");
        Sink s = Sink.take();
        try {
            s.raw = consumer;
            return stream(s, series, start, end, order, limit, false);
        } finally {
            s.release();
        }
    }

    /**
     * The points of a series of any type in {@code [start, end)}, oldest first, with nulls.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @return the points
     * @throws TsdbException e.g. {@link Status#ERR_NOTFOUND} for an unknown series
     */
    public ValuePoints queryValues(long series, long start, long end) throws TsdbException {
        return queryValues(series, start, end, Order.ASC, 0);
    }

    /**
     * The points of a series of any type in {@code [start, end)} in the given order, at most {@code limit}.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param order by timestamp
     * @param limit at most this many points; 0 for all
     * @return the points
     * @throws TsdbException e.g. {@link Status#ERR_NOTFOUND} for an unknown series
     * @throws IllegalArgumentException for a negative limit
     */
    public ValuePoints queryValues(long series, long start, long end, Order order, long limit) throws TsdbException {
        ValueType type = seriesType(series);
        Sink s = new Sink();
        s.collect = true;
        s.type = type;
        s.room(1); /* allocates the typed array, so an empty result has one too */
        stream(s, series, start, end, order, limit, true);
        int n = s.fill;
        return new ValuePoints(Arrays.copyOf(s.ts, n), s.column(n, true));
    }

    /**
     * Streams the points of a series of any type in {@code [start, end)}, oldest first, chunk by chunk.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param consumer takes each chunk
     * @return true if every point was delivered, false if the consumer stopped
     * @throws TsdbException e.g. {@link Status#ERR_NOTFOUND} for an unknown series
     */
    public boolean queryValues(long series, long start, long end, ValuesConsumer consumer) throws TsdbException {
        return queryValues(series, start, end, Order.ASC, 0, consumer);
    }

    /**
     * Streams the points of a series of any type in {@code [start, end)} in the given order, at most {@code limit}
     * of them, in chunks of up to {@link #CHUNK} points; as
     * {@link #queryRaw(long, long, long, Order, long, RawConsumer)} otherwise.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param order by timestamp
     * @param limit at most this many points; 0 for all
     * @param consumer takes each chunk; its arrays are reused for the next one
     * @return true if every point was delivered, false if the consumer stopped
     * @throws TsdbException e.g. {@link Status#ERR_NOTFOUND} for an unknown series
     * @throws IllegalArgumentException for a negative limit
     */
    public boolean queryValues(long series, long start, long end, Order order, long limit, ValuesConsumer consumer)
            throws TsdbException {
        Objects.requireNonNull(consumer, "consumer");
        Sink s = Sink.take();
        try {
            s.values = consumer;
            return stream(s, series, start, end, order, limit, true);
        } finally {
            s.release();
        }
    }

    // ------------------------------------------------------------------------------------------------ aggregates

    private static Bucket bucket(MemorySegment m, long o) {
        return new Bucket(m.get(JAVA_LONG, o), m.get(JAVA_LONG, o + 8), m.get(JAVA_DOUBLE, o + 16),
                m.get(JAVA_DOUBLE, o + 24), m.get(JAVA_DOUBLE, o + 32), m.get(JAVA_LONG, o + 40),
                m.get(JAVA_LONG, o + 48), m.get(JAVA_LONG, o + 56));
    }

    private static void checkOpts(Order order, long limit) {
        Objects.requireNonNull(order, "order");
        if (limit < 0)
            throw new IllegalArgumentException("negative limit");
    }

    /** Reads and frees the engine's bucket array. */
    private static List<Bucket> buckets(MemorySegment out, MemorySegment nout) {
        MemorySegment b = out.get(PTR, 0);
        long n = nout.get(SIZE_T, 0);
        if (b.address() == 0)
            return new ArrayList<>();
        Native.OUTSTANDING.incrementAndGet();
        try {
            MemorySegment m = b.reinterpret(n * BUCKET.byteSize());
            List<Bucket> list = new ArrayList<>((int) n);
            for (long i = 0, o = 0; i < n; i++, o += 64)
                list.add(new Bucket(m.get(JAVA_LONG, o), m.get(JAVA_LONG, o + 8), m.get(JAVA_DOUBLE, o + 16),
                        m.get(JAVA_DOUBLE, o + 24), m.get(JAVA_DOUBLE, o + 32), m.get(JAVA_LONG, o + 40),
                        m.get(JAVA_LONG, o + 48), m.get(JAVA_LONG, o + 56)));
            return list;
        } finally {
            Native.free(b);
        }
    }

    /**
     * Aggregates a series over {@code [start, end)}: one bucket for {@code intervalMs} 0, else buckets aligned to
     * the epoch. Only buckets with rows are returned, oldest first.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param intervalMs the bucket length, or 0 for one bucket
     * @return the buckets
     * @throws TsdbException e.g. {@link Status#ERR_TOO_MANY_BUCKETS}
     */
    public List<Bucket> queryAgg(long series, long start, long end, long intervalMs) throws TsdbException {
        return queryAgg(series, start, end, intervalMs, Order.ASC, 0);
    }

    /**
     * Aggregates a series over {@code [start, end)} into buckets, in the given order, at most {@code limit}.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param intervalMs the bucket length, or 0 for one bucket
     * @param order by bucket start
     * @param limit at most this many buckets; 0 for all
     * @return the buckets
     * @throws TsdbException e.g. {@link Status#ERR_TOO_MANY_BUCKETS}
     * @throws IllegalArgumentException for a negative limit
     */
    public List<Bucket> queryAgg(long series, long start, long end, long intervalMs, Order order, long limit)
            throws TsdbException {
        checkOpts(order, limit);
        return arrayQuery((d, a, out, nout) -> (int) Native.QUERY_AGG_EX.invokeExact(d, series, start, end,
                intervalMs, opts(a, order, limit), out, nout), BUCKET.byteSize(), EmbeddedDb::bucket);
    }

    /**
     * Aggregates a series over {@code [start, end)} by calendar periods: in UTC by the engine, in another zone by
     * bounds computed here ({@link Group#bounds}), exactly as {@link TsdbClient} does it.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param group the periods, or {@code null} for one bucket over the range
     * @return the buckets with rows, oldest first
     * @throws TsdbException e.g. {@link Status#ERR_TOO_MANY_BUCKETS}
     */
    public List<Bucket> queryGroup(long series, long start, long end, Group group) throws TsdbException {
        return queryGroup(series, start, end, group, Order.ASC, 0);
    }

    /**
     * Aggregates a series over {@code [start, end)} by calendar periods, in the given order, at most {@code limit}.
     * An unbounded range in a time zone is cut to the series' first and last rows first.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param group the periods, or {@code null} for one bucket over the range
     * @param order by bucket start
     * @param limit at most this many buckets; 0 for all
     * @return the buckets with rows
     * @throws TsdbException e.g. {@link Status#ERR_TOO_MANY_BUCKETS}
     * @throws IllegalArgumentException for a negative limit
     */
    public List<Bucket> queryGroup(long series, long start, long end, Group group, Order order, long limit)
            throws TsdbException {
        if (group != null && group.zone() != null && !group.zone().normalized().equals(ZoneOffset.UTC)) {
            Objects.requireNonNull(order, "order");
            if (limit < 0)
                throw new IllegalArgumentException("negative limit");
            long[] bounds = localBounds(series, start, end, group, false);
            return bounds == null ? new ArrayList<>() : queryBounds(series, start, end, bounds, order, limit);
        }
        MemorySegment d = enter();
        int rc;
        try (Arena a = Arena.ofConfined()) {
            MemorySegment o = opts(a, order, limit);
            MemorySegment g = MemorySegment.NULL;
            if (group != null) {
                g = a.allocate(Native.GROUP);
                g.set(JAVA_INT, 0, group.unit().code());
                g.set(JAVA_INT, 4, group.count());
            }
            MemorySegment out = a.allocate(PTR);
            MemorySegment nout = a.allocate(SIZE_T);
            try {
                rc = (int) Native.QUERY_GROUP_EX.invokeExact(d, series, start, end, g, o, out, nout);
            } catch (Throwable t) {
                throw rethrow(t);
            }
            saveStats();
            check(rc, "query");
            return buckets(out, nout);
        } finally {
            exit();
        }
    }

    /**
     * The same grouping done by the engine's own calendar code ({@code tsdb_query_group_local}): for checking that
     * it and {@link Group#bounds} agree.
     */
    List<Bucket> queryGroupNative(long series, long start, long end, Group group, Order order, long limit)
            throws TsdbException {
        MemorySegment d = enter();
        int rc;
        try (Arena a = Arena.ofConfined()) {
            MemorySegment o = opts(a, order, limit);
            MemorySegment g = a.allocate(Native.GROUP);
            g.set(JAVA_INT, 0, group.unit().code());
            g.set(JAVA_INT, 4, group.count());
            g.set(PTR, 8, group.zone() == null ? MemorySegment.NULL : a.allocateFrom(group.zone().getId()));
            MemorySegment out = a.allocate(PTR);
            MemorySegment nout = a.allocate(SIZE_T);
            try {
                rc = (int) Native.QUERY_GROUP_LOCAL.invokeExact(d, series, start, end, g, o, out, nout);
            } catch (Throwable t) {
                throw rethrow(t);
            }
            check(rc, "query");
            return buckets(out, nout);
        } finally {
            exit();
        }
    }

    /** {@code tsdb_calendar_bounds}: the engine's bounds, to compare with {@link Group#bounds}. */
    static long[] calendarBoundsNative(String zone, int unit, int count, long start, long end) throws TsdbException {
        checkCString(zone, "zone");
        try (Arena a = Arena.ofConfined()) {
            MemorySegment out = a.allocate(PTR);
            MemorySegment nout = a.allocate(SIZE_T);
            int rc;
            try {
                MemorySegment z = zone == null ? MemorySegment.NULL : a.allocateFrom(zone);
                rc = (int) Native.CALENDAR_BOUNDS.invokeExact(z, unit, count, start, end, out, nout);
            } catch (Throwable t) {
                throw rethrow(t);
            }
            check(rc, "calendar bounds");
            MemorySegment b = out.get(PTR, 0);
            long n = nout.get(SIZE_T, 0);
            if (b.address() == 0)
                return new long[0];
            Native.OUTSTANDING.incrementAndGet();
            try {
                return b.reinterpret(n * 8).toArray(JAVA_LONG);
            } finally {
                Native.free(b);
            }
        }
    }

    /**
     * Aggregates a series over {@code [start, end)} in buckets that begin at the given UTC instants: bucket
     * {@code i} holds the rows in {@code [bounds[i], bounds[i + 1])}, the last one every row from
     * {@code bounds[n - 1]} on. Only buckets with rows are returned, oldest first.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param bounds the bucket starts, strictly increasing; at least one
     * @return the buckets with rows
     * @throws TsdbException with {@link Status#ERR_INVALID} if the bounds do not strictly increase
     * @throws IllegalArgumentException if there are no bounds
     */
    public List<Bucket> queryBounds(long series, long start, long end, long[] bounds) throws TsdbException {
        return queryBounds(series, start, end, bounds, Order.ASC, 0);
    }

    /**
     * Aggregates a series over {@code [start, end)} in buckets that begin at the given UTC instants, in the given
     * order, at most {@code limit}.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param bounds the bucket starts, strictly increasing; at least one
     * @param order by bucket start
     * @param limit at most this many buckets; 0 for all
     * @return the buckets with rows
     * @throws TsdbException with {@link Status#ERR_INVALID} if the bounds do not strictly increase
     * @throws IllegalArgumentException if there are no bounds, or for a negative limit
     */
    public List<Bucket> queryBounds(long series, long start, long end, long[] bounds, Order order, long limit)
            throws TsdbException {
        if (bounds.length == 0)
            throw new IllegalArgumentException("no bounds");
        MemorySegment d = enter();
        int rc;
        try (Arena a = Arena.ofConfined()) {
            MemorySegment o = opts(a, order, limit);
            MemorySegment b = a.allocateFrom(JAVA_LONG, bounds);
            MemorySegment out = a.allocate(PTR);
            MemorySegment nout = a.allocate(SIZE_T);
            try {
                rc = (int) Native.QUERY_BOUNDS.invokeExact(d, series, start, end, b, (long) bounds.length, o, out,
                        nout);
            } catch (Throwable t) {
                throw rethrow(t);
            }
            saveStats();
            check(rc, "query");
            return buckets(out, nout);
        } finally {
            exit();
        }
    }

    // ------------------------------------------------------------------------------------------------ the spread

    /** An engine call that returns an array through {@code out} and {@code nout}. */
    private interface ArrayCall {
        int invoke(MemorySegment d, Arena a, MemorySegment out, MemorySegment nout) throws Throwable;
    }

    /** One element of an engine array at byte offset {@code o}. */
    private interface Element<T> {
        T read(MemorySegment m, long o);
    }

    /** Runs {@code call} under the read lock and reads and frees the array it returned. */
    private <T> List<T> arrayQuery(ArrayCall call, long size, Element<T> element) throws TsdbException {
        MemorySegment d = enter();
        int rc;
        try (Arena a = Arena.ofConfined()) {
            MemorySegment out = a.allocate(PTR);
            MemorySegment nout = a.allocate(SIZE_T);
            try {
                rc = call.invoke(d, a, out, nout);
            } catch (Throwable t) {
                throw rethrow(t);
            }
            saveStats();
            check(rc, "query");
            MemorySegment b = out.get(PTR, 0);
            long n = nout.get(SIZE_T, 0);
            if (b.address() == 0)
                return new ArrayList<>();
            Native.OUTSTANDING.incrementAndGet();
            try {
                MemorySegment m = b.reinterpret(n * size);
                List<T> list = new ArrayList<>((int) n);
                for (long i = 0; i < n; i++)
                    list.add(element.read(m, i * size));
                return list;
            } finally {
                Native.free(b);
            }
        } finally {
            exit();
        }
    }

    private static SpreadBucket spreadBucket(MemorySegment m, long o) {
        Bucket b = new Bucket(m.get(JAVA_LONG, o), m.get(JAVA_LONG, o + 8), m.get(JAVA_DOUBLE, o + 16),
                m.get(JAVA_DOUBLE, o + 24), m.get(JAVA_DOUBLE, o + 32), m.get(JAVA_LONG, o + 40),
                m.get(JAVA_LONG, o + 48), m.get(JAVA_LONG, o + 56));
        return new SpreadBucket(b, m.get(JAVA_DOUBLE, o + 64), m.get(JAVA_DOUBLE, o + 72), m.get(JAVA_DOUBLE, o + 80),
                m.get(JAVA_DOUBLE, o + 88), m.get(JAVA_DOUBLE, o + 96));
    }

    private static TwaBucket twaBucket(MemorySegment m, long o) {
        return new TwaBucket(m.get(JAVA_LONG, o), m.get(JAVA_DOUBLE, o + 8), m.get(JAVA_DOUBLE, o + 16),
                m.get(JAVA_LONG, o + 24), m.get(JAVA_LONG, o + 32));
    }

    /**
     * True for a group in a zone other than UTC. Its periods become bounds here, with {@link Group#bounds}
     * (java.time), as in {@link #queryGroup(long, long, long, Group, Order, long)} and {@link TsdbClient}: the
     * engine's calendar reads the system's tzdata and knows other zone ids than the JDK ("UTC+2", "GMT-05:30"), so
     * handing it the zone would group differently from those.
     */
    private static boolean local(Group g) {
        return g != null && g.zone() != null && !g.zone().normalized().equals(ZoneOffset.UTC);
    }

    /** A {@code tsdb_group} in UTC; NULL for no group. */
    private static MemorySegment group(Arena a, Group g) {
        if (g == null)
            return MemorySegment.NULL;
        MemorySegment s = a.allocate(Native.GROUP);
        s.set(JAVA_INT, 0, g.unit().code());
        s.set(JAVA_INT, 4, g.count());
        s.set(PTR, 8, MemorySegment.NULL);
        return s;
    }

    /**
     * The UTC instants at which the local periods of {@code group} begin, for a query over {@code [start, end)};
     * {@code null} if there is nothing to ask for. As {@code TsdbClient.localBounds}: the aggregates cut the range
     * to the first and last row there is; the time-weighted averages ({@code bounded}) cover a bounded side to its
     * end, since a reading before the range carries into it, and cut only an open side.
     */
    private long[] localBounds(long series, long start, long end, Group group, boolean bounded) throws TsdbException {
        long lo = start, hi = end == Long.MAX_VALUE ? Long.MIN_VALUE : end - 1;
        if (!bounded || start == Long.MIN_VALUE) {
            ValuePoints first = queryValues(series, start, end, Order.ASC, 1);
            if (first.size() == 0)
                return null;
            lo = first.timestamps()[0];
        }
        if (!bounded || end == Long.MAX_VALUE) {
            ValuePoints last = queryValues(series, start, end, Order.DESC, 1);
            if (last.size() == 0)
                return null;
            hi = last.timestamps()[0];
        }
        if (lo > hi)
            return null;
        /* the newest row may sit at Long.MAX_VALUE: then the range is beyond what bounds cover anyway */
        long[] bounds = group.bounds(lo, hi == Long.MAX_VALUE ? Long.MAX_VALUE : hi + 1);
        return bounds.length == 0 ? null : bounds;
    }

    private static void checkBounds(long[] bounds) {
        if (Objects.requireNonNull(bounds, "bounds").length == 0)
            throw new IllegalArgumentException("no bounds");
    }

    /**
     * Aggregates a series over {@code [start, end)} in fixed intervals, oldest first, with the spread of each bucket:
     * range, population and sample variance and standard deviation. As {@link TsdbClient#querySpread(long, long,
     * long, long)}.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param intervalMs the bucket length, or 0 for one bucket
     * @return the buckets with rows
     * @throws TsdbException e.g. {@link Status#ERR_INVALID} for a negative interval
     */
    public List<SpreadBucket> querySpread(long series, long start, long end, long intervalMs) throws TsdbException {
        return querySpread(series, start, end, intervalMs, Order.ASC, 0);
    }

    /**
     * As {@link #querySpread(long, long, long, long)}, in the given order, at most {@code limit} buckets.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param intervalMs the bucket length, or 0 for one bucket
     * @param order by bucket start
     * @param limit at most this many buckets; 0 for all
     * @return the buckets with rows
     * @throws TsdbException e.g. {@link Status#ERR_INVALID} for a negative interval
     * @throws IllegalArgumentException for a negative limit
     */
    public List<SpreadBucket> querySpread(long series, long start, long end, long intervalMs, Order order, long limit)
            throws TsdbException {
        checkOpts(order, limit);
        return arrayQuery((d, a, out, nout) -> (int) Native.QUERY_SPREAD.invokeExact(d, series, start, end,
                intervalMs, opts(a, order, limit), out, nout), Native.SPREAD_BUCKET.byteSize(),
                EmbeddedDb::spreadBucket);
    }

    /**
     * The spread in calendar buckets, as {@link #queryGroup(long, long, long, Group, Order, long)} groups; the
     * periods of a zone other than UTC become bounds here ({@link Group#bounds}) over the rows there are.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param group the periods, or {@code null} for one bucket over the range
     * @param order by bucket start
     * @param limit at most this many buckets; 0 for all
     * @return the buckets with rows
     * @throws TsdbException e.g. {@link Status#ERR_TOO_MANY_BUCKETS}
     * @throws IllegalArgumentException for a negative limit
     */
    public List<SpreadBucket> querySpreadGroup(long series, long start, long end, Group group, Order order,
                                               long limit) throws TsdbException {
        checkOpts(order, limit);
        if (local(group)) {
            long[] bounds = localBounds(series, start, end, group, false);
            return bounds == null ? new ArrayList<>() : querySpreadBounds(series, start, end, bounds, order, limit);
        }
        return arrayQuery((d, a, out, nout) -> (int) Native.QUERY_SPREAD_GROUP.invokeExact(d, series, start, end,
                group(a, group), opts(a, order, limit), out, nout), Native.SPREAD_BUCKET.byteSize(),
                EmbeddedDb::spreadBucket);
    }

    /**
     * The spread in buckets that begin at the given UTC instants, as
     * {@link #queryBounds(long, long, long, long[], Order, long)}.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param bounds the bucket starts, strictly increasing; at least one
     * @param order by bucket start
     * @param limit at most this many buckets; 0 for all
     * @return the buckets with rows
     * @throws TsdbException with {@link Status#ERR_INVALID} if the bounds do not strictly increase
     * @throws IllegalArgumentException if there are no bounds, or for a negative limit
     */
    public List<SpreadBucket> querySpreadBounds(long series, long start, long end, long[] bounds, Order order,
                                                long limit) throws TsdbException {
        checkBounds(bounds);
        checkOpts(order, limit);
        return arrayQuery((d, a, out, nout) -> (int) Native.QUERY_SPREAD_BOUNDS.invokeExact(d, series, start, end,
                a.allocateFrom(JAVA_LONG, bounds), (long) bounds.length, opts(a, order, limit), out, nout),
                Native.SPREAD_BUCKET.byteSize(), EmbeddedDb::spreadBucket);
    }

    // ------------------------------------------------------------------------------------- time-weighted averages

    private static MemorySegment twaOpts(Arena a, TwaOptions o) {
        MemorySegment s = a.allocate(Native.TWA_OPTS).fill((byte) 0);
        s.set(JAVA_INT, 0, o.method() == TwaMethod.LINEAR ? 1 : 0);
        s.set(JAVA_LONG, 8, o.maxGapMs());
        s.set(JAVA_INT, 16, o.order() == Order.DESC ? 1 : 0);
        s.set(SIZE_T, 24, o.limit());
        return s;
    }

    /**
     * Time-weighted averages of a series over {@code [start, end)} in fixed intervals: each reading weighted by how
     * long it held. As {@link TsdbClient#queryTwa(long, long, long, long, TwaOptions)}.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param intervalMs the bucket length, or 0 for one bucket starting at {@code start}
     * @param opts method, gap limit, order and limit
     * @return the buckets with readings or coverage
     * @throws TsdbException with {@link Status#ERR_INVALID} for a negative interval or a state series
     */
    public List<TwaBucket> queryTwa(long series, long start, long end, long intervalMs, TwaOptions opts)
            throws TsdbException {
        Objects.requireNonNull(opts, "opts");
        return arrayQuery((d, a, out, nout) -> (int) Native.QUERY_TWA.invokeExact(d, series, start, end, intervalMs,
                twaOpts(a, opts), out, nout), Native.TWA_BUCKET.byteSize(), EmbeddedDb::twaBucket);
    }

    /**
     * Time-weighted averages in calendar buckets; the periods of a zone other than UTC become bounds here
     * ({@link Group#bounds}) over {@code [start, end)}, an open side cut to the rows there are.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param group the periods, or {@code null} for one bucket over the range
     * @param opts method, gap limit, order and limit
     * @return the buckets with readings or coverage
     * @throws TsdbException with {@link Status#ERR_INVALID} for a state series
     */
    public List<TwaBucket> queryTwaGroup(long series, long start, long end, Group group, TwaOptions opts)
            throws TsdbException {
        Objects.requireNonNull(opts, "opts");
        if (local(group)) {
            long[] bounds = localBounds(series, start, end, group, true);
            return bounds == null ? new ArrayList<>() : queryTwaBounds(series, start, end, bounds, opts);
        }
        return arrayQuery((d, a, out, nout) -> (int) Native.QUERY_TWA_GROUP.invokeExact(d, series, start, end,
                group(a, group), twaOpts(a, opts), out, nout), Native.TWA_BUCKET.byteSize(), EmbeddedDb::twaBucket);
    }

    /**
     * Time-weighted averages in buckets that begin at the given UTC instants; segments before {@code bounds[0]}
     * do not count.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param bounds the bucket starts, strictly increasing; at least one
     * @param opts method, gap limit, order and limit
     * @return the buckets with readings or coverage
     * @throws TsdbException with {@link Status#ERR_INVALID} if the bounds do not increase or for a state series
     * @throws IllegalArgumentException if there are no bounds
     */
    public List<TwaBucket> queryTwaBounds(long series, long start, long end, long[] bounds, TwaOptions opts)
            throws TsdbException {
        Objects.requireNonNull(opts, "opts");
        checkBounds(bounds);
        return arrayQuery((d, a, out, nout) -> (int) Native.QUERY_TWA_BOUNDS.invokeExact(d, series, start, end,
                a.allocateFrom(JAVA_LONG, bounds), (long) bounds.length, twaOpts(a, opts), out, nout),
                Native.TWA_BUCKET.byteSize(), EmbeddedDb::twaBucket);
    }

    // ------------------------------------------------------------------------------------------------ LTTB

    /**
     * Downsamples a series for a chart: at most {@code threshold} points of {@code [start, end)} that keep the shape
     * of the line (Largest-Triangle-Three-Buckets), oldest first, with their stored values. As
     * {@link TsdbClient#queryLttb(long, long, long, long)}.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param threshold the most points to keep, at least 3
     * @return the chosen points
     * @throws TsdbException with {@link Status#ERR_INVALID} for a threshold below 3 or a state series; an unknown
     *     series has no points, as over TCP
     */
    public ValuePoints queryLttb(long series, long start, long end, long threshold) throws TsdbException {
        return queryLttb(series, start, end, threshold, Order.ASC, 0);
    }

    /**
     * As {@link #queryLttb(long, long, long, long)}, newest first with {@link Order#DESC} and only the first
     * {@code limit} of the chosen points in that order; the choice does not change.
     *
     * @param series the series id
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param threshold the most points to keep, at least 3
     * @param order oldest or newest first
     * @param limit the most points to return; 0 for all
     * @return the chosen points
     * @throws TsdbException as {@link #queryLttb(long, long, long, long)}
     * @throws IllegalArgumentException for a negative limit
     */
    public ValuePoints queryLttb(long series, long start, long end, long threshold, Order order, long limit)
            throws TsdbException {
        checkOpts(order, limit);
        /* as tsdbd and the Rust Db: the threshold is checked first, and an unknown series has no points */
        if (Long.compareUnsigned(threshold, 3) < 0) /* a size_t: -1 is a huge threshold, as over TCP */
            throw new TsdbException(Status.ERR_INVALID, "an LTTB threshold below 3", null);
        ValueType type;
        try {
            type = seriesType(series);
        } catch (TsdbException e) {
            if (e.status() != Status.ERR_NOTFOUND)
                throw e;
            return new ValuePoints(new long[0], Column.ofFloats());
        }
        Sink s = new Sink();
        s.collect = true;
        s.type = type;
        s.room(1);
        stream(s, series, start, end, order, limit, LTTB, threshold);
        int n = s.fill;
        return new ValuePoints(Arrays.copyOf(s.ts, n), s.column(n, true));
    }

    // ------------------------------------------------------------------------------------------- many series

    /*
     * Many series in one call: tsdb_query_multi_collect, one downcall that answers every series (on the database's
     * query threads) and returns one result per series. Calendar periods in a zone other than UTC go as in
     * TsdbClient: two VALUES probes give the first and last row of every series, Group.bounds (java.time) one union
     * of bounds, and the engine trims it per series (TSDB_MULTI_TRIM_*), so the periods are those of the single
     * calls; TWA with closed range sides that differ between series goes series by series.
     */

    /** One call of tsdb_query_multi_collect. */
    private static final class MultiCall {
        final int kind;
        final long start, end;
        Order order = Order.ASC;
        long limit;
        long interval;
        long[] intervals;
        Group group; /* UTC only */
        long[] bounds;
        int trim;
        long threshold;
        TwaOptions twa;

        MultiCall(int kind, long start, long end) {
            this.kind = kind;
            this.start = start;
            this.end = end;
        }
    }

    /** The result of one series, from its tsdb_series_result (status 0). */
    private interface SeriesReader<T> {
        T read(MemorySegment r);
    }

    private static final long RESULT = Native.SERIES_RESULT.byteSize();

    private static MemorySegment longs(Arena a, long[] v) {
        return v == null ? MemorySegment.NULL : a.allocateFrom(JAVA_LONG, v);
    }

    private <T> List<SeriesResult<T>> multiCollect(MultiCall q, long[] series, MultiOptions m, SeriesReader<T> reader)
            throws TsdbException {
        int n = series.length;
        MemorySegment d = enter();
        try (Arena a = Arena.ofConfined()) {
            MemorySegment qs = a.allocate(Native.MULTI_QUERY).fill((byte) 0);
            qs.set(JAVA_INT, Native.offset(Native.MULTI_QUERY, "kind"), q.kind);
            qs.set(JAVA_LONG, Native.offset(Native.MULTI_QUERY, "start"), q.start);
            qs.set(JAVA_LONG, Native.offset(Native.MULTI_QUERY, "end"), q.end);
            qs.set(PTR, Native.offset(Native.MULTI_QUERY, "starts"), longs(a, m.starts()));
            qs.set(PTR, Native.offset(Native.MULTI_QUERY, "ends"), longs(a, m.ends()));
            MemorySegment.copy(opts(a, q.order, q.limit), 0, qs, Native.offset(Native.MULTI_QUERY, "opts"),
                    Native.QUERY_OPTS.byteSize());
            qs.set(JAVA_LONG, Native.offset(Native.MULTI_QUERY, "interval_ms"), q.interval);
            qs.set(PTR, Native.offset(Native.MULTI_QUERY, "intervals"), longs(a, q.intervals));
            qs.set(PTR, Native.offset(Native.MULTI_QUERY, "group"), group(a, q.group));
            qs.set(PTR, Native.offset(Native.MULTI_QUERY, "bounds"), longs(a, q.bounds));
            qs.set(SIZE_T, Native.offset(Native.MULTI_QUERY, "nbounds"), q.bounds == null ? 0 : q.bounds.length);
            qs.set(JAVA_INT, Native.offset(Native.MULTI_QUERY, "trim"), q.trim);
            qs.set(SIZE_T, Native.offset(Native.MULTI_QUERY, "threshold"), q.threshold);
            if (q.twa != null)
                MemorySegment.copy(twaOpts(a, q.twa), 0, qs, Native.offset(Native.MULTI_QUERY, "twa"),
                        Native.TWA_OPTS.byteSize());
            MemorySegment ids = n == 0 ? MemorySegment.NULL : a.allocateFrom(JAVA_LONG, series);
            MemorySegment mo = a.allocate(Native.MULTI_OPTS).fill((byte) 0);
            mo.set(JAVA_INT, Native.offset(Native.MULTI_OPTS, "threads"), m.threads());
            mo.set(JAVA_LONG, Native.offset(Native.MULTI_OPTS, "max_rows"), m.maxRows());
            MemorySegment out = a.allocate(PTR);
            out.set(PTR, 0, MemorySegment.NULL);
            int rc;
            try {
                rc = (int) Native.QUERY_MULTI_COLLECT.invokeExact(d, qs, ids, (long) n, mo, out);
            } catch (Throwable t) {
                throw rethrow(t);
            }
            saveStats();
            MemorySegment r = out.get(PTR, 0);
            try {
                check(rc, "query");
                MemorySegment all = r.reinterpret(n * RESULT);
                List<SeriesResult<T>> list = new ArrayList<>(n);
                for (int i = 0; i < n; i++) {
                    MemorySegment one = all.asSlice(i * RESULT, RESULT);
                    int st = one.get(JAVA_INT, Native.offset(Native.SERIES_RESULT, "status"));
                    list.add(st != 0 ? SeriesResult.failed(st, null) : SeriesResult.of(reader.read(one)));
                }
                return list;
            } finally {
                if (r.address() != 0) {
                    try {
                        Native.SERIES_RESULTS_FREE.invokeExact(r, (long) n);
                    } catch (Throwable t) {
                        throw rethrow(t);
                    }
                }
            }
        } finally {
            exit();
        }
    }

    private static ValuePoints readValues(MemorySegment r) {
        int n = (int) r.get(SIZE_T, Native.offset(Native.SERIES_RESULT, "n"));
        long vo = Native.offset(Native.SERIES_RESULT, "values");
        ValueType t = typeOf(r.get(JAVA_INT, vo));
        long[] ts = new long[n];
        if (n == 0)
            return null; /* typed afterwards: the engine sends no type with no rows */
        MemorySegment.copy(r.get(PTR, Native.offset(Native.SERIES_RESULT, "ts")).reinterpret(n * 8L), JAVA_LONG, 0,
                ts, 0, n);
        MemorySegment vm = r.get(PTR, vo + 8);
        Column c;
        switch (t) {
        case FLOAT32: {
            float[] v = new float[n];
            MemorySegment.copy(vm.reinterpret(n * 4L), JAVA_FLOAT, 0, v, 0, n);
            c = Column.ofFloats(v);
            break;
        }
        case FLOAT64: {
            double[] v = new double[n];
            MemorySegment.copy(vm.reinterpret(n * 8L), JAVA_DOUBLE, 0, v, 0, n);
            c = Column.ofDoubles(v);
            break;
        }
        case INT64: {
            long[] v = new long[n];
            MemorySegment.copy(vm.reinterpret(n * 8L), JAVA_LONG, 0, v, 0, n);
            c = Column.ofLongs(v);
            break;
        }
        case BOOL: {
            boolean[] v = new boolean[n];
            MemorySegment b = vm.reinterpret(n);
            for (int i = 0; i < n; i++)
                v[i] = b.get(JAVA_BYTE, i) != 0;
            c = Column.ofBools(v);
            break;
        }
        default: {
            int[] v = new int[n];
            MemorySegment.copy(vm.reinterpret(n * 4L), JAVA_INT, 0, v, 0, n);
            c = Column.ofStates(v);
        }
        }
        MemorySegment valid = r.get(PTR, vo + 16);
        if (valid.address() != 0) {
            MemorySegment bits = valid.reinterpret((n + 7) >>> 3);
            BitSet nulls = new BitSet();
            for (int i = 0; i < n; i++)
                if ((bits.get(JAVA_BYTE, i >>> 3) >>> (i & 7) & 1) == 0)
                    nulls.set(i);
            if (!nulls.isEmpty())
                c = c.withNulls(nulls);
        }
        return new ValuePoints(ts, c);
    }

    private static Column emptyColumn(ValueType t) {
        switch (t) {
        case FLOAT32: return Column.ofFloats();
        case FLOAT64: return Column.ofDoubles();
        case INT64: return Column.ofLongs();
        case BOOL: return Column.ofBools();
        default: return Column.ofStates();
        }
    }

    private static <T> SeriesReader<List<T>> bucketsOf(long size, Element<T> element) {
        return r -> {
            int n = (int) r.get(SIZE_T, Native.offset(Native.SERIES_RESULT, "n"));
            List<T> list = new ArrayList<>(n);
            if (n > 0) {
                MemorySegment m = r.get(PTR, Native.offset(Native.SERIES_RESULT, "buckets")).reinterpret(n * size);
                for (int i = 0; i < n; i++)
                    list.add(element.read(m, i * size));
            }
            return Collections.unmodifiableList(list);
        };
    }

    private static void checkMulti(long[] series, MultiOptions m) {
        Objects.requireNonNull(series, "series");
        Objects.requireNonNull(m, "options");
        if (m.starts() != null && m.starts().length != series.length)
            throw new IllegalArgumentException(m.starts().length + " starts for " + series.length + " series");
        if (m.ends() != null && m.ends().length != series.length)
            throw new IllegalArgumentException(m.ends().length + " ends for " + series.length + " series");
    }

    private List<SeriesResult<ValuePoints>> rowsMulti(MultiCall q, long[] series, MultiOptions m)
            throws TsdbException {
        checkOpts(q.order, q.limit);
        checkMulti(series, m);
        List<SeriesResult<ValuePoints>> r = multiCollect(q, series, m, EmbeddedDb::readValues);
        Map<Long, ValueType> types = new HashMap<>();
        for (int i = 0; i < r.size(); i++) {
            if (!r.get(i).ok() || r.get(i).get() != null)
                continue;
            ValueType t = types.get(series[i]);
            if (t == null) {
                try {
                    t = seriesType(series[i]);
                } catch (TsdbException e) {
                    if (e.status() != Status.ERR_NOTFOUND)
                        throw e;
                    t = ValueType.FLOAT32; /* an unknown id has no rows, as over TCP */
                }
                types.put(series[i], t);
            }
            r.set(i, SeriesResult.of(new ValuePoints(new long[0], emptyColumn(t))));
        }
        return Collections.unmodifiableList(r);
    }

    /**
     * The rows of every series in {@code series} over {@code [start, end)} (or each series' own range from
     * {@code m}), as {@link #queryValues(long, long, long, Order, long)} gives them for one, in one call, on up to
     * {@code m.threads()} threads ({@link EmbeddedOptions#requestThreads}). As
     * {@link TsdbClient#queryValuesMulti(long[], long, long, Order, long, MultiOptions)}: every series reads the
     * same instant of each volume, an unknown id has no rows.
     *
     * @param series the series ids; an id may come more than once
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param order the order of each series' rows
     * @param limit rows per series at most (0: all)
     * @param m threads, a cap on all rows, per-series ranges
     * @return one result per series, in the order of {@code series}
     * @throws TsdbException {@link Status#ERR_TOO_MANY_BUCKETS} past {@code m.maxRows()} rows in all
     */
    public List<SeriesResult<ValuePoints>> queryValuesMulti(long[] series, long start, long end, Order order,
                                                            long limit, MultiOptions m) throws TsdbException {
        MultiCall q = new MultiCall(Native.MULTI_VALUES, start, end);
        q.order = order;
        q.limit = limit;
        return rowsMulti(q, series, m);
    }

    /**
     * LTTB of every series in {@code series}, as {@link #queryLttb(long, long, long, long, Order, long)} gives it for
     * one, in one call. As {@link TsdbClient#queryLttbMulti}.
     *
     * @param series the series ids
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param threshold points per series to keep
     * @param order the order of each series' points
     * @param limit points per series at most (0: all)
     * @param m threads, a cap on all rows, per-series ranges
     * @return one result per series, in the order of {@code series}
     * @throws TsdbException {@link Status#ERR_TOO_MANY_BUCKETS} past {@code m.maxRows()} rows in all
     */
    public List<SeriesResult<ValuePoints>> queryLttbMulti(long[] series, long start, long end, long threshold,
                                                          Order order, long limit, MultiOptions m)
            throws TsdbException {
        MultiCall q = new MultiCall(Native.MULTI_LTTB, start, end);
        q.order = order;
        q.limit = limit;
        q.threshold = threshold;
        return rowsMulti(q, series, m);
    }

    /**
     * Buckets of every series in {@code series}, as {@link #queryAgg}, {@link #queryGroup} or {@link #queryBounds}
     * give them for one, in one call. As {@link TsdbClient#queryBucketsMulti}.
     *
     * @param series the series ids
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param b the buckets
     * @param order the order of each series' buckets
     * @param limit buckets per series at most (0: all)
     * @param m threads, a cap on all buckets, per-series ranges
     * @return one result per series, in the order of {@code series}
     * @throws TsdbException {@link Status#ERR_INVALID} for a negative {@link Buckets#every} interval,
     *     {@link Status#ERR_TOO_MANY_BUCKETS} past {@code m.maxRows()} buckets in all
     */
    public List<SeriesResult<List<Bucket>>> queryBucketsMulti(long[] series, long start, long end, Buckets b,
                                                              Order order, long limit, MultiOptions m)
            throws TsdbException {
        MultiCall q = new MultiCall(Native.MULTI_BUCKETS, start, end);
        q.order = order;
        q.limit = limit;
        return bucketMulti(q, b, series, m, bucketsOf(BUCKET.byteSize(), EmbeddedDb::bucket),
                (s, lo, hi, g) -> queryGroup(s, lo, hi, g, order, limit));
    }

    /**
     * Buckets with the spread of every series in {@code series}, as {@link #querySpread} gives them for one, in
     * one call. As {@link TsdbClient#querySpreadMulti}.
     *
     * @param series the series ids
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param b the buckets
     * @param order the order of each series' buckets
     * @param limit buckets per series at most (0: all)
     * @param m threads, a cap on all buckets, per-series ranges
     * @return one result per series, in the order of {@code series}
     * @throws TsdbException as {@link #queryBucketsMulti}
     */
    public List<SeriesResult<List<SpreadBucket>>> querySpreadMulti(long[] series, long start, long end, Buckets b,
                                                                   Order order, long limit, MultiOptions m)
            throws TsdbException {
        MultiCall q = new MultiCall(Native.MULTI_SPREAD, start, end);
        q.order = order;
        q.limit = limit;
        return bucketMulti(q, b, series, m, bucketsOf(Native.SPREAD_BUCKET.byteSize(), EmbeddedDb::spreadBucket),
                (s, lo, hi, g) -> querySpreadGroup(s, lo, hi, g, order, limit));
    }

    /**
     * Time-weighted averages of every series in {@code series}, as {@link #queryTwa} gives them for one, in one
     * call. As {@link TsdbClient#queryTwaMulti}.
     *
     * @param series the series ids
     * @param start the first timestamp included
     * @param end the first timestamp excluded
     * @param b the buckets
     * @param opts the method, the longest gap, order and limit per series
     * @param m threads, a cap on all buckets, per-series ranges
     * @return one result per series, in the order of {@code series}
     * @throws TsdbException as {@link #queryBucketsMulti}
     */
    public List<SeriesResult<List<TwaBucket>>> queryTwaMulti(long[] series, long start, long end, Buckets b,
                                                             TwaOptions opts, MultiOptions m) throws TsdbException {
        Objects.requireNonNull(opts, "opts");
        MultiCall q = new MultiCall(Native.MULTI_TWA, start, end);
        q.twa = opts;
        return bucketMulti(q, b, series, m, bucketsOf(Native.TWA_BUCKET.byteSize(), EmbeddedDb::twaBucket),
                (s, lo, hi, g) -> queryTwaGroup(s, lo, hi, g, opts));
    }

    /** The single call of a bucket kind for one series and a local group. */
    private interface SingleGroup<T> {
        List<T> call(long series, long start, long end, Group g) throws TsdbException;
    }

    private <T> List<SeriesResult<List<T>>> bucketMulti(MultiCall q, Buckets b, long[] series, MultiOptions m,
                                                        SeriesReader<List<T>> reader, SingleGroup<T> single)
            throws TsdbException {
        Objects.requireNonNull(b, "buckets");
        boolean twa = q.kind == Native.MULTI_TWA;
        if (twa)
            checkOpts(q.twa.order(), q.twa.limit());
        else
            checkOpts(q.order, q.limit);
        checkMulti(series, m);
        switch (b.kind()) {
        case EVERY:
            if (b.interval() < 0)
                throw new TsdbException(Status.ERR_INVALID, "negative interval", null);
            q.interval = b.interval();
            break;
        case INTERVALS:
            if (b.intervals().length != series.length)
                throw new IllegalArgumentException(b.intervals().length + " intervals for " + series.length
                        + " series");
            q.intervals = b.intervals();
            break;
        case BOUNDS:
            checkBounds(b.bounds());
            q.bounds = b.bounds();
            break;
        case GROUP:
            if (local(b.group()))
                return Collections.unmodifiableList(multiLocal(q, b.group(), series, m, reader, single));
            q.group = b.group();
            break;
        default:
            break;
        }
        if (series.length == 0)
            return List.of();
        return Collections.unmodifiableList(multiCollect(q, series, m, reader));
    }

    /** Calendar periods in a zone: one union of bounds, trimmed per series by the engine. */
    private <T> List<SeriesResult<List<T>>> multiLocal(MultiCall q, Group g, long[] series, MultiOptions m,
                                                       SeriesReader<List<T>> reader, SingleGroup<T> single)
            throws TsdbException {
        int n = series.length;
        List<SeriesResult<List<T>>> out = new ArrayList<>(n);
        if (n == 0)
            return out;
        boolean twa = q.kind == Native.MULTI_TWA;
        long[] starts = m.starts(), ends = m.ends();
        boolean us = uniform(starts), ue = uniform(ends);
        long s0 = starts != null ? starts[0] : q.start, e0 = ends != null ? ends[0] : q.end;
        /* buckets are cut to the rows on both sides; TWA only on open sides (a closed one is the range's edge) */
        boolean openLo = !twa || (us && s0 == Long.MIN_VALUE);
        boolean openHi = !twa || (ue && e0 == Long.MAX_VALUE);
        long[] bounds = null;
        if (!twa || !((!us && !openLo) || (!ue && !openHi))) {
            long l = s0, h = e0 == Long.MIN_VALUE ? Long.MIN_VALUE : e0 - 1;
            MultiOptions pm = new MultiOptions(m.threads(), 0, starts, ends);
            for (int side = 0; side < 2; side++) {
                if (side == 0 ? !openLo : !openHi)
                    continue;
                MultiCall vq = new MultiCall(Native.MULTI_VALUES, q.start, q.end);
                vq.order = side == 0 ? Order.ASC : Order.DESC;
                vq.limit = 1;
                long edge = side == 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
                for (SeriesResult<ValuePoints> r : multiCollect(vq, series, pm, EmbeddedDb::readValues)) {
                    ValuePoints p = r.ok() ? r.get() : null;
                    if (p != null && p.size() > 0)
                        edge = side == 0 ? Math.min(edge, p.timestamps()[0]) : Math.max(edge, p.timestamps()[0]);
                }
                if (side == 0)
                    l = edge;
                else
                    h = edge;
            }
            if (l > h) { /* no rows anywhere (or an empty range): no buckets, as the single call answers */
                for (int i = 0; i < n; i++)
                    out.add(SeriesResult.of(List.of()));
                return out;
            }
            try {
                bounds = g.bounds(l, h == Long.MAX_VALUE ? Long.MAX_VALUE : h + 1);
            } catch (TsdbException e) {
                if (e.status() != Status.ERR_TOO_MANY_BUCKETS)
                    throw e;
            }
        }
        if (bounds == null) { /* series by series through the single calls */
            long rows = 0;
            for (int i = 0; i < n; i++) {
                try {
                    List<T> v = single.call(series[i], starts != null ? starts[i] : q.start,
                            ends != null ? ends[i] : q.end, g);
                    rows += v.size();
                    if (m.maxRows() != 0 && rows > m.maxRows())
                        throw new TsdbException(Status.ERR_TOO_MANY_BUCKETS);
                    out.add(SeriesResult.of(v));
                } catch (TsdbException e) {
                    if (m.maxRows() != 0 && rows > m.maxRows())
                        throw e;
                    out.add(SeriesResult.failed(e.status(), e.getMessage()));
                }
            }
            return out;
        }
        q.bounds = bounds;
        q.trim = (openLo ? Native.MULTI_TRIM_LO : 0) | (openHi ? Native.MULTI_TRIM_HI : 0);
        return multiCollect(q, series, m, reader);
    }

    private static boolean uniform(long[] a) {
        if (a == null)
            return true;
        for (long v : a)
            if (v != a[0])
                return false;
        return true;
    }

    // ------------------------------------------------------------------------------------------- deletes, retention

    /**
     * Deletes every row of a series with {@code start <= ts < end}; {@code start >= end} deletes nothing. It returns
     * once the delete is durable and visible; merges remove the rows from disk later. A delete flushes the memtable
     * and writes a small part per partition, about what a large insert costs.
     *
     * @param series the series id
     * @param start the first timestamp deleted
     * @param end the first timestamp kept after the range
     * @throws TsdbException e.g. {@link Status#ERR_READONLY}
     */
    public void deleteRange(long series, long start, long end) throws TsdbException {
        MemorySegment d = enter();
        try {
            int rc;
            try {
                rc = (int) Native.DELETE_RANGE.invokeExact(d, series, start, end);
            } catch (Throwable t) {
                throw rethrow(t);
            }
            checkWrite(d, rc, "delete");
        } finally {
            exit();
        }
    }

    /**
     * Replaces the rows of a series in {@code [start, end)} by {@code timestamps} and {@code values}, atomically: a
     * reader, and a crash, sees either the old rows or the new ones. Every timestamp must lie in the range; no rows
     * make it a delete. The column's type must be the series' type; its nulls are written as nulls.
     *
     * @param series the series id
     * @param start the first timestamp replaced
     * @param end the first timestamp kept after the range
     * @param timestamps ms since the epoch, each in {@code [start, end)}
     * @param values the values; as long as {@code timestamps}
     * @throws IllegalArgumentException if the lengths differ
     * @throws TsdbException with {@link Status#ERR_INVALID} for a timestamp outside the range or a column of another
     *     type than the series
     */
    public void replaceRange(long series, long start, long end, long[] timestamps, Column values)
            throws TsdbException {
        Objects.requireNonNull(timestamps, "timestamps");
        Objects.requireNonNull(values, "values");
        int n = timestamps.length;
        if (values.size() != n)
            throw new IllegalArgumentException(n + " timestamps and " + values.size() + " values");
        MemorySegment d = enter();
        try (Arena a = Arena.ofConfined()) {
            MemorySegment ts = a.allocateFrom(JAVA_LONG, timestamps);
            MemorySegment col = a.allocate(Native.VALUES).fill((byte) 0);
            col.set(JAVA_INT, 0, values.type().code());
            col.set(PTR, 8, columnValues(a, values, 0, n));
            col.set(PTR, 16, validity(a, values, 0, n));
            int rc;
            try {
                rc = (int) Native.REPLACE_RANGE.invokeExact(d, series, start, end, ts, col, (long) n);
            } catch (Throwable t) {
                throw rethrow(t);
            }
            checkWrite(d, rc, "replace");
        } finally {
            exit();
        }
    }

    /**
     * Sets how long a series keeps its rows, in ms: rows older than the wall clock minus {@code limitMs} disappear
     * from queries at once and from disk as merges get to them; 0 keeps them forever. Durable when this returns.
     *
     * @param series the series id
     * @param limitMs the age limit in ms, 0 for none
     * @throws TsdbException with {@link Status#ERR_INVALID} for a negative limit, {@link Status#ERR_NOTFOUND} for an
     *     unknown series
     */
    public void setRetention(long series, long limitMs) throws TsdbException {
        MemorySegment d = enter();
        try {
            int rc;
            try {
                rc = (int) Native.SET_RETENTION.invokeExact(d, series, limitMs);
            } catch (Throwable t) {
                throw rethrow(t);
            }
            checkWrite(d, rc, "retention");
        } finally {
            exit();
        }
    }

    /**
     * Returns the retention limit of a series.
     *
     * @param series the series id
     * @return the age limit in ms, 0 for none
     * @throws TsdbException with {@link Status#ERR_NOTFOUND} for an unknown series
     */
    public long retention(long series) throws TsdbException {
        MemorySegment d = enter();
        try (Arena a = Arena.ofConfined()) {
            MemorySegment v = a.allocate(JAVA_LONG);
            int rc;
            try {
                rc = (int) Native.GET_RETENTION.invokeExact(d, series, v);
            } catch (Throwable t) {
                throw rethrow(t);
            }
            check(rc, "retention");
            return v.get(JAVA_LONG, 0);
        } finally {
            exit();
        }
    }


    // ------------------------------------------------------------------------------------------------ maintenance

    /**
     * Makes everything written so far durable without creating parts.
     *
     * @throws TsdbException {@link Status#ERR_IO} if the database failed
     */
    public void commit() throws TsdbException {
        simple(Native.COMMIT, "commit");
    }

    /**
     * Commits, then turns the memtable into parts.
     *
     * @throws TsdbException {@link Status#ERR_IO} if the database failed
     */
    public void flush() throws TsdbException {
        simple(Native.FLUSH, "flush");
    }

    /**
     * Merges every partition down to one part.
     *
     * @throws TsdbException {@link Status#ERR_IO} if the database failed
     */
    public void optimize() throws TsdbException {
        simple(Native.OPTIMIZE, "optimize");
    }

    /**
     * Picks up what the writer published since the last refresh (read-only databases; a follower does this on its
     * own).
     *
     * @throws TsdbException e.g. {@link Status#ERR_IO}
     */
    public void refresh() throws TsdbException {
        simple(Native.REFRESH, "refresh");
    }

    private void simple(java.lang.invoke.MethodHandle h, String what) throws TsdbException {
        MemorySegment d = enter();
        try {
            int rc;
            try {
                rc = (int) h.invokeExact(d);
            } catch (Throwable t) {
                throw rethrow(t);
            }
            checkWrite(d, rc, what);
        } finally {
            exit();
        }
    }

    /**
     * Drops whole partitions that end at or before {@code ts}.
     *
     * @param ts ms since the epoch
     * @throws TsdbException e.g. {@link Status#ERR_READONLY}
     */
    public void dropBefore(long ts) throws TsdbException {
        MemorySegment d = enter();
        try {
            int rc;
            try {
                rc = (int) Native.DROP_BEFORE.invokeExact(d, ts);
            } catch (Throwable t) {
                throw rethrow(t);
            }
            checkWrite(d, rc, "drop");
        } finally {
            exit();
        }
    }

    /**
     * What failed for good, if anything: after a storage error whose outcome on disk is unknown, every write of
     * the volume returns {@link Status#ERR_IO} until the database is reopened.
     *
     * @return null while healthy, else the directory, operation and error
     * @throws TsdbException if the database is closed
     */
    public String failed() throws TsdbException {
        MemorySegment d = enter();
        try {
            return failed(d);
        } finally {
            exit();
        }
    }

    private static String failed(MemorySegment d) {
        try (Arena a = Arena.ofConfined()) {
            MemorySegment msg = a.allocate(512);
            int rc = (int) Native.FAILED.invokeExact(d, msg, 512L);
            return rc == 0 ? null : msg.getString(0);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    /**
     * Storage statistics.
     *
     * @return parts, rows, bytes, merges and memtable rows
     * @throws TsdbException if the database is closed
     */
    public Stats stats() throws TsdbException {
        MemorySegment d = enter();
        try (Arena a = Arena.ofConfined()) {
            MemorySegment s = a.allocate(Native.STATS);
            int rc;
            try {
                rc = (int) Native.GET_STATS.invokeExact(d, s);
            } catch (Throwable t) {
                throw rethrow(t);
            }
            check(rc, "stats");
            return new Stats(s.get(JAVA_LONG, 0), s.get(JAVA_LONG, 8), s.get(JAVA_LONG, 16), s.get(JAVA_LONG, 24),
                    s.get(JAVA_LONG, 32), s.get(JAVA_LONG, 40), s.get(JAVA_LONG, 48), s.get(JAVA_LONG, 56));
        } finally {
            exit();
        }
    }

    /**
     * What the calling thread's last query on any embedded database did: rows covered, decoded and answered from
     * granule statistics, bytes, time. Zero before the first query.
     *
     * @return the statistics
     * @throws TsdbException if the database is closed
     */
    public QueryStats lastQueryStats() throws TsdbException {
        enter();
        exit();
        QueryStats s = LAST.get();
        return s != null ? s : new QueryStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    /**
     * The part files, as the engine lists them.
     *
     * @return one entry per part
     * @throws TsdbException if the database is closed
     */
    public List<PartInfo> parts() throws TsdbException {
        MemorySegment d = enter();
        List<Object> out = new ArrayList<>();
        long id = IDS.getAndIncrement();
        int rc;
        try {
            SINKS.put(id, out);
            rc = (int) Native.FOREACH_PART.invokeExact(d, PART_STUB, MemorySegment.ofAddress(id));
        } catch (Throwable t) {
            throw rethrow(t);
        } finally {
            SINKS.remove(id);
            exit();
        }
        check(rc, "parts");
        List<PartInfo> parts = new ArrayList<>(out.size());
        for (Object o : out) {
            if (o instanceof Throwable t)
                throw rethrow(t);
            parts.add((PartInfo) o);
        }
        return parts;
    }

    @Override
    public String toString() {
        return "EmbeddedDb[" + dir + (isOpen() ? "" : ", closed") + "]";
    }
}
