package ai.intellistream.tsdb.embedded;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import static ai.intellistream.tsdb.embedded.Native.OPTIONS;
import static ai.intellistream.tsdb.embedded.Native.offset;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

/**
 * Options for {@link EmbeddedDb#open(String, EmbeddedOptions)}: {@code tsdb_options} of {@code tsdb.h}. A new
 * instance holds the library's defaults ({@code tsdb_options_init}); each setter changes one field and returns this
 * instance, so options read as a chain:
 *
 * <pre>{@code
 * EmbeddedOptions o = new EmbeddedOptions().walSyncMs(10).mergeThreads(1);
 * }</pre>
 *
 * Partition length and granule size are fixed when a database is created; later opens ignore them.
 */
public final class EmbeddedOptions {
    private long partitionMs;
    private boolean partitionClasses;
    private int granuleRows;
    private int zstdLevel;
    private int insertZstdLevel;
    private int stats;
    private int statsSweepMbS;
    private int valueCodec;
    private boolean fsync;
    private int mergeThreads;
    private int queryThreads;
    private int requestThreads;
    private long bufferRows;
    private int flushIntervalMs;
    private int walSyncMs;
    private long walMaxBatchRows;
    private long maxPartBytes;
    private boolean readOnly;
    private boolean follow;
    private int refreshMs;
    private int mergeIdleS;
    private int obsoletePartLifetimeS;
    private boolean verifyChecksums;
    private boolean pinIndex;
    private int retentionMergeS;
    private boolean createVolumes;
    private long scratchBytes = DEFAULT_SCRATCH_BYTES;

    /** The default of {@link #scratchBytes(long)}: 256 MiB. */
    public static final long DEFAULT_SCRATCH_BYTES = 256L << 20;

    /** The library's defaults. */
    public EmbeddedOptions() {
        try (Arena a = Arena.ofConfined()) {
            MemorySegment o = a.allocate(OPTIONS);
            try {
                Native.OPTIONS_INIT.invokeExact(o);
            } catch (Throwable t) {
                throw Native.rethrow(t);
            }
            partitionMs = o.get(JAVA_LONG, offset(OPTIONS, "partition_ms"));
            partitionClasses = o.get(JAVA_INT, offset(OPTIONS, "partition_classes")) != 0;
            granuleRows = o.get(JAVA_INT, offset(OPTIONS, "granule_rows"));
            zstdLevel = o.get(JAVA_INT, offset(OPTIONS, "zstd_level"));
            insertZstdLevel = o.get(JAVA_INT, offset(OPTIONS, "insert_zstd_level"));
            stats = o.get(JAVA_INT, offset(OPTIONS, "stats"));
            statsSweepMbS = o.get(JAVA_INT, offset(OPTIONS, "stats_sweep_mb_s"));
            valueCodec = o.get(JAVA_INT, offset(OPTIONS, "value_codec"));
            fsync = o.get(JAVA_INT, offset(OPTIONS, "fsync")) != 0;
            mergeThreads = o.get(JAVA_INT, offset(OPTIONS, "merge_threads"));
            queryThreads = o.get(JAVA_INT, offset(OPTIONS, "query_threads"));
            requestThreads = o.get(JAVA_INT, offset(OPTIONS, "request_threads"));
            bufferRows = o.get(JAVA_LONG, offset(OPTIONS, "buffer_rows"));
            flushIntervalMs = o.get(JAVA_INT, offset(OPTIONS, "flush_interval_ms"));
            walSyncMs = o.get(JAVA_INT, offset(OPTIONS, "wal_sync_ms"));
            walMaxBatchRows = o.get(JAVA_LONG, offset(OPTIONS, "wal_max_batch_rows"));
            maxPartBytes = o.get(JAVA_LONG, offset(OPTIONS, "max_part_bytes"));
            readOnly = o.get(JAVA_INT, offset(OPTIONS, "read_only")) != 0;
            follow = o.get(JAVA_INT, offset(OPTIONS, "follow")) != 0;
            refreshMs = o.get(JAVA_INT, offset(OPTIONS, "refresh_ms"));
            mergeIdleS = o.get(JAVA_INT, offset(OPTIONS, "merge_idle_s"));
            obsoletePartLifetimeS = o.get(JAVA_INT, offset(OPTIONS, "obsolete_part_lifetime_s"));
            verifyChecksums = o.get(JAVA_INT, offset(OPTIONS, "verify_checksums")) != 0;
            pinIndex = o.get(JAVA_INT, offset(OPTIONS, "pin_index")) != 0;
            retentionMergeS = o.get(JAVA_INT, offset(OPTIONS, "retention_merge_s"));
            createVolumes = o.get(JAVA_INT, offset(OPTIONS, "create_volumes")) != 0;
        }
    }

    /** The options as a {@code tsdb_options} struct in {@code arena}. */
    MemorySegment toNative(Arena arena) {
        MemorySegment o = arena.allocate(OPTIONS);
        o.set(JAVA_LONG, offset(OPTIONS, "partition_ms"), partitionMs);
        o.set(JAVA_INT, offset(OPTIONS, "partition_classes"), partitionClasses ? 1 : 0);
        o.set(JAVA_INT, offset(OPTIONS, "granule_rows"), granuleRows);
        o.set(JAVA_INT, offset(OPTIONS, "zstd_level"), zstdLevel);
        o.set(JAVA_INT, offset(OPTIONS, "insert_zstd_level"), insertZstdLevel);
        o.set(JAVA_INT, offset(OPTIONS, "stats"), stats);
        o.set(JAVA_INT, offset(OPTIONS, "stats_sweep_mb_s"), statsSweepMbS);
        o.set(JAVA_INT, offset(OPTIONS, "value_codec"), valueCodec);
        o.set(JAVA_INT, offset(OPTIONS, "fsync"), fsync ? 1 : 0);
        o.set(JAVA_INT, offset(OPTIONS, "merge_threads"), mergeThreads);
        o.set(JAVA_INT, offset(OPTIONS, "query_threads"), queryThreads);
        o.set(JAVA_INT, offset(OPTIONS, "request_threads"), requestThreads);
        o.set(JAVA_LONG, offset(OPTIONS, "buffer_rows"), bufferRows);
        o.set(JAVA_INT, offset(OPTIONS, "flush_interval_ms"), flushIntervalMs);
        o.set(JAVA_INT, offset(OPTIONS, "wal_sync_ms"), walSyncMs);
        o.set(JAVA_LONG, offset(OPTIONS, "wal_max_batch_rows"), walMaxBatchRows);
        o.set(JAVA_LONG, offset(OPTIONS, "max_part_bytes"), maxPartBytes);
        o.set(JAVA_INT, offset(OPTIONS, "read_only"), readOnly ? 1 : 0);
        o.set(JAVA_INT, offset(OPTIONS, "follow"), follow ? 1 : 0);
        o.set(JAVA_INT, offset(OPTIONS, "refresh_ms"), refreshMs);
        o.set(JAVA_INT, offset(OPTIONS, "merge_idle_s"), mergeIdleS);
        o.set(JAVA_INT, offset(OPTIONS, "obsolete_part_lifetime_s"), obsoletePartLifetimeS);
        o.set(JAVA_INT, offset(OPTIONS, "verify_checksums"), verifyChecksums ? 1 : 0);
        o.set(JAVA_INT, offset(OPTIONS, "pin_index"), pinIndex ? 1 : 0);
        o.set(JAVA_INT, offset(OPTIONS, "retention_merge_s"), retentionMergeS);
        o.set(JAVA_INT, offset(OPTIONS, "create_volumes"), createVolumes ? 1 : 0);
        /* clock_ms and clock_arg stay NULL (allocate zeroes): retention reads the wall clock */
        return o;
    }

    /**
     * Partition length, fixed at creation.
     *
     * @param ms milliseconds per partition; default one day
     * @return this
     */
    public EmbeddedOptions partitionMs(long ms) {
        partitionMs = ms;
        return this;
    }

    /**
     * Partition classes, fixed at creation: a series slower than about one row per 1.3 s (at the defaults: under
     * eight granules per partition) gets partitions of 7, 28 or 364 base partitions, chosen from its rate.
     *
     * @param on default true; false keeps every series in base partitions
     * @return this
     */
    public EmbeddedOptions partitionClasses(boolean on) {
        partitionClasses = on;
        return this;
    }

    /**
     * Rows per granule, fixed at creation.
     *
     * @param rows default 8192
     * @return this
     */
    public EmbeddedOptions granuleRows(int rows) {
        granuleRows = rows;
        return this;
    }

    /**
     * zstd level for the parts merges write.
     *
     * @param level default 1; 0 disables zstd
     * @return this
     */
    public EmbeddedOptions zstdLevel(int level) {
        zstdLevel = level;
        return this;
    }

    /**
     * zstd level for the parts inserts and memtable flushes write; background merges re-encode them at
     * {@link #zstdLevel(int)}.
     *
     * @param level default 0: none (faster to write, larger until merged); -1: the merges' level
     * @return this
     */
    public EmbeddedOptions insertZstdLevel(int level) {
        insertZstdLevel = level;
        return this;
    }

    /**
     * Statistics of what is stored (docs/design-stats.md).
     *
     * @param mode -1: off (no sweeper thread, no query counters); 0: lazy, the default; 1: from the open
     * @param sweepMbS the sweeper reads at most this many MB/s of granule indexes; 0: 256; negative: no cap
     * @return this
     */
    public EmbeddedOptions stats(int mode, int sweepMbS) {
        stats = mode;
        statsSweepMbS = sweepMbS;
        return this;
    }

    /**
     * Value codec: 0 auto (default), 1 Gorilla, 2 Chimp ({@code TSDB_VALUES_*}).
     *
     * @param codec the codec number
     * @return this
     */
    public EmbeddedOptions valueCodec(int codec) {
        valueCodec = codec;
        return this;
    }

    /**
     * Whether parts are fsync'd before they are published.
     *
     * @param on default true
     * @return this
     */
    public EmbeddedOptions fsync(boolean on) {
        fsync = on;
        return this;
    }

    /**
     * Background merge threads.
     *
     * @param n default 2; 0 disables merges
     * @return this
     */
    public EmbeddedOptions mergeThreads(int n) {
        mergeThreads = n;
        return this;
    }

    /**
     * Threads of the database's query scheduler, shared by every query of this database.
     *
     * @param n default: every logical CPU
     * @return this
     */
    public EmbeddedOptions queryThreads(int n) {
        queryThreads = n;
        return this;
    }

    /**
     * Threads one query, or one query over many series ({@link EmbeddedDb#queryValuesMulti} and the others), uses
     * at most; a {@link ai.intellistream.tsdb.MultiOptions#threads} of 0 means this.
     *
     * @param n default: the library's (0)
     * @return this
     */
    public EmbeddedOptions requestThreads(int n) {
        requestThreads = n;
        return this;
    }

    /**
     * Memtable rows before it is flushed into parts.
     *
     * @param rows default 1 Mi
     * @return this
     */
    public EmbeddedOptions bufferRows(long rows) {
        bufferRows = rows;
        return this;
    }

    /**
     * Memtable age before it is flushed into parts.
     *
     * @param ms default 5000
     * @return this
     */
    public EmbeddedOptions flushIntervalMs(int ms) {
        flushIntervalMs = ms;
        return this;
    }

    /**
     * WAL durability: 0, every small write waits for fsync (group commit, the default); N &gt; 0, the WAL is
     * fsync'd every N ms in the background; -1, no WAL.
     *
     * @param ms the mode
     * @return this
     */
    public EmbeddedOptions walSyncMs(int ms) {
        walSyncMs = ms;
        return this;
    }

    /**
     * Inserts below this many rows go through the WAL and memtable, larger ones straight to parts.
     *
     * @param rows default 65536
     * @return this
     */
    public EmbeddedOptions walMaxBatchRows(long rows) {
        walMaxBatchRows = rows;
        return this;
    }

    /**
     * Background merges never produce parts larger than this.
     *
     * @param bytes default 4 GiB
     * @return this
     */
    public EmbeddedOptions maxPartBytes(long bytes) {
        maxPartBytes = bytes;
        return this;
    }

    /**
     * Opens without writing, merging or locking exclusively.
     *
     * @param on default false
     * @return this
     */
    public EmbeddedOptions readOnly(boolean on) {
        readOnly = on;
        return this;
    }

    /**
     * Read only, and keep following a writer: new parts, merges and WAL rows.
     *
     * @param on default false
     * @return this
     */
    public EmbeddedOptions follow(boolean on) {
        follow = on;
        return this;
    }

    /**
     * How often a follower polls the writer's MANIFEST.
     *
     * @param ms default 100
     * @return this
     */
    public EmbeddedOptions refreshMs(int ms) {
        refreshMs = ms;
        return this;
    }

    /**
     * A partition without a new part for this long is merged down to one part.
     *
     * @param seconds default 300; -1 never
     * @return this
     */
    public EmbeddedOptions mergeIdleS(int seconds) {
        mergeIdleS = seconds;
        return this;
    }

    /**
     * How long parts replaced by merges are kept for readers on other hosts.
     *
     * @param seconds default 480
     * @return this
     */
    public EmbeddedOptions obsoletePartLifetimeS(int seconds) {
        obsoletePartLifetimeS = seconds;
        return this;
    }

    /**
     * Whether queries check each granule's checksum before decoding it (a mismatch fails the query as corrupt).
     * Structural checksums (part headers and indexes, WAL, MANIFEST, dictionaries) are always checked.
     *
     * @param on default false: storage with checksums of its own (ZFS, ECC memory) catches the damage already
     * @return this
     */
    public EmbeddedOptions verifyChecksums(boolean on) {
        verifyChecksums = on;
        return this;
    }

    /**
     * Whether each part's granule index and tombstones are copied into memory when the open checks them, so damage
     * that reaches the file later cannot change what queries see. Costs about 32 to 51 bytes of RAM per granule.
     *
     * @param on default false: read in place (damage after the open can give wrong answers, never a read outside
     *     the part)
     * @return this
     */
    public EmbeddedOptions pinIndex(boolean on) {
        pinIndex = on;
        return this;
    }

    /**
     * A part holding rows past their series' retention is rewritten without them once it is this old.
     *
     * @param seconds default 3600; -1 never (merges still drop them)
     * @return this
     */
    public EmbeddedOptions retentionMergeS(int seconds) {
        retentionMergeS = seconds;
        return this;
    }

    /**
     * Whether {@link EmbeddedDb#seriesAt} may add a volume for a directory not yet listed (after the same path
     * checks as {@code tsdb volume add}). Off, an unknown directory is refused (not found), as tsdbd does.
     *
     * @param on default true
     * @return this
     */
    public EmbeddedOptions createVolumes(boolean on) {
        createVolumes = on;
        return this;
    }

    /**
     * Native memory an {@link EmbeddedDb} keeps for copying inserted arrays, reused from call to call (not a
     * {@code tsdb_options} field). Each thread inserting at once uses its own block of up to 32 MiB (a call that
     * needs more copies into memory of its own, as every call beyond this limit does); the blocks stay until
     * {@link EmbeddedDb#close}.
     *
     * @param bytes default 256 MiB; 0 keeps nothing, so every insert allocates and frees its own copy
     * @return this
     * @throws IllegalArgumentException if negative
     */
    public EmbeddedOptions scratchBytes(long bytes) {
        if (bytes < 0)
            throw new IllegalArgumentException("scratchBytes < 0");
        scratchBytes = bytes;
        return this;
    }

    long scratchBytes() {
        return scratchBytes;
    }

    @Override
    public String toString() {
        return "EmbeddedOptions[partitionMs=" + partitionMs + ", granuleRows=" + granuleRows + ", zstdLevel="
                + zstdLevel + ", insertZstdLevel=" + insertZstdLevel + ", valueCodec=" + valueCodec + ", fsync=" + fsync + ", mergeThreads=" + mergeThreads
                + ", queryThreads=" + queryThreads + ", requestThreads=" + requestThreads + ", bufferRows=" + bufferRows + ", flushIntervalMs="
                + flushIntervalMs + ", walSyncMs=" + walSyncMs + ", walMaxBatchRows=" + walMaxBatchRows
                + ", maxPartBytes=" + maxPartBytes + ", readOnly=" + readOnly + ", follow=" + follow + ", refreshMs="
                + refreshMs + ", mergeIdleS=" + mergeIdleS + ", obsoletePartLifetimeS=" + obsoletePartLifetimeS
                + ", verifyChecksums=" + verifyChecksums + ", pinIndex=" + pinIndex + ", retentionMergeS=" + retentionMergeS
                + ", createVolumes=" + createVolumes + ", scratchBytes=" + scratchBytes + "]";
    }
}
