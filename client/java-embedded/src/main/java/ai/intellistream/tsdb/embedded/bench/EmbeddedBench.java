package ai.intellistream.tsdb.embedded.bench;

import ai.intellistream.tsdb.Bucket;
import ai.intellistream.tsdb.Group;
import ai.intellistream.tsdb.Stats;
import ai.intellistream.tsdb.TsdbException;
import ai.intellistream.tsdb.Unit;
import ai.intellistream.tsdb.embedded.EmbeddedDb;
import ai.intellistream.tsdb.embedded.EmbeddedOptions;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

/**
 * tsdb-bench for the embedded database: the same loads and the same queries, so the embedded Java numbers stand
 * next to C's. Three modes, each the counterpart of a tsdb-bench mode:
 *
 * <pre>
 *   EmbeddedBench --dir DIR --load FILE [--threads 4] [--batch 1000000] [--no-fsync] [--zstd 1] [--optimize]
 *   EmbeddedBench --dir DIR --qps N [--series 100] [--points 1000000] [--seconds 3] [--warmup 1.5]
 *   EmbeddedBench --dir DIR [--file ROWS.rowbinary] [--series 100] [--points 1000000] [--dataset NAME]
 *                 [--threads 4] [--batch 1000000] [--no-fsync] [--query-threads N]
 * </pre>
 *
 * The rows come from {@code tsdb-bench --export ROWS.rowbinary} (u32 series, i64 ts, f32 value, 16 bytes a row).
 *
 * <p>{@code --load} and {@code --qps} are what bench/clients/run.sh runs, as {@code tsdb-bench --load} and
 * {@code tsdb-bench --qps} (tools/cbench.h): {@code --load} removes DIR, names the series as tsdb-bench does (ids
 * equal to the file's indexes), gives each of --threads threads a contiguous slice of the file in batches of --batch
 * rows, and prints a {@code RESULT kind=load client=embedded-java-fsync} (or {@code -nofsync}) line; check the
 * database with {@code tsdb-bench --dir DIR --verify FILE} afterwards. {@code --qps N} runs the three window classes
 * with N threads and cb_window's generator and seeds for --seconds each, after an untimed JIT warm-up of --warmup
 * seconds per class on one thread, and prints {@code RESULT kind=query client=embedded-java} lines.
 *
 * <p>Without either, the database in DIR is loaded from --file (if given; every threads-th batch to each thread) and
 * queried as tsdb-bench's default run does: a raw scan of every series, aggregates over the whole range, calendar
 * grouping (UTC and Europe/Oslo, best of 3), and random windows for 2 s with 1 and all CPUs, with the generator and
 * seeds of that run ({@code tsdb-bench --keep} leaves such a database).
 */
public final class EmbeddedBench {
    private static final long T0 = BenchCommon.T0;
    private static final String[] KINDS = BenchCommon.KINDS;
    private static final long[] STEP_MS = BenchCommon.STEPS;

    private EmbeddedBench() {
    }

    private static int series = 100;
    private static long points = 1_000_000;
    private static int dataset = -1;

    static long stepOf(int s) {
        return STEP_MS[dataset >= 0 ? dataset : s % (KINDS.length - 1)];
    }

    static long mix(long x) {
        return BenchCommon.mix(x);
    }

    private static double now() {
        return System.nanoTime() / 1e9;
    }

    /**
     * Runs the benchmark.
     *
     * @param args see the class description
     * @throws Exception on any failure
     */
    public static void main(String[] args) throws Exception {
        String dir = null;
        String file = null;
        String loadFile = null;
        int threads = 4;
        int batch = 1_000_000;
        boolean fsync = true;
        boolean optimize = false;
        int zstd = 1;
        int queryThreads = 0;
        int qps = 0;
        double seconds = 3;
        double warmup = 1.5;
        for (int i = 0; i < args.length; i++) {
            if (!args[i].equals("--no-fsync") && !args[i].equals("--optimize") && i + 1 >= args.length)
                throw new IllegalArgumentException("missing value for " + args[i]);
            switch (args[i]) {
            case "--dir": dir = args[++i]; break;
            case "--file": file = args[++i]; break;
            case "--load": loadFile = args[++i]; break;
            case "--qps": qps = Integer.parseInt(args[++i]); break;
            case "--seconds": seconds = Double.parseDouble(args[++i]); break;
            case "--warmup": warmup = Double.parseDouble(args[++i]); break;
            case "--series": series = Integer.parseInt(args[++i]); break;
            case "--points": points = Long.parseLong(args[++i]); break;
            case "--dataset": dataset = List.of(KINDS).indexOf(args[++i]); break;
            case "--threads": threads = Integer.parseInt(args[++i]); break;
            case "--batch": batch = Integer.parseInt(args[++i]); break;
            case "--zstd": zstd = Integer.parseInt(args[++i]); break;
            case "--no-fsync": fsync = false; break;
            case "--optimize": optimize = true; break;
            case "--query-threads": queryThreads = Integer.parseInt(args[++i]); break;
            default: throw new IllegalArgumentException("unknown argument " + args[i]);
            }
        }
        if (dir == null)
            throw new IllegalArgumentException("--dir DIR is required");
        if (threads < 1 || batch < 1 || series < 1 || qps < 0 || qps > 256)
            throw new IllegalArgumentException("bad --threads, --batch, --series or --qps");
        if (loadFile != null) {
            harnessLoad(dir, loadFile, threads, batch, fsync, zstd, optimize);
            return;
        }
        EmbeddedOptions o = new EmbeddedOptions().fsync(fsync);
        if (queryThreads > 0)
            o.queryThreads(queryThreads);
        if (qps > 0) {
            if (!Files.isDirectory(Paths.get(dir))) /* open would create an empty database */
                throw new IllegalArgumentException("--qps: " + dir + " is not a database directory");
            try (EmbeddedDb db = EmbeddedDb.open(dir, o)) {
                harnessQueries(db, qps, seconds, warmup);
            }
            return;
        }
        try (EmbeddedDb db = EmbeddedDb.open(dir, o)) {
            System.out.printf("embedded Java: %d series x %d points, %s, simd %s, library %s%n", series, points,
                    dataset < 0 ? "mixed" : KINDS[dataset], EmbeddedDb.simdLevel(), EmbeddedDb.librarySource());
            if (file != null)
                load(db, file, threads, batch);
            queries(db);
        }
    }

    /** tsdb-bench --load (mode_load) without its verification, which {@code tsdb-bench --verify} does. */
    private static void harnessLoad(String dir, String file, int threads, int batch, boolean fsync, int zstd,
                                    boolean optimize) throws Exception {
        Path root = Paths.get(dir);
        if (Files.exists(root)) {
            try (Stream<Path> w = Files.walk(root)) {
                for (Path p : (Iterable<Path>) w.sorted(Comparator.reverseOrder())::iterator)
                    Files.delete(p);
            }
        }
        try (FileChannel ch = FileChannel.open(Paths.get(file), StandardOpenOption.READ);
             Arena arena = Arena.ofShared();
             EmbeddedDb db = EmbeddedDb.open(dir, new EmbeddedOptions().fsync(fsync).zstdLevel(zstd))) {
            MemorySegment m = BenchCommon.mapRows(ch, arena);
            long rows = m.byteSize() / BenchCommon.ROW;
            int nseries = 0; /* the file's highest series index + 1, as cb_expect_make */
            for (long r = 0; r < rows; r++)
                nseries = Math.max(nseries, BenchCommon.seriesAt(m, r) + 1);
            for (int s = 0; s < nseries; s++) {
                int id = db.series(BenchCommon.seriesName(s));
                if (id != s)
                    throw new IllegalStateException("series " + BenchCommon.seriesName(s) + " got id " + id);
            }
            BenchCommon.warmFill(m, null, batch);
            Thread[] th = new Thread[threads];
            Exception[] err = new Exception[1];
            double cpu0 = BenchCommon.cpuSeconds();
            double t0 = now();
            for (int t = 0; t < threads; t++) {
                long a = rows * t / threads;
                long b = rows * (t + 1) / threads;
                th[t] = new Thread(() -> {
                    int[] sv = new int[batch];
                    long[] tv = new long[batch];
                    float[] vv = new float[batch];
                    try {
                        for (long r0 = a; r0 < b; r0 += batch) {
                            int n = (int) Math.min(batch, b - r0);
                            BenchCommon.fill(m, r0, n, null, sv, tv, vv);
                            db.insert(sv, tv, vv, 0, n);
                        }
                    } catch (IOException e) { /* TsdbException included */
                        synchronized (err) {
                            err[0] = e;
                        }
                    }
                });
                th[t].start();
            }
            for (Thread t : th)
                t.join();
            synchronized (err) {
                if (err[0] != null)
                    throw err[0];
            }
            double dt = now() - t0;
            BenchCommon.reportLoad(fsync ? "embedded-java-fsync" : "embedded-java-nofsync", threads, rows, dt,
                    BenchCommon.cpuSeconds() - cpu0, String.format(Locale.ROOT,
                            ", batches of %d, fsync %s, zstd %d", batch, fsync ? "on" : "off", zstd));
            if (optimize) {
                t0 = now();
                db.optimize();
                System.out.printf(Locale.ROOT, "optimize %.2f s%n", now() - t0);
            }
            Stats st = db.stats();
            System.out.printf(Locale.ROOT, "storage %.2f MiB, %.3f bytes/row%n", st.bytes() / 1048576.0,
                    (double) st.bytes() / st.rows());
        }
    }

    /** tsdb-bench --qps (mode_qps, qw_loop), with QueryBench's JIT warm-up first. */
    private static void harnessQueries(EmbeddedDb db, int nt, double seconds, double warmup) throws Exception {
        if (warmup > 0) {
            for (BenchCommon.Klass k : BenchCommon.CLASSES)
                qwLoop(db, k, 0, warmup, new long[3]);
        }
        for (BenchCommon.Klass k : BenchCommon.CLASSES) {
            long[][] out = new long[nt][3]; /* queries, rows, nanoseconds */
            Throwable[] failed = new Throwable[1];
            Thread[] th = new Thread[nt];
            double cpu0 = BenchCommon.cpuSeconds();
            for (int i = 0; i < nt; i++) {
                int idx = i;
                th[i] = new Thread(() -> {
                    try {
                        qwLoop(db, k, idx, seconds, out[idx]);
                    } catch (Throwable e) {
                        synchronized (failed) {
                            failed[0] = e;
                        }
                    }
                }, "querier-" + i);
                th[i].start();
            }
            for (Thread t : th)
                t.join();
            synchronized (failed) {
                if (failed[0] != null)
                    throw new IllegalStateException("query failed", failed[0]);
            }
            long queries = 0, rows = 0, nanos = 0;
            for (long[] o : out) {
                queries += o[0];
                rows += o[1];
                nanos = Math.max(nanos, o[2]);
            }
            BenchCommon.reportQuery("embedded-java", k, nt, queries, rows, nanos / 1e9,
                    BenchCommon.cpuSeconds() - cpu0);
            if (rows == 0)
                throw new IllegalStateException("class " + k.id() + " returned no rows: the database is empty or does"
                        + " not hold the data set");
        }
    }

    /** One thread's queries for {@code seconds}; fills {queries, rows, nanoseconds}. */
    private static void qwLoop(EmbeddedDb db, BenchCommon.Klass k, int thread, double seconds, long[] out)
            throws TsdbException {
        long[] rows = {0};
        long queries = 0;
        BenchCommon.Window w = new BenchCommon.Window(thread);
        long t0 = System.nanoTime();
        long limit = (long) (seconds * 1e9);
        while (System.nanoTime() - t0 < limit) {
            for (int i = 0; i < 16; i++) {
                w.next(series, points, k.window());
                if (!k.agg()) {
                    db.queryRaw(w.series, w.start, w.end, (ts, v, n) -> {
                        rows[0] += n;
                        return true;
                    });
                } else {
                    for (Bucket b : db.queryAgg(w.series, w.start, w.end, k.bucket() * BenchCommon.stepMs(w.series)))
                        rows[0] += b.count();
                }
                queries++;
            }
        }
        out[0] = queries;
        out[1] = rows[0];
        out[2] = System.nanoTime() - t0;
    }

    private static void load(EmbeddedDb db, String file, int threads, int batch) throws Exception {
        for (int s = 0; s < series; s++)
            db.series("s" + s); /* ids 0 .. series-1, as tsdb-bench's */
        try (FileChannel ch = FileChannel.open(Paths.get(file), StandardOpenOption.READ)) {
            long rows = ch.size() / 16;
            long batches = (rows + batch - 1) / batch;
            Thread[] th = new Thread[threads];
            Exception[] err = new Exception[1];
            double t0 = now();
            for (int t = 0; t < threads; t++) {
                int first = t;
                th[t] = new Thread(() -> {
                    int[] sv = new int[batch];
                    long[] tv = new long[batch];
                    float[] vv = new float[batch];
                    try {
                        for (long b = first; b < batches; b += threads) {
                            long r0 = b * batch;
                            int n = (int) Math.min(batch, rows - r0);
                            /* A mapping per batch: a ByteBuffer is limited to 2 GiB, the file is not. Reading a
                             * mapped MemorySegment row by row was 10x slower here: the loop runs a few times per
                             * thread and the JIT never caught up, so loading measured the parser, not the insert. */
                            ByteBuffer m = ch.map(FileChannel.MapMode.READ_ONLY, r0 * 16, n * 16L)
                                    .order(ByteOrder.LITTLE_ENDIAN);
                            for (int i = 0, off = 0; i < n; i++, off += 16) {
                                sv[i] = m.getInt(off);
                                tv[i] = m.getLong(off + 4);
                                vv[i] = m.getFloat(off + 12);
                            }
                            db.insert(sv, tv, vv, 0, n);
                        }
                    } catch (IOException e) { /* TsdbException included */
                        err[0] = e;
                    }
                });
                th[t].start();
            }
            for (Thread t : th)
                t.join();
            if (err[0] != null)
                throw err[0];
            double ti = now() - t0;
            Stats st = db.stats();
            System.out.printf("insert          %6.2f s  %7.2f M rows/s   (%d parts, %d merges so far)%n", ti,
                    rows / ti / 1e6, st.parts(), st.merges());
            t0 = now();
            db.optimize();
            st = db.stats();
            System.out.printf("optimize        %6.2f s  -> %d parts in %d partitions%n", now() - t0, st.parts(),
                    st.partitions());
        }
    }

    private static void queries(EmbeddedDb db) throws Exception {
        Stats st = db.stats();
        long total = st.rows();
        System.out.printf("storage    %8.2f MiB  %.3f bytes/row%n%n", st.bytes() / 1048576.0,
                (double) st.bytes() / st.rows());
        double t0 = now();
        double[] acc = {0, 0};
        for (int s = 0; s < series; s++)
            db.queryRaw(s, Long.MIN_VALUE, Long.MAX_VALUE, (ts, v, n) -> {
                acc[0] += n;
                acc[1] += v[n - 1]; /* touch the data */
                return true;
            });
        double tr = now() - t0;
        System.out.printf("raw scan, all series, 1 thread            %8.3f s  %7.1f M rows/s  (%.0f rows)%n", tr,
                acc[0] / tr / 1e6, acc[0]);
        String[] aggLabels = {"agg whole range, 1 bucket per series", "agg buckets of 86400 samples",
            "agg buckets of 3600 samples", "agg buckets of 60 samples (decodes all)",
            "agg buckets of 10 samples (decodes all)"};
        long[] aggBuckets = {0, 86400, 3600, 60, 10};
        for (int k = 0; k < aggBuckets.length; k++) {
            t0 = now();
            long rows = 0;
            long buckets = 0;
            for (int s = 0; s < series; s++) {
                List<Bucket> b = db.queryAgg(s, Long.MIN_VALUE, Long.MAX_VALUE, aggBuckets[k] * stepOf(s));
                for (Bucket x : b)
                    rows += x.count();
                buckets += b.size();
            }
            double ta = now() - t0;
            System.out.printf("%-42s %8.3f s  %7.1f M rows/s  %6.2f ms/series  (%d buckets)%n", aggLabels[k], ta,
                    rows / ta / 1e6, ta / series * 1e3, buckets);
            if (rows != total)
                System.out.printf("  !! row count mismatch: %d != %d%n", rows, total);
        }
        String[] groupLabels = {"group by 15 minutes (UTC)", "group by hour (UTC)", "group by day (Europe/Oslo)",
            "group by week (Europe/Oslo)", "group by month (Europe/Oslo)", "group by year (Europe/Oslo)"};
        Group[] groups = {Group.of(Unit.MINUTE, 15), Group.of(Unit.HOUR), Group.of(Unit.DAY).in("Europe/Oslo"),
            Group.of(Unit.WEEK).in("Europe/Oslo"), Group.of(Unit.MONTH).in("Europe/Oslo"),
            Group.of(Unit.YEAR).in("Europe/Oslo")};
        for (int k = 0; k < groups.length; k++) {
            double best = 1e9;
            long rows = 0;
            long buckets = 0;
            for (int rep = 0; rep < 3; rep++) {
                t0 = now();
                rows = 0;
                buckets = 0;
                for (int s = 0; s < series; s++) {
                    List<Bucket> b = db.queryGroup(s, Long.MIN_VALUE, Long.MAX_VALUE, groups[k]);
                    for (Bucket x : b)
                        rows += x.count();
                    buckets += b.size();
                }
                best = Math.min(best, now() - t0);
            }
            System.out.printf("%-42s %8.3f s  %7.1f M rows/s  (%d buckets)%n", groupLabels[k], best,
                    rows / best / 1e6, buckets);
        }
        System.out.printf("%nrandom windows (2 s each):%n");
        int cpus = Runtime.getRuntime().availableProcessors();
        qps(db, "raw, 3600 samples", false, 3600, 0, 1);
        qps(db, "raw, 3600 samples", false, 3600, 0, cpus);
        qps(db, "agg, 86400 samples in buckets of 300", true, 86400, 300, 1);
        qps(db, "agg, 86400 samples in buckets of 300", true, 86400, 300, cpus);
        if (points > 7 * 86400) {
            qps(db, "agg, 604800 samples in buckets of 3600", true, 604800, 3600, 1);
            qps(db, "agg, 604800 samples in buckets of 3600", true, 604800, 3600, cpus);
        }
    }

    /** tsdb-bench's run_qps and query_loop of its default run (seed + 77, not cb_seed): the same windows. */
    private static void qps(EmbeddedDb db, String label, boolean agg, long window, long bucket, int threads)
            throws Exception {
        long[] queries = new long[threads];
        long[] rows = new long[threads];
        double[] secs = new double[threads];
        Thread[] th = new Thread[threads];
        Exception[] err = new Exception[1];
        for (int t = 0; t < threads; t++) {
            int seed = t;
            th[t] = new Thread(() -> {
                long r = mix(seed + 77L);
                long[] got = {0};
                double t0 = now();
                try {
                    while (now() - t0 < 2.0) {
                        for (int k = 0; k < 64; k++) {
                            r = mix(r);
                            int s = (int) Long.remainderUnsigned(r, series);
                            long step = stepOf(s);
                            long span = points > window ? points - window : 1;
                            long start = T0 + Long.remainderUnsigned(r >>> 20, span) * step;
                            long end = start + window * step;
                            if (!agg) {
                                db.queryRaw(s, start, end, (ts, v, n) -> {
                                    got[0] += n;
                                    return true;
                                });
                            } else {
                                for (Bucket b : db.queryAgg(s, start, end, bucket * step))
                                    got[0] += b.count();
                            }
                            queries[seed]++;
                        }
                    }
                } catch (TsdbException e) {
                    err[0] = e;
                }
                rows[seed] = got[0];
                secs[seed] = now() - t0;
            });
            th[t].start();
        }
        long nq = 0;
        long nr = 0;
        double s = 0;
        for (int t = 0; t < threads; t++) {
            th[t].join();
            nq += queries[t];
            nr += rows[t];
            s = Math.max(s, secs[t]);
        }
        if (err[0] != null)
            throw err[0];
        System.out.printf("  %-46s %3d thr %10.0f q/s  %8.1f us/q  %7.1f Mrows/s%n", label, threads, nq / s,
                s * threads / nq * 1e6, nr / s / 1e6);
    }
}
