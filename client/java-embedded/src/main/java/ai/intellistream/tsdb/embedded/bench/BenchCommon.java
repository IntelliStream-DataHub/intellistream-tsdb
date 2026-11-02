package ai.intellistream.tsdb.embedded.bench;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.management.ManagementFactory;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Locale;

/**
 * What the embedded benchmark shares with {@code tools/cbench.h} (tsdb-bench and the client benchmarks of
 * bench/clients/run.sh): the random query windows, the three window classes, process CPU time and the RESULT lines.
 * A copy of the TCP client's package-private {@code ai.intellistream.tsdb.bench.BenchCommon}, which this module
 * cannot see; the constants and the generator are copied from the C header: change all of them together.
 */
final class BenchCommon {
    static final long T0 = 1_704_067_200_000L; /* 2024-01-01, CB_T0 */
    /** tools/datasets.h ds_info: names and steps in ms; series s has kind s % 7 (the last kind is not used). */
    static final String[] KINDS = {"hdd_temp", "cpu_temp", "tank_bar", "tank_psi", "vibration", "flow_rate",
        "flow_total", "random"};
    static final long[] STEPS = {60_000, 1_000, 10_000, 10_000, 1, 1_000, 1_000, 1_000};

    /** A window class: raw rows, or aggregate buckets. */
    record Klass(String id, String label, boolean agg, long window, long bucket) {
    }

    /** cb_classes. */
    static final Klass[] CLASSES = {
        new Klass("raw", "raw, 3600 samples", false, 3600, 0),
        new Klass("agg300", "agg, 86400 samples in buckets of 300", true, 86_400, 300),
        new Klass("agg3600", "agg, 604800 samples in buckets of 3600", true, 604_800, 3600),
    };

    private BenchCommon() {
    }

    /** ds_mix. */
    static long mix(long x) {
        x ^= x >>> 33;
        x *= 0xff51afd7ed558ccdL;
        x ^= x >>> 33;
        x *= 0xc4ceb9fe1a85ec53L;
        return x ^ (x >>> 33);
    }

    /** cb_seed: the generator state of query thread {@code thread} (CB_SEED_BASE 991). */
    static long seed(int thread) {
        return mix(thread + 991L);
    }

    /** cb_step_ms. */
    static long stepMs(int series) {
        return STEPS[series % (KINDS.length - 1)];
    }

    /** The name tsdb-bench --load and tsdbc-bench give series s, so that ids follow the file's indexes. */
    static String seriesName(int s) {
        return KINDS[s % (KINDS.length - 1)] + "." + Integer.toUnsignedString(s);
    }

    /** cb_window: one random window of a thread; {@link #next} advances the generator. */
    static final class Window {
        long state;
        int series;
        long start, end;

        Window(int thread) {
            state = seed(thread);
        }

        void next(int nseries, long points, long window) {
            state = mix(state);
            series = (int) Long.remainderUnsigned(state, nseries);
            long span = points > window ? points - window : 1;
            start = T0 + Long.remainderUnsigned(state >>> 20, span) * stepMs(series);
            end = start + window * stepMs(series);
        }
    }

    /** CPU seconds (user + system, all threads, the JIT and the collector included) the process has used. */
    static double cpuSeconds() {
        return ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean())
                .getProcessCpuTime() / 1e9;
    }

    /** cb_report_query with unit "thr". */
    static void reportQuery(String client, Klass k, int threads, long queries, long rows, double secs, double cpu) {
        System.out.println(String.format(Locale.ROOT, "  %-44s %3d thr %10.0f q/s  %8.1f us/q  %7.1f Mrows/s",
                k.label(), threads, queries / secs, secs * threads / queries * 1e6, rows / secs / 1e6));
        System.out.println(String.format(Locale.ROOT,
                "RESULT kind=query client=%s class=%s threads=%d queries=%d rows=%d secs=%.4f cpu=%.4f", client,
                k.id(), threads, queries, rows, secs, cpu));
    }

    /** cb_report_load. */
    static void reportLoad(String client, int conns, long rows, double secs, double cpu, String extra) {
        System.out.println(String.format(Locale.ROOT,
                "load (%s): %d rows in %.2f s = %.2f M rows/s, %.2f bytes/row, client CPU %.2f s (%.2f s per M rows), "
                        + "%d connections/threads%s", client, rows, secs, rows / secs / 1e6, 0.0, cpu,
                cpu / (rows / 1e6), conns, extra));
        System.out.println(String.format(Locale.ROOT,
                "RESULT kind=load client=%s conns=%d compression=0 rows=%d secs=%.4f bytes=0 cpu=%.4f", client, conns,
                rows, secs, cpu));
    }

    /*
     * The RowBinary file of the loads (u32 series, i64 ts, f32 value; 16 bytes a row), as tools/cbench.h's
     * cb_file_open reads it: mapped whole and populated before the clock starts (MAP_POPULATE there, load() here),
     * so a load measures inserting, not page faults on the file.
     */
    static final int ROW = 16;
    private static final ValueLayout.OfInt U32 = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfLong I64 = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfFloat F32 = ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    /** The whole file, mapped in {@code arena} and read in. */
    static MemorySegment mapRows(FileChannel ch, Arena arena) throws IOException {
        MemorySegment m = ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size(), arena);
        m.load();
        return m;
    }

    /** The series index of row {@code r}. */
    static int seriesAt(MemorySegment m, long r) {
        return m.get(U32, r * ROW);
    }

    /**
     * Rows {@code [r, r + n)} into the arrays from index 0, the series through {@code ids} when it is not null.
     * Returns the first series index that ids does not cover, or -1.
     */
    static long fill(MemorySegment m, long r, int n, long[] ids, long[] s, long[] t, float[] v) {
        long o = r * ROW;
        for (int i = 0; i < n; i++, o += ROW) {
            int ix = m.get(U32, o);
            if (ids != null && (ix < 0 || ix >= ids.length))
                return Integer.toUnsignedLong(ix);
            s[i] = ids != null ? ids[ix] : Integer.toUnsignedLong(ix);
            t[i] = m.get(I64, o + 4);
            v[i] = m.get(F32, o + 12);
        }
        return -1;
    }

    /**
     * Runs {@link #fill} on the first rows on this thread until it is compiled (its time per call stops falling),
     * before the clock starts. The C benchmarks' copy loop is compiled ahead of time; without this the load's
     * threads would start it in the interpreter and in profiled code at once, and count that as loading.
     */
    static void warmFill(MemorySegment m, long[] ids, int batch) {
        int n = (int) Math.min(Math.min(batch, 1 << 16), m.byteSize() / ROW);
        long[] s = new long[n];
        long[] t = new long[n];
        float[] v = new float[n];
        long best = Long.MAX_VALUE;
        long deadline = System.nanoTime() + 3_000_000_000L;
        for (int calls = 0, still = 0; still < 200 && System.nanoTime() < deadline; calls++) {
            long t0 = System.nanoTime();
            fill(m, 0, n, ids, s, t, v);
            long dt = System.nanoTime() - t0;
            if (dt < best * 9 / 10) {
                best = dt;
                still = 0;
            } else {
                still++;
            }
        }
    }
}
