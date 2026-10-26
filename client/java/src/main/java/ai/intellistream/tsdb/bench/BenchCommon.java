package ai.intellistream.tsdb.bench;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Locale;

/**
 * What the Java benchmarks share with {@code tools/cbench.h} (the C benchmarks): the random query windows, the
 * three window classes, process CPU time and the result lines that {@code bench/clients/run.sh} reads. The
 * constants and the generator are copied from the C header; change them together.
 */
final class BenchCommon {
    static final long T0 = 1_704_067_200_000L; /* 2024-01-01 */
    /** Step in ms of series s: tools/datasets.h ds_info[s % 7].step_ms. */
    private static final long[] STEPS = {60_000, 1_000, 10_000, 10_000, 1, 1_000, 1_000};

    /** A window class: raw rows, or aggregate buckets. */
    record Klass(String id, String label, boolean agg, long window, long bucket) {
    }

    static final Klass[] CLASSES = {
        new Klass("raw", "raw, 3600 samples", false, 3600, 0),
        new Klass("agg300", "agg, 86400 samples in buckets of 300", true, 86_400, 300),
        new Klass("agg3600", "agg, 604800 samples in buckets of 3600", true, 604_800, 3600),
    };

    private BenchCommon() {
    }

    static long mix(long x) {
        x ^= x >>> 33;
        x *= 0xff51afd7ed558ccdL;
        x ^= x >>> 33;
        x *= 0xc4ceb9fe1a85ec53L;
        return x ^ (x >>> 33);
    }

    /** The generator state of query thread {@code thread}. */
    static long seed(int thread) {
        return mix(thread + 991L);
    }

    static long stepMs(int series) {
        return STEPS[series % 7];
    }

    /** One random window of a thread: the generator state is advanced by {@link #next}. */
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

    static void reportQuery(String client, Klass k, int threads, long queries, long rows, double secs, double cpu) {
        System.out.println(String.format(Locale.ROOT, "  %-44s %3d conn %10.0f q/s  %8.1f us/q  %7.1f Mrows/s", k.label(),
                threads, queries / secs, secs * threads / queries * 1e6, rows / secs / 1e6));
        System.out.println(String.format(Locale.ROOT,
                "RESULT kind=query client=%s class=%s threads=%d queries=%d rows=%d secs=%.4f cpu=%.4f", client,
                k.id(), threads, queries, rows, secs, cpu));
    }

    static void reportLoad(String client, int conns, int compression, long rows, double secs, long bytes, double cpu,
                           String extra) {
        System.out.println(String.format(Locale.ROOT,
                "load (%s): %d rows in %.2f s = %.2f M rows/s, %.2f bytes/row, client CPU %.2f s (%.2f s per M rows), "
                        + "%d connections/threads%s", client, rows, secs, rows / secs / 1e6, bytes / (double) rows, cpu,
                cpu / (rows / 1e6), conns, extra));
        System.out.println(String.format(Locale.ROOT,
                "RESULT kind=load client=%s conns=%d compression=%d rows=%d secs=%.4f bytes=%d cpu=%.4f", client, conns,
                compression, rows, secs, bytes, cpu));
    }

    /*
     * The RowBinary file of the loads (u32 series, i64 ts, f32 value; 16 bytes a row), as tools/cbench.h's
     * cb_file_open reads it: mapped whole and read in before the clock starts (MAP_POPULATE there, load() here), so
     * a load measures inserting, not page faults on the file. A ByteBuffer holds at most 2 GiB: 1 GiB pieces.
     */
    static final int ROW = 16;
    private static final int PIECE_SHIFT = 26; /* rows per mapping: 1 GiB */

    /** The whole file, mapped and read in. */
    static ByteBuffer[] mapRows(FileChannel ch) throws IOException {
        long rows = ch.size() / ROW;
        ByteBuffer[] m = new ByteBuffer[(int) ((rows + (1L << PIECE_SHIFT) - 1) >>> PIECE_SHIFT)];
        for (int i = 0; i < m.length; i++) {
            long r = (long) i << PIECE_SHIFT;
            MappedByteBuffer b = ch.map(FileChannel.MapMode.READ_ONLY, r * ROW,
                    Math.min(1L << PIECE_SHIFT, rows - r) * ROW);
            b.load();
            m[i] = b.order(ByteOrder.LITTLE_ENDIAN);
        }
        return m;
    }

    /** The series index of row {@code r}. */
    static int seriesAt(ByteBuffer[] m, long r) {
        return m[(int) (r >>> PIECE_SHIFT)].getInt((int) (r & ((1 << PIECE_SHIFT) - 1)) * ROW);
    }

    /**
     * Rows {@code [r, r + n)} into the arrays from index 0, the series through {@code ids}. Returns the first
     * series index that ids does not cover, or -1.
     */
    static long fill(ByteBuffer[] m, long r, int n, int[] ids, int[] s, long[] t, float[] v) {
        for (int i = 0; i < n;) {
            ByteBuffer b = m[(int) (r >>> PIECE_SHIFT)];
            int first = (int) (r & ((1 << PIECE_SHIFT) - 1));
            int k = Math.min(n - i, (b.capacity() / ROW) - first);
            long bad = fill(b, first * ROW, k, ids, s, t, v, i);
            if (bad >= 0)
                return bad;
            i += k;
            r += k;
        }
        return -1;
    }

    private static long fill(ByteBuffer b, int o, int k, int[] ids, int[] s, long[] t, float[] v, int i0) {
        for (int i = i0, end = i0 + k; i < end; i++, o += ROW) {
            int id = b.getInt(o);
            if (id < 0 || id >= ids.length)
                return Integer.toUnsignedLong(id);
            s[i] = ids[id];
            t[i] = b.getLong(o + 4);
            v[i] = b.getFloat(o + 12);
        }
        return -1;
    }

    /**
     * Runs {@link #fill} on the first rows on this thread until it is compiled (its time per call stops falling),
     * before the clock starts. The C benchmarks' copy loop is compiled ahead of time; without this the load's
     * threads would start it in the interpreter and in profiled code at once, and count that as loading.
     */
    static void warmFill(ByteBuffer[] m, int[] ids, int batch) {
        int n = (int) Math.min(Math.min(batch, 1 << 16), m.length == 0 ? 0 : m[0].capacity() / ROW);
        int[] s = new int[n];
        long[] t = new long[n];
        float[] v = new float[n];
        long best = Long.MAX_VALUE;
        long deadline = System.nanoTime() + 3_000_000_000L;
        for (int still = 0; n > 0 && still < 200 && System.nanoTime() < deadline;) {
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
