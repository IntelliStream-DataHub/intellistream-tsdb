package ai.intellistream.tsdb.bench;

import java.lang.management.ManagementFactory;
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
}
