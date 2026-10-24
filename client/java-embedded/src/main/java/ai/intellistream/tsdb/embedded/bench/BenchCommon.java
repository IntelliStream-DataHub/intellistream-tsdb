package ai.intellistream.tsdb.embedded.bench;

import java.lang.management.ManagementFactory;
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
}
