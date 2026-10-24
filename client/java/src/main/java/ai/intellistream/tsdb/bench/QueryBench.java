package ai.intellistream.tsdb.bench;

import ai.intellistream.tsdb.Bucket;
import ai.intellistream.tsdb.TsdbClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Query benchmark against tsdbd, the Java counterpart of {@code tsdbc-bench query}:
 *
 * <pre>
 *   QueryBench [--host 127.0.0.1] [--port 7070] [--series 100] [--points 1000000] [--conns 1] [--seconds 3]
 *            [--warmup 1.5]
 * </pre>
 *
 * Every connection (a thread each) draws windows from the same generator and seed as the C benchmark
 * ({@code BenchCommon}, tools/cbench.h): raw rows of 3600 samples and two aggregate classes. The raw rows are
 * counted in the consumer and not kept; the aggregates are counted from the returned buckets. The output is the
 * line of {@code tsdbc-bench query} for each class, and a RESULT line for bench/clients/run.sh. The JVM is warmed up
 * first (untimed, on one connection, {@code --warmup} seconds of each class, 0 to skip), so that the first measured
 * class is not charged with the JIT compiler.
 */
public final class QueryBench {
    private QueryBench() {
    }

    /**
     * Runs the benchmark.
     *
     * @param args the options; see the class description
     * @throws Exception if the server fails
     */
    public static void main(String[] args) throws Exception {
        String host = "127.0.0.1";
        int port = TsdbClient.DEFAULT_PORT, conns = 1, series = 100;
        long points = 1_000_000;
        double seconds = 3, warmup = 1.5;
        for (int i = 0; i < args.length; i++) {
            if (i + 1 >= args.length)
                usage("missing value for " + args[i]);
            String v = args[i + 1];
            switch (args[i]) {
            case "--host": host = v; break;
            case "--port": port = Integer.parseInt(v); break;
            case "--series": series = Integer.parseInt(v); break;
            case "--points": points = Long.parseLong(v); break;
            case "--conns": conns = Integer.parseInt(v); break;
            case "--seconds": seconds = Double.parseDouble(v); break;
            case "--warmup": warmup = Double.parseDouble(v); break;
            default: usage("unknown option " + args[i]);
            }
            i++;
        }
        if (conns < 1 || conns > 256 || series < 1)
            usage(null);
        if (warmup > 0) {
            for (BenchCommon.Klass k : BenchCommon.CLASSES)
                query(host, port, k, 0, series, points, warmup, new long[3]);
        }
        for (BenchCommon.Klass k : BenchCommon.CLASSES) {
            long[][] out = new long[conns][3]; /* queries, rows, nanoseconds */
            AtomicReference<Throwable> failed = new AtomicReference<>();
            List<Thread> threads = new ArrayList<>();
            double cpu0 = BenchCommon.cpuSeconds();
            for (int c = 0; c < conns; c++) {
                final int idx = c, ns = series;
                final long pts = points;
                final double secs = seconds;
                final String h = host;
                final int p = port;
                Thread t = new Thread(() -> {
                    try {
                        query(h, p, k, idx, ns, pts, secs, out[idx]);
                    } catch (Throwable e) {
                        failed.compareAndSet(null, e);
                    }
                }, "querier-" + c);
                t.start();
                threads.add(t);
            }
            for (Thread t : threads)
                t.join();
            if (failed.get() != null) {
                System.err.println("QueryBench: " + failed.get());
                System.exit(1);
            }
            long queries = 0, rows = 0, nanos = 0;
            for (long[] o : out) {
                queries += o[0];
                rows += o[1];
                nanos = Math.max(nanos, o[2]);
            }
            if (rows == 0) {
                System.err.println("QueryBench: class " + k.id() + " returned no rows: the server is empty or does not"
                        + " hold the data set");
                System.exit(1);
            }
            BenchCommon.reportQuery("java", k, conns, queries, rows, nanos / 1e9, BenchCommon.cpuSeconds() - cpu0);
        }
    }

    private static void usage(String why) {
        if (why != null)
            System.err.println("QueryBench: " + why);
        System.err.println("usage: QueryBench [--host H] [--port P] [--series N] [--points N] [--conns N]"
                + " [--seconds S] [--warmup S]");
        System.exit(2);
    }

    /** One connection's queries for {@code seconds}; fills {queries, rows, nanoseconds}. */
    private static void query(String host, int port, BenchCommon.Klass k, int thread, int nseries, long points,
                              double seconds, long[] out) throws Exception {
        long[] rows = {0};
        long queries = 0;
        BenchCommon.Window w = new BenchCommon.Window(thread);
        try (TsdbClient c = TsdbClient.connect(host, port)) {
            long t0 = System.nanoTime();
            long limit = (long) (seconds * 1e9);
            while (System.nanoTime() - t0 < limit) {
                for (int i = 0; i < 16; i++) {
                    w.next(nseries, points, k.window());
                    if (!k.agg()) {
                        c.queryRaw(w.series, w.start, w.end, (ts, v, n) -> {
                            rows[0] += n;
                            return true;
                        });
                    } else {
                        for (Bucket b : c.queryAgg(w.series, w.start, w.end, k.bucket() * BenchCommon.stepMs(w.series)))
                            rows[0] += b.count();
                    }
                    queries++;
                }
            }
            out[2] = System.nanoTime() - t0;
        }
        out[0] = queries;
        out[1] = rows[0];
    }
}
