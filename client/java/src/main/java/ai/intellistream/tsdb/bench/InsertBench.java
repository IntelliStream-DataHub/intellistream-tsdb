package ai.intellistream.tsdb.bench;

import ai.intellistream.tsdb.TsdbClient;
import ai.intellistream.tsdb.TsdbException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Insert benchmark against tsdbd, the Java counterpart of {@code tsdbc-bench load}:
 *
 * <pre>
 *   InsertBench FILE.rowbinary [--host 127.0.0.1] [--port 7070] [--conns 1] [--batch 1000000]
 *               [--compression 1 | off]
 * </pre>
 *
 * FILE holds rows of (u32 series, i64 ts ms, f32 value), 16 bytes each, as {@code tsdb-bench --export} writes
 * them. The series are created first, named as tsdbc-bench names them; then the file is split into contiguous
 * slices, one per connection, and each connection sends its slice in batches with the many-series layout, with up
 * to four batches in flight before it waits for their acknowledgements. The file is mapped and read in before the
 * clock starts, as tsdbc-bench does (MAP_POPULATE), and so is the JIT compilation of the loop that copies its rows
 * into the arrays; rows are copied from the mapping during the run. The result is rows per second, the bytes written to the connections per row and the CPU time of the
 * process, in the format of tsdbc-bench ({@code BenchCommon}).
 */
public final class InsertBench {
    private static final String[] KINDS = {"hdd_temp", "cpu_temp", "tank_bar", "tank_psi", "vibration", "flow_rate",
        "flow_total"}; /* tools/datasets.h, as tsdbc-bench names its series */
    private static final int ROW = BenchCommon.ROW;
    private static final int INFLIGHT = 4;

    private InsertBench() {
    }

    /**
     * Runs the benchmark.
     *
     * @param args the file and options; see the class description
     * @throws Exception if the file cannot be read or the server fails
     */
    public static void main(String[] args) throws Exception {
        String host = "127.0.0.1", file = null;
        int port = TsdbClient.DEFAULT_PORT, conns = 1, batch = 1_000_000, level = 1;
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (!a.startsWith("--") && file == null) {
                file = a;
                continue;
            }
            if (i + 1 >= args.length)
                usage("missing value for " + a);
            String v = args[++i];
            switch (a) {
            case "--host": host = v; break;
            case "--port": port = Integer.parseInt(v); break;
            case "--conns": conns = Integer.parseInt(v); break;
            case "--batch": batch = Integer.parseInt(v); break;
            case "--compression": level = v.equals("off") ? 0 : Integer.parseInt(v); break;
            default: usage("unknown option " + a);
            }
        }
        if (file == null || conns < 1 || conns > 256 || batch < 1)
            usage(null);

        Path path = Paths.get(file);
        try (FileChannel fc = FileChannel.open(path, StandardOpenOption.READ)) {
            long size = fc.size();
            if (size % ROW != 0)
                usage(file + ": " + size + " bytes is not a whole number of 16-byte rows");
            long n = size / ROW;
            ByteBuffer[] rows = BenchCommon.mapRows(fc);
            int[] ids = createSeries(rows, n, host, port);
            BenchCommon.warmFill(rows, ids, batch);

            long[] bytes = new long[conns];
            AtomicReference<Throwable> failed = new AtomicReference<>();
            List<Thread> threads = new ArrayList<>();
            final String h = host;
            final int p = port, b = batch, l = level;
            long t0 = System.nanoTime();
            double cpu0 = BenchCommon.cpuSeconds();
            for (int c = 0; c < conns; c++) {
                long from = n * c / conns, to = n * (c + 1) / conns;
                int k = c;
                Thread t = new Thread(() -> {
                    try {
                        bytes[k] = load(rows, from, to, ids, h, p, b, l);
                    } catch (Throwable e) {
                        failed.compareAndSet(null, e);
                    }
                }, "loader-" + c);
                t.start();
                threads.add(t);
            }
            for (Thread t : threads)
                t.join();
            double dt = (System.nanoTime() - t0) / 1e9, cpu = BenchCommon.cpuSeconds() - cpu0;
            if (failed.get() != null) {
                System.err.println("InsertBench: " + failed.get());
                System.exit(1);
            }
            long wire = 0;
            for (long x : bytes)
                wire += x;
            BenchCommon.reportLoad("java", conns, level, n, dt, wire, cpu, ", batches of " + batch + ", "
                    + (level == 0 ? "plain" : "zstd level " + level));
        }
    }

    private static void usage(String why) {
        if (why != null)
            System.err.println("InsertBench: " + why);
        System.err.println("usage: InsertBench FILE.rowbinary [--host H] [--port P] [--conns N] [--batch ROWS]"
                + " [--compression LEVEL|off]");
        System.exit(2);
    }

    /** Creates series 0 .. the largest id in the first 10M rows, as tsdbc-bench does; returns file id to id. */
    private static int[] createSeries(ByteBuffer[] rows, long n, String host, int port) throws IOException {
        int max = 0;
        long scan = Math.min(n, 10_000_000);
        for (long r = 0; r < scan; r++)
            max = Math.max(max, BenchCommon.seriesAt(rows, r));
        int[] ids = new int[max + 1];
        try (TsdbClient c = TsdbClient.connect(host, port)) {
            for (int i = 0; i <= max; i++)
                ids[i] = c.series(KINDS[i % KINDS.length] + "." + i);
        }
        return ids;
    }

    /** Sends rows [from, to) over one connection; returns the bytes written to it. */
    private static long load(ByteBuffer[] rows, long from, long to, int[] ids, String host, int port, int batch,
                             int level) throws IOException {
        int[] s = new int[batch];
        long[] t = new long[batch];
        float[] v = new float[batch];
        try (TsdbClient c = TsdbClient.connect(host, port)) {
            c.setCompression(level);
            int inflight = 0;
            for (long r = from; r < to; r += batch) {
                int k = (int) Math.min(batch, to - r);
                if (inflight == INFLIGHT) { /* bound the pipeline, as tsdbc-bench */
                    c.await();
                    inflight = 0;
                }
                long bad = BenchCommon.fill(rows, r, k, ids, s, t, v);
                if (bad >= 0)
                    throw new TsdbException(-4, "series " + bad + " beyond those created from the first rows", null);
                c.insertAsync(s, t, v, 0, k); /* sent before it returns: the arrays may be refilled */
                inflight++;
            }
            c.await();
            return c.bytesSent();
        }
    }
}
