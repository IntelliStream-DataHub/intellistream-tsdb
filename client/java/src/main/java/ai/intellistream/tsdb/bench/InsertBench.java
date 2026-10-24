package ai.intellistream.tsdb.bench;

import ai.intellistream.tsdb.TsdbClient;
import ai.intellistream.tsdb.TsdbException;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
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
 * to four batches in flight before it waits for their acknowledgements. Rows are read from the memory-mapped file
 * during the run. The result is rows per second, the bytes written to the connections per row and the CPU time of the
 * process, in the format of tsdbc-bench ({@code BenchCommon}).
 */
public final class InsertBench {
    private static final String[] KINDS = {"hdd_temp", "cpu_temp", "tank_bar", "tank_psi", "vibration", "flow_rate",
        "flow_total"}; /* tools/datasets.h, as tsdbc-bench names its series */
    private static final int ROW = 16;
    private static final int INFLIGHT = 4;
    private static final long SEGMENT = 1L << 30; /* bytes mapped at a time */

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
            int[] ids = createSeries(fc, n, host, port);

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
                        bytes[k] = load(fc, from, to, ids, h, p, b, l);
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
    private static int[] createSeries(FileChannel fc, long n, String host, int port) throws IOException {
        int max = 0;
        long scan = Math.min(n, 10_000_000);
        for (long seg = 0; seg < scan; seg += SEGMENT / ROW) {
            int rows = (int) Math.min(SEGMENT / ROW, scan - seg);
            MappedByteBuffer m = map(fc, seg, rows);
            for (int i = 0; i < rows; i++)
                max = Math.max(max, m.getInt(i * ROW));
        }
        int[] ids = new int[max + 1];
        try (TsdbClient c = TsdbClient.connect(host, port)) {
            for (int i = 0; i <= max; i++)
                ids[i] = c.series(KINDS[i % KINDS.length] + "." + i);
        }
        return ids;
    }

    private static MappedByteBuffer map(FileChannel fc, long row, int rows) throws IOException {
        MappedByteBuffer m = fc.map(FileChannel.MapMode.READ_ONLY, row * ROW, (long) rows * ROW);
        m.order(ByteOrder.LITTLE_ENDIAN);
        return m;
    }

    /** Sends rows [from, to) over one connection; returns the bytes written to it. */
    private static long load(FileChannel fc, long from, long to, int[] ids, String host, int port, int batch,
                             int level) throws IOException {
        int[] s = new int[batch];
        long[] t = new long[batch];
        float[] v = new float[batch];
        long segRows = Math.max(batch, SEGMENT / ROW / batch * batch); /* whole batches per mapping */
        try (TsdbClient c = TsdbClient.connect(host, port)) {
            c.setCompression(level);
            int inflight = 0;
            for (long seg = from; seg < to; seg += segRows) {
                int rows = (int) Math.min(segRows, to - seg);
                MappedByteBuffer m = map(fc, seg, rows);
                for (int off = 0; off < rows; off += batch) {
                    int k = Math.min(batch, rows - off);
                    if (inflight == INFLIGHT) { /* bound the pipeline, as tsdbc-bench */
                        c.await();
                        inflight = 0;
                    }
                    for (int i = 0, o = off * ROW; i < k; i++, o += ROW) {
                        int id = m.getInt(o);
                        if (id < 0 || id >= ids.length)
                            throw new TsdbException(-4, "series " + Integer.toUnsignedString(id)
                                    + " beyond those created from the first rows", null);
                        s[i] = ids[id];
                        t[i] = m.getLong(o + 4);
                        v[i] = m.getFloat(o + 12);
                    }
                    c.insertAsync(s, t, v, 0, k); /* sent before it returns: the arrays may be refilled */
                    inflight++;
                }
            }
            c.await();
            return c.bytesSent();
        }
    }
}
