package ai.intellistream.tsdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import com.github.luben.zstd.Zstd;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Packed, zstd-compressed inserts (protocol version 3): against tsdbd, and against a fake server of any version. */
class CompressionTest {
    static final long T0 = TsdbClientTest.T0;

    static Path scratch;
    static TsdbServer server;

    @BeforeAll
    static void start() throws Exception {
        scratch = TsdbServer.scratch("tsdbd-java-zstd-");
        server = TsdbServer.start(scratch.resolve("db"));
    }

    @AfterAll
    static void stop() throws Exception {
        try {
            if (server != null)
                assertEquals(0, server.stop(), () -> TsdbClientTest.logOf(server));
        } finally {
            TsdbServer.delete(scratch);
        }
    }

    /** A regular clock and sensor-like values: what packing is for. */
    static long[] clock(int n) {
        return TsdbClientTest.seq(T0, 1000, n);
    }

    static float[] readings(int n) {
        float[] v = new float[n];
        for (int i = 0; i < n; i++)
            v[i] = 20 + (i % 600) / 10f;
        return v;
    }

    @Test
    void packedAndPlainStoreTheSameData() throws Exception {
        int n = 300_000;
        long[] ts = clock(n);
        BitSet nulls = new BitSet();
        for (int i = 0; i < n; i += 997)
            nulls.set(i);
        long[] longs = new long[n];
        double[] doubles = new double[n];
        boolean[] bools = new boolean[n];
        for (int i = 0; i < n; i++) {
            longs[i] = (i & 1) == 0 ? Long.MAX_VALUE - i : -i * 1_000_003L;
            doubles[i] = Math.sin(i) * 1e6;
            bools[i] = i % 7 < 3;
        }
        long[] wire = new long[2];
        for (int level : new int[] {1, 0}) {
            try (TsdbClient c = server.client()) {
                c.setCompression(level);
                assertEquals(level, c.compression());
                long before = c.bytesSent();
                String p = "same." + level + ".";
                c.insert(c.series(p + "f32"), ts, readings(n));
                c.insertSeriesValues(c.seriesCreate(p + "i64", ValueType.INT64), ts,
                        Column.ofLongs(longs).withNulls(nulls));
                c.insertSeriesValuesAsync(c.seriesCreate(p + "f64", ValueType.FLOAT64), ts, Column.ofDoubles(doubles));
                int b = c.seriesCreate(p + "bool", ValueType.BOOL);
                int[] many = new int[n];
                java.util.Arrays.fill(many, b);
                c.insertValues(many, ts, Column.ofBools(bools).withNulls(nulls));
                wire[level] = c.bytesSent() - before;
            }
        }
        try (TsdbClient c = server.client()) {
            for (String t : new String[] {"f32", "i64", "f64", "bool"}) {
                ValuePoints packed = c.queryValues(c.series("same.1." + t, false), Long.MIN_VALUE, Long.MAX_VALUE);
                ValuePoints plain = c.queryValues(c.series("same.0." + t, false), Long.MIN_VALUE, Long.MAX_VALUE);
                assertEquals(n, packed.size(), t);
                assertArrayEquals(ts, packed.timestamps(), t);
                assertArrayEquals(plain.timestamps(), packed.timestamps(), t);
                assertEquals(plain.values().nulls(), packed.values().nulls(), t);
                for (int i = 0; i < n; i++)
                    assertEquals(plain.values().getDouble(i), packed.values().getDouble(i), 0.0, t + " row " + i);
            }
            assertArrayEquals(readings(n), c.queryPoints(c.series("same.1.f32", false), T0, Long.MAX_VALUE).values());
        }
        assertTrue(wire[1] * 2 < wire[0], "packed " + wire[1] + " bytes, plain " + wire[0]);
    }

    @Test
    void largeBatchesSplitAndPipeline() throws Exception {
        int n = 2_000_000;
        long[] ts = clock(n);
        float[] v = readings(n);
        try (TsdbClient c = server.client()) {
            int id = c.series("large.packed");
            for (int off = 0; off < n; off += 500_000)
                c.insertAsync(id, ts, v, off, 500_000);
            c.await();
            double perRow = c.bytesSent() / (double) n;
            assertTrue(perRow < 4, perRow + " bytes a row on the wire");
            Bucket b = c.queryAgg(id, Long.MIN_VALUE, Long.MAX_VALUE, 0).get(0);
            assertEquals(n, b.count());
            double sum = 0;
            for (float x : v)
                sum += x;
            assertEquals(sum, b.sum(), 1e-3);
        }
    }

    @Test
    void compressionLevels() throws Exception {
        try (TsdbClient c = server.client()) {
            assertEquals(1, c.compression(), "level 1 by default, as the C client");
            assertThrows(IllegalArgumentException.class, () -> c.setCompression(23));
            assertThrows(IllegalArgumentException.class, () -> c.setCompression(-(1 << 17) - 1));
            int id = c.series("levels");
            int k = 0;
            for (int level : new int[] {-5, 0, 3, 19, 1}) {
                c.setCompression(level);
                c.insert(id, new long[] {T0 + k}, new float[] {level});
                k++;
            }
            assertArrayEquals(new float[] {-5, 0, 3, 19, 1},
                    c.queryPoints(id, Long.MIN_VALUE, Long.MAX_VALUE).values());

            /* the level reaches zstd: fast levels send more bytes than strong ones */
            int big = c.series("levels.big");
            long[] sent = new long[2];
            int[] levels = {-50, 19};
            for (int i = 0; i < 2; i++) {
                c.setCompression(levels[i]);
                long before = c.bytesSent();
                c.insert(big, clock(100_000), readings(100_000));
                sent[i] = c.bytesSent() - before;
            }
            assertTrue(sent[0] > sent[1] * 3 / 2, "level -50: " + sent[0] + " bytes, level 19: " + sent[1]);
        }
    }

    // ------------------------------------------------------------------ a fake server

    /** One received request frame. */
    record Frame(int op, ByteBuffer payload) {
        int flags() {
            return payload.get(9) & 0xff;
        }
    }

    /**
     * Answers HELLO with {@code version} and everything else with status 0 and the body {@code replies} gives for
     * the request (none if it gives null); records the frames.
     */
    static final class FakeServer implements AutoCloseable {
        final ServerSocketChannel listener;
        final List<Frame> frames = Collections.synchronizedList(new ArrayList<>());
        final Thread thread;
        final Function<Frame, byte[]> replies;

        FakeServer(int version) throws IOException {
            this(version, f -> null);
        }

        FakeServer(int version, Function<Frame, byte[]> replies) throws IOException {
            this.replies = replies;
            listener = ServerSocketChannel.open().bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            thread = new Thread(() -> serve(version), "fake-tsdbd");
            thread.setDaemon(true);
            thread.start();
        }

        int port() throws IOException {
            return ((InetSocketAddress) listener.getLocalAddress()).getPort();
        }

        private void serve(int version) {
            try (SocketChannel s = listener.accept()) {
                ByteBuffer hdr = ByteBuffer.allocate(9).order(ByteOrder.LITTLE_ENDIAN);
                for (;;) {
                    hdr.clear();
                    if (!readFully(s, hdr))
                        return;
                    int len = hdr.getInt(0), id = hdr.getInt(4), op = hdr.get(8);
                    ByteBuffer p = ByteBuffer.allocate(len - 5).order(ByteOrder.LITTLE_ENDIAN);
                    if (!readFully(s, p))
                        return;
                    p.flip();
                    Frame f = new Frame(op, p);
                    frames.add(f);
                    byte[] body = op == Protocol.OP_HELLO ? null : replies.apply(f);
                    if (op == Protocol.OP_HELLO)
                        body = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(version).array();
                    else if (body == null)
                        body = new byte[0];
                    ByteBuffer r = ByteBuffer.allocate(13 + body.length).order(ByteOrder.LITTLE_ENDIAN);
                    r.putInt(9 + body.length).putInt(id).put(Protocol.OP_REPLY).putInt(0).put(body);
                    r.flip();
                    while (r.hasRemaining())
                        s.write(r);
                }
            } catch (IOException e) {
                /* the client went away */
            }
        }

        private static boolean readFully(SocketChannel s, ByteBuffer b) throws IOException {
            while (b.hasRemaining())
                if (s.read(b) < 0)
                    return false;
            return true;
        }

        List<Integer> ops() {
            synchronized (frames) {
                return frames.stream().map(Frame::op).toList();
            }
        }

        @Override
        public void close() throws IOException {
            listener.close();
            try {
                thread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    static final int HELLO = Protocol.OP_HELLO, INSERT = Protocol.OP_INSERT, VALUES = Protocol.OP_INSERT_VALUES;

    /** Inserts a float32 batch and an int64 batch with a null; returns the frames the server saw. */
    static FakeServer run(int version, int level) throws Exception {
        FakeServer f = new FakeServer(version);
        try (TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
            c.setCompression(level);
            c.insert(5, clock(100), readings(100));
            BitSet nulls = new BitSet();
            nulls.set(3);
            c.insertAsync(new int[] {5, 6, 5}, clock(3), new float[] {1, 2, 3});
            c.insertSeriesValues(6, clock(10), Column.ofLongs(new long[10]).withNulls(nulls));
            c.insert(5, new long[0], new float[0]); /* empty: plain, as the C client */
        }
        f.close();
        return f;
    }

    @Test
    void oldServersGetPlainArrays() throws Exception {
        FakeServer v2 = run(2, 1);
        assertEquals(List.of(HELLO, INSERT, INSERT, VALUES, INSERT), v2.ops(), "HELLO once, then version 1/2 ops");
        assertEquals(Protocol.VALID, v2.frames.get(3).flags());
        assertEquals(16 + 10 * 16 + 2, v2.frames.get(3).payload().limit(), "plain int64 layout");
    }

    @Test
    void version3ServersGetPackedFrames() throws Exception {
        FakeServer v3 = run(3, 1);
        assertEquals(List.of(HELLO, VALUES, VALUES, VALUES, INSERT), v3.ops());
        for (int i = 1; i <= 3; i++)
            assertEquals(Protocol.PACKED, v3.frames.get(i).flags() & Protocol.PACKED, "frame " + i);
        assertEquals(Protocol.PACKED | Protocol.VALID, v3.frames.get(3).flags());

        /* the first frame decodes to exactly what was inserted */
        ByteBuffer p = v3.frames.get(1).payload();
        assertEquals(100, p.getInt(0));
        assertEquals(5, p.getInt(4));
        assertEquals(ValueType.FLOAT32.code, p.get(8));
        assertEquals(0, p.getShort(10) | p.getInt(12), "reserved bytes are zero");
        int rawLen = p.getInt(16);
        byte[] z = new byte[p.limit() - 20];
        p.get(20, z);
        byte[] raw = Zstd.decompress(z, rawLen);
        assertEquals(rawLen, raw.length);
        assertArrayEquals(PackTest.reshape(clock(100), Column.ofFloats(readings(100)), null, 0, 100), raw);

        /* the many-series frame carries the series ids */
        ByteBuffer m = v3.frames.get(2).payload();
        assertEquals(Protocol.MANY_SERIES, m.getInt(4));
        byte[] mz = new byte[m.limit() - 20];
        m.get(20, mz);
        assertArrayEquals(PackTest.reshape(clock(3), Column.ofFloats(1, 2, 3), new int[] {5, 6, 5}, 0, 3),
                Zstd.decompress(mz, m.getInt(16)));
    }

    @Test
    void levelZeroNeverAsks() throws Exception {
        FakeServer f = run(3, 0);
        assertEquals(List.of(INSERT, INSERT, VALUES, INSERT), f.ops(), "no HELLO, plain frames");
        assertEquals(Protocol.VALID, f.frames.get(2).flags());
    }

    @Test
    void helloDecidesToo() throws Exception {
        FakeServer f = new FakeServer(2);
        try (TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
            assertEquals(2, c.hello());
            c.insert(1, clock(5), readings(5));
        }
        f.close();
        assertEquals(List.of(HELLO, INSERT), f.ops(), "the version from hello() is kept");
    }
}
