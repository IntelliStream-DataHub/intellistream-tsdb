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

/** Packed, zstd-compressed inserts: against tsdbd, and the frames they make, read by a fake server. */
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
                long b = c.seriesCreate(p + "bool", ValueType.BOOL);
                long[] many = new long[n];
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
            long id = c.series("large.packed");
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
            long id = c.series("levels");
            int k = 0;
            for (int level : new int[] {-5, 0, 3, 19, 1}) {
                c.setCompression(level);
                c.insert(id, new long[] {T0 + k}, new float[] {level});
                k++;
            }
            assertArrayEquals(new float[] {-5, 0, 3, 19, 1},
                    c.queryPoints(id, Long.MIN_VALUE, Long.MAX_VALUE).values());

            /* the level reaches zstd: fast levels send more bytes than strong ones */
            long big = c.series("levels.big");
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
            return payload.get(5) & 0xff;
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
        final Function<Frame, Integer> statuses;

        FakeServer(int version) throws IOException {
            this(version, f -> null);
        }

        FakeServer(int version, Function<Frame, byte[]> replies) throws IOException {
            this(version, replies, f -> 0);
        }

        /** {@code statuses} gives the status of each reply after HELLO; a reply with a status other than 0 is empty. */
        FakeServer(int version, Function<Frame, byte[]> replies, Function<Frame, Integer> statuses) throws IOException {
            this.replies = replies;
            this.statuses = statuses;
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
                    int status = op == Protocol.OP_HELLO ? 0 : statuses.apply(f);
                    byte[] body = op == Protocol.OP_HELLO || status != 0 ? null : replies.apply(f);
                    if (op == Protocol.OP_HELLO) /* from 12 on: u16 flags 0 (no login), u16 0 mechanisms */
                        body = ByteBuffer.allocate(version < 12 ? 4 : 8).order(ByteOrder.LITTLE_ENDIAN)
                                .putInt(version).array();
                    else if (body == null)
                        body = new byte[0];
                    ByteBuffer r = ByteBuffer.allocate(13 + body.length).order(ByteOrder.LITTLE_ENDIAN);
                    r.putInt(9 + body.length).putInt(id).put(Protocol.OP_REPLY).putInt(status).put(body);
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
            c.insertAsync(new long[] {5, 6, 5}, clock(3), new float[] {1, 2, 3});
            c.insertSeriesValues(6, clock(10), Column.ofLongs(new long[10]).withNulls(nulls));
            c.insert(5, new long[0], new float[0]); /* empty: plain, as the C client */
        }
        f.close();
        return f;
    }

    @Test
    void plainFramesCarryU64Ids() throws Exception {
        FakeServer f = run(Protocol.VERSION, 0);
        assertEquals(List.of(HELLO, INSERT, INSERT, VALUES, INSERT), f.ops(), "HELLO once, at connect");
        ByteBuffer one = f.frames.get(1).payload();
        assertEquals(16 + 100 * 12, one.limit());
        assertEquals(100, one.getInt(0));
        assertEquals(0, one.getInt(4));
        assertEquals(5, one.getLong(8));
        /* three rows, series per row: 16 + 3 * 12 = 52, padded to 56, then 3 x u64 */
        ByteBuffer many = f.frames.get(2).payload();
        assertEquals(56 + 24, many.limit());
        assertEquals(3, many.getInt(0));
        assertEquals(0, many.getInt(4));
        assertEquals(Protocol.MANY_SERIES, many.getLong(8));
        assertEquals(0, many.getInt(52), "padding");
        assertEquals(List.of(5L, 6L, 5L), List.of(many.getLong(56), many.getLong(64), many.getLong(72)));
        /* int64 with a null: the INSERT_VALUES header is n, type, flags, 0, series */
        ByteBuffer v = f.frames.get(3).payload();
        assertEquals(10, v.getInt(0));
        assertEquals(ValueType.INT64.code, v.get(4));
        assertEquals(Protocol.VALID, v.get(5));
        assertEquals(0, v.getShort(6));
        assertEquals(6, v.getLong(8));
        assertEquals(16 + 10 * 16 + 2, v.limit(), "plain int64 layout");
    }

    @Test
    void idsAboveU32GoOutWhole() throws Exception {
        long big = 0x1_0000_0005L, wide = 0xFFFF_FFFF_0000_0001L;
        for (int level : new int[] {0, 1}) {
            FakeServer f = new FakeServer(Protocol.VERSION);
            try (TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
                c.setCompression(level);
                c.insert(big, clock(4), readings(4));
                c.insert(new long[] {big, wide, 7, wide}, clock(4), readings(4));
                c.insertSeriesValues(wide, clock(4), Column.ofLongs(1, 2, 3, 4));
                c.insertValues(new long[] {wide, 0, big, 1L << 63}, clock(4), Column.ofLongs(1, 2, 3, 4));
            }
            f.close();
            ByteBuffer single = f.frames.get(1).payload(), many = f.frames.get(2).payload();
            ByteBuffer values = f.frames.get(3).payload(), valuesMany = f.frames.get(4).payload();
            assertEquals(big, single.getLong(8), "level " + level);
            assertEquals(Protocol.MANY_SERIES, many.getLong(8), "level " + level);
            assertEquals(wide, values.getLong(8), "level " + level);
            assertEquals(Protocol.MANY_SERIES, valuesMany.getLong(8), "level " + level);
            if (level == 0) { /* the ids follow the padded values, in full */
                int at = 16 + 4 * 12;
                assertEquals(List.of(big, wide, 7L, wide), List.of(many.getLong(at), many.getLong(at + 8),
                        many.getLong(at + 16), many.getLong(at + 24)));
                int vat = 16 + 4 * 16;
                assertEquals(List.of(wide, 0L, big, 1L << 63), List.of(valuesMany.getLong(vat),
                        valuesMany.getLong(vat + 8), valuesMany.getLong(vat + 16), valuesMany.getLong(vat + 24)));
            } else { /* the packed series column is wrapping zigzag deltas, whatever the ids are */
                ByteBuffer m = valuesMany;
                byte[] z = new byte[m.limit() - 20];
                m.get(20, z);
                assertArrayEquals(PackTest.reshape(clock(4), Column.ofLongs(1, 2, 3, 4),
                        new long[] {wide, 0, big, 1L << 63}, 0, 4), Zstd.decompress(z, m.getInt(16)));
            }
        }
    }

    @Test
    void packedFramesByDefault() throws Exception {
        FakeServer v3 = run(Protocol.VERSION, 1);
        assertEquals(List.of(HELLO, VALUES, VALUES, VALUES, INSERT), v3.ops());
        for (int i = 1; i <= 3; i++)
            assertEquals(Protocol.PACKED, v3.frames.get(i).flags() & Protocol.PACKED, "frame " + i);
        assertEquals(Protocol.PACKED | Protocol.VALID, v3.frames.get(3).flags());

        /* the first frame decodes to exactly what was inserted */
        ByteBuffer p = v3.frames.get(1).payload();
        assertEquals(100, p.getInt(0));
        assertEquals(ValueType.FLOAT32.code, p.get(4));
        assertEquals(Protocol.PACKED, p.get(5));
        assertEquals(0, p.getShort(6), "reserved bytes are zero");
        assertEquals(5, p.getLong(8));
        int rawLen = p.getInt(16);
        byte[] z = new byte[p.limit() - 20];
        p.get(20, z);
        byte[] raw = Zstd.decompress(z, rawLen);
        assertEquals(rawLen, raw.length);
        assertArrayEquals(PackTest.reshape(clock(100), Column.ofFloats(readings(100)), null, 0, 100), raw);

        /* the many-series frame carries the series ids */
        ByteBuffer m = v3.frames.get(2).payload();
        assertEquals(Protocol.MANY_SERIES, m.getLong(8));
        byte[] mz = new byte[m.limit() - 20];
        m.get(20, mz);
        assertArrayEquals(PackTest.reshape(clock(3), Column.ofFloats(1, 2, 3), new long[] {5, 6, 5}, 0, 3),
                Zstd.decompress(mz, m.getInt(16)));
    }

    @Test
    void levelZeroSendsPlainFrames() throws Exception {
        FakeServer f = run(Protocol.VERSION, 0);
        assertEquals(List.of(HELLO, INSERT, INSERT, VALUES, INSERT), f.ops(), "HELLO at connect, plain frames");
        assertEquals(Protocol.VALID, f.frames.get(3).flags());
    }

    @Test
    void helloAsksAgain() throws Exception {
        FakeServer f = new FakeServer(Protocol.VERSION);
        try (TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
            assertEquals(Protocol.VERSION, c.hello());
            c.insert(1, clock(5), readings(5));
        }
        f.close();
        assertEquals(List.of(HELLO, HELLO, VALUES), f.ops(), "connect asks once, hello() once more");
    }
}
