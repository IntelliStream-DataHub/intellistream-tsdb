// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

/**
 * Protocol 13 (docs/ha.md 9 and 15.4): the bodies of UNAVAILABLE, MOVED, PARTIAL and DROPPED, retry after
 * {@code retry_after} within a time budget, MOVED followed to the node it names, several nodes in a connect string,
 * TOPOLOGY. The refusals come from fake servers (a script per test); the outage is real: tsdbd on a storage domain
 * that outagefs takes away ({@code build/outage_server}, tests/outage_server.c).
 */
class HaTest {
    static final int WHY_NFS_DOWN = 1;
    static final int OP_FLUSH = 8;
    static final int OP_QUERY_RAW = 5;
    static final long SID = 0x1_0000_0001L;

    // ------------------------------------------------------------------ bodies

    static ByteBuffer buf(int n) {
        return ByteBuffer.allocate(n).order(ByteOrder.LITTLE_ENDIAN);
    }

    static byte[] unavailEntry(int vol, int retryAfterMs) {
        return buf(28).putShort((short) vol).put((byte) WHY_NFS_DOWN).put((byte) 0).putInt(7).putLong(123_456)
                .putInt(retryAfterMs).putLong(99).array();
    }

    static byte[] unavailableBody(int vol, int retryAfterMs) {
        return cat(buf(2).putShort((short) 1).array(), unavailEntry(vol, retryAfterMs));
    }

    static byte[] movedBody(String addr) {
        byte[] a = addr.getBytes(StandardCharsets.UTF_8);
        return buf(2 + 2 + 4 + 1 + 4 + 2 + a.length).putShort((short) 1).putShort((short) 2).putInt(5).put((byte) 0)
                .putInt(7).putShort((short) a.length).put(a).array();
    }

    static byte[] cat(byte[]... parts) {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        for (byte[] p : parts)
            o.writeBytes(p);
        return o.toByteArray();
    }

    /** The bytes the C codec makes (tests/data/wire13-vectors.txt, {@code test_wire --emit}). */
    static ByteBuffer vector(String name) throws IOException {
        Path p = Path.of(System.getProperty("tsdb.vectors", "../../tests/data/wire13-vectors.txt"));
        for (String l : Files.readAllLines(p)) {
            if (l.startsWith(name + " ")) {
                String hex = l.substring(name.length() + 1).trim();
                byte[] b = new byte[hex.length() / 2];
                for (int i = 0; i < b.length; i++)
                    b[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
                return ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN);
            }
        }
        throw new IllegalStateException("no vector " + name);
    }

    // ------------------------------------------------------------------ a fake server

    record Req(int conn, int op, int id, int nth) {
    }

    record Reply(int status, byte[] body) {
    }

    static Reply ok() {
        return new Reply(0, new byte[0]);
    }

    static Reply unavailable(int retryAfterMs) {
        return new Reply(-11, unavailableBody(1, retryAfterMs));
    }

    /** Listens on a port of its own; every connection is answered by the script (HELLO excepted: protocol 13). */
    static final class Fake implements AutoCloseable {
        final ServerSocket ss;
        final List<Req> log = Collections.synchronizedList(new ArrayList<>());
        private final Function<Req, Reply> script;

        Fake(Function<Req, Reply> script) throws IOException {
            this.script = script;
            ss = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            Thread t = new Thread(() -> {
                int conn = 0;
                try {
                    for (;;) {
                        Socket s = ss.accept();
                        int c = conn++;
                        Thread h = new Thread(() -> serve(c, s));
                        h.setDaemon(true);
                        h.start();
                    }
                } catch (IOException e) {
                    /* closed */
                }
            });
            t.setDaemon(true);
            t.start();
        }

        int port() {
            return ss.getLocalPort();
        }

        String addr() {
            return "127.0.0.1:" + port();
        }

        List<Req> seen(int op) {
            synchronized (log) {
                return log.stream().filter(r -> r.op() == op).toList();
            }
        }

        private void serve(int conn, Socket s) {
            try (s) {
                DataInputStream in = new DataInputStream(s.getInputStream());
                OutputStream out = s.getOutputStream();
                for (;;) {
                    byte[] h = new byte[9];
                    in.readFully(h);
                    ByteBuffer hb = ByteBuffer.wrap(h).order(ByteOrder.LITTLE_ENDIAN);
                    int len = hb.getInt(), id = hb.getInt(), op = h[8] & 0xff;
                    in.readFully(new byte[len - 5]);
                    Reply r;
                    if (op == 1) {
                        r = new Reply(0, buf(8).putInt(13).putInt(0).array());
                    } else {
                        Req req;
                        synchronized (log) {
                            int nth = (int) log.stream().filter(q -> q.op() == op).count();
                            req = new Req(conn, op, id, nth);
                            log.add(req);
                        }
                        r = script.apply(req);
                    }
                    byte[] f = buf(13 + r.body().length).putInt(9 + r.body().length).putInt(id).put((byte) 0)
                            .putInt(r.status()).put(r.body()).array();
                    out.write(f);
                    out.flush();
                }
            } catch (IOException e) {
                /* the client went away */
            }
        }

        @Override
        public void close() throws IOException {
            ss.close();
        }
    }

    static boolean isInsert(Req r) {
        return r.op() == Protocol.OP_INSERT || r.op() == Protocol.OP_INSERT_VALUES;
    }

    // ------------------------------------------------------------------ bodies against the C codec's bytes

    @Test
    void theCodecsBytesDecode() throws Exception {
        ErrorInfo e = ErrorInfo.of(-11, vector("unavailable"), Protocol.OP_INSERT);
        assertTrue(e.hasBody());
        assertEquals(2, e.unavailable().size());
        assertEquals(new ErrorInfo.Unavailable(3, 1, 0, 7, 1_700_000_000_123L, 300, 0x1122334455667788L),
                e.unavailable().get(0));
        assertEquals(ErrorInfo.VOL_ANY, e.unavailable().get(1).vol());
        assertEquals(-5, e.unavailable().get(1).sinceMs());
        assertEquals(60000, e.retryAfterMs());
        e = ErrorInfo.of(-11, vector("unavailable-resend"), Protocol.OP_INSERT);
        assertEquals(1, e.unavailable().size());
        assertEquals(0xA1B2C3D4E5F60718L, e.resendFrom());
        e = ErrorInfo.of(-10, vector("moved"), Protocol.OP_INSERT);
        assertTrue(e.hasBody());
        assertEquals(new ErrorInfo.Moved(1, 9, ErrorInfo.MOVED_HANDOFF, 4, "db2.example:7070"), e.moved().get(0));
        assertEquals("", e.moved().get(1).addr());
        e = ErrorInfo.of(-12, vector("partial"), Protocol.OP_INSERT);
        assertTrue(e.hasBody());
        assertEquals(List.of(new ErrorInfo.PartialVol(1, 0), new ErrorInfo.PartialVol(3, -11)), e.volumes());
        assertEquals(10, e.rows());
        List<Integer> rows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            if (e.rowNotApplied(i))
                rows.add(i);
        }
        assertEquals(List.of(2, 5, 8), rows);
        assertEquals(2, e.unavailable().size());
        e = ErrorInfo.of(-13, vector("dropped"), Protocol.OP_INSERT);
        assertTrue(e.hasBody());
        assertEquals(5, e.droppedVol());
        assertEquals(1_700_000_000_999L, e.droppedBefore());
        /* exactly 10 bytes, as the C codec reads it: a byte more is malformed */
        byte[] d = new byte[vector("dropped").remaining()];
        vector("dropped").get(d);
        assertEquals(10, d.length);
        assertFalse(ErrorInfo.of(-13, ByteBuffer.wrap(cat(d, new byte[1])), Protocol.OP_INSERT).hasBody());
        Topology t = Topology.decode(vector("topology"));
        assertNotNull(t);
        assertEquals(0xDEADBEEFL, t.generation());
        assertEquals(2, t.nodes().size());
        assertEquals(new Topology.Node(2, false, "10.0.0.2:7070"), t.nodes().get(1));
        assertEquals(1, t.volumes().get(0).role());
        assertEquals(0xffff, t.volumes().get(0).domain());
        assertEquals("/srv/a", t.volumes().get(0).path());
        assertEquals(2, t.volumes().get(1).health());
        assertEquals(2, t.volumes().get(1).owner());
        assertEquals("10.0.0.2:7070", t.volumes().get(1).addr());
        /* every prefix of every body is malformed, never an exception */
        for (String n : new String[] {"unavailable", "moved", "partial", "dropped"}) {
            byte[] v = new byte[vector(n).remaining()];
            vector(n).get(v);
            for (int st = -13; st <= -10; st++) {
                for (int k = 0; k < v.length; k++)
                    ErrorInfo.of(st, ByteBuffer.wrap(v, 0, k).slice(), Protocol.OP_INSERT);
            }
        }
        byte[] v = new byte[vector("topology").remaining()];
        vector("topology").get(v);
        for (int k = 0; k < v.length; k++)
            assertEquals(null, Topology.decode(ByteBuffer.wrap(v, 0, k).slice()), "prefix " + k);
    }

    @Test
    void nodesParse() {
        assertEquals("a:1", ErrorInfo.parseNode("a:1"));
        assertEquals("a:7070", ErrorInfo.parseNode(" a "));
        assertEquals("[::1]:9", ErrorInfo.parseNode("[::1]:9"));
        assertEquals("[::1]:7070", ErrorInfo.parseNode("[::1]"));
        assertEquals("[::1]:7070", ErrorInfo.parseNode("::1"));
        assertEquals(null, ErrorInfo.parseNode("a:0"));
        assertEquals(null, ErrorInfo.parseNode("a:x"));
        assertEquals(null, ErrorInfo.parseNode(""));
        assertEquals(null, ErrorInfo.parseNode("[::1"));
    }

    // ------------------------------------------------------------------ retry

    @Test
    void unavailableIsRetriedAfterRetryAfter() throws Exception {
        try (Fake f = new Fake(r -> r.nth() == 0 ? unavailable(300) : ok());
                TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
            long t0 = System.nanoTime();
            c.flush();
            long ms = (System.nanoTime() - t0) / 1_000_000;
            /* the server asked for 300 ms, so the request goes again no sooner (mutation "ignore retry_after") */
            assertTrue(ms >= 280 && ms < 5000, "retried after " + ms + " ms");
            List<Req> seen = f.seen(OP_FLUSH);
            assertEquals(2, seen.size());
            assertEquals(seen.get(0).id(), seen.get(1).id(), "the same request again");
            ErrorInfo e = c.lastError();
            assertEquals(-11, e.status());
            assertTrue(e.hasBody(), "the body was dropped");
            assertEquals(1, e.unavailable().size());
            ErrorInfo.Unavailable u = e.unavailable().get(0);
            assertEquals(List.of(1, WHY_NFS_DOWN, 7), List.of(u.vol(), u.reason(), (int) u.epoch()));
            assertEquals(123_456, u.sinceMs());
            assertEquals(99, u.outageId());
            assertEquals(300, e.retryAfterMs());
            c.flush(); /* the connection goes on with the next request */
        }
    }

    @Test
    void theBudgetRunsOut() throws Exception {
        try (Fake f = new Fake(r -> unavailable(400)); TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
            c.setRetry(Duration.ofMillis(1500));
            long t0 = System.nanoTime();
            TsdbException e = assertThrows(TsdbException.class, c::flush);
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertInstanceOf(UnavailableException.class, e);
            assertEquals(Status.ERR_UNAVAILABLE, e.status());
            assertEquals(400, e.info().unavailable().get(0).retryAfterMs());
            assertTrue(ms >= 1000 && ms < 4000, ms + " ms");
            int n = f.seen(OP_FLUSH).size();
            assertTrue(n >= 3 && n <= 5, n + " tries in 1.5 s at 400 ms");
            assertTrue(c.isOpen(), "the refusal is an answer: the connection is fine");
        }
    }

    @Test
    void retryOffReturnsTheFirstRefusal() throws Exception {
        try (Fake f = new Fake(r -> unavailable(5)); TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
            c.setRetry(Duration.ZERO);
            assertEquals(Status.ERR_UNAVAILABLE, assertThrows(TsdbException.class, c::flush).status());
            assertEquals(1, f.seen(OP_FLUSH).size());
            assertTrue(c.lastError().hasBody());
        }
    }

    @Test
    void aRefusalWithoutABodyWaitsASecond() throws Exception {
        try (Fake f = new Fake(r -> r.nth() == 0 ? new Reply(-11, new byte[0]) : ok());
                TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
            long t0 = System.nanoTime();
            c.flush();
            assertTrue((System.nanoTime() - t0) / 1_000_000 >= 950);
            assertFalse(c.lastError().hasBody());
            assertEquals(-11, c.lastError().status());
        }
    }

    @Test
    void aMalformedBodyIsOnlyAStatus() throws Exception {
        byte[] b = cat(buf(2).putShort((short) 2).array(), unavailEntry(1, 10)); /* two promised, one present */
        try (Fake f = new Fake(r -> r.nth() == 0 ? new Reply(-11, b) : ok());
                TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
            c.flush();
            assertFalse(c.lastError().hasBody());
        }
    }

    @Test
    void aStreamedReplyRefusedBeforeItBeginsIsRetried() throws Exception {
        /* QUERY_RAW: the empty chunk (last 1, n 0) comes first in an error reply, the body after it */
        try (Fake f = new Fake(r -> r.nth() == 0
                ? new Reply(-11, cat(new byte[] {1, 0, 0, 0, 0}, unavailableBody(1, 50)))
                : new Reply(0, new byte[] {1, 0, 0, 0, 0}));
                TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
            assertEquals(0, c.queryPoints(SID, 0, Long.MAX_VALUE).size());
            assertEquals(2, f.seen(OP_QUERY_RAW).size());
            assertTrue(c.lastError().hasBody());
        }
    }

    @Test
    void pipelinedInsertsGoAgainInOrderFromTheFirstRefused() throws Exception {
        /* four frames; the second is refused once: it and every frame after it are sent again, in order */
        try (Fake f = new Fake(r -> isInsert(r) && r.nth() == 1 ? unavailable(50) : ok());
                TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
            for (long i = 0; i < 4; i++)
                c.insertAsync(SID, new long[] {i}, new float[] {i});
            c.await();
            List<Integer> ids = new ArrayList<>();
            for (Req r : f.log) {
                if (isInsert(r))
                    ids.add(r.id());
            }
            int first = ids.get(0);
            assertEquals(List.of(first, first + 1, first + 2, first + 3, first + 1, first + 2, first + 3), ids);
            assertEquals(0, c.pending());
        }
    }

    @Test
    void aRefusedPipelinePastTheBudgetFailsTheAwait() throws Exception {
        try (Fake f = new Fake(r -> unavailable(500)); TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
            c.setRetry(Duration.ofMillis(700));
            c.insertAsync(SID, new long[] {1}, new float[] {1});
            UnavailableException e = assertThrows(UnavailableException.class, c::await);
            assertEquals(500, e.info().retryAfterMs());
            assertTrue(c.isOpen());
        }
    }

    @Test
    void aQueryMultiEndRefusedCarriesItsBody() throws Exception {
        /* the END frame (u32 index 0xffffffff, u32 answered) of an UNAVAILABLE multi, the body after it */
        byte[] end = cat(buf(8).putInt(Protocol.MULTI_END).putInt(0).array(), unavailableBody(1, 50));
        try (Fake f = new Fake(r -> new Reply(-11, end)); TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
            UnavailableException e = assertThrows(UnavailableException.class,
                    () -> c.queryValuesMulti(new long[] {SID}, 0, Long.MAX_VALUE));
            assertTrue(e.info().hasBody());
            assertEquals(50, e.info().retryAfterMs());
            assertTrue(c.isOpen(), "an answer, not a protocol error");
        }
    }

    static Reply statusReply(int lenDelta, String text) {
        byte[] t = text.getBytes(StandardCharsets.UTF_8);
        return new Reply(0, cat(buf(4).putInt(t.length + lenDelta).array(), t));
    }

    @Test
    void theStatusTextComesWithoutItsLength() throws Exception {
        try (Fake f = new Fake(r -> statusReply(0, "node.id=1\nvolumes=2\n"));
                TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
            assertEquals("node.id=1\nvolumes=2\n", c.serverStatus());
        }
    }

    @Test
    void aStatusWhoseLengthIsWrongIsCorrupt() throws Exception {
        for (int delta : new int[] {-1, 1}) {
            try (Fake f = new Fake(r -> statusReply(delta, "node.id=1\n"));
                    TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
                TsdbException e = assertThrows(TsdbException.class, c::serverStatus);
                assertEquals(Status.ERR_CORRUPT, e.status());
            }
        }
    }

    // ------------------------------------------------------------------ MOVED, nodes

    @Test
    void followingAMovedTakesAtMostTheTimeLeftOfTheBudget() throws Exception {
        /* B accepts the connection (the kernel does) and never answers the handshake: without the cap this call
         * waits the connect timeout */
        try (ServerSocket silent = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
                Fake a = new Fake(r -> new Reply(-10, movedBody("127.0.0.1:" + silent.getLocalPort())));
                TsdbClient c = TsdbClient.connect("127.0.0.1", a.port())) {
            c.setRetry(Duration.ofMillis(1000));
            long t0 = System.nanoTime();
            assertThrows(MovedException.class, c::flush);
            assertTrue((System.nanoTime() - t0) / 1_000_000 < 2000, "took " + (System.nanoTime() - t0) / 1_000_000);
        }
    }

    @Test
    void movedIsFollowedToTheNodeItNames() throws Exception {
        try (Fake b = new Fake(r -> ok()); Fake a = new Fake(r -> new Reply(-10, movedBody(b.addr())));
                TsdbClient c = TsdbClient.connect("127.0.0.1", a.port())) {
            c.flush();
            assertEquals(1, a.seen(OP_FLUSH).size());
            assertEquals(1, b.seen(OP_FLUSH).size(), "the request went to the new owner");
            ErrorInfo e = c.lastError();
            assertEquals(-10, e.status());
            assertTrue(e.hasBody());
            assertEquals(2, e.moved().get(0).vol());
            assertEquals(5, e.moved().get(0).epoch());
            assertEquals(7, e.moved().get(0).nodeId());
            assertEquals(b.addr(), e.moved().get(0).addr());
            c.flush(); /* later requests stay on B */
            assertEquals(1, a.seen(OP_FLUSH).size());
            assertEquals(2, b.seen(OP_FLUSH).size());
        }
    }

    @Test
    void movedWithoutAnAddressGoesToTheNextNode() throws Exception {
        try (Fake b = new Fake(r -> ok()); Fake a = new Fake(r -> new Reply(-10, movedBody("")));
                TsdbClient c = TsdbClient.connectNodes(a.addr() + "," + b.addr())) {
            c.flush(); /* from whichever the client picked, the other one is the owner */
            assertEquals(1, b.seen(OP_FLUSH).size());
            assertTrue(a.seen(OP_FLUSH).size() <= 1);
        }
    }

    @Test
    void movedWithNowhereToGoIsTheAnswer() throws Exception {
        try (Fake a = new Fake(r -> new Reply(-10, movedBody(""))); TsdbClient c = TsdbClient.connect("127.0.0.1", a.port())) {
            assertInstanceOf(MovedException.class, assertThrows(TsdbException.class, c::flush));
            assertTrue(c.isOpen());
        }
    }

    @Test
    void connectNodesSkipsADeadNode() throws Exception {
        int dead;
        try (ServerSocket s = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            dead = s.getLocalPort();
        }
        try (Fake b = new Fake(r -> ok())) {
            for (int i = 0; i < 4; i++) {
                try (TsdbClient c = TsdbClient.connectNodes("127.0.0.1:" + dead + ", " + b.addr())) {
                    c.flush();
                }
            }
            assertEquals(4, b.seen(OP_FLUSH).size());
        }
        assertThrows(TsdbException.class, () -> TsdbClient.connectNodes("127.0.0.1:" + dead));
        assertEquals(Status.ERR_INVALID, assertThrows(TsdbException.class, () -> TsdbClient.connectNodes("")).status());
        assertEquals(Status.ERR_INVALID, assertThrows(TsdbException.class, () -> TsdbClient.connectNodes("a:0")).status());
    }

    // ------------------------------------------------------------------ PARTIAL and DROPPED

    @Test
    void partialAndDroppedAreDecoded() throws Exception {
        try (Fake f = new Fake(r -> {
            if (r.nth() == 0) {
                ByteBuffer b = buf(2 + 8 + 4 + 2);
                b.putShort((short) 2).putShort((short) 1).putShort((short) 0).putShort((short) 3).putShort((short) -11);
                b.putInt(10).put((byte) 0x24).put((byte) 0x01);
                return new Reply(-12, cat(b.array(), unavailableBody(3, 100)));
            }
            return new Reply(-13, buf(10).putShort((short) 5).putLong(1_700_000_000_999L).array());
        }); TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
            /* neither is retried: PARTIAL is partly applied, DROPPED is final */
            PartialException p = assertThrows(PartialException.class, c::flush);
            ErrorInfo e = p.info();
            assertTrue(e.hasBody());
            assertEquals(2, e.volumes().size());
            assertEquals(new ErrorInfo.PartialVol(3, -11), e.volumes().get(1));
            assertEquals(10, e.rows());
            List<Integer> rows = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                if (e.rowNotApplied(i))
                    rows.add(i);
            }
            assertEquals(List.of(2, 5, 8), rows);
            assertEquals(100, e.unavailable().get(0).retryAfterMs());
            DroppedException d = assertThrows(DroppedException.class, c::flush);
            assertTrue(d.info().hasBody());
            assertEquals(5, d.info().droppedVol());
            assertEquals(1_700_000_000_999L, d.info().droppedBefore());
            assertEquals(2, f.seen(OP_FLUSH).size());
        }
    }

    // ------------------------------------------------------------------ tsdbd

    @Test
    void topologyOfASingleServer() throws Exception {
        Path dir = TsdbServer.scratch("tsdb-java-topo");
        try (TsdbServer s = TsdbServer.start(dir); TsdbClient c = s.client()) {
            Topology t = c.topology();
            assertEquals(1, t.nodes().size());
            assertTrue(t.nodes().get(0).alive());
            assertFalse(t.volumes().isEmpty());
            for (Topology.Vol v : t.volumes()) {
                assertEquals(1, v.role(), "owner");
                assertEquals(0, v.health(), "available");
            }
            assertEquals(t.generation(), c.topology().generation(), "the generation names the answer");
            /* --no-auth: STATUS is a superuser's, and --no-auth has none */
            try {
                String text = c.serverStatus();
                assertTrue(text.startsWith("node.id="), "the length prefix is not part of the text: " + text);
            } catch (TsdbException e) {
                assertEquals(Status.ERR_DENIED, e.status());
            }
        } finally {
            TsdbServer.delete(dir);
        }
    }

    /** tests/outage_server.c: tsdbd with volume A on a domain that outagefs can take away. */
    static final class Outage implements AutoCloseable {
        final Process p;
        final OutputStream stdin;
        final BufferedReader out;
        final int port, volA;
        final long sidA, sidRoot;
        final Path dir;

        Outage(Path dir) throws IOException {
            this.dir = dir;
            Path bin = TsdbServer.binary().resolveSibling("outage_server");
            Path lib = TsdbServer.binary().resolveSibling("liboutagefs.so");
            assumeTrue(Files.isExecutable(bin) && Files.exists(lib), bin + " or " + lib + " is missing (make builds them on Linux)");
            ProcessBuilder pb = new ProcessBuilder(bin.toString(), dir.toString());
            pb.environment().put("OUTAGEFS_LIB", lib.toString());
            pb.environment().put("TSDBD", TsdbServer.binary().toString());
            pb.environment().remove("LD_PRELOAD");
            pb.redirectError(ProcessBuilder.Redirect.INHERIT);
            p = pb.start();
            stdin = p.getOutputStream();
            out = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8));
            String line = out.readLine();
            String[] f = line == null ? new String[0] : line.trim().split("\\s+");
            assertTrue(f.length == 5 && f[0].equals("READY"), "outage_server said " + line);
            port = Integer.parseInt(f[1]);
            sidA = Long.parseLong(f[2]);
            sidRoot = Long.parseLong(f[3]);
            volA = Integer.parseInt(f[4]);
        }

        void command(String cmd) throws IOException {
            stdin.write((cmd + "\n").getBytes(StandardCharsets.UTF_8));
            stdin.flush();
            assertEquals("ok", out.readLine(), cmd);
        }

        TsdbClient client() throws TsdbException {
            return TsdbClient.connect("127.0.0.1", port);
        }

        @Override
        public void close() throws IOException {
            try {
                stdin.write("quit\n".getBytes(StandardCharsets.UTF_8));
                stdin.flush();
            } catch (IOException e) {
                /* gone */
            }
            try {
                if (!p.waitFor(60, TimeUnit.SECONDS))
                    p.destroyForcibly();
            } catch (InterruptedException e) {
                p.destroyForcibly();
                Thread.currentThread().interrupt();
            }
            try {
                TsdbServer.delete(dir);
            } catch (IOException e) {
                /* a scratch directory */
            }
        }
    }

    @Test
    void anOutageGivesUnavailableWithItsBodyAndTheClientRetriesThenSucceeds() throws Exception {
        Path dir = TsdbServer.scratch("tsdb-java-outage");
        try (Outage o = new Outage(dir); TsdbClient c = o.client()) {
            c.setRetry(Duration.ZERO);
            c.insert(o.sidA, new long[] {1000}, new float[] {1});
            assertTrue(c.topology().volumes().stream().allMatch(v -> v.health() == 0));

            o.command("down");
            UnavailableException e = assertThrows(UnavailableException.class,
                    () -> c.insert(o.sidA, new long[] {2000}, new float[] {2}));
            ErrorInfo info = e.info();
            assertTrue(info.hasBody(), "the body was dropped");
            ErrorInfo.Unavailable u = info.unavailable().get(0);
            assertEquals(o.volA, u.vol());
            assertTrue(u.reason() != 0 && u.retryAfterMs() > 0 && u.sinceMs() > 0, u.toString());
            c.insert(o.sidRoot, new long[] {2001}, new float[] {2}); /* the root answers while A is away */
            /* QUERY_MULTI over both: the down series is refused in its own (bare) frame, the other is answered, and
             * the connection stays usable */
            List<SeriesResult<ValuePoints>> m = c.queryValuesMulti(new long[] {o.sidA, o.sidRoot}, 0, Long.MAX_VALUE);
            assertEquals(2, m.size()); /* mutation "body on per-series frames": the connection is lost */
            assertFalse(m.get(0).ok());
            assertEquals(Status.ERR_UNAVAILABLE, m.get(0).status());
            assertTrue(m.get(1).ok());
            c.insert(o.sidRoot, new long[] {2002}, new float[] {2});
            Topology t = c.topology();
            Topology.Vol a = t.volumes().stream().filter(v -> v.vol() == o.volA).findFirst().orElseThrow();
            assertEquals(Topology.HEALTH_UNAVAILABLE, a.health());
            assertTrue(a.reason() != 0);
            assertTrue(t.volumes().stream().filter(v -> v.vol() != o.volA).allMatch(v -> v.health() == 0));

            /* a budget shorter than the outage: the status comes back after about the budget */
            try (TsdbClient c2 = o.client()) {
                c2.setRetry(Duration.ofMillis(1200));
                long t0 = System.nanoTime();
                assertThrows(UnavailableException.class, () -> c2.insert(o.sidA, new long[] {3000}, new float[] {3}));
                assertTrue((System.nanoTime() - t0) / 1_000_000 < 4000);
            }

            /* the default budget: the call waits, the volume heals, the call succeeds (once) */
            long[] took = new long[1];
            Throwable[] failed = new Throwable[1];
            Thread h = new Thread(() -> {
                try (TsdbClient c3 = o.client()) {
                    long t0 = System.nanoTime();
                    c3.insert(o.sidA, new long[] {4000}, new float[] {4}); /* mutation "no retry": UNAVAILABLE */
                    took[0] = (System.nanoTime() - t0) / 1_000_000;
                } catch (Throwable x) {
                    failed[0] = x;
                }
            });
            h.start();
            Thread.sleep(1500);
            o.command("heal");
            h.join();
            assertEquals(null, failed[0]);
            assertTrue(took[0] >= 1200 && took[0] < 25_000, took[0] + " ms");

            /* rows 1000 and 4000, not the refused 2000 and 3000 */
            int n = 0;
            for (int i = 0; i < 100; i++) {
                try {
                    n = c.queryPoints(o.sidA, 0, Long.MAX_VALUE).size();
                    break;
                } catch (TsdbException x) {
                    Thread.sleep(100);
                }
            }
            assertEquals(2, n);
        }
    }
}
