package ai.intellistream.tsdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import jdk.net.ExtendedSocketOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Request timeouts against fake servers that stall. No tsdbd here: each test runs a listener that accepts and then
 * misbehaves in one way, and checks that the call fails with a timed-out ERR_IO within bounds, that the client is
 * unusable afterwards, and that nothing hangs.
 */
/* a regression in the timeouts is a hang: fail the test (the wait honours the interrupt) instead of stalling the build */
@Timeout(60)
class TimeoutTest {
    static final Duration TIMEOUT = Duration.ofMillis(300);
    static final long BOUND_MS = 5_000;

    /** What a fake server does with its connection. */
    interface Behaviour {
        void run(Socket s, CountDownLatch release) throws IOException;
    }

    ServerSocket listener;
    Thread thread;
    final CountDownLatch release = new CountDownLatch(1);

    @AfterEach
    void stop() throws Exception {
        release.countDown();
        if (listener != null)
            listener.close();
        if (thread != null)
            thread.join(5_000);
    }

    /** Accepts one connection, answers its HELLO (the client asks at connect) and then runs {@code b}. */
    void serve(Behaviour b) throws Exception {
        listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        thread = new Thread(() -> {
            try (Socket s = listener.accept()) {
                answerHello(s);
                b.run(s, release);
            } catch (IOException e) {
                /* the test is over */
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    TsdbClient connect(Behaviour b) throws Exception {
        serve(b);
        TsdbClient c = TsdbClient.connect("127.0.0.1", listener.getLocalPort(), Duration.ofSeconds(5), TIMEOUT);
        c.setCompression(0);
        return c;
    }

    /** Reads the HELLO a client sends at connect and answers it with the version the client speaks. */
    static void answerHello(Socket s) throws IOException {
        int[] r = readRequest(s);
        if (r[1] != Protocol.OP_HELLO)
            throw new IOException("first request " + r[1] + ", not HELLO");
        s.getOutputStream().write(helloReply(r[0], Protocol.VERSION));
    }

    static byte[] helloReply(int id, int version) {
        return ByteBuffer.allocate(17).order(ByteOrder.LITTLE_ENDIAN).putInt(13).putInt(id).put((byte) 0).putInt(0)
                .putInt(version).array();
    }

    /** Reads one request frame and returns its id and op. */
    static int[] readRequest(Socket s) throws IOException {
        DataInputStream in = new DataInputStream(s.getInputStream());
        byte[] h = new byte[4];
        in.readFully(h);
        int len = ByteBuffer.wrap(h).order(ByteOrder.LITTLE_ENDIAN).getInt();
        byte[] body = new byte[len];
        in.readFully(body);
        ByteBuffer b = ByteBuffer.wrap(body).order(ByteOrder.LITTLE_ENDIAN);
        return new int[] {b.getInt(), b.get()};
    }

    /** A reply with status 0 and no payload. */
    static byte[] ack(int id) {
        return ByteBuffer.allocate(13).order(ByteOrder.LITTLE_ENDIAN).putInt(9).putInt(id).put((byte) 0).putInt(0)
                .array();
    }

    static void hold(CountDownLatch release) throws IOException {
        try {
            release.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    interface Call {
        void run() throws Exception;
    }

    /** The call timed out within bounds, and the client now fails at once. */
    static void assertTimedOut(TsdbClient c, long startNanos, Call call) throws Exception {
        TsdbException e = assertThrows(TsdbException.class, call::run);
        long ms = (System.nanoTime() - startNanos) / 1_000_000;
        assertEquals(Status.ERR_IO, e.status());
        assertTrue(e.isTimeout(), "not a timeout: " + e);
        assertTrue(ms >= TIMEOUT.toMillis() - 50, "returned after " + ms + " ms, before the timeout");
        assertTrue(ms < BOUND_MS, "took " + ms + " ms");
        assertFalse(c.isOpen());
        long again = System.nanoTime();
        TsdbException f = assertThrows(TsdbException.class, () -> c.series("x"));
        assertEquals(Status.ERR_IO, f.status());
        assertTrue((System.nanoTime() - again) / 1_000_000 < 100, "a broken client must fail fast");
        assertThrows(TsdbException.class, c::await);
    }

    @Test
    void serverThatNeverAnswers() throws Exception {
        TsdbClient c = connect((s, r) -> {
            readRequest(s);
            hold(r);
        });
        assertEquals(TIMEOUT, c.requestTimeout());
        long t = System.nanoTime();
        assertTimedOut(c, t, () -> c.series("a"));
    }

    @Test
    void serverThatAcceptsAndNeverReads() throws Exception {
        TsdbClient c = connect((s, r) -> hold(r));
        long t = System.nanoTime();
        assertTimedOut(c, t, () -> c.series("a"));
    }

    @Test
    void serverThatAnswersHalfAFrame() throws Exception {
        TsdbClient c = connect((s, r) -> {
            int[] req = readRequest(s);
            OutputStream out = s.getOutputStream();
            out.write(ack(req[0]), 0, 6);
            out.flush();
            hold(r);
        });
        long t = System.nanoTime();
        assertTimedOut(c, t, () -> c.series("a"));
    }

    @Test
    void serverThatAnswersTheHeaderButNotThePayload() throws Exception {
        TsdbClient c = connect((s, r) -> {
            int[] req = readRequest(s);
            byte[] a = ack(req[0]);
            ByteBuffer.wrap(a).order(ByteOrder.LITTLE_ENDIAN).putInt(0, 9 + 4); /* announces 4 payload bytes */
            s.getOutputStream().write(a);
            s.getOutputStream().flush();
            hold(r);
        });
        long t = System.nanoTime();
        assertTimedOut(c, t, () -> c.series("a"));
    }

    @Test
    void serverThatStopsAnsweringAmongPipelinedInserts() throws Exception {
        TsdbClient c = connect((s, r) -> {
            for (int i = 0; i < 5; i++) {
                int[] req = readRequest(s);
                s.getOutputStream().write(ack(req[0]));
                s.getOutputStream().flush();
            }
            hold(r);
        });
        long[] ts = {1, 2, 3};
        float[] v = {1f, 2f, 3f};
        for (int i = 0; i < 20; i++)
            c.insertAsync(1, ts, v);
        assertEquals(20, c.pending());
        long t = System.nanoTime();
        assertTimedOut(c, t, c::await);
    }

    @Test
    void serverThatStopsReadingAmongPipelinedInserts() throws Exception {
        /* takes 10 inserts, then stops reading: the socket buffers fill up and a send blocks */
        TsdbClient c = connect((s, r) -> {
            for (int i = 0; i < 10; i++) {
                int[] req = readRequest(s);
                s.getOutputStream().write(ack(req[0]));
                s.getOutputStream().flush();
            }
            hold(r);
        });
        int n = 200_000;
        long[] ts = new long[n];
        float[] v = new float[n];
        long t = System.nanoTime();
        int[] sent = {0};
        TsdbException e = assertThrows(TsdbException.class, () -> {
            while (sent[0] < 1000 && (System.nanoTime() - t) / 1_000_000 < BOUND_MS) {
                c.insertAsync(1, ts, v);
                sent[0]++;
            }
        });
        assertTrue(sent[0] >= 10, "the first inserts fit in the buffers: " + sent[0]);
        assertEquals(Status.ERR_IO, e.status());
        assertTrue(e.isTimeout(), e.toString());
        assertTrue((System.nanoTime() - t) / 1_000_000 < BOUND_MS);
        assertFalse(c.isOpen());
        assertThrows(TsdbException.class, c::await);
        c.close(); /* nothing to wait for */
    }

    @Test
    void aSlowButLiveServerIsNotCutOff() throws Exception {
        /* the timeout bounds each wait: a reply in two pieces, each within it, passes */
        TsdbClient c = connect((s, r) -> {
            while (true) {
                int[] req = readRequest(s);
                byte[] a = ack(req[0]);
                OutputStream out = s.getOutputStream();
                sleep(TIMEOUT.toMillis() / 3);
                out.write(a, 0, 6);
                out.flush();
                sleep(TIMEOUT.toMillis() / 3);
                out.write(a, 6, a.length - 6);
                out.flush();
            }
        });
        for (int i = 0; i < 3; i++)
            c.flush();
        assertTrue(c.isOpen());
        c.close(); /* the fake server loops until the connection ends */
    }

    static void sleep(long ms) throws IOException {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Test
    void noTimeoutByDefaultAndItCanBeChanged() throws Exception {
        serve((s, r) -> hold(r));
        try (TsdbClient c = TsdbClient.connect("127.0.0.1", listener.getLocalPort())) {
            assertEquals(Duration.ZERO, c.requestTimeout());
            c.setRequestTimeout(Duration.ofSeconds(7));
            assertEquals(Duration.ofSeconds(7), c.requestTimeout());
            c.setRequestTimeout(Duration.ZERO);
            assertEquals(Duration.ZERO, c.requestTimeout());
            assertThrows(IllegalArgumentException.class, () -> c.setRequestTimeout(Duration.ofMillis(-1)));
        }
        assertThrows(IllegalArgumentException.class, () -> TsdbClient.connect("127.0.0.1", 1, Duration.ZERO,
                Duration.ofMillis(-1)));
    }

    @Test
    void timeoutCanBeSetOnALiveClient() throws Exception {
        TsdbClient c = connect((s, r) -> hold(r));
        c.setRequestTimeout(Duration.ofMillis(150));
        long t = System.nanoTime();
        TsdbException e = assertThrows(TsdbException.class, () -> c.series("a"));
        long ms = (System.nanoTime() - t) / 1_000_000;
        assertTrue(e.isTimeout());
        assertTrue(ms >= 100 && ms < 2_000, ms + " ms");
    }

    /** A listener whose accept queue is full: further SYNs are dropped, so a connect hangs whatever the routing. */
    void fillAcceptQueue() throws Exception {
        listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        int connected = 0;
        for (int i = 0; i < 5; i++) {
            try {
                Socket s = new Socket();
                held.add(s);
                s.connect(listener.getLocalSocketAddress(), 200);
                connected++;
            } catch (IOException e) {
                break;
            }
        }
        assertTrue(connected > 0 && connected < 5, "the accept queue must fill up, " + connected + " connected");
    }

    final java.util.List<Socket> held = new java.util.ArrayList<>();

    @AfterEach
    void releaseHeld() throws IOException {
        for (Socket s : held)
            s.close();
    }

    @Test
    void connectGivesUp() throws Exception {
        fillAcceptQueue();
        long t = System.nanoTime();
        TsdbException e = assertThrows(TsdbException.class, () -> TsdbClient.connect("127.0.0.1",
                listener.getLocalPort(), Duration.ofMillis(300), TIMEOUT));
        long ms = (System.nanoTime() - t) / 1_000_000;
        assertEquals(Status.ERR_IO, e.status());
        assertTrue(ms >= 250 && ms < BOUND_MS, ms + " ms");
    }

    @Test
    void aConnectTimeUnderOneMillisecondStillBoundsTheConnect() throws Exception {
        fillAcceptQueue();
        long t = System.nanoTime();
        assertThrows(TsdbException.class, () -> TsdbClient.connect("127.0.0.1", listener.getLocalPort(),
                Duration.ofNanos(500_000), TIMEOUT));
        assertTrue((System.nanoTime() - t) / 1_000_000 < BOUND_MS, "0 ms would wait as long as the OS does");
    }

    @Test
    void aNegativeTimeoutIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> TsdbClient.connect("127.0.0.1", 1, Duration.ofMillis(-1),
                TIMEOUT));
        assertThrows(IllegalArgumentException.class, () -> TsdbClient.connect("127.0.0.1", 1, TIMEOUT,
                Duration.ofMillis(-1)));
    }

    @Test
    void anIoErrorThatIsNoTimeoutIsNotReportedAsOne() throws Exception {
        /* the server hangs up: the call fails with ERR_IO, but nothing timed out */
        TsdbClient c = connect((s, r) -> s.close());
        c.setRequestTimeout(Duration.ofSeconds(10));
        TsdbException e = assertThrows(TsdbException.class, () -> c.series("a"));
        assertEquals(Status.ERR_IO, e.status());
        assertFalse(e.isTimeout(), e.toString());
        /* and a refused connection */
        listener.close();
        TsdbException f = assertThrows(TsdbException.class, () -> TsdbClient.connect("127.0.0.1",
                listener.getLocalPort(), TIMEOUT, TIMEOUT));
        assertFalse(f.isTimeout(), f.toString());
    }

    @Test
    void keepAliveIsOnByDefaultAndCanBeChanged() throws Exception {
        serve((s, r) -> hold(r));
        try (TsdbClient c = TsdbClient.connect("127.0.0.1", listener.getLocalPort())) {
            assertEquals(Boolean.TRUE, c.socketOption(StandardSocketOptions.SO_KEEPALIVE));
            assertProbes(c, 30, 10, 3);

            c.setKeepAlive(Duration.ofSeconds(5), Duration.ofMillis(1500), 2);
            assertProbes(c, 5, 2, 2); /* rounded up to whole seconds */

            c.setKeepAlive(false);
            assertEquals(Boolean.FALSE, c.socketOption(StandardSocketOptions.SO_KEEPALIVE));
            c.setKeepAlive(true);
            assertEquals(Boolean.TRUE, c.socketOption(StandardSocketOptions.SO_KEEPALIVE));
            assertProbes(c, 30, 10, 3);
        }
    }

    /** The probe values, where the platform lets a program read them. */
    static void assertProbes(TsdbClient c, int idle, int interval, int count) throws IOException {
        Integer i = c.socketOption(ExtendedSocketOptions.TCP_KEEPIDLE);
        if (i == null)
            return;
        assertEquals(idle, i);
        assertEquals(interval, c.socketOption(ExtendedSocketOptions.TCP_KEEPINTERVAL));
        assertEquals(count, c.socketOption(ExtendedSocketOptions.TCP_KEEPCOUNT));
    }
}
