package ai.intellistream.tsdb.pulsar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.intellistream.tsdb.Status;
import ai.intellistream.tsdb.TsdbClient;
import ai.intellistream.tsdb.TsdbException;
import ai.intellistream.tsdb.ValuePoints;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A tsdb that stops answering: the sink's connection times out, closes, and the batch goes again on a new connection.
 * The "server" is a proxy in front of a real tsdbd whose first connection is accepted and never served.
 */
class RequestTimeoutTest {
    private Path dir;
    private Tsdbd server;
    private ServerSocket listener;
    private final CountDownLatch release = new CountDownLatch(1);
    private final AtomicInteger accepted = new AtomicInteger();
    private final List<Socket> open = new ArrayList<>();
    private TsdbSink sink;

    @BeforeEach
    void start() throws Exception {
        dir = Tsdbd.scratch("tsdb-pulsar-timeout-");
        server = Tsdbd.start(dir);
        listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        Thread t = new Thread(this::accept);
        t.setDaemon(true);
        t.start();
    }

    @AfterEach
    void stop() throws Exception {
        release.countDown();
        if (sink != null)
            sink.close();
        listener.close();
        synchronized (open) {
            for (Socket s : open)
                s.close();
        }
        server.close();
        Tsdbd.delete(dir);
    }

    /** The first connection goes nowhere; the others are piped to the real server. */
    private void accept() {
        try {
            while (true) {
                Socket in = listener.accept();
                synchronized (open) {
                    open.add(in);
                }
                if (accepted.getAndIncrement() == 0) {
                    answerHello(in); /* the client asks at connect; every request after it stalls */
                    continue;
                }
                Socket out = new Socket(InetAddress.getLoopbackAddress(), server.port);
                synchronized (open) {
                    open.add(out);
                }
                pipe(in, out);
                pipe(out, in);
            }
        } catch (IOException e) {
            /* closed */
        }
    }

    /** Reads the HELLO a client sends at connect and answers protocol 11 (u32 len, u32 id, u8 op, u32 version). */
    private static void answerHello(Socket s) throws IOException {
        java.io.DataInputStream in = new java.io.DataInputStream(s.getInputStream());
        java.nio.ByteBuffer h = java.nio.ByteBuffer.allocate(9).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        in.readFully(h.array());
        int len = h.getInt(0), id = h.getInt(4);
        in.readFully(new byte[len - 5]);
        s.getOutputStream().write(java.nio.ByteBuffer.allocate(17).order(java.nio.ByteOrder.LITTLE_ENDIAN)
                .putInt(13).putInt(id).put((byte) 0).putInt(0).putInt(11).array());
    }

    private static void pipe(Socket from, Socket to) {
        Thread t = new Thread(() -> {
            byte[] b = new byte[65536];
            try (InputStream i = from.getInputStream()) {
                OutputStream o = to.getOutputStream();
                for (int n; (n = i.read(b)) >= 0;) {
                    o.write(b, 0, n);
                    o.flush();
                }
            } catch (IOException e) {
                /* closed */
            }
        });
        t.setDaemon(true);
        t.start();
    }

    private TsdbSinkConfig config() {
        TsdbSinkConfig c = new TsdbSinkConfig();
        c.port = listener.getLocalPort();
        c.lingerMs = 5;
        c.retryInitialMs = 20;
        c.retryMaxMs = 100;
        c.connectTimeoutMs = 1000;
        c.requestTimeoutMs = 300;
        c.closeTimeoutMs = 5000;
        return c;
    }

    @Test
    void theDefaultIsThirtySecondsAndZeroIsAllowed() {
        TsdbSinkConfig c = new TsdbSinkConfig();
        assertEquals(30_000, c.requestTimeoutMs);
        c.validate();
        c.requestTimeoutMs = 0;
        c.validate();
        c.requestTimeoutMs = -1;
        assertThrows(IllegalArgumentException.class, c::validate);
    }

    @Test
    void aTimeoutIsRetryable() {
        TsdbException e = new TsdbException(Status.ERR_IO, "timed out waiting for the server",
                new java.net.SocketTimeoutException("no progress"));
        assertTrue(e.isTimeout());
        assertEquals(ErrorClassifier.Kind.RETRYABLE, new ErrorClassifier(java.util.Set.of(), java.util.Set.of()).classify(e));
    }

    @Test
    void theConnectionTimesOutAndIsNotUsableAfterwards() throws Exception {
        TsdbSinkConfig c = config();
        c.validate();
        TsdbConnection conn = TsdbConnection.client(c).connect(); /* the stalled first connection */
        long t = System.nanoTime();
        TsdbException e = assertThrows(TsdbException.class, () -> conn.lookup("pump"));
        long ms = (System.nanoTime() - t) / 1_000_000;
        assertTrue(e.isTimeout(), e.toString());
        assertTrue(ms >= 250 && ms < 5_000, ms + " ms");
        assertFalse(conn.isOpen());
        conn.close();
    }

    @Test
    void aBatchThatStalledIsWrittenOnANewConnection() throws Exception {
        TsdbSinkConfig c = config();
        c.validate();
        sink = new TsdbSink(TsdbConnection.client(c));
        sink.open(c, null, new CapturingDeadLetter());
        FakeRecord r = new FakeRecord(0, "{\"series\":\"pump\",\"ts\":1000,\"value\":1.5}");
        sink.write(r);
        for (int i = 0; i < 1000 && !r.settled(); i++)
            Thread.sleep(10);
        assertTrue(r.settled(), "never acknowledged");
        assertEquals(1, r.acks.get());
        assertEquals(0, r.fails.get());
        assertTrue(sink.metrics().retries() >= 1, "no retry: the stall was not noticed");
        assertTrue(accepted.get() >= 2);
        try (TsdbClient cl = server.client()) {
            ValuePoints p = cl.queryValues(cl.series("pump", false), Long.MIN_VALUE, Long.MAX_VALUE);
            assertEquals(1, p.size());
            assertEquals(1.5, p.values().doubles()[0]);
        }
    }
}
