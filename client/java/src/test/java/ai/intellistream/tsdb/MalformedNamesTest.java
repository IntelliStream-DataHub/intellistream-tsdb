package ai.intellistream.tsdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Names the client must refuse before sending, and name replies it must refuse on receipt (fake servers). */
@Timeout(30)
class MalformedNamesTest {
    ServerSocket listener;
    Thread thread;
    final AtomicInteger requests = new AtomicInteger();

    @AfterEach
    void stop() throws Exception {
        if (listener != null)
            listener.close();
        if (thread != null)
            thread.join(5_000);
    }

    /** A fake server that answers every request with status 0 and the given payload. */
    TsdbClient connect(byte[] payload) throws Exception {
        listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        thread = new Thread(() -> {
            try (Socket s = listener.accept()) {
                OutputStream out = s.getOutputStream();
                for (;;) {
                    int[] req = TimeoutTest.readRequest(s);
                    int id = req[0];
                    if (req[1] == Protocol.OP_HELLO) { /* the client asks at connect */
                        out.write(TimeoutTest.helloReply(id, Protocol.VERSION));
                        continue;
                    }
                    requests.incrementAndGet();
                    out.write(ByteBuffer.allocate(13 + payload.length).order(ByteOrder.LITTLE_ENDIAN)
                            .putInt(9 + payload.length).putInt(id).put((byte) 0).putInt(0).put(payload).array());
                }
            } catch (IOException e) {
                /* the test is over */
            }
        });
        thread.setDaemon(true);
        thread.start();
        TsdbClient c = TsdbClient.connect("127.0.0.1", listener.getLocalPort(), Duration.ofSeconds(5),
                Duration.ofSeconds(5));
        return c;
    }

    @Test
    void oneByteNameReplyIsCorruptAndBreaksTheConnection() throws Exception {
        try (TsdbClient c = connect(new byte[] {5})) {
            TsdbException e = assertThrows(TsdbException.class, () -> c.seriesName(1));
            assertEquals(Status.ERR_CORRUPT, e.status());
            assertFalse(c.isOpen());
        }
        try (TsdbClient c = connectAgain(new byte[] {5})) {
            TsdbException e = assertThrows(TsdbException.class, () -> c.stateName(1, 2));
            assertEquals(Status.ERR_CORRUPT, e.status());
            assertFalse(c.isOpen());
        }
    }

    /** Protocol 11 answers SERIES and SERIES_CREATE with a u64: the 4 bytes of an older reply are corrupt. */
    @Test
    void aFourByteSeriesIdReplyIsCorrupt() throws Exception {
        try (TsdbClient c = connect(new byte[] {1, 0, 0, 0})) {
            TsdbException e = assertThrows(TsdbException.class, () -> c.series("a"));
            assertEquals(Status.ERR_CORRUPT, e.status());
            assertFalse(c.isOpen());
        }
        try (TsdbClient c = connectAgain(new byte[] {1, 0, 0, 0})) {
            TsdbException e = assertThrows(TsdbException.class, () -> c.seriesCreate("a", ValueType.INT64));
            assertEquals(Status.ERR_CORRUPT, e.status());
        }
    }

    /** A u64 id comes back whole, high bit included. */
    @Test
    void aWideSeriesIdReplyIsKept() throws Exception {
        try (TsdbClient c = connect(new byte[] {5, 0, 0, 0, 1, 0, 0, (byte) 0x80})) {
            assertEquals(0x8000_0001_0000_0005L, c.series("a"));
        }
    }

    TsdbClient connectAgain(byte[] payload) throws Exception {
        stop();
        return connect(payload);
    }

    @Test
    void unpairedSurrogatesAndNulAreRefusedBeforeSending() throws Exception {
        try (TsdbClient c = connect(new byte[] {1, 0, 0, 0, 0, 0, 0, 0})) {
            for (String bad : new String[] {"pump\uD800", "pump\uDC00", "\uDC00\uD800", "pump\uD800x", "a\0b"}) {
                TsdbException e = assertThrows(TsdbException.class, () -> c.series(bad, true), bad);
                assertEquals(Status.ERR_INVALID, e.status());
                e = assertThrows(TsdbException.class, () -> c.seriesCreate(bad, ValueType.FLOAT64, null), bad);
                assertEquals(Status.ERR_INVALID, e.status());
                e = assertThrows(TsdbException.class, () -> c.state(1, bad), bad);
                assertEquals(Status.ERR_INVALID, e.status());
            }
            assertEquals(0, requests.get());
            /* a pair is a character outside the BMP and is fine; the connection is still usable */
            assertEquals(1, c.series("pump\uD83D\uDE00", true));
            assertEquals(1, requests.get());
        }
    }
}
