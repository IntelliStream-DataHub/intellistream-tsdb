// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

/**
 * latestByName looks the names it does not know up together (a round trip per window, not per name), against a fake
 * server of protocol 14 that holds the replies to SERIES until the client has been quiet for 100 ms and records the
 * most requests it held at once. Names starting "missing" are NOTFOUND, "flaky" is UNAVAILABLE the first time, the
 * rest get an id; QUERY_LATEST answers a NONE record per id.
 */
class LatestNamesTest {
    static final class Lookup implements AutoCloseable {
        final ServerSocket listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        final AtomicInteger maxHeld = new AtomicInteger(), total = new AtomicInteger();
        final Thread thread = new Thread(this::serve, "fake-lookup");

        Lookup() throws IOException {
            thread.setDaemon(true);
            thread.start();
        }

        int port() {
            return listener.getLocalPort();
        }

        private static void reply(OutputStream o, int id, int status, byte[] body) throws IOException {
            ByteBuffer b = ByteBuffer.allocate(13 + body.length).order(ByteOrder.LITTLE_ENDIAN);
            b.putInt(9 + body.length).putInt(id).put(Protocol.OP_REPLY).putInt(status).put(body);
            o.write(b.array());
        }

        private void serve() {
            try (Socket s = listener.accept()) {
                s.setSoTimeout(100);
                DataInputStream in = new DataInputStream(s.getInputStream());
                OutputStream out = s.getOutputStream();
                List<int[]> ids = new ArrayList<>();
                List<String> held = new ArrayList<>();
                boolean flaked = false;
                long seq = 0;
                for (;;) {
                    byte[] h = new byte[9];
                    try {
                        in.readFully(h);
                    } catch (SocketTimeoutException quiet) {
                        maxHeld.set(Math.max(maxHeld.get(), held.size()));
                        for (int k = 0; k < held.size(); k++) {
                            String name = held.get(k);
                            int id = ids.get(k)[0];
                            if (name.startsWith("missing")) {
                                reply(out, id, Status.ERR_NOTFOUND, new byte[0]);
                            } else if (name.startsWith("flaky") && !flaked) {
                                flaked = true;
                                reply(out, id, Status.ERR_UNAVAILABLE, new byte[0]);
                            } else {
                                reply(out, id, 0, ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
                                        .putLong((1L << 32) | ++seq).array());
                            }
                        }
                        held.clear();
                        ids.clear();
                        continue;
                    }
                    ByteBuffer hb = ByteBuffer.wrap(h).order(ByteOrder.LITTLE_ENDIAN);
                    int len = hb.getInt(0), id = hb.getInt(4), op = h[8];
                    byte[] p = new byte[len - 5];
                    s.setSoTimeout(0);
                    in.readFully(p);
                    s.setSoTimeout(100);
                    ByteBuffer pb = ByteBuffer.wrap(p).order(ByteOrder.LITTLE_ENDIAN);
                    if (op == Protocol.OP_HELLO) {
                        reply(out, id, 0, ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putInt(14).array());
                    } else if (op == Protocol.OP_SERIES) {
                        total.incrementAndGet();
                        int n = pb.getShort(1) & 0xffff;
                        held.add(new String(p, 3, n, StandardCharsets.UTF_8));
                        ids.add(new int[] {id});
                    } else if (op == Protocol.OP_QUERY_LATEST) {
                        int n = pb.getInt(4);
                        ByteBuffer b = ByteBuffer.allocate(12 + n * 24).order(ByteOrder.LITTLE_ENDIAN);
                        b.putInt(0).putInt(n).put((byte) 1).position(12);
                        for (int i = 0; i < n; i++)
                            b.putInt(0).put((byte) 2).position(12 + (i + 1) * 24);
                        reply(out, id, 0, b.array());
                    } else {
                        reply(out, id, Status.ERR_INVALID, new byte[0]);
                    }
                }
            } catch (IOException e) {
                /* the client closed */
            }
        }

        @Override
        public void close() throws IOException {
            listener.close();
        }
    }

    @Test
    void namesAreLookedUpTogether() throws Exception {
        try (Lookup f = new Lookup(); TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
            String[] names = new String[40];
            for (int i = 0; i < names.length; i++)
                names[i] = i == 7 ? "missing7" : i == 9 ? "flaky9" : "n" + i;
            names[11] = "x".repeat(70_000);
            names[12] = "";
            List<SeriesResult<Latest>> r = c.latestByName(names, LatestOptions.defaults());
            for (int i = 0; i < names.length; i++) {
                int want = i == 7 ? Status.ERR_NOTFOUND : i == 11 || i == 12 ? Status.ERR_INVALID : 0;
                assertEquals(want, want == 0 ? 0 : r.get(i).status(), "name " + i);
                assertEquals(want == 0, r.get(i).ok(), "name " + i);
            }
            assertTrue(f.maxHeld.get() >= 38, "the 38 sendable names were held together, not " + f.maxHeld.get());
            c.latestByName(names, LatestOptions.defaults()); /* only the NOTFOUND name is asked again */
            assertEquals(38 + 1 + 1, f.total.get(), "the window, the flaky name again, the NOTFOUND name once more");
        }
    }
}
