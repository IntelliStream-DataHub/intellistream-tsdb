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
                    } else if (op == Protocol.OP_QUERY_VALUES) {
                        long series = pb.getLong(0);
                        int n = series == ((1L << 32) | 999) ? 1_100_000 : 5; /* 17.6 MiB > IN_KEEP (16 MiB) vs small */
                        int blen = 8 + n * 16;
                        ByteBuffer hdr = ByteBuffer.allocate(13 + 8).order(ByteOrder.LITTLE_ENDIAN);
                        hdr.putInt(9 + blen).putInt(id).put(Protocol.OP_REPLY).putInt(0);
                        hdr.put((byte) 1).put((byte) 2).put((byte) 0).put((byte) 0).putInt(n); /* last=1, INT64 */
                        out.write(hdr.array());
                        byte[] chunk = new byte[64 * 1024];
                        ByteBuffer cb = ByteBuffer.wrap(chunk).order(ByteOrder.LITTLE_ENDIAN);
                        for (int off = 0; off < n;) {
                            int m = Math.min(n - off, chunk.length / 8);
                            cb.clear();
                            for (int i = 0; i < m; i++)
                                cb.putLong(off + i);
                            out.write(chunk, 0, m * 8);
                            off += m;
                        }
                        for (int off = 0; off < n;) {
                            int m = Math.min(n - off, chunk.length / 8);
                            cb.clear();
                            for (int i = 0; i < m; i++)
                                cb.putLong((off + i) * 3L + 7L);
                            out.write(chunk, 0, m * 8);
                            off += m;
                        }
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
            /* 100% cache hit fast path sends zero SERIES requests */
            String[] cached = new String[] {"n0", "n1", "n2", "flaky9"};
            List<SeriesResult<Latest>> hit = c.latestByName(cached, LatestOptions.defaults());
            assertEquals(cached.length, hit.size());
            for (SeriesResult<Latest> e : hit)
                assertTrue(e.ok());
            assertEquals(38 + 1 + 1, f.total.get(), "all-hit fast path sent no SERIES lookups");
            /* >1 MiB QUERY_LATEST (65536 * 24 + 12 = 1.5 MiB) retains the grown receive buffer up to IN_KEEP */
            long[] bigIds = new long[65_536];
            for (int i = 0; i < bigIds.length; i++)
                bigIds[i] = (1L << 32) | (i + 1);
            List<SeriesResult<Latest>> big1 = c.latest(bigIds, LatestOptions.defaults());
            assertEquals(bigIds.length, big1.size());
            int capAfterFirst = c.inCapacity();
            assertTrue(capAfterFirst >= 12 + bigIds.length * 24, "grew to hold >1 MiB reply: " + capAfterFirst);
            List<SeriesResult<Latest>> big2 = c.latest(bigIds, LatestOptions.defaults());
            assertEquals(bigIds.length, big2.size());
            assertEquals(capAfterFirst, c.inCapacity(), "retained receive buffer across repeated >1 MiB replies");

            /* >16 MiB (IN_KEEP) reply grows receive buffer above 16 MiB and shrinks back to inBase (1 MiB) on endReply */
            int[] peakCap = new int[1];
            int[] gotRows = new int[1];
            c.queryValues((1L << 32) | 999, 0, Long.MAX_VALUE, (ts, col, n) -> {
                peakCap[0] = Math.max(peakCap[0], c.inCapacity());
                long[] vals = col.longs();
                for (int i = 0; i < n; i++) {
                    long row = gotRows[0] + i;
                    assertEquals(row, ts[i], "ts at " + row);
                    assertEquals(row * 3L + 7L, vals[i], "val at " + row);
                }
                gotRows[0] += n;
                return true;
            });
            assertEquals(1_100_000, gotRows[0]);
            assertTrue(peakCap[0] > (16 << 20), "grew past IN_KEEP (16 MiB): " + peakCap[0]);
            assertEquals(1 << 20, c.inCapacity(), "shrunk back to inBase (1 MiB) after oversized reply");

            /* Subsequent reads on the same connection stay intact and keep inBase */
            ValuePoints after = c.queryValues((1L << 32) | 1, 0, Long.MAX_VALUE);
            assertEquals(5, after.size());
            long[] afterTs = after.timestamps();
            long[] afterVals = after.values().longs();
            for (int i = 0; i < 5; i++) {
                assertEquals(i, afterTs[i]);
                assertEquals(i * 3L + 7L, afterVals[i]);
            }
            List<SeriesResult<Latest>> afterLatest = c.latestByName(cached, LatestOptions.defaults());
            assertEquals(cached.length, afterLatest.size());
            for (SeriesResult<Latest> e : afterLatest)
                assertTrue(e.ok());
            assertEquals(1 << 20, c.inCapacity(), "stayed on inBase after subsequent small replies");
        }
    }
}
