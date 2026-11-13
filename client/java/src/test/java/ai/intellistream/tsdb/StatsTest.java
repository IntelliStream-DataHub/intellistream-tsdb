// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import ai.intellistream.tsdb.CompressionTest.FakeServer;

/**
 * STATS_SERIES and STATS_TENANTS (protocol 15): the numbers of series and tenants on tsdbd, the rights (a tenant admin
 * sees only its tenant; denied and missing look alike), more ids than one request holds, and fake servers for a
 * server of protocol 14, a work budget that stops a reply early and a tenant list that changes under a cursor.
 */
class StatsTest {
    static final long T0 = 1_700_000_000_000L;
    static final long MISSING = (1L << 32) | 0xffffff00L;

    static long fill(TsdbClient c, String name, int n) throws Exception {
        long id = c.series(name);
        long[] ts = new long[n];
        float[] v = new float[n];
        for (int i = 0; i < n; i++) {
            ts[i] = T0 + i * 1000L;
            v[i] = i;
        }
        c.insert(id, ts, v);
        return id;
    }

    @Test
    void seriesStatsOnTcp() throws Exception {
        try (TsdbServer srv = TsdbServer.start(TsdbServer.scratch("tsdb-java-stats-").resolve("db"));
                TsdbClient c = srv.client()) {
            long a = fill(c, "st.a", 1000);
            long b = c.seriesCreate("st.b", ValueType.INT64);
            c.flush();
            List<SeriesResult<SeriesStat>> r = c.seriesStats(new long[] {a, b, MISSING}, false);
            assertEquals(3, r.size());
            SeriesStat s = r.get(0).get();
            assertEquals(1000, s.rows());
            assertEquals(ValueType.FLOAT32, s.type());
            assertEquals(T0, s.firstTs());
            assertEquals(T0 + 999_000, s.lastTs());
            assertEquals(12_000, s.logicalBytes());
            assertTrue(s.tsBytes() > 0 && s.valueBytes() > 0 && s.parts() >= 1);
            assertEquals(0, r.get(1).get().rows());
            assertEquals(ValueType.INT64, r.get(1).get().type());
            assertEquals(Status.ERR_NOTFOUND, r.get(2).status());
            assertEquals(List.of(), c.seriesStats(new long[0], false));
            assertEquals(1000, c.seriesStats(new long[] {a}, true).get(0).get().rows());

            /* more ids than one request holds: the answers come in request order */
            long[] many = new long[70_000];
            for (int i = 0; i < many.length; i++)
                many[i] = i % 2 == 0 ? a : b;
            List<SeriesResult<SeriesStat>> big = c.seriesStats(many, false);
            assertEquals(many.length, big.size());
            for (int i = 0; i < big.size(); i++)
                assertEquals(i % 2 == 0 ? 1000 : 0, big.get(i).get().rows(), "record " + i);
            assertEquals(2500, c.seriesStats(java.util.Arrays.copyOf(many, 2500), true).size());
        }
    }

    @Test
    void tenantsFromAFileAndTheHotLists() throws Exception {
        Path dir = TsdbServer.scratch("tsdb-java-tenants-");
        Path file = dir.resolve("tenants");
        Files.writeString(file, "acme/\nacme/lab/\n");
        try (TsdbServer srv = TsdbServer.start(dir.resolve("db"), false, "--tenants", file.toString());
                TsdbClient c = srv.client()) {
            long[] ids = new long[4];
            String[] names = {"acme/a", "acme/b", "acme/lab/c", "other/d"};
            for (int i = 0; i < 4; i++)
                ids[i] = fill(c, names[i], 100);
            c.flush();
            TenantStats t = null;
            for (int i = 0; i < 100; i++) {
                t = c.tenantStats(null, TenantOptions.defaults());
                if (t.rows().size() == 3 && t.rows().get(1).numbers().series() == 2
                        && t.rows().get(2).numbers().series() == 1 && t.rows().get(0).numbers().series() == 1)
                    break;
                Thread.sleep(100);
            }
            assertEquals(List.of("", "acme/", "acme/lab/"), t.rows().stream().map(TenantStats.Row::prefix).toList());
            assertTrue(t.rows().stream().allMatch(r -> r.volume() == TenantStats.VOLUME_SUM && r.status() == 0));

            TenantOptions o = TenantOptions.defaults().withVolumes().withCaller().withHot(5);
            t = c.tenantStats(new String[] {"", "acme/", "nope/"}, o);
            assertEquals(5, t.rows().size());
            assertEquals(List.of(TenantStats.VOLUME_SUM, 0, TenantStats.VOLUME_SUM, 0, TenantStats.VOLUME_SUM),
                    t.rows().stream().map(TenantStats.Row::volume).toList());
            assertEquals(Status.ERR_NOTFOUND, t.rows().get(4).status());
            assertNotNull(t.rows().get(0).caller());
            assertNull(t.rows().get(1).caller());
            assertTrue(t.rows().get(0).caller().requestsInsert() >= 4);
            List<TenantStats.Hot> hot = t.rows().get(2).hot();
            assertEquals(3, hot.stream().filter(h -> h.series() == 0).count());
            assertTrue(hot.stream().anyMatch(h -> h.kind() == TenantStats.HotKind.WRITES && h.series() == ids[0]));
            assertTrue(hot.stream().noneMatch(h -> h.series() == ids[3] || h.series() == ids[2]));
            assertEquals(t.rows().get(2).numbers().rows(), t.rows().get(3).numbers().rows());
            assertThrows(IllegalArgumentException.class, () -> TenantOptions.defaults().withHot(257));
        }
    }

    @Test
    void aTenantAdminSeesOnlyItsTenant() throws Exception {
        Path dir = TsdbServer.scratch("tsdb-java-tenant-admin-").resolve("db");
        TsdbServer.userCli(dir, "root-pw", "add", "root", "--superuser");
        try (TsdbServer srv = TsdbServer.start(dir, true, "--tenants-settle", "1");
                TsdbClient root = TsdbClient.connect("127.0.0.1", srv.port,
                        ConnectOptions.defaults().user("root").password("root-pw"))) {
            long a = fill(root, "acme/a", 10);
            long b = fill(root, "other/b", 10);
            root.flush();
            root.userCreate("acme/boss", "boss-pw".toCharArray(), false);
            root.grant("acme/boss", Selector.prefix("acme/"), Rights.READ);
            root.grant("acme/boss", Selector.prefix("acme/"), Rights.ADMIN);
            root.userCreate("u", "u-pw".toCharArray(), false);
            root.grant("u", Selector.prefix("acme/"), Rights.READ);
            TenantStats t = null;
            for (int i = 0; i < 100; i++) {
                t = root.tenantStats(null, TenantOptions.defaults());
                if (t.rows().stream().anyMatch(r -> r.prefix().equals("acme/")))
                    break;
                Thread.sleep(100);
            }
            assertEquals("", t.rows().get(0).prefix());
            assertTrue(t.partsListed() >= 0);

            try (TsdbClient boss = TsdbClient.connect("127.0.0.1", srv.port,
                    ConnectOptions.defaults().user("acme/boss").password("boss-pw"))) {
                t = boss.tenantStats(null, TenantOptions.defaults().withCaller());
                assertEquals(List.of("acme/"), t.rows().stream().map(TenantStats.Row::prefix).toList());
                assertEquals(0, t.partsCounted());
                assertEquals(0, t.partsListed());
                t = boss.tenantStats(new String[] {"other/", "nope/", ""}, TenantOptions.defaults());
                assertTrue(t.rows().stream().allMatch(r -> r.status() == Status.ERR_DENIED));
            }
            try (TsdbClient u = TsdbClient.connect("127.0.0.1", srv.port,
                    ConnectOptions.defaults().user("u").password("u-pw"))) {
                assertTrue(u.tenantStats(null, TenantOptions.defaults()).rows().isEmpty());
                assertEquals(Status.ERR_DENIED,
                        u.tenantStats(new String[] {"acme/"}, TenantOptions.defaults()).rows().get(0).status());
                List<SeriesResult<SeriesStat>> r = u.seriesStats(new long[] {a, b, MISSING}, false);
                assertEquals(Status.OK, r.get(0).status());
                /* an existing series outside the grants and a missing one answer alike */
                assertEquals(Status.ERR_DENIED, r.get(1).status());
                assertEquals(Status.ERR_DENIED, r.get(2).status());
                assertEquals(0, r.get(0).get().readsErr());
            }
            List<SeriesResult<SeriesStat>> r = root.seriesStats(new long[] {a, MISSING}, false);
            assertEquals(10, r.get(0).get().rows());
            assertEquals(Status.ERR_NOTFOUND, r.get(1).status());
        }
    }

    @Test
    void aServerOfProtocol14IsSentNothing() throws Exception {
        try (FakeServer f = new FakeServer(14); TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
            assertEquals(Status.ERR_INVALID,
                    assertThrows(TsdbException.class, () -> c.seriesStats(new long[] {1L << 32}, false)).status());
            assertEquals(Status.ERR_INVALID, assertThrows(TsdbException.class,
                    () -> c.tenantStats(null, TenantOptions.defaults())).status());
            assertEquals(List.of((int) Protocol.OP_HELLO), f.ops());
        }
    }

    @Test
    void aReplyThatStopsAtTheWorkBudgetIsAskedForAgain() throws Exception {
        /* answers at most 3 records a request; a record's rows field says which id it is for */
        try (FakeServer f = new FakeServer(15, fr -> {
            ByteBuffer p = fr.payload();
            int n = p.getInt(0), done = Math.min(n, 3);
            ByteBuffer b = ByteBuffer.allocate(8 + done * 176).order(ByteOrder.LITTLE_ENDIAN);
            b.putInt(done).putInt(0);
            for (int i = 0; i < done; i++) {
                int at = b.position();
                b.put(at + 5, (byte) 1);
                b.putLong(at + 8, p.getLong(8 + 8 * i));
                b.position(at + 176);
            }
            return b.array();
        }); TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
            long[] ids = new long[10];
            for (int i = 0; i < 10; i++)
                ids[i] = (1L << 32) | i;
            List<SeriesResult<SeriesStat>> r = c.seriesStats(ids, false);
            for (int i = 0; i < 10; i++)
                assertEquals(ids[i], r.get(i).get().rows());
            assertEquals(List.of((int) Protocol.OP_HELLO, 46, 46, 46, 46), f.ops());
        }
    }

    @Test
    void repliesOutOfShapeFailTheConnection() throws Exception {
        List<byte[]> bad = new ArrayList<>();
        bad.add(new byte[4]);
        byte[] one = new byte[8 + 176];
        one[0] = 1;
        byte[] second = one.clone();
        second[4] = 1; /* the second word must be 0 */
        bad.add(second);
        byte[] over = one.clone();
        over[0] = 2; /* done above what was asked */
        bad.add(over);
        byte[] type = one.clone();
        type[8 + 5] = 9;
        bad.add(type);
        bad.add(java.util.Arrays.copyOf(one, 8 + 100));
        for (byte[] body : bad) {
            try (FakeServer f = new FakeServer(15, fr -> body); TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
                assertEquals(Status.ERR_CORRUPT,
                        assertThrows(TsdbException.class, () -> c.seriesStats(new long[] {1L << 32}, false)).status());
                assertThrows(Exception.class, () -> c.seriesStats(new long[] {1L << 32}, false));
            }
        }
    }

    @Test
    void aTenantListThatChangesUnderACursorRestartsTheListing() throws Exception {
        int[] call = {0};
        java.util.function.BiFunction<String, int[], byte[]> reply = (name, v) -> {
            byte[] nm = name.getBytes(StandardCharsets.UTF_8);
            int padded = (nm.length + 7) & ~7;
            ByteBuffer b = ByteBuffer.allocate(40 + 16 + padded + 18 * 8).order(ByteOrder.LITTLE_ENDIAN);
            b.putInt(1).putInt(0).putInt(v[0]).putInt(v[1]).putLong(7).putLong(0).putLong(0);
            b.putInt(0).putInt(-1).putInt(0).putShort((short) nm.length).putShort((short) 0);
            b.put(nm);
            return b.array();
        };
        try (FakeServer f = new FakeServer(15, fr -> {
            int cursor = fr.payload().getInt(8);
            switch (++call[0]) {
            case 1: return reply.apply("a/", new int[] {1, 1});
            case 2:
                assertEquals(1, cursor);
                return reply.apply("zzz/", new int[] {1, 2});
            case 3:
                assertEquals(0, cursor);
                return reply.apply("b/", new int[] {1, 2});
            default:
                assertEquals(1, cursor);
                return reply.apply("c/", new int[] {-1, 2});
            }
        }); TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
            TenantStats t = c.tenantStats(null, TenantOptions.defaults());
            assertEquals(2, t.generation());
            assertEquals(List.of("b/", "c/"), t.rows().stream().map(TenantStats.Row::prefix).toList());
            assertEquals(List.of((int) Protocol.OP_HELLO, 47, 47, 47, 47), f.ops());
            assertFalse(t.partial());
        }
    }
}
