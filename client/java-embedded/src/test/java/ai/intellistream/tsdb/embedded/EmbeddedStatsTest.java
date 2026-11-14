// SPDX-License-Identifier: AGPL-3.0-only
package ai.intellistream.tsdb.embedded;

import ai.intellistream.tsdb.SeriesResult;
import ai.intellistream.tsdb.SeriesStat;
import ai.intellistream.tsdb.Status;
import ai.intellistream.tsdb.TenantOptions;
import ai.intellistream.tsdb.TenantStats;
import ai.intellistream.tsdb.TsdbClient;
import ai.intellistream.tsdb.ValueType;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The storage numbers of series and tenants on the engine, and as tsdbd answers on the same directory. */
class EmbeddedStatsTest {
    static final long T0 = 1_700_000_000_000L;

    @TempDir
    Path tmp;

    /** The numbers that rest on disk: what a process counted since it started (hot lists, ingest) is its own. */
    static String atRest(SeriesStat s) {
        return List.of(s.cls(), s.type(), s.rows(), s.nulls(), s.logicalBytes(), s.tsBytes(), s.valueBytes(),
                s.indexBytes(), s.tombBytes(), s.parts(), s.partitions(), s.granules(), s.fullGranules(), s.firstTs(),
                s.lastTs()).toString();
    }

    static String atRest(TenantStats.Row r) {
        TenantStats.Numbers n = r.numbers();
        return List.of(r.status(), r.volume(), r.prefix(), n.series(), n.rows(), n.nulls(), n.logicalBytes(),
                n.tsBytes(), n.valueBytes(), n.indexBytes(), n.granules()).toString();
    }

    @Test
    void embeddedAndTsdbdAgree() throws Exception {
        Path bin = Paths.get(System.getProperty("tsdbd", "../../build/tsdbd"));
        Assumptions.assumeTrue(Files.isExecutable(bin), bin + " is missing: run make first");
        Path dir = tmp.resolve("db");
        Path tenants = tmp.resolve("tenants");
        Files.writeString(tenants, "acme/\nacme/lab/\n");
        String[] names = {"acme/a", "acme/b", "acme/lab/c", "other/d"};
        long[] ids = new long[4];
        long missing = (1L << 32) | 0xffffff00L;
        List<SeriesResult<SeriesStat>> series;
        TenantStats t;
        TenantOptions o = TenantOptions.defaults().withVolumes().withHot(3);
        try (EmbeddedDb db = EmbeddedDb.open(dir.toString(), new EmbeddedOptions().fsync(false).stats(1, 0))) {
            db.setTenants("acme/", "acme/lab/");
            long[] ts = new long[100];
            for (int i = 0; i < 100; i++)
                ts[i] = T0 + i * 1000L;
            for (int k = 0; k < 4; k++) {
                ids[k] = k == 1 ? db.seriesCreate(names[k], ValueType.FLOAT64) : db.series(names[k]);
                for (int b = 0; b < 50; b++) {
                    long[] t5 = new long[100];
                    for (int i = 0; i < 100; i++)
                        t5[i] = T0 + (b * 100L + i) * 1000L;
                    if (k == 1) {
                        ai.intellistream.tsdb.Column c = ai.intellistream.tsdb.Column.ofDoubles(new double[100]);
                        db.insertSeriesValues(ids[k], t5, c);
                    } else {
                        float[] v = new float[100];
                        for (int i = 0; i < 100; i++)
                            v[i] = (b * 100 + i) * (k + 1);
                        db.insert(ids[k], t5, v);
                    }
                }
            }
            db.flush();
            db.statsSettle(30_000);
            series = db.seriesStats(new long[] {ids[0], ids[1], ids[2], ids[3], missing}, false);
            assertEquals(5000, series.get(0).get().rows());
            assertEquals(ValueType.FLOAT64, series.get(1).get().type());
            assertEquals(Status.ERR_NOTFOUND, series.get(4).status());
            t = db.tenantStats(null, o);
            assertEquals(List.of("", "", "acme/", "acme/", "acme/lab/", "acme/lab/"),
                    t.rows().stream().map(TenantStats.Row::prefix).toList());
            assertEquals(2, t.rows().get(2).numbers().series());
            assertEquals(1, t.rows().get(4).numbers().series());
            assertEquals(1, t.rows().get(0).numbers().series());
            assertNull(t.rows().get(0).caller());
            List<TenantStats.Hot> hot = t.rows().get(2).hot();
            assertEquals(3, hot.stream().filter(h -> h.series() == 0).count());
            assertTrue(hot.stream().anyMatch(h -> h.kind() == TenantStats.HotKind.WRITES && h.series() == ids[0]));
            assertTrue(hot.stream().noneMatch(h -> h.series() == ids[2] || h.series() == ids[3]));
            assertEquals(Status.ERR_NOTFOUND,
                    db.tenantStats(new String[] {"nope/"}, TenantOptions.defaults()).rows().get(0).status());
            List<SeriesResult<SeriesStat>> res = db.seriesStats(new long[] {ids[0], ids[1]}, true);
            assertEquals(5000 * 2, res.get(0).get().rows() + res.get(1).get().rows());
            assertThrows(IllegalArgumentException.class, () -> TenantOptions.defaults().withHot(300));
        }
        Path log = tmp.resolve("tsdbd.log");
        Path portFile = tmp.resolve("tsdbd.port");
        Process server = new ProcessBuilder(bin.toString(), "--dir", dir.toString(), "--port", "0", "--port-file",
                portFile.toString(), "--no-fsync", "--workers", "2", "--no-auth", "--tenants", tenants.toString(),
                "--exit-with-parent=" + ProcessHandle.current().pid())
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            int port = -1;
            for (int i = 0; i < 1500 && server.isAlive() && port < 0; i++) {
                if (Files.exists(portFile)) {
                    String[] pf = Files.readString(portFile, StandardCharsets.UTF_8).trim().split("\\s+");
                    assertEquals(server.pid(), Long.parseLong(pf[1]), "the port file names another process");
                    port = Integer.parseInt(pf[0]);
                } else {
                    Thread.sleep(20);
                }
            }
            assertTrue(server.isAlive() && port > 0, "tsdbd did not serve");
            try (TsdbClient tcp = TsdbClient.connect("127.0.0.1", port)) {
                List<SeriesResult<SeriesStat>> net =
                        tcp.seriesStats(new long[] {ids[0], ids[1], ids[2], ids[3], missing}, false);
                for (int i = 0; i < 4; i++)
                    assertEquals(atRest(series.get(i).get()), atRest(net.get(i).get()), "series " + i);
                assertEquals(Status.ERR_NOTFOUND, net.get(4).status());
                TenantStats nt = null;
                for (int i = 0; i < 100; i++) {
                    nt = tcp.tenantStats(null, o);
                    if (!nt.partial() && nt.rows().get(2).numbers().series() == 2
                            && nt.rows().get(4).numbers().series() == 1)
                        break;
                    Thread.sleep(100);
                }
                assertEquals(t.listHash(), nt.listHash());
                assertEquals(t.rows().size(), nt.rows().size());
                for (int i = 0; i < t.rows().size(); i++)
                    assertEquals(atRest(t.rows().get(i)), atRest(nt.rows().get(i)), "row " + i);
            }
        } finally {
            server.destroy();
            server.waitFor();
        }
    }
}
