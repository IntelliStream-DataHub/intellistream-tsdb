// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;

/** Data survives a SIGTERM and restart of the server, and a client sees the server go away as ERR_IO. */
class RestartTest {
    @Test
    void dataSurvivesRestart() throws Exception {
        Path scratch = TsdbServer.scratch("tsdbd-java-restart-");
        Path dir = scratch.resolve("db");
        try {
            long id;
            long[] ts = TsdbClientTest.seq(TsdbClientTest.T0, 1000, 5000);
            float[] v = new float[ts.length];
            for (int i = 0; i < v.length; i++)
                v[i] = i * 0.5f;
            TsdbClient stale;
            try (TsdbServer s = TsdbServer.start(dir)) {
                stale = s.client();
                try (TsdbClient c = s.client()) {
                    id = c.series("restart.me");
                    c.insert(id, ts, v, 0, 4000);   /* into parts after the flush */
                    c.flush();
                    c.insert(id, ts, v, 4000, 1000); /* WAL and memtable only */
                }
                assertEquals(0, s.stop(), s.log());
            }
            TsdbException e = org.junit.jupiter.api.Assertions.assertThrows(TsdbException.class, stale::hello);
            assertEquals(Status.ERR_IO, e.status());
            assertFalse(stale.isOpen());
            stale.close();

            try (TsdbServer s = TsdbServer.start(dir); TsdbClient c = s.client()) {
                assertEquals(id, c.series("restart.me", false));
                Points p = c.queryPoints(id, Long.MIN_VALUE, Long.MAX_VALUE);
                assertArrayEquals(ts, p.timestamps());
                assertArrayEquals(v, p.values());
                assertEquals(0, s.stop(), s.log());
            }
        } finally {
            TsdbServer.delete(scratch);
        }
    }
}
