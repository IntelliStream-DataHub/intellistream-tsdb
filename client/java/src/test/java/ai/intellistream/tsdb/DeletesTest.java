// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/** Protocol version 7 against a live build/tsdbd: range deletes, replaces, retention. */
class DeletesTest {
    static final long T0 = TsdbClientTest.T0;
    static final long HOUR = 3_600_000;

    static Path scratch;
    static TsdbServer server;
    static TsdbClient c;

    @BeforeAll
    static void start() throws Exception {
        scratch = TsdbServer.scratch("tsdbd-java-deletes-");
        server = TsdbServer.start(scratch.resolve("db"));
        c = server.client();
    }

    @AfterAll
    static void stop() throws Exception {
        try {
            if (c != null)
                c.close();
        } finally {
            if (server != null)
                assertEquals(0, server.stop(), () -> TsdbClientTest.logOf(server));
            TsdbServer.delete(scratch);
        }
    }

    /** The rows of an int64 series as "ts=value" strings, "ts=null" for a null. */
    static List<String> rows(long id) throws TsdbException {
        ValuePoints p = c.queryValues(id, Long.MIN_VALUE, Long.MAX_VALUE);
        List<String> r = new ArrayList<>();
        Column v = p.values();
        for (int i = 0; i < p.size(); i++)
            r.add(p.timestamps()[i] + "=" + (v.isNull(i) ? "null" : v.longs()[i]));
        return r;
    }

    static List<String> want(long[] ts, String... v) {
        List<String> r = new ArrayList<>();
        for (int i = 0; i < ts.length; i++)
            r.add(ts[i] + "=" + v[i]);
        return r;
    }

    static void invalid(Executable e) {
        TsdbException x = assertThrows(TsdbException.class, e);
        assertEquals(Status.ERR_INVALID, x.status(), x::getMessage);
    }

    @Test
    void deletesAndReplaces() throws Exception {
        long id = c.seriesCreate("d", ValueType.INT64);
        long[] ts = new long[10];
        long[] vals = new long[10];
        for (int i = 0; i < 10; i++) {
            ts[i] = T0 + 1000L * i;
            vals[i] = i;
        }
        c.insertSeriesValues(id, ts, Column.ofLongs(vals));
        c.flush();

        c.deleteRange(id, T0 + 2000, T0 + 5000); /* rows 2, 3, 4 */
        c.deleteRange(id, T0 + 7000, T0 + 7001);
        c.deleteRange(id, T0 + 9000, T0 + 9000); /* empty */
        assertEquals(want(new long[] {ts[0], ts[1], ts[5], ts[6], ts[8], ts[9]}, "0", "1", "5", "6", "8", "9"),
                rows(id));

        /* a later insert into a deleted range is visible */
        c.insertSeriesValues(id, new long[] {ts[3]}, Column.ofLongs(33));
        assertEquals(7, rows(id).size());

        /* replace, with a null among the new rows */
        BitSet nulls = new BitSet();
        nulls.set(1);
        c.replaceRange(id, T0, T0 + 6000, new long[] {T0 + 500, T0 + 5500}, Column.ofLongs(100, 0).withNulls(nulls));
        assertEquals(want(new long[] {T0 + 500, T0 + 5500, ts[6], ts[8], ts[9]}, "100", "null", "6", "8", "9"),
                rows(id));

        /* a bitmap of more than one byte */
        int n = 11;
        long[] rts = new long[n];
        long[] rv = new long[n];
        for (int k = 0; k < n; k++) {
            rts[k] = T0 + 100L * k;
            rv[k] = -k;
        }
        nulls = new BitSet();
        nulls.set(9);
        c.replaceRange(id, T0, T0 + 1100, rts, Column.ofLongs(rv).withNulls(nulls));
        List<String> got = rows(id);
        assertEquals(n + 4, got.size());
        assertEquals(rts[9] + "=null", got.get(9));
        assertEquals(rts[10] + "=-10", got.get(10));

        /* no rows: a delete */
        c.replaceRange(id, Long.MIN_VALUE, Long.MAX_VALUE, new long[0], Column.ofLongs());
        c.optimize();
        assertTrue(rows(id).isEmpty());

        /* a timestamp outside the range, a column of another type, lengths that differ */
        invalid(() -> c.replaceRange(id, T0, T0 + 10, new long[] {T0 + 10}, Column.ofLongs(1)));
        invalid(() -> c.replaceRange(id, T0, T0 + 10, new long[] {T0}, Column.ofDoubles(1.0)));
        assertThrows(IllegalArgumentException.class,
                () -> c.replaceRange(id, T0, T0 + 10, new long[] {T0}, Column.ofLongs(1, 2)));
        /* the connection is still good after errors from the server */
        c.insertSeriesValues(id, new long[] {T0}, Column.ofLongs(5));
        assertEquals(List.of(T0 + "=5"), rows(id));
    }

    @Test
    void aPendingInsertErrorIsNotTakenForAnOldServer() throws Exception {
        long id = c.seriesCreate("p", ValueType.INT64);
        c.insertSeriesValuesAsync(id, new long[] {T0}, Column.ofDoubles(1.0));
        invalid(() -> c.deleteRange(id, Long.MIN_VALUE, Long.MAX_VALUE));
        c.deleteRange(id, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    @Test
    void retention() throws Exception {
        long id = c.seriesCreate("r", ValueType.INT64);
        assertEquals(0, c.retention(id));
        long now = System.currentTimeMillis();
        long[] ts = {now - 2 * HOUR, now - HOUR / 6};
        c.insertSeriesValues(id, ts, Column.ofLongs(1, 2));
        c.setRetention(id, HOUR);
        assertEquals(HOUR, c.retention(id));
        assertEquals(want(new long[] {ts[1]}, "2"), rows(id));
        invalid(() -> c.setRetention(id, -1));
        c.setRetention(id, 0);
        assertEquals(0, c.retention(id));
    }
}
