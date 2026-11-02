package ai.intellistream.tsdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import ai.intellistream.tsdb.CompressionTest.FakeServer;
import org.junit.jupiter.api.Test;

/**
 * Grouping by local periods (protocol version 4): the client computes the UTC bounds and sends QUERY_BOUNDS.
 * Against tsdbd, compared with what the C code gives for the same rows ({@code build/tsdb}, the embedded CLI, which
 * groups with {@code tsdb_calendar_bounds} as the C client does); and against a fake server.
 */
class TimeZoneGroupTest {
    static final long MIN = 60_000L, HOUR = 3_600_000L, DAY = 86_400_000L;

    static long utc(int y, int m, int d, int h) {
        return LocalDateTime.of(y, m, d, h, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    /** One grouping, as the Java client and the CLI ({@code 1d}, {@code 5mo}, ...) spell it. */
    record Spec(String series, String zone, Unit unit, int count, String cli) {
    }

    /**
     * Oslo days are 24 hours except across DST changes, and begin at 23:00 UTC in winter, 22:00 UTC in summer; every
     * grouping in every zone gives what the C code gives.
     */
    @Test
    void sameBucketsAsTheC() throws Exception {
        Path scratch = TsdbServer.scratch("tsdbd-java-tz-");
        try {
            Path db = scratch.resolve("db");
            Map<Spec, List<Bucket>> java = new LinkedHashMap<>();
            List<Spec> specs = new ArrayList<>();
            try (TsdbServer s = TsdbServer.start(db); TsdbClient c = s.client()) {
                assertEquals(Protocol.VERSION, c.hello());
                /* every 10 minutes through 2024 and a bit, and random rows over 50 years; int64 sums are exact */
                long tenMin = c.seriesCreate("ten.min", ValueType.INT64);
                long t0 = utc(2023, 12, 25, 0);
                int n = (int) ((utc(2025, 1, 10, 0) - t0) / (10 * MIN));
                long[] ts = new long[n], v = new long[n];
                for (int i = 0; i < n; i++) {
                    ts[i] = t0 + i * 10 * MIN;
                    v[i] = i % 97;
                }
                c.insertSeriesValues(tenMin, ts, Column.ofLongs(v));
                long sparse = c.seriesCreate("sparse", ValueType.INT64);
                Random r = new Random(42);
                long lo = utc(1990, 1, 1, 0), hi = utc(2040, 1, 1, 0);
                long[] st = new long[2_000], sv = new long[st.length]; /* a partition a row: few */
                for (int i = 0; i < st.length; i++) {
                    st[i] = lo + Math.floorMod(r.nextLong(), hi - lo);
                    sv[i] = r.nextInt(2000) - 1000;
                }
                c.insertSeriesValues(sparse, st, Column.ofLongs(sv));

                /* Oslo days across both DST changes of 2024 */
                List<Bucket> days = c.queryGroup(tenMin, Long.MIN_VALUE, Long.MAX_VALUE,
                        Group.of(Unit.DAY).in(ZoneId.of("Europe/Oslo")));
                assertEquals(n, days.stream().mapToLong(Bucket::count).sum());
                for (Bucket b : days) {
                    long hourOfDay = Math.floorMod(b.start(), DAY) / HOUR;
                    boolean summer = b.start() >= utc(2024, 3, 31, 22) && b.start() <= utc(2024, 10, 26, 22);
                    assertEquals(summer ? 22 : 23, hourOfDay, "Oslo day from " + b.start());
                }
                Map<Long, Long> perDay = new LinkedHashMap<>();
                for (Bucket b : days)
                    perDay.put(b.start(), b.count());
                assertEquals(23 * 6L, perDay.get(utc(2024, 3, 30, 23)), "2024-03-31 has 23 hours");
                assertEquals(25 * 6L, perDay.get(utc(2024, 10, 26, 22)), "2024-10-27 has 25 hours");
                assertEquals(24 * 6L, perDay.get(utc(2024, 6, 30, 22)));

                /* the newest two days first, as bounds: what queryGroup sends */
                List<Bucket> last = c.queryGroup(tenMin, Long.MIN_VALUE, Long.MAX_VALUE,
                        Group.of(Unit.DAY).in("Europe/Oslo"), Order.DESC, 2);
                assertEquals(List.of(days.get(days.size() - 1), days.get(days.size() - 2)), last);
                long[] bounds = Group.of(Unit.DAY).in("Europe/Oslo").bounds(t0, ts[n - 1] + 1);
                assertEquals(days, c.queryBounds(tenMin, Long.MIN_VALUE, Long.MAX_VALUE, bounds));
                assertEquals(last, c.queryBounds(tenMin, Long.MIN_VALUE, Long.MAX_VALUE, bounds, Order.DESC, 2));

                /* rows outside the range do not count; no rows, no buckets */
                List<Bucket> june = c.queryGroup(tenMin, utc(2024, 6, 1, 0), utc(2024, 7, 1, 0),
                        Group.of(Unit.MONTH).in("Europe/Oslo"));
                assertEquals(2, june.size(), "June in Oslo, and the first two hours of July");
                assertEquals(utc(2024, 5, 31, 22), june.get(0).start());
                assertEquals(utc(2024, 6, 30, 22), june.get(1).start());
                assertEquals(12, june.get(1).count());
                assertTrue(c.queryGroup(tenMin, 0, 1000, Group.of(Unit.DAY).in("Europe/Oslo")).isEmpty());
                assertTrue(c.queryGroup(0x7fff0000, Long.MIN_VALUE, Long.MAX_VALUE,
                        Group.of(Unit.DAY).in("Europe/Oslo")).isEmpty(), "unknown series");

                /* bounds the server refuses */
                assertEquals(Status.ERR_INVALID, assertThrows(TsdbException.class,
                        () -> c.queryBounds(tenMin, Long.MIN_VALUE, Long.MAX_VALUE, new long[] {5, 5})).status());
                assertTrue(c.isOpen());

                String[] zones = {"Europe/Oslo", "America/St_Johns", "Australia/Lord_Howe", "Asia/Kathmandu",
                    "America/Sao_Paulo", "Pacific/Chatham", "UTC"};
                Object[][] groups = {{Unit.DAY, 1, "1d"}, {Unit.DAY, 3, "3d"}, {Unit.WEEK, 1, "1w"},
                    {Unit.MONTH, 1, "1mo"}, {Unit.MONTH, 5, "5mo"}, {Unit.QUARTER, 1, "1q"}, {Unit.YEAR, 1, "1y"},
                    {Unit.HOUR, 5, "5h"}, {Unit.MINUTE, 45, "45m"}};
                for (String series : new String[] {"ten.min", "sparse"})
                    for (String z : zones)
                        for (Object[] g : groups)
                            if (series.equals("ten.min") || g[0] != Unit.MINUTE) /* 584,000 bounds: slow, not new */
                                specs.add(new Spec(series, z, (Unit) g[0], (Integer) g[1], (String) g[2]));
                for (Spec sp : specs) {
                    long id = sp.series().equals("sparse") ? sparse : tenMin;
                    java.put(sp, c.queryGroup(id, Long.MIN_VALUE, Long.MAX_VALUE,
                            new Group(sp.unit(), sp.count(), ZoneId.of(sp.zone()))));
                }
                c.close();
                assertEquals(0, s.stop(), s.log());
            }
            /* the server is gone: the CLI opens the directory itself */
            for (Spec sp : specs) {
                List<Bucket> want = cli(db, sp);
                List<Bucket> got = java.get(sp);
                assertEquals(want.size(), got.size(), sp + ": buckets");
                for (int i = 0; i < want.size(); i++) {
                    Bucket w = want.get(i), g = got.get(i);
                    String what = sp + ", bucket " + i;
                    assertEquals(w.start(), g.start(), what);
                    assertEquals(w.count(), g.count(), what);
                    assertEquals(w.isum(), g.isum(), what);
                    assertEquals(w.imin(), g.imin(), what);
                    assertEquals(w.imax(), g.imax(), what);
                }
            }
        } finally {
            TsdbServer.delete(scratch);
        }
    }

    static final DateTimeFormatter CLI_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    /** Runs {@code build/tsdb <db> query <series> min max all <group> --tz <zone>} and reads its buckets. */
    static List<Bucket> cli(Path db, Spec sp) throws IOException, InterruptedException {
        Path bin = TsdbServer.binary().resolveSibling("tsdb");
        Process p = new ProcessBuilder(bin.toString(), db.toString(), "query", sp.series(), "min", "max", "all",
                sp.cli(), "--tz", sp.zone())
                .redirectError(ProcessBuilder.Redirect.DISCARD)
                .start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(p.waitFor(60, TimeUnit.SECONDS));
        assertEquals(0, p.exitValue(), sp + ": " + out);
        List<Bucket> b = new ArrayList<>();
        for (String line : out.split("\n")) {
            if (line.isEmpty() || line.startsWith("time\t"))
                continue;
            String[] f = line.split("\t");
            long start = LocalDateTime.parse(f[0], CLI_TIME).toInstant(ZoneOffset.UTC).toEpochMilli();
            b.add(new Bucket(start, Long.parseLong(f[1]), 0, 0, 0, Long.parseLong(f[2]), Long.parseLong(f[3]),
                    Long.parseLong(f[4])));
        }
        return b;
    }

    // ------------------------------------------------------------------ fake servers

    static final int HELLO = Protocol.OP_HELLO, VALUES = Protocol.OP_QUERY_VALUES,
            BUCKETS = Protocol.OP_QUERY_BUCKETS, BOUNDS = Protocol.OP_QUERY_BOUNDS;

    /** Replies with no buckets, and with one float32 row at {@code row} to QUERY_VALUES (or none if negative). */
    static byte[] reply(CompressionTest.Frame f, long row) {
        ByteBuffer b = ByteBuffer.allocate(20).order(ByteOrder.LITTLE_ENDIAN);
        if (f.op() == VALUES) {
            b.put((byte) 1).put((byte) 0).put((byte) 0).put((byte) 0).putInt(row >= 0 ? 1 : 0);
            if (row >= 0)
                b.putLong(row).putFloat(1);
        } else {
            b.putInt(0);
        }
        byte[] a = new byte[b.position()];
        b.flip().get(a);
        return a;
    }

    static String zoneOf(ByteBuffer p) {
        int len = p.getShort(46) & 0xffff;
        byte[] z = new byte[len];
        p.get(48, z);
        return new String(z, StandardCharsets.UTF_8);
    }

    /** A local group: first and last row, then the bounds between them. */
    @Test
    void localGroupsGetBounds() throws Exception {
        long row = utc(2024, 3, 31, 12);
        FakeServer f = new FakeServer(Protocol.VERSION, fr -> reply(fr, row));
        Group days = Group.of(Unit.DAY, 2).in("America/St_Johns");
        try (TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
            assertTrue(c.queryGroup(9, Long.MIN_VALUE, Long.MAX_VALUE, days, Order.DESC, 3).isEmpty());
        }
        f.close();
        assertEquals(List.of(HELLO, VALUES, VALUES, BOUNDS), f.ops());
        ByteBuffer asc = f.frames.get(1).payload(), desc = f.frames.get(2).payload();
        assertEquals(33, asc.limit());
        assertEquals(0, asc.get(24), "oldest first");
        assertEquals(1, asc.getLong(25), "limit 1");
        assertEquals(1, desc.get(24), "newest first");
        assertEquals(1, desc.getLong(25), "limit 1");

        ByteBuffer p = f.frames.get(3).payload();
        assertEquals(9, p.getLong(0));
        assertEquals(Long.MIN_VALUE, p.getLong(8), "the query's own range");
        assertEquals(Long.MAX_VALUE, p.getLong(16));
        assertEquals(1, p.get(24), "descending");
        assertEquals(3, p.getLong(25));
        long[] want = days.bounds(row, row + 1);
        assertEquals(want.length, p.getInt(33));
        assertEquals(37 + 8 * want.length, p.limit());
        long[] sent = new long[want.length];
        for (int i = 0; i < sent.length; i++)
            sent[i] = p.getLong(37 + 8 * i);
        assertArrayEquals(want, sent);
        assertArrayEquals(new long[] {utc(2024, 3, 30, 2) + 30 * MIN}, sent, "2-day periods from 1970, local");
    }

    /** UTC needs no bounds, whatever the zone is called; an empty series needs no QUERY_BOUNDS. */
    @Test
    void utcAndEmptySeries() throws Exception {
        FakeServer f = new FakeServer(Protocol.VERSION, fr -> reply(fr, -1));
        try (TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
            c.queryGroup(1, 0, 10, Group.of(Unit.DAY).in("Etc/UTC"));
            c.queryGroup(1, 0, 10, Group.of(Unit.DAY).in(ZoneOffset.UTC));
            c.queryGroup(1, 0, 10, Group.of(Unit.DAY));
            c.queryGroup(1, 0, 10, null);
            assertTrue(c.queryGroup(1, 0, 10, Group.of(Unit.DAY).in("Asia/Kathmandu")).isEmpty());
        }
        f.close();
        assertEquals(List.of(HELLO, BUCKETS, BUCKETS, BUCKETS, BUCKETS, VALUES, VALUES), f.ops());
        for (int i = 1; i <= 4; i++)
            assertEquals("", zoneOf(f.frames.get(i).payload()));
        assertEquals(0, f.frames.get(4).payload().get(32), "no unit: one bucket");
    }

    /** Arguments QUERY_BOUNDS cannot carry are refused before anything is sent. */
    @Test
    void boundsArguments() throws Exception {
        FakeServer f = new FakeServer(Protocol.VERSION, fr -> reply(fr, -1));
        try (TsdbClient c = TsdbClient.connect("127.0.0.1", f.port())) {
            assertThrows(IllegalArgumentException.class,
                    () -> c.queryBounds(1, 0, 10, new long[0]));
            assertThrows(NullPointerException.class,
                    () -> c.queryBounds(1, 0, 10, null));
            assertThrows(IllegalArgumentException.class,
                    () -> c.queryBounds(1, 0, 10, new long[] {1}, Order.ASC, -1));
            /* a request larger than the send buffer */
            long[] many = new long[300_000];
            for (int i = 0; i < many.length; i++)
                many[i] = i * 1000L;
            assertTrue(c.queryBounds(1, 0, 10, many).isEmpty());
        }
        f.close();
        assertEquals(List.of(HELLO, BOUNDS), f.ops());
        ByteBuffer p = f.frames.get(1).payload();
        assertEquals(300_000, p.getInt(33));
        assertEquals(299_999_000L, p.getLong(37 + 8 * 299_999));
    }

}
