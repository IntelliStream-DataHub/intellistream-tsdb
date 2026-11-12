// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.zone.ZoneRulesProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.junit.jupiter.api.Test;

/**
 * The Java bounds of local calendar periods are those of the C client, {@code tsdb_calendar_bounds}.
 *
 * <p>{@code calendar-bounds.txt.gz} holds its output for 40 zones (fixed offsets, half and quarter hour zones,
 * 30-minute DST, negative DST, DST changes at midnight, skipped days) times every unit, many counts and ranges over
 * decades and centuries, plus random ranges; {@code src/test/c/calendar_fixtures.c} prints it. The C code reads the
 * system's zoneinfo, the JDK its own tzdb: where their data differ, so do the bounds, and the zones where that is
 * known are listed below. Every other zone has to match exactly.
 */
class CalendarBoundsTest {
    static final String FIXTURES = "/calendar-bounds.txt.gz";

    /**
     * Zones whose history before 1970 differs whatever the versions, compared from 1970 on. The tz database keeps
     * the older history of zones that agree since 1970 in its "backzone" file, which the system's zoneinfo (Ubuntu's)
     * includes and the JDK's tzdb leaves out: there Oslo before 1970 is Berlin, and Amsterdam is Brussels. Dublin
     * differs for two weeks of 1925. Of the 591 zones both know, 123 differ this way.
     */
    static final Set<String> HISTORY_BEFORE_1970 = Set.of("Europe/Oslo", "Europe/Amsterdam", "Europe/Dublin");

    /**
     * Zones that changed between the JDK's tzdb (2026a, when this was written) and the fixtures' tzdata (2026e),
     * skipped while the versions differ. With the same version they have to match like the rest.
     */
    static final Map<String, String> TZDB_CHANGES = Map.of(
            "Africa/Casablanca", "Morocco stays on +00:00 from 2026-09-20 (C: all fixed units align to +00:00)",
            "America/Vancouver", "British Columbia stays on -07:00 from November 2026 (as do Alberta and Manitoba)",
            "Asia/Tehran", "Iran's DST began on 1979-05-26, not 1979-05-27");

    /** One line of the fixtures: a call of tsdb_calendar_bounds and what it returned. */
    record Case(String zone, Unit unit, int count, long start, long end, int rc, long[] bounds, String line) {
        static Case parse(String line) {
            String[] f = line.split(" ");
            int n = Integer.parseInt(f[6]);
            long[] b = new long[n];
            int m = 0;
            for (int i = 7; i < f.length; i++) {
                if (m == 0) {
                    b[m++] = Long.parseLong(f[i]);
                    continue;
                }
                int star = f[i].indexOf('*');
                long d = Long.parseLong(star < 0 ? f[i] : f[i].substring(0, star));
                int r = star < 0 ? 1 : Integer.parseInt(f[i].substring(star + 1));
                for (int k = 0; k < r; k++, m++)
                    b[m] = b[m - 1] + d;
            }
            if (m != n)
                throw new IllegalStateException("bad fixture line: " + line);
            return new Case(f[0], CalendarBoundsTest.unit(Integer.parseInt(f[1])), Integer.parseUnsignedInt(f[2]),
                    Long.parseLong(f[3]), Long.parseLong(f[4]), Integer.parseInt(f[5]), b, line);
        }
    }

    static Unit unit(int code) {
        for (Unit u : Unit.values())
            if (u.code == code)
                return u;
        throw new IllegalArgumentException("unit " + code);
    }

    /** What sameBoundsAsTheCClient does with each case, given the fixtures' tzdata version. */
    interface CaseCheck {
        void check(String version, Case c);
    }

    /**
     * The fixtures: the header's tzdata version, then the cases, each handed to {@code each} as it is read and then
     * dropped: some cases hold 31 million bounds (252 MB), and keeping them all needs more heap than a 16 GB CI runner's
     * default JVM has. Returns the number of cases.
     */
    static int read(BufferedReader r, CaseCheck each) throws IOException {
        String version = null;
        int n = 0;
        for (String line; (line = r.readLine()) != null;) {
            if (line.startsWith("#")) {
                int v = line.indexOf("zoneinfo version ");
                if (v >= 0)
                    version = line.substring(v + "zoneinfo version ".length()).trim();
                continue;
            }
            if (!line.isBlank()) {
                each.check(version, Case.parse(line));
                n++;
            }
        }
        return n;
    }

    /**
     * Compares Java's bounds for a case with C's, leaving out those before {@code from}; null if they agree,
     * otherwise a description of the difference.
     */
    static String check(Case c, long from) {
        Group g = new Group(c.unit(), c.count(), ZoneId.of(c.zone()));
        long[] b;
        try {
            b = g.bounds(c.start(), c.end());
        } catch (TsdbException e) {
            return e.status() == c.rc() ? null : "status " + e.status() + ", C " + c.rc();
        }
        if (c.rc() != 0)
            return "no error, C " + c.rc();
        long[] x = from(b, from), y = from(c.bounds(), from);
        if (Arrays.equals(x, y))
            return null;
        long[] d = differ(x, y);
        return x.length + " bounds, C " + y.length + "; they differ from " + instant(d[0]) + " to " + instant(d[1]);
    }

    static long[] from(long[] b, long from) {
        int i = 0;
        while (i < b.length && b[i] < from)
            i++;
        return i == 0 ? b : Arrays.copyOfRange(b, i, b.length);
    }

    /** The first and last bound in one array but not the other (both strictly increase). */
    static long[] differ(long[] x, long[] y) {
        long lo = Long.MAX_VALUE, hi = Long.MIN_VALUE;
        int i = 0, j = 0;
        while (i < x.length || j < y.length) {
            long v;
            if (j == y.length || (i < x.length && x[i] < y[j])) {
                v = x[i++];
            } else if (i == x.length || y[j] < x[i]) {
                v = y[j++];
            } else {
                i++;
                j++;
                continue;
            }
            lo = Math.min(lo, v);
            hi = Math.max(hi, v);
        }
        return new long[] {lo, hi};
    }

    static String instant(long ms) {
        return ms + " (" + java.time.Instant.ofEpochMilli(ms) + ")";
    }

    static String jdkTzdb() {
        return ZoneRulesProvider.getVersions("Europe/Oslo").lastKey();
    }

    @Test
    void sameBoundsAsTheCClient() throws IOException {
        String[] version = {null};
        Map<String, List<String>> wrong = new LinkedHashMap<>();
        int[] skipped = {0};
        int cases;
        try (InputStream in = CalendarBoundsTest.class.getResourceAsStream(FIXTURES)) {
            assertTrue(in != null, FIXTURES + " is missing");
            cases = read(new BufferedReader(new InputStreamReader(new GZIPInputStream(in), StandardCharsets.UTF_8)),
                    (v, c) -> {
                        version[0] = v;
                        if (!jdkTzdb().equals(v) && TZDB_CHANGES.containsKey(c.zone())) {
                            skipped[0]++;
                            return;
                        }
                        String d = check(c, HISTORY_BEFORE_1970.contains(c.zone()) ? 0 : Long.MIN_VALUE);
                        if (d != null)
                            wrong.computeIfAbsent(c.zone(), z -> new ArrayList<>()).add(d + "\n      "
                                    + c.line().substring(0, Math.min(c.line().length(), 160)));
                    });
        }
        assertTrue(cases > 4000, "fixtures: " + cases + " cases");
        if (!wrong.isEmpty()) {
            StringBuilder s = new StringBuilder("bounds differ from C (fixtures: tzdata " + version[0] + ", JDK tzdb "
                    + jdkTzdb() + ") in " + wrong.size() + " zones:\n");
            wrong.forEach((z, l) -> s.append("  ").append(z).append(": ").append(l.size()).append(" cases, e.g. ")
                    .append(l.get(0)).append('\n'));
            fail(s.toString());
        }
        assertTrue(skipped[0] < cases / 4, skipped[0] + " cases skipped");
    }

    static long utc(int y, int m, int d, int h, int min) {
        return LocalDateTime.of(y, m, d, h, min).toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    static long day(int y, int m, int d) {
        return LocalDate.of(y, m, d).toEpochDay() * 86_400_000L;
    }

    /** Days around both DST changes in Oslo: 23 and 25 hours; midnight is 23:00 or 22:00 UTC. */
    @Test
    void osloDays() throws TsdbException {
        Group g = Group.of(Unit.DAY).in("Europe/Oslo");
        assertArrayEquals(new long[] {utc(2024, 3, 29, 23, 0), utc(2024, 3, 30, 23, 0), utc(2024, 3, 31, 22, 0),
            utc(2024, 4, 1, 22, 0)}, g.bounds(day(2024, 3, 30), day(2024, 4, 2)));
        assertArrayEquals(new long[] {utc(2024, 10, 26, 22, 0), utc(2024, 10, 27, 23, 0)},
                g.bounds(day(2024, 10, 27), day(2024, 10, 28)));
    }

    /**
     * A local midnight the clocks skip begins the day when they jump. Toronto went from 23:30 to 00:30 on
     * 1919-03-30, so 1919-03-31 began at 04:30 UTC. {@code ZonedDateTime.of(midnight, zone)} would say 05:00 UTC
     * (it shifts a skipped time forward by the length of the gap); {@code LocalDate.atStartOfDay(zone)} agrees.
     */
    @Test
    void skippedMidnights() throws TsdbException {
        Group toronto = Group.of(Unit.DAY).in("America/Toronto");
        assertArrayEquals(new long[] {utc(1919, 3, 30, 5, 0), utc(1919, 3, 31, 4, 30)},
                toronto.bounds(utc(1919, 3, 30, 12, 0), utc(1919, 4, 1, 0, 0)));
        assertEquals(utc(1919, 3, 31, 5, 0), ZonedDateTime.of(LocalDateTime.of(1919, 3, 31, 0, 0),
                ZoneId.of("America/Toronto")).toInstant().toEpochMilli(), "what the bound must not be");

        /* jumps that begin exactly at midnight: Sao Paulo's DST began at 00:00 */
        Group sp = Group.of(Unit.DAY).in("America/Sao_Paulo");
        assertArrayEquals(new long[] {utc(2018, 11, 3, 3, 0), utc(2018, 11, 4, 3, 0), utc(2018, 11, 5, 2, 0)},
                sp.bounds(utc(2018, 11, 3, 12, 0), utc(2018, 11, 5, 12, 0)));
        /* whole days skipped: Kiritimati went from -10:00 to +14:00 and had no 1994-12-31, Apia no 2011-12-30 */
        assertArrayEquals(new long[] {utc(1994, 12, 30, 10, 0), utc(1994, 12, 31, 10, 0), utc(1995, 1, 1, 10, 0)},
                Group.of(Unit.DAY).in("Pacific/Kiritimati").bounds(utc(1994, 12, 30, 12, 0), utc(1995, 1, 1, 12, 0)));
        assertArrayEquals(new long[] {utc(2011, 12, 29, 10, 0), utc(2011, 12, 30, 10, 0), utc(2011, 12, 31, 10, 0)},
                Group.of(Unit.DAY).in("Pacific/Apia").bounds(utc(2011, 12, 29, 12, 0), utc(2011, 12, 31, 12, 0)));
    }

    /** A local midnight that occurs twice begins the day the first time (Havana, 2019-11-03 01:00 back to 00:00). */
    @Test
    void repeatedMidnights() throws TsdbException {
        /* Cuba ends DST at 01:00 local, back to 00:00: midnight of the 3rd happens at 04:00 and 05:00 UTC */
        Group h = Group.of(Unit.DAY).in("America/Havana");
        long[] b = h.bounds(utc(2019, 11, 2, 12, 0), utc(2019, 11, 3, 12, 0));
        assertArrayEquals(new long[] {utc(2019, 11, 2, 4, 0), utc(2019, 11, 3, 4, 0)}, b);
    }

    /** Hours and minutes align to the zone's standard time, so they are uneven in half and quarter hour zones. */
    @Test
    void fixedUnitsAlignToStandardTime() throws TsdbException {
        long[] b = Group.of(Unit.HOUR, 6).in("Asia/Kathmandu").bounds(day(2024, 1, 1), day(2024, 1, 2));
        assertEquals(utc(2023, 12, 31, 18, 15), b[0], "00:00 local is 18:15 UTC the day before");
        long[] st = Group.of(Unit.HOUR, 2).in("America/St_Johns").bounds(day(2024, 7, 1), day(2024, 7, 1) + 1);
        assertArrayEquals(new long[] {utc(2024, 6, 30, 23, 30)}, st, "-03:30 standard time, also in summer");
    }

    /**
     * Where the JDK's tzdb has another standard time than zoneinfo, fixed units still align as in C: Ireland's
     * standard time is IST, +01:00 (the JDK says GMT), Namibia's +02:00 since 2017 (the JDK says +01:00).
     */
    @Test
    void standardTimeAsInZoneinfo() throws TsdbException {
        Group threeHours = Group.of(Unit.HOUR, 3);
        long t = day(2024, 1, 1);
        assertArrayEquals(new long[] {utc(2023, 12, 31, 23, 0)}, threeHours.in("Europe/Dublin").bounds(t, t + 1));
        assertArrayEquals(new long[] {utc(2023, 12, 31, 23, 0)}, threeHours.in("Eire").bounds(t, t + 1));
        assertArrayEquals(new long[] {utc(2024, 1, 1, 0, 0)}, threeHours.in("Europe/London").bounds(t, t + 1));
        assertArrayEquals(new long[] {utc(2023, 12, 31, 22, 0)}, threeHours.in("Africa/Windhoek").bounds(t, t + 1));
    }

    @Test
    void edges() throws TsdbException {
        Group g = Group.of(Unit.DAY).in("Europe/Oslo");
        assertEquals(0, g.bounds(5, 5).length);
        assertEquals(0, g.bounds(5, 4).length);
        assertEquals(Status.ERR_INVALID, assertThrows(TsdbException.class,
                () -> g.bounds(0, CalendarBounds.LIMIT + 1)).status());
        assertEquals(Status.ERR_INVALID, assertThrows(TsdbException.class,
                () -> g.bounds(Long.MIN_VALUE, 0)).status());
        assertEquals(Status.ERR_TOO_MANY_BUCKETS, assertThrows(TsdbException.class,
                () -> Group.of(Unit.SECOND).bounds(0, 3 * 365 * 86_400_000L)).status());
        assertArrayEquals(Group.of(Unit.DAY, 1).bounds(0, 10 * 86_400_000L),
                new Group(Unit.DAY, 0, null).bounds(0, 10 * 86_400_000L), "count 0 means 1");
        assertThrows(IllegalArgumentException.class, () -> Group.of(Unit.DAY).in("Not/A_Zone"));
        assertTrue(Group.of(Unit.DAY).in("Etc/UTC").utc());
        assertTrue(Group.of(Unit.DAY).in("+00:00").utc());
        assertTrue(!Group.of(Unit.DAY).in("Europe/London").utc());
    }

    /**
     * Counts so large that the first period begins before the range of a long: the first bound is
     * {@link Long#MIN_VALUE}, which still holds every row from the start on. (The C code overflows there, and
     * for months and quarters returns a bound in the far future instead.)
     */
    @Test
    void hugeCounts() throws TsdbException {
        for (Unit u : new Unit[] {Unit.MONTH, Unit.QUARTER, Unit.YEAR}) {
            long[] b = new Group(u, -1, ZoneId.of("Europe/Oslo")).bounds(-7_200_000L, 0);
            assertArrayEquals(new long[] {Long.MIN_VALUE, -3_600_000L}, b, u.toString());
        }
    }

    /** Compares the Java bounds with fixtures on stdin, for any set of zones; prints the differences. */
    public static void main(String[] args) throws IOException {
        BufferedReader r = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        Map<String, Integer> wrong = new LinkedHashMap<>();
        int n = 0;
        for (String line; (line = r.readLine()) != null;) {
            if (line.startsWith("#")) {
                System.out.println(line);
                continue;
            }
            Case c = Case.parse(line);
            n++;
            String d = check(c, Long.MIN_VALUE);
            if (d != null) {
                wrong.merge(c.zone(), 1, Integer::sum);
                String[] f = line.split(" ", 8);
                System.out.println(String.join(" ", Arrays.copyOf(f, 7)) + ": " + d);
            }
        }
        System.out.println(n + " cases, JDK tzdb " + jdkTzdb() + "; " + wrong.size() + " zones differ: " + wrong);
    }
}
