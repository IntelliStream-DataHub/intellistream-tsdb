// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneOffsetTransitionRule;
import java.time.zone.ZoneRules;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Local calendar periods as UTC bucket bounds: the Java form of {@code tsdb_calendar_bounds}
 * ({@code client/tsdb_calendar.c}), which defines them. The server knows UTC only; for "per day in Europe/Oslo"
 * the client computes the UTC instant each local day begins and asks for buckets that begin there (QUERY_BOUNDS).
 *
 * <p>The C code reads the system's zoneinfo and this class the JDK's own tzdb; with the same tzdb version both give
 * the same bounds (CalendarBoundsTest checks it against the C output). Where java.time and the C code resolve local
 * times differently, this class follows the C code:
 * <ul>
 * <li>A local start that occurs twice (the clocks go back across it) begins the period at its first occurrence, the
 *     larger offset. java.time's default, the earlier offset, is the same.
 * <li>A local start that does not exist (the clocks jump over it) begins the period when the jump happens, the
 *     transition instant. {@code ZonedDateTime.of} would instead shift the local time forward by the length of the
 *     gap, a different instant unless the gap begins exactly at the start (Toronto went from 23:30 to 00:30 on
 *     1919-03-30: its next day began at 04:30 UTC, not 05:00).
 * </ul>
 */
final class CalendarBounds {
    /** Beyond about 200,000 years from 1970 the C arithmetic would overflow, and the range is refused. */
    static final long LIMIT = 1000L * 86400 * 366 * 200000;
    /** The most periods one call returns, as {@code MAX_BOUNDS} in C. */
    static final int MAX_BOUNDS = 64 << 20;

    private static final long[] UNIT_MS = {0, 1000, 60_000, 3_600_000};
    /** Local seconds below this give UTC instants before {@link Long#MIN_VALUE} ms, whatever the offset. */
    private static final long MIN_LOCAL = Long.MIN_VALUE / 1000 + 2 * 86400;

    private CalendarBounds() {
    }

    /**
     * The UTC start (ms) of every period of {@code count} units that overlaps {@code [start, end)}, in {@code zone}.
     *
     * @param zone the time zone; {@code null} means UTC
     * @param unit the unit
     * @param count units per period, unsigned; 0 means 1
     * @param start the first instant included, ms
     * @param end the first instant excluded, ms
     * @return the bounds, strictly increasing; empty if {@code start >= end}
     * @throws TsdbException {@link Status#ERR_INVALID} for a range beyond about 200,000 years from 1970,
     *     {@link Status#ERR_TOO_MANY_BUCKETS} for more than 64 Mi periods
     */
    static long[] of(ZoneId zone, Unit unit, int count, long start, long end) throws TsdbException {
        if (start >= end)
            return new long[0];
        if (start < -LIMIT || end > LIMIT)
            throw new TsdbException(Status.ERR_INVALID, "time range beyond 200,000 years from 1970", null);
        if (zone == null)
            zone = ZoneOffset.UTC;
        ZoneRules rules = zone.getRules();
        long c = count == 0 ? 1 : Integer.toUnsignedLong(count);
        boolean fixed = unit.code <= Unit.HOUR.code;
        long k0, k1, interval = 0, shift = 0;
        if (fixed) {
            interval = UNIT_MS[unit.code] * c;
            shift = standardOffset(zone) * 1000L;
            k0 = Math.floorDiv(start + shift, interval);
            k1 = Math.floorDiv(end - 1 + shift, interval);
        } else {
            k0 = Math.floorDiv(index(rules, unit, start), c);
            k1 = Math.floorDiv(index(rules, unit, end - 1), c);
        }
        if (k1 - k0 + 1 > MAX_BOUNDS)
            throw new TsdbException(Status.ERR_TOO_MANY_BUCKETS, (k1 - k0 + 1) + " periods", null);
        long[] b = new long[(int) (k1 - k0 + 1)];
        int m = 0;
        for (long k = k0; k <= k1; k++) {
            long t = fixed ? k * interval - shift : utcMs(rules, localStart(unit, k * c));
            if (m == 0 || t > b[m - 1]) /* bounds strictly increase */
                b[m++] = t;
        }
        return m == b.length ? b : Arrays.copyOf(b, m);
    }

    /**
     * The offset that fixed-length units align to: the zone's standard time from now on, which the C code takes
     * from the POSIX TZ string at the end of a zoneinfo file. The JDK's tzdb differs from zoneinfo where the tz
     * database has negative DST: Ireland's standard time is IST (+01:00) with GMT as winter time, and Morocco's was
     * +01:00 with +00:00 during Ramadan, but the JDK turns these into a +00:00 standard time with positive DST (and
     * keeps a +01:00 "standard" time for Namibia, whose clocks have stayed at +02:00 since 2017). So:
     * <ul>
     * <li>a zone whose offset stops changing (Namibia; Morocco, whose changes the data lists one by one) aligns to
     *     its last offset, as the C code does with a TZ string without DST;
     * <li>Ireland aligns to its summer offset, IST;
     * <li>every other zone with DST to the standard offset of its yearly rules.
     * </ul>
     */
    static int standardOffset(ZoneId zone) {
        ZoneRules rules = zone.getRules();
        List<ZoneOffsetTransitionRule> yearly = rules.getTransitionRules();
        if (yearly.isEmpty())
            return rules.getOffset(Instant.MAX).getTotalSeconds();
        ZoneOffsetTransitionRule last = yearly.get(yearly.size() - 1);
        if (NEGATIVE_DST.contains(zone.getId()))
            return Math.max(last.getOffsetBefore().getTotalSeconds(), last.getOffsetAfter().getTotalSeconds());
        return last.getStandardOffset().getTotalSeconds();
    }

    /** Zones with yearly rules whose standard time the tz database puts in summer (negative DST). */
    private static final Set<String> NEGATIVE_DST = Set.of("Europe/Dublin", "Eire");

    private static int offset(ZoneRules rules, long second) {
        return rules.getOffset(Instant.ofEpochSecond(second)).getTotalSeconds();
    }

    /** The index of the local period containing {@code ms}: days, ISO weeks, months, quarters or years since 1970. */
    private static long index(ZoneRules rules, Unit unit, long ms) {
        long s = Math.floorDiv(ms, 1000L), local = s + offset(rules, s);
        long day = Math.floorDiv(local, 86400L);
        switch (unit) {
        case DAY:
            return day;
        case WEEK:
            return Math.floorDiv(day + 3, 7L); /* 1970-01-01 was a Thursday */
        default:
            long[] ymd = civil(day);
            long month = (ymd[0] - 1970) * 12 + (ymd[1] - 1);
            return unit == Unit.MONTH ? month : unit == Unit.QUARTER ? Math.floorDiv(month, 3L) : ymd[0] - 1970;
        }
    }

    /** Local seconds (since 1970-01-01T00:00 local) at which period {@code idx} begins. */
    private static long localStart(Unit unit, long idx) {
        switch (unit) {
        case DAY:
            return idx * 86400;
        case WEEK:
            return (idx * 7 - 3) * 86400;
        case MONTH:
        case QUARTER:
            long month = unit == Unit.MONTH ? idx : idx * 3;
            long y = Math.floorDiv(month, 12L);
            return daysFromCivil(1970 + y, month - y * 12 + 1, 1) * 86400;
        default:
            return daysFromCivil(1970 + idx, 1, 1) * 86400;
        }
    }

    /**
     * The UTC instant (ms) at which local time {@code local} (seconds) first occurs, or, if the clocks skip it, the
     * instant they jump past it. {@link Long#MIN_VALUE} if that lies before the range of a long.
     */
    private static long utcMs(ZoneRules rules, long local) {
        if (local < MIN_LOCAL)
            return Long.MIN_VALUE; /* only for counts of millions of years, before 1970 */
        long u;
        try {
            LocalDateTime t = LocalDateTime.ofEpochSecond(local, 0, ZoneOffset.UTC);
            ZoneOffsetTransition gap = rules.getTransition(t);
            if (gap != null && gap.isGap()) {
                u = gap.toEpochSecond();
            } else {
                List<ZoneOffset> valid = rules.getValidOffsets(t); /* two where the clocks went back */
                int off = Integer.MIN_VALUE;
                for (ZoneOffset o : valid)
                    off = Math.max(off, o.getTotalSeconds()); /* the larger offset is the earlier instant */
                u = local - off;
            }
        } catch (DateTimeException e) { /* beyond java.time's years: as far from 1970 as a long reaches anyway */
            return Long.MIN_VALUE;
        }
        return u < Long.MIN_VALUE / 1000 ? Long.MIN_VALUE : u * 1000;
    }

    /** Days since 1970-01-01 of a proleptic Gregorian date (Howard Hinnant's days_from_civil, as in C). */
    static long daysFromCivil(long y, long m, long d) {
        y -= m <= 2 ? 1 : 0;
        long era = Math.floorDiv(y, 400L);
        long yoe = y - era * 400;
        long doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
        long doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        return era * 146097 + doe - 719468;
    }

    /** {year, month, day} of a day number, the inverse of {@link #daysFromCivil}. */
    static long[] civil(long days) {
        long z = days + 719468;
        long era = Math.floorDiv(z, 146097L);
        long doe = z - era * 146097;
        long yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        long doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        long mp = (5 * doy + 2) / 153;
        long d = doy - (153 * mp + 2) / 5 + 1;
        long m = mp < 10 ? mp + 3 : mp - 9;
        return new long[] {yoe + era * 400 + (m <= 2 ? 1 : 0), m, d};
    }
}
