// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * Calendar grouping for {@link TsdbClient#queryGroup}: {@code count} {@code unit}s per bucket, in a time zone.
 *
 * <pre>{@code
 * Group days = Group.of(Unit.DAY).in("Europe/Oslo");      // local days: 23, 24 or 25 hours long
 * Group quarterHours = Group.of(Unit.MINUTE, 15);          // in UTC
 * long[] starts = days.bounds(start, end);                 // the UTC instant each day begins
 * }</pre>
 *
 * <p>Multiples are counted from 1970 in local time, like ClickHouse's {@code toStartOfInterval}; each bucket's
 * start is the UTC instant its local period begins. A local start that occurs twice (the clocks go back) begins the
 * period the first time; one the clocks skip begins it at the moment they jump.
 *
 * <p>The server knows UTC only: the client turns a time zone into UTC bounds itself, with the JDK's time zone
 * data ({@code java.time}).
 *
 * @param unit the calendar unit
 * @param count units per bucket; 0 means 1. Read as an unsigned 32-bit integer.
 * @param zone the time zone, or {@code null} for UTC
 */
public record Group(Unit unit, int count, ZoneId zone) {
    /**
     * Checks the arguments.
     *
     * @param unit the calendar unit
     * @param count units per bucket; 0 means 1
     * @param zone the time zone, or {@code null} for UTC
     * @throws NullPointerException if {@code unit} is null
     */
    public Group {
        if (unit == null)
            throw new NullPointerException("unit");
    }

    /**
     * One unit per bucket, in UTC.
     *
     * @param unit the calendar unit
     * @return the grouping
     */
    public static Group of(Unit unit) {
        return new Group(unit, 1, null);
    }

    /**
     * {@code count} units per bucket, in UTC.
     *
     * @param unit the calendar unit
     * @param count units per bucket
     * @return the grouping
     */
    public static Group of(Unit unit, int count) {
        return new Group(unit, count, null);
    }

    /**
     * The same grouping in another time zone.
     *
     * @param zone the time zone, or {@code null} for UTC
     * @return a new grouping
     */
    public Group in(ZoneId zone) {
        return new Group(unit, count, zone);
    }

    /**
     * The same grouping in a time zone given by name.
     *
     * @param zone an IANA name such as "Europe/Oslo", "UTC", or a fixed offset such as "+02:00" or "-0530";
     *     {@code null} for UTC
     * @return a new grouping
     * @throws IllegalArgumentException if the JDK does not know the zone
     */
    public Group in(String zone) {
        if (zone == null)
            return in((ZoneId) null);
        try {
            return in(ZoneId.of(zone));
        } catch (DateTimeException e) {
            throw new IllegalArgumentException("unknown time zone " + zone, e);
        }
    }

    /**
     * The UTC instants at which the buckets of this grouping begin: one for every period that overlaps
     * {@code [start, end)}, the first one at or before {@code start}. These are the bounds that
     * {@link TsdbClient#queryGroup} sends to the server, and that {@link TsdbClient#queryBounds} takes.
     *
     * <p>Seconds, minutes and hours are fixed lengths, aligned to multiples of their length in the zone's standard
     * time (so 6-hour buckets in India begin at 00:00, 06:00, ... local standard time). Days, weeks, months, quarters
     * and years are local calendar periods.
     *
     * @param start the first instant included, ms since the epoch
     * @param end the first instant excluded, ms since the epoch
     * @return the bounds in ms since the epoch, strictly increasing; empty if {@code start >= end}
     * @throws TsdbException with {@link Status#ERR_INVALID} for a range more than about 200,000 years from 1970, or
     *     {@link Status#ERR_TOO_MANY_BUCKETS} for more than 64 Mi buckets
     */
    public long[] bounds(long start, long end) throws TsdbException {
        return CalendarBounds.of(zone, unit, count, start, end);
    }

    /** Whether the zone is UTC, so the server can group by itself. */
    boolean utc() {
        return zone == null || zone.normalized().equals(ZoneOffset.UTC);
    }
}
