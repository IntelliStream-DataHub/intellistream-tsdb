package ai.intellistream.tsdb;

/**
 * Calendar units for {@link TsdbClient#queryGroup}.
 *
 * <p>{@link #SECOND}, {@link #MINUTE} and {@link #HOUR} make fixed-length buckets, aligned to multiples of their
 * length since the epoch in the zone's standard time. {@link #DAY}, {@link #WEEK} (ISO, from Monday),
 * {@link #MONTH}, {@link #QUARTER} and {@link #YEAR} are local calendar periods: a day across a DST change lasts 23
 * or 25 hours.
 */
public enum Unit {
    /** Seconds. */
    SECOND(1),
    /** Minutes. */
    MINUTE(2),
    /** Hours. */
    HOUR(3),
    /** Local days. */
    DAY(4),
    /** ISO weeks, starting on Monday. */
    WEEK(5),
    /** Calendar months. */
    MONTH(6),
    /** Calendar quarters. */
    QUARTER(7),
    /** Calendar years. */
    YEAR(8);

    final int code; /* tsdb_unit */

    Unit(int code) {
        this.code = code;
    }
}
