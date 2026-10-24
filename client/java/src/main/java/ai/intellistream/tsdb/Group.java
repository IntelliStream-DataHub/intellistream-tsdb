package ai.intellistream.tsdb;

/**
 * Calendar grouping for {@link TsdbClient#queryGroup}: {@code count} {@code unit}s per bucket, in a time zone.
 *
 * <p>Multiples are counted from 1970 in local time, like ClickHouse's {@code toStartOfInterval}; each bucket's
 * start is the UTC instant its local period begins.
 *
 * @param unit the calendar unit
 * @param count units per bucket; 0 means 1. Sent as an unsigned 32-bit integer.
 * @param timezone {@code null} or "UTC", a fixed offset ("+02:00", "-0530"), or an IANA name such as
 *     "Europe/Oslo", resolved on the server
 */
public record Group(Unit unit, int count, String timezone) {
    /**
     * Validates the arguments.
     *
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
     * @param zone the time zone, as for {@link #timezone()}
     * @return a new grouping
     */
    public Group in(String zone) {
        return new Group(unit, count, zone);
    }
}
