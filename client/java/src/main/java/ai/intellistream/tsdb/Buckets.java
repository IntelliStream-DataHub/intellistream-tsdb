package ai.intellistream.tsdb;

import java.util.Objects;

/**
 * The buckets of a query over many series ({@link TsdbClient#queryBucketsMulti}, {@code querySpreadMulti},
 * {@code queryTwaMulti}): one bucket over the whole range, fixed intervals (the same for every series or one per
 * series), calendar periods in a time zone, or bounds given by the caller.
 */
public final class Buckets {
    /** Which form of buckets. */
    public enum Kind {
        /** One bucket over the whole range. */
        ONE,
        /** Fixed intervals, the same for every series. */
        EVERY,
        /** Fixed intervals, one per series. */
        INTERVALS,
        /** Calendar periods in a time zone. */
        GROUP,
        /** Bounds given by the caller. */
        BOUNDS
    }

    private static final Buckets ONE = new Buckets(Kind.ONE, 0, null, null, null);

    private final Kind kind;
    private final long interval;
    private final long[] intervals;
    private final Group group;
    private final long[] bounds;

    private Buckets(Kind kind, long interval, long[] intervals, Group group, long[] bounds) {
        this.kind = kind;
        this.interval = interval;
        this.intervals = intervals;
        this.group = group;
        this.bounds = bounds;
    }

    /**
     * One bucket over the whole range of each series.
     *
     * @return the buckets
     */
    public static Buckets one() {
        return ONE;
    }

    /**
     * Buckets of {@code intervalMs} for every series, aligned to multiples of it (0: one bucket). A negative interval
     * fails the call with {@link Status#ERR_INVALID}, as the single call does.
     *
     * @param intervalMs the bucket length in ms
     * @return the buckets
     */
    public static Buckets every(long intervalMs) {
        return new Buckets(Kind.EVERY, intervalMs, null, null, null);
    }

    /**
     * Buckets of {@code intervalMs[i]} for series {@code i} (0: one bucket). A negative interval is that series'
     * error ({@link Status#ERR_INVALID}). The array is not copied: do not change it during the call.
     *
     * @param intervalMs one interval per series, as long as the series list
     * @return the buckets
     */
    public static Buckets intervals(long[] intervalMs) {
        return new Buckets(Kind.INTERVALS, 0, Objects.requireNonNull(intervalMs, "intervalMs"), null, null);
    }

    /**
     * Calendar periods, as {@link TsdbClient#queryGroup}: in UTC the server groups; in another zone the client
     * computes one list of bounds over the rows of all series and the server trims it to each series' rows.
     *
     * @param group the unit, count and zone
     * @return the buckets
     */
    public static Buckets group(Group group) {
        return new Buckets(Kind.GROUP, 0, null, Objects.requireNonNull(group, "group"), null);
    }

    /**
     * Bounds given by the caller, as {@link TsdbClient#queryBounds}: the same for every series. The array is not
     * copied: do not change it during the call.
     *
     * @param bounds strictly increasing bucket starts in ms; the last one ends the last bucket
     * @return the buckets
     */
    public static Buckets bounds(long[] bounds) {
        return new Buckets(Kind.BOUNDS, 0, null, null, Objects.requireNonNull(bounds, "bounds"));
    }

    /**
     * The form of these buckets.
     *
     * @return the kind
     */
    public Kind kind() {
        return kind;
    }

    /**
     * The interval of {@link Kind#EVERY}, else 0.
     *
     * @return the interval in ms
     */
    public long interval() {
        return interval;
    }

    /**
     * The per-series intervals of {@link Kind#INTERVALS}, else null.
     *
     * @return the intervals in ms (not a copy)
     */
    public long[] intervals() {
        return intervals;
    }

    /**
     * The group of {@link Kind#GROUP}, else null.
     *
     * @return the group
     */
    public Group group() {
        return group;
    }

    /**
     * The bounds of {@link Kind#BOUNDS}, else null.
     *
     * @return the bounds in ms (not a copy)
     */
    public long[] bounds() {
        return bounds;
    }

    @Override
    public String toString() {
        switch (kind) {
        case EVERY:
            return "Buckets.every(" + interval + ")";
        case INTERVALS:
            return "Buckets.intervals(" + intervals.length + " intervals)";
        case GROUP:
            return "Buckets.group(" + group + ")";
        case BOUNDS:
            return "Buckets.bounds(" + bounds.length + " bounds)";
        default:
            return "Buckets.one()";
        }
    }
}
