package ai.intellistream.tsdb;

/**
 * Options of a query over many series: threads, a cap on the rows in all, and per-series ranges.
 *
 * <p>{@code starts} and {@code ends}, where not null, give series {@code i} the range
 * {@code [starts[i], ends[i])} instead of the call's {@code start} and {@code end}; each must be as long as the
 * series list. The arrays are not copied: do not change them during the call.
 *
 * @param threads threads of the server the request uses at most; 0: the server's {@code --request-threads}, more is
 *     clamped to it (0 to 65535)
 * @param maxRows rows or buckets of all series together at most; past it the call fails with
 *     {@link Status#ERR_TOO_MANY_BUCKETS}; 0: the server's {@code --multi-max-rows}, more is clamped to it
 * @param starts per-series range starts, or null
 * @param ends per-series range ends, or null
 */
public record MultiOptions(int threads, long maxRows, long[] starts, long[] ends) {
    private static final MultiOptions DEFAULTS = new MultiOptions(0, 0, null, null);

    /**
     * Checks the values.
     *
     * @param threads threads at most, 0 to 65535
     * @param maxRows rows at most, not negative
     * @param starts per-series range starts, or null
     * @param ends per-series range ends, or null
     */
    public MultiOptions {
        if (threads < 0 || threads > 0xffff)
            throw new IllegalArgumentException("threads " + threads + ": 0 to 65535");
        if (maxRows < 0)
            throw new IllegalArgumentException("negative maxRows");
    }

    /**
     * The server's defaults, the call's range for every series.
     *
     * @return the options
     */
    public static MultiOptions defaults() {
        return DEFAULTS;
    }

    /**
     * These options with another thread count.
     *
     * @param n threads at most (0: the server's default)
     * @return the options
     */
    public MultiOptions threads(int n) {
        return new MultiOptions(n, maxRows, starts, ends);
    }

    /**
     * These options with another cap on the rows of all series.
     *
     * @param n rows or buckets at most (0: the server's default)
     * @return the options
     */
    public MultiOptions maxRows(long n) {
        return new MultiOptions(threads, n, starts, ends);
    }

    /**
     * These options with per-series range starts.
     *
     * @param a one start per series, or null for the call's start
     * @return the options
     */
    public MultiOptions starts(long[] a) {
        return new MultiOptions(threads, maxRows, a, ends);
    }

    /**
     * These options with per-series range ends.
     *
     * @param a one end per series, or null for the call's end
     * @return the options
     */
    public MultiOptions ends(long[] a) {
        return new MultiOptions(threads, maxRows, starts, a);
    }
}
