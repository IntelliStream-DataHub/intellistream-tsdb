package ai.intellistream.tsdb;

/**
 * Options of {@link TsdbClient#latest}: a bound on the rows, nulls, and threads.
 *
 * @param end only rows before it; {@link Long#MAX_VALUE}: no bound
 * @param ends a bound per series instead of {@code end} (as long as the id list), or null; {@code end} must then be
 *     {@link Long#MAX_VALUE}. The array is not copied: do not change it during the call.
 * @param withNulls answer the newest row even when it is a null ({@link Latest.Kind#NULL}); the slow path
 * @param threads threads of the server one chunk uses at most; 0: the server's {@code --request-threads}, more is
 *     clamped to it (0 to 65535)
 */
public record LatestOptions(long end, long[] ends, boolean withNulls, int threads) {
    private static final LatestOptions DEFAULTS = new LatestOptions(Long.MAX_VALUE, null, false, 0);

    /**
     * Checks the values.
     *
     * @param end only rows before it
     * @param ends a bound per series, or null
     * @param withNulls whether a null may be the answer
     * @param threads threads at most, 0 to 65535
     */
    public LatestOptions {
        if (threads < 0 || threads > 0xffff)
            throw new IllegalArgumentException("threads " + threads + ": 0 to 65535");
        if (ends != null && end != Long.MAX_VALUE)
            throw new IllegalArgumentException("both end and ends given");
    }

    /**
     * No bound, the newest non-null value, the server's threads.
     *
     * @return the options
     */
    public static LatestOptions defaults() {
        return DEFAULTS;
    }

    /**
     * These options with a bound on the rows.
     *
     * @param t only rows before it
     * @return the options
     */
    public LatestOptions end(long t) {
        return new LatestOptions(t, null, withNulls, threads);
    }

    /**
     * These options with a bound per series.
     *
     * @param a one bound per series
     * @return the options
     */
    public LatestOptions ends(long[] a) {
        return new LatestOptions(Long.MAX_VALUE, a, withNulls, threads);
    }

    /**
     * These options with nulls allowed as the answer, or not.
     *
     * @param b whether the newest row may be a null
     * @return the options
     */
    public LatestOptions withNulls(boolean b) {
        return new LatestOptions(end, ends, b, threads);
    }

    /**
     * These options with another thread count.
     *
     * @param n threads at most (0: the server's default)
     * @return the options
     */
    public LatestOptions threads(int n) {
        return new LatestOptions(end, ends, withNulls, n);
    }
}
