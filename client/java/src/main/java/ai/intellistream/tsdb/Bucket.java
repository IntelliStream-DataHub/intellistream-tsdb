package ai.intellistream.tsdb;

/**
 * One bucket of an aggregate query.
 *
 * <p>Aggregates skip nulls: {@code count} is the number of known values, and a bucket whose rows are all null is
 * returned with count 0 (buckets without rows are not returned). For floats, NaN values are ignored by {@code min}
 * and {@code max} (NaN if there are no others) but propagate into {@code sum}. For int64 and bool series ({@code
 * false} 0, {@code true} 1) the sum, minimum and maximum are also exact in {@code isum}, {@code imin} and {@code
 * imax}; {@code isum} saturates at {@link Long#MIN_VALUE} and {@link Long#MAX_VALUE}. For state series only
 * {@code count} is meaningful.
 *
 * @param start the bucket's start in ms since the epoch; for a single bucket over the whole range, the query start
 * @param count the number of known values; an unsigned 64-bit integer on the wire
 * @param sum the sum of the values
 * @param min the smallest value
 * @param max the largest value
 * @param isum the exact sum, for int64 and bool series
 * @param imin the exact minimum, for int64 and bool series
 * @param imax the exact maximum, for int64 and bool series
 */
public record Bucket(long start, long count, double sum, double min, double max, long isum, long imin, long imax) {
    /**
     * The mean value.
     *
     * @return {@code sum / count}, or 0 for an empty bucket
     */
    public double avg() {
        return count != 0 ? sum / count : 0.0;
    }
}
