package ai.intellistream.tsdb;

/**
 * One bucket of an aggregate query.
 *
 * <p>NaN values are ignored by {@code min} and {@code max} (which are NaN if every value is), but propagate into
 * {@code sum}.
 *
 * @param start the bucket's start in ms since the epoch; for a single bucket over the whole range, the query start
 * @param count the number of points; an unsigned 64-bit integer on the wire
 * @param sum the sum of the values
 * @param min the smallest value
 * @param max the largest value
 */
public record Bucket(long start, long count, double sum, float min, float max) {
    /**
     * The mean value.
     *
     * @return {@code sum / count}, or 0 for an empty bucket
     */
    public double avg() {
        return count != 0 ? sum / count : 0.0;
    }
}
