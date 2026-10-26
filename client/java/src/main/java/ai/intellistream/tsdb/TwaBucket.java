package ai.intellistream.tsdb;

/**
 * One bucket of a time-weighted average ({@link TsdbClient#queryTwa}): every reading weighted by how long it held.
 *
 * @param start the bucket's start in ms since the epoch; for a single bucket, the query's start
 * @param twa {@code area / coveredMs}, NaN when nothing is covered
 * @param area the integral of the value over time, in value x ms
 * @param coveredMs the time covered by segments, in ms
 * @param count the known readings in the bucket (and in the query's range)
 */
public record TwaBucket(long start, double twa, double area, long coveredMs, long count) {
}
