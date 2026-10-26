package ai.intellistream.tsdb;

/**
 * One bucket of a spread query ({@link TsdbClient#querySpread}): the count, sum, minimum and maximum of a plain
 * aggregate, and how far the values spread. Variances are NaN for a bucket of nulls only, the sample ones also for a
 * single value.
 *
 * @param bucket count, sum, min and max, as from the plain queries
 * @param range {@code max - min}
 * @param varPop the population variance
 * @param varSamp the sample variance
 * @param stddevPop the population standard deviation
 * @param stddevSamp the sample standard deviation
 */
public record SpreadBucket(Bucket bucket, double range, double varPop, double varSamp, double stddevPop,
                           double stddevSamp) {
}
