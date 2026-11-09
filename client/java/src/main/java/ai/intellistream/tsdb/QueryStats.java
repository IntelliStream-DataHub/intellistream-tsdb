// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

/**
 * What the server did for a query ({@code tsdb_query_stats}), from {@link TsdbClient#lastQueryStats()}. Every
 * count is an unsigned 64-bit integer on the wire.
 *
 * @param elapsedS how long the query ran on the server, in seconds
 * @param rows rows covered: {@code rowsDecoded + rowsFromStats + memtableRows}
 * @param rowsDecoded rows decompressed (whole granules)
 * @param rowsFromStats rows answered from granule statistics, without decoding
 * @param memtableRows rows read from memtables (not yet in parts)
 * @param bytes uncompressed size of the rows covered (timestamp and value)
 * @param bytesRead compressed bytes of the granules decoded
 * @param parts parts with rows of the series in the range
 * @param granules their granules in the range
 * @param granulesFromStats granules answered from their index entry
 * @param resultRows rows, or buckets, returned
 */
public record QueryStats(double elapsedS, long rows, long rowsDecoded, long rowsFromStats, long memtableRows,
                         long bytes, long bytesRead, long parts, long granules, long granulesFromStats,
                         long resultRows) {
}
