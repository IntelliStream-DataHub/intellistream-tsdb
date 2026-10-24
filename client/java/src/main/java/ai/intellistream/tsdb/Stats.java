package ai.intellistream.tsdb;

/**
 * Storage statistics of the server's database ({@code tsdb_stats}). Every count is an unsigned 64-bit integer on
 * the wire.
 *
 * @param parts part files
 * @param partitions time partitions
 * @param rows rows in parts
 * @param bytes bytes in parts
 * @param merges merges done since the server started
 * @param mergedRows rows written by those merges
 * @param passthroughGranules granules merges copied without decoding
 * @param memtableRows rows in the WAL-backed memtable, not yet in parts
 */
public record Stats(long parts, long partitions, long rows, long bytes, long merges, long mergedRows,
                    long passthroughGranules, long memtableRows) {
}
