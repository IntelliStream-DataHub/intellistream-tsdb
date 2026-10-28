package ai.intellistream.tsdb.embedded;

/**
 * One part file of the database, as {@link EmbeddedDb#parts()} lists them ({@code tsdb_part_info}).
 *
 * @param name the file name
 * @param partition the number of the base partition ({@code partitionMs}) its partition starts in; with partition
 *                  classes a partition is (partition, cls)
 * @param minTs the oldest row's timestamp
 * @param maxTs the newest row's timestamp
 * @param rows rows in the part
 * @param bytes size of the file
 * @param granules granules in the part
 * @param level merge level: 0 for a part an insert wrote
 * @param createdS when the file was written, Unix seconds
 * @param writtenS when its newest rows were inserted, Unix seconds; idle merges go by this
 * @param cls the partition class, 0 to 3: 1, 7, 28 or 364 base partitions
 * @param startMs the partition's start, ms since the epoch
 * @param endMs the partition's end (exclusive)
 */
public record PartInfo(String name, long partition, long minTs, long maxTs, long rows, long bytes, long granules,
                       int level, long createdS, long writtenS, int cls, long startMs, long endMs) {
}
