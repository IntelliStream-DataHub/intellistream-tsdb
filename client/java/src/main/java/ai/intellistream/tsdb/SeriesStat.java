// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

import java.nio.ByteBuffer;

/**
 * The storage numbers of one series ({@link TsdbClient#seriesStats}): "physical", rows and bytes as stored,
 * including rows a newer write, a tombstone or retention hides until a merge removes them. The 64-bit counters are
 * unsigned on the wire and never reach 2^63 in practice.
 *
 * @param cls the partition class, 0 to 3
 * @param type the series' type
 * @param flags {@link #NO_RANGE}, {@link #WRITE_FROM_MTIME}, {@link #NO_BOUND}
 * @param rows rows in parts and memtables
 * @param nulls nulls in parts
 * @param memtableRows rows in the memtables
 * @param logicalBytes part rows times (8 + the width of the type)
 * @param tsBytes bytes of timestamps
 * @param valueBytes bytes of values
 * @param indexBytes the series' share of the granule indexes
 * @param tombBytes 24 bytes per tombstone
 * @param parts parts that hold the series
 * @param partitions partitions that hold it
 * @param granules granules
 * @param fullGranules granules of a full 8192 rows
 * @param firstTs first stored row (0 with {@link #NO_RANGE})
 * @param lastTs last stored row (0 with {@link #NO_RANGE})
 * @param lastWriteMs the newest part holding it (its written time) or the creation of its memtable
 * @param residentBytes bytes of its granules in the page cache, when asked for; else 0
 * @param readsEst queries in the last 60 to 120 seconds (sampled), 0 when it is not in the hot-reads list
 * @param readsErr the error bound of {@code readsEst}; 0 for a user who does not read every series
 * @param writesEst rows written in the last 60 to 120 seconds
 * @param writesErr the error bound of {@code writesEst}
 * @param liveRows reserved: 0
 */
public record SeriesStat(int cls, ValueType type, int flags, long rows, long nulls, long memtableRows,
        long logicalBytes, long tsBytes, long valueBytes, long indexBytes, long tombBytes, long parts, long partitions,
        long granules, long fullGranules, long firstTs, long lastTs, long lastWriteMs, long residentBytes,
        long readsEst, long readsErr, long writesEst, long writesErr, long liveRows) {
    /** {@code flags}: no stored row at or after the retention cutoff; {@code firstTs} and {@code lastTs} are 0. */
    public static final int NO_RANGE = 2;
    /** {@code flags}: {@code lastWriteMs} comes from the file time of a part written before format 9. */
    public static final int WRITE_FROM_MTIME = 4;
    /** {@code flags}: {@code readsErr} and {@code writesErr} are 0 because the user does not read every series. */
    public static final int NO_BOUND = 8;
    /** Bytes of one record on the wire ({@code tsdb_series_stat}). */
    public static final int WIRE_LEN = 176;

    /**
     * Decodes the record at {@code p} of a little-endian buffer ({@code tsdb_series_stat}, 176 bytes).
     *
     * @param b the buffer
     * @param p the offset of the record
     * @return the record, or null when its type is out of range
     */
    public static SeriesStat of(ByteBuffer b, int p) {
        ValueType t = ValueType.of(b.get(p + 5) & 0xff);
        if (t == null)
            return null;
        return new SeriesStat(b.get(p + 4) & 0xff, t, b.getShort(p + 6) & 0xffff, b.getLong(p + 8), b.getLong(p + 16),
                b.getLong(p + 24), b.getLong(p + 32), b.getLong(p + 40), b.getLong(p + 48), b.getLong(p + 56),
                b.getLong(p + 64), b.getLong(p + 72), b.getLong(p + 80), b.getLong(p + 88), b.getLong(p + 96),
                b.getLong(p + 104), b.getLong(p + 112), b.getLong(p + 120), b.getLong(p + 128), b.getLong(p + 136),
                b.getLong(p + 144), b.getLong(p + 152), b.getLong(p + 160), b.getLong(p + 168));
    }
}
