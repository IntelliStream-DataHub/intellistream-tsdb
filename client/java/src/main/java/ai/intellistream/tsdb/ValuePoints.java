// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

/**
 * The points of a typed raw query ({@link TsdbClient#queryValues}): timestamps and a {@link Column} of the same
 * length. The arrays belong to the caller.
 */
public final class ValuePoints {
    private final long[] timestamps;
    private final Column values;

    /**
     * Points over the given timestamps and column, which are not copied (for other implementations of the API, such
     * as the embedded database).
     *
     * @param timestamps ms since the epoch
     * @param values the values, in the same positions; of the same size as {@code timestamps}
     * @throws IllegalArgumentException if the sizes differ
     */
    public ValuePoints(long[] timestamps, Column values) {
        if (timestamps.length != values.size())
            throw new IllegalArgumentException(timestamps.length + " timestamps and " + values.size() + " values");
        this.timestamps = timestamps;
        this.values = values;
    }

    /**
     * The number of points.
     *
     * @return the length of the timestamps and of the column
     */
    public int size() {
        return timestamps.length;
    }

    /**
     * Timestamps in ms since the epoch, in the requested order.
     *
     * @return the timestamp column
     */
    public long[] timestamps() {
        return timestamps;
    }

    /**
     * The values, with their type and nulls, in the same positions as {@link #timestamps()}.
     *
     * @return the value column
     */
    public Column values() {
        return values;
    }

    /**
     * The value type of the series.
     *
     * @return the column's type
     */
    public ValueType type() {
        return values.type();
    }

    @Override
    public String toString() {
        return "ValuePoints[size=" + size() + ", " + values + "]";
    }
}
