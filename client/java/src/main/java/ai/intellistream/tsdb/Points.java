package ai.intellistream.tsdb;

/**
 * The points of a raw query, as two columns of the same length.
 *
 * <p>The arrays are the ones the client filled; they are not copied again, and the caller may keep or modify them.
 */
public final class Points {
    private final long[] timestamps;
    private final float[] values;

    /**
     * Points over the given arrays, which are not copied (for other implementations of the API, such as the
     * embedded database).
     *
     * @param timestamps ms since the epoch
     * @param values the values, in the same positions; as long as {@code timestamps}
     * @throws IllegalArgumentException if the lengths differ
     */
    public Points(long[] timestamps, float[] values) {
        if (timestamps.length != values.length)
            throw new IllegalArgumentException(timestamps.length + " timestamps and " + values.length + " values");
        this.timestamps = timestamps;
        this.values = values;
    }

    /**
     * The number of points.
     *
     * @return the length of both arrays
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
     * The values, in the same positions as {@link #timestamps()}.
     *
     * @return the value column
     */
    public float[] values() {
        return values;
    }

    /**
     * One timestamp.
     *
     * @param i the index, below {@link #size()}
     * @return the timestamp of point {@code i}
     */
    public long timestamp(int i) {
        return timestamps[i];
    }

    /**
     * One value.
     *
     * @param i the index, below {@link #size()}
     * @return the value of point {@code i}
     */
    public float value(int i) {
        return values[i];
    }

    @Override
    public String toString() {
        return "Points[size=" + size() + "]";
    }
}
