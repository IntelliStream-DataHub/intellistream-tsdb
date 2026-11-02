package ai.intellistream.tsdb;

/**
 * The value type of a series ({@code tsdb_type}), chosen when the series is created and fixed after.
 *
 * <p>In a {@link Column} each type has its own primitive array: {@code float[]}, {@code double[]}, {@code long[]},
 * {@code boolean[]} and {@code int[]} (state codes).
 */
public enum ValueType {
    /** 32-bit floats, the type of series created by {@link TsdbClient#series(String)}. */
    FLOAT32(0, 4),
    /** 64-bit floats. */
    FLOAT64(1, 8),
    /** 64-bit signed integers; sums, minima and maxima are exact. {@link Long#MIN_VALUE} is reserved for null. */
    INT64(2, 8),
    /** Booleans; aggregates count {@code true} as 1 and {@code false} as 0. */
    BOOL(3, 1),
    /**
     * Codes of a state dictionary ({@link TsdbClient#state(long, String)}), one per name, shared by the state series
     * of a volume. Code 0 is reserved for null. Aggregates only count.
     */
    STATE(4, 4);

    final int code;  /* on the wire */
    final int width; /* bytes per value on the wire */

    ValueType(int code, int width) {
        this.code = code;
        this.width = width;
    }

    /**
     * The type's code: on the wire, and the C library's {@code tsdb_type}.
     *
     * @return the code
     */
    public int code() {
        return code;
    }

    /** The type with this wire code, or {@code null}. */
    static ValueType of(int code) {
        ValueType[] all = values();
        return code >= 0 && code < all.length ? all[code] : null;
    }
}
