package ai.intellistream.tsdb;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Objects;

/**
 * A column of values of one {@link ValueType}, held in a primitive array, with optional nulls.
 *
 * <p>Create one with {@link #ofFloats}, {@link #ofDoubles}, {@link #ofLongs}, {@link #ofBools} or
 * {@link #ofStates}, and mark unknown values with {@link #withNulls(BitSet)}: a set bit {@code i} makes value
 * {@code i} null, whatever the array holds there. Read a column with the accessor of its type, such as
 * {@link #longs()}, together with {@link #isNull(int)}.
 *
 * <p>The arrays are not copied: a column made from an array sees later changes to it, and the arrays of a column
 * returned by a query belong to the caller. Only the first {@link #size()} entries count; the arrays of the reused
 * column a {@link ValuesConsumer} receives can be longer.
 *
 * <p>Some values are reserved by the database as the representation of null and read back as null:
 * {@link Long#MIN_VALUE} in an int64 column, state code 0, and one NaN payload per float type
 * ({@code TSDB_NULL_F32_BITS}, {@code TSDB_NULL_F64_BITS} in {@code tsdb.h}). Other NaNs are values.
 */
public final class Column {
    private final ValueType type;
    int size;
    float[] floats;
    double[] doubles;
    long[] longs;
    boolean[] bools;
    int[] states;
    BitSet nulls; /* set bit: null; null when every value is known */

    private Column(ValueType type, int size) {
        this.type = type;
        this.size = size;
    }

    /**
     * A float32 column.
     *
     * @param values the values
     * @return a column over {@code values}
     */
    public static Column ofFloats(float... values) {
        Column c = new Column(ValueType.FLOAT32, values.length);
        c.floats = values;
        return c;
    }

    /**
     * A float64 column.
     *
     * @param values the values
     * @return a column over {@code values}
     */
    public static Column ofDoubles(double... values) {
        Column c = new Column(ValueType.FLOAT64, values.length);
        c.doubles = values;
        return c;
    }

    /**
     * An int64 column.
     *
     * @param values the values; {@link Long#MIN_VALUE} reads back as null
     * @return a column over {@code values}
     */
    public static Column ofLongs(long... values) {
        Column c = new Column(ValueType.INT64, values.length);
        c.longs = values;
        return c;
    }

    /**
     * A bool column.
     *
     * @param values the values
     * @return a column over {@code values}
     */
    public static Column ofBools(boolean... values) {
        Column c = new Column(ValueType.BOOL, values.length);
        c.bools = values;
        return c;
    }

    /**
     * A state column.
     *
     * @param codes state codes from {@link TsdbClient#state(long, String)}, unsigned; 0 reads back as null
     * @return a column over {@code codes}
     */
    public static Column ofStates(int... codes) {
        Column c = new Column(ValueType.STATE, codes.length);
        c.states = codes;
        return c;
    }

    /**
     * The same values with nulls.
     *
     * @param nulls bit {@code i} set when value {@code i} is unknown; bits from {@link #size()} on are ignored.
     *     The set is copied.
     * @return a new column sharing this one's array
     */
    public Column withNulls(BitSet nulls) {
        Column c = new Column(type, size);
        c.floats = floats;
        c.doubles = doubles;
        c.longs = longs;
        c.bools = bools;
        c.states = states;
        BitSet n = (BitSet) Objects.requireNonNull(nulls, "nulls").clone();
        if (n.length() > size)
            n.clear(size, n.length());
        c.nulls = n.isEmpty() ? null : n;
        return c;
    }

    /**
     * The value type.
     *
     * @return the type, which decides the accessor that returns the values
     */
    public ValueType type() {
        return type;
    }

    /**
     * The number of values.
     *
     * @return how many entries of the array count
     */
    public int size() {
        return size;
    }

    /**
     * Whether a value is null.
     *
     * @param i the index, below {@link #size()}
     * @return {@code true} if value {@code i} is unknown
     */
    public boolean isNull(int i) {
        Objects.checkIndex(i, size);
        return nulls != null && nulls.get(i);
    }

    /**
     * Whether any value is null.
     *
     * @return {@code true} if at least one value is unknown
     */
    public boolean hasNulls() {
        return nulls != null;
    }

    /**
     * The positions of the nulls.
     *
     * @return a copy: bit {@code i} set when value {@code i} is unknown
     */
    public BitSet nulls() {
        return nulls != null ? (BitSet) nulls.clone() : new BitSet();
    }

    /**
     * The values of a float32 column.
     *
     * @return the array, not copied
     * @throws IllegalStateException for another type
     */
    public float[] floats() {
        expect(ValueType.FLOAT32);
        return floats;
    }

    /**
     * The values of a float64 column.
     *
     * @return the array, not copied
     * @throws IllegalStateException for another type
     */
    public double[] doubles() {
        expect(ValueType.FLOAT64);
        return doubles;
    }

    /**
     * The values of an int64 column.
     *
     * @return the array, not copied
     * @throws IllegalStateException for another type
     */
    public long[] longs() {
        expect(ValueType.INT64);
        return longs;
    }

    /**
     * The values of a bool column.
     *
     * @return the array, not copied
     * @throws IllegalStateException for another type
     */
    public boolean[] bools() {
        expect(ValueType.BOOL);
        return bools;
    }

    /**
     * The codes of a state column; {@link TsdbClient#stateName(long, int)} turns them into names.
     *
     * @return the array, not copied
     * @throws IllegalStateException for another type
     */
    public int[] states() {
        expect(ValueType.STATE);
        return states;
    }

    /**
     * One value as a double, whatever the type: bools are 0 or 1, state codes their unsigned value. int64 values
     * beyond 2^53 lose precision; use {@link #longs()} for those.
     *
     * @param i the index, below {@link #size()}
     * @return the value, or NaN if it is null
     */
    public double getDouble(int i) {
        if (isNull(i))
            return Double.NaN;
        switch (type) {
        case FLOAT32: return floats[i];
        case FLOAT64: return doubles[i];
        case INT64: return longs[i];
        case BOOL: return bools[i] ? 1 : 0;
        default: return Integer.toUnsignedLong(states[i]);
        }
    }

    @Override
    public String toString() {
        return "Column[" + type + ", size=" + size + (nulls != null ? ", nulls=" + nulls.cardinality() : "") + "]";
    }

    private void expect(ValueType t) {
        if (type != t)
            throw new IllegalStateException("a " + type + " column, not " + t);
    }

    // ------------------------------------------------- for the client: building columns from replies

    /** An empty column with room for {@code cap} values. */
    static Column allocate(ValueType type, int cap) {
        Column c = new Column(type, 0);
        c.grow(cap);
        return c;
    }

    int capacity() {
        switch (type) {
        case FLOAT32: return floats.length;
        case FLOAT64: return doubles.length;
        case INT64: return longs.length;
        case BOOL: return bools.length;
        default: return states.length;
        }
    }

    /** Resizes the array to {@code cap} entries, keeping the first {@link #size}. */
    void grow(int cap) {
        switch (type) {
        case FLOAT32: floats = floats == null ? new float[cap] : Arrays.copyOf(floats, cap); break;
        case FLOAT64: doubles = doubles == null ? new double[cap] : Arrays.copyOf(doubles, cap); break;
        case INT64: longs = longs == null ? new long[cap] : Arrays.copyOf(longs, cap); break;
        case BOOL: bools = bools == null ? new boolean[cap] : Arrays.copyOf(bools, cap); break;
        default: states = states == null ? new int[cap] : Arrays.copyOf(states, cap); break;
        }
    }

    /** Marks value {@code i} null. */
    void setNull(int i) {
        if (nulls == null)
            nulls = new BitSet();
        nulls.set(i);
    }
}
