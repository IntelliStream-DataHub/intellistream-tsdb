// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

/**
 * The newest row of one series ({@link TsdbClient#latest}): its timestamp and value, a null, or none.
 *
 * <p>The value is kept as the eight bytes the wire carries; read it with the accessor of the series' {@link #type()}.
 */
public final class Latest {
    /** What an answer holds. */
    public enum Kind {
        /** The newest non-null row. */
        VALUE,
        /** The newest row is a null (only with {@link LatestOptions#withNulls}). */
        NULL,
        /** The series has no such row. */
        NONE
    }

    private static final Latest NONE = new Latest(Kind.NONE, null, 0, 0);

    private final Kind kind;
    private final ValueType type;
    private final long ts;
    private final long bits;

    private Latest(Kind kind, ValueType type, long ts, long bits) {
        this.kind = kind;
        this.type = type;
        this.ts = ts;
        this.bits = bits;
    }

    /**
     * The answer of a series with no row.
     *
     * @return the answer
     */
    public static Latest none() {
        return NONE;
    }

    /**
     * An answer as the wire and the C library carry it (for the bindings: {@link TsdbClient} and the embedded
     * database make these).
     *
     * @param kind 0 value, 1 null, 2 none
     * @param type the series' {@code tsdb_type}; ignored with none
     * @param ts the row's timestamp
     * @param bits the value slot: float32 bits in the low four bytes, float64 or int64 bits, bool 0 or 1, state code
     * @return the answer, or null when {@code kind} or {@code type} is out of range
     */
    public static Latest of(int kind, int type, long ts, long bits) {
        switch (kind) {
        case 0:
        case 1: {
            ValueType t = ValueType.of(type);
            return t == null ? null : new Latest(kind == 0 ? Kind.VALUE : Kind.NULL, t, ts, kind == 0 ? bits : 0);
        }
        case 2:
            return NONE;
        default:
            return null;
        }
    }

    /**
     * Value, null or none.
     *
     * @return the kind
     */
    public Kind kind() {
        return kind;
    }

    /**
     * The series' type.
     *
     * @return the type, or null for {@link Kind#NONE}
     */
    public ValueType type() {
        return type;
    }

    /**
     * The row's timestamp.
     *
     * @return milliseconds since the epoch; 0 for {@link Kind#NONE}
     */
    public long ts() {
        return ts;
    }

    /**
     * The value slot as stored: float32 bits in the low four bytes, float64 or int64 bits, bool 0 or 1, state code.
     *
     * @return the bits; 0 unless {@link Kind#VALUE}
     */
    public long bits() {
        return bits;
    }

    /**
     * The value of a float32 or float64 series as a double (an int64 series' value converted).
     *
     * @return the value
     * @throws IllegalStateException unless the kind is {@link Kind#VALUE} and the type numeric
     */
    public double asDouble() {
        check();
        switch (type) {
        case FLOAT32: return Float.intBitsToFloat((int) bits);
        case FLOAT64: return Double.longBitsToDouble(bits);
        case INT64: return (double) bits;
        default: throw new IllegalStateException("not a numeric series: " + type);
        }
    }

    /**
     * The value of an int64 series.
     *
     * @return the value
     * @throws IllegalStateException unless the kind is {@link Kind#VALUE} and the type {@link ValueType#INT64}
     */
    public long asLong() {
        check();
        if (type != ValueType.INT64)
            throw new IllegalStateException("not an int64 series: " + type);
        return bits;
    }

    /**
     * The value of a bool series.
     *
     * @return the value
     * @throws IllegalStateException unless the kind is {@link Kind#VALUE} and the type {@link ValueType#BOOL}
     */
    public boolean asBoolean() {
        check();
        if (type != ValueType.BOOL)
            throw new IllegalStateException("not a bool series: " + type);
        return bits != 0;
    }

    /**
     * The code of a state series' value (see {@link TsdbClient#stateName}).
     *
     * @return the code
     * @throws IllegalStateException unless the kind is {@link Kind#VALUE} and the type {@link ValueType#STATE}
     */
    public int asState() {
        check();
        if (type != ValueType.STATE)
            throw new IllegalStateException("not a state series: " + type);
        return (int) bits;
    }

    private void check() {
        if (kind != Kind.VALUE)
            throw new IllegalStateException("no value: " + kind);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Latest l && l.kind == kind && l.type == type && l.ts == ts && l.bits == bits;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(kind, type, ts, bits);
    }

    @Override
    public String toString() {
        return kind == Kind.NONE ? "Latest[none]" : "Latest[" + kind + " " + type + " ts=" + ts + " bits=" + bits + "]";
    }
}
