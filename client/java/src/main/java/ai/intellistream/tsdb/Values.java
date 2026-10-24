package ai.intellistream.tsdb;

import java.nio.ByteBuffer;

/**
 * The value column of the wire protocol. Values are float32 today; every place that sizes, writes or reads values
 * goes through this class, so a new value type (float64, int64, bool, dictionary-encoded states, nulls) starts here
 * and in the array types of the public methods that take or return values.
 *
 * <p>Columns move with bulk copies through views of little-endian buffers, which are plain memory copies on
 * little-endian hosts.
 */
final class Values {
    /** Bytes per value on the wire. */
    static final int BYTES = Float.BYTES;

    private Values() {
    }

    /** Writes {@code n} values at the buffer's position and advances it; the caller checks the space. */
    static void put(ByteBuffer b, float[] a, int off, int n) {
        b.asFloatBuffer().put(a, off, n);
        b.position(b.position() + n * BYTES);
    }

    /** Reads {@code n} values at the buffer's position and advances it. */
    static void get(ByteBuffer b, float[] a, int off, int n) {
        b.asFloatBuffer().get(a, off, n);
        b.position(b.position() + n * BYTES);
    }

    /** Reads one value (the min and max of an aggregate bucket). */
    static float getOne(ByteBuffer b) {
        return b.getFloat();
    }

    /** A value column of {@code n} entries. */
    static float[] alloc(int n) {
        return new float[n];
    }
}
