// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * The value columns of the wire protocol: every place that sizes, writes or reads values goes through here and
 * {@link ValueType}, so a new type starts in those two classes and in {@link Column}.
 *
 * <p>Columns move with bulk copies through views of little-endian buffers, which are plain memory copies on
 * little-endian hosts; only bools are converted one by one (to and from bytes 0 and 1).
 */
final class Values {
    /** Bytes per value of the version 1 operations (INSERT, QUERY_RAW): float32. */
    static final int BYTES = ValueType.FLOAT32.width;

    private Values() {
    }

    /** Writes {@code n} float32 values at the buffer's position and advances it; the caller checks the space. */
    static void put(ByteBuffer b, float[] a, int off, int n) {
        b.asFloatBuffer().put(a, off, n);
        b.position(b.position() + n * BYTES);
    }

    /** Reads {@code n} float32 values at the buffer's position and advances it. */
    static void get(ByteBuffer b, float[] a, int off, int n) {
        b.asFloatBuffer().get(a, off, n);
        b.position(b.position() + n * BYTES);
    }

    /** A float32 column of {@code n} entries. */
    static float[] alloc(int n) {
        return new float[n];
    }

    /** Writes values {@code [off, off + n)} of {@code c} at the buffer's position; the caller checks the space. */
    static void put(ByteBuffer b, Column c, int off, int n) {
        int p = b.position();
        switch (c.type()) {
        case FLOAT32: b.asFloatBuffer().put(c.floats, off, n); break;
        case FLOAT64: b.asDoubleBuffer().put(c.doubles, off, n); break;
        case INT64: b.asLongBuffer().put(c.longs, off, n); break;
        case BOOL:
            for (int i = 0; i < n; i++)
                b.put(p + i, (byte) (c.bools[off + i] ? 1 : 0));
            break;
        default: b.asIntBuffer().put(c.states, off, n); break;
        }
        b.position(p + n * c.type().width);
    }

    /** Reads {@code n} values at the buffer's position into {@code c} from index {@code at}, and advances. */
    static void get(ByteBuffer b, Column c, int at, int n) {
        int p = b.position();
        switch (c.type()) {
        case FLOAT32: b.asFloatBuffer().get(c.floats, at, n); break;
        case FLOAT64: b.asDoubleBuffer().get(c.doubles, at, n); break;
        case INT64: b.asLongBuffer().get(c.longs, at, n); break;
        case BOOL:
            for (int i = 0; i < n; i++)
                c.bools[at + i] = b.get(p + i) != 0;
            break;
        default: b.asIntBuffer().get(c.states, at, n); break;
        }
        b.position(p + n * c.type().width);
    }

    /**
     * The validity bitmap of rows {@code [off, off + n)} of {@code c} (bit i, LSB first, set when value i is
     * known), or {@code null} when none of them is null.
     */
    static byte[] validity(Column c, int off, int n) {
        if (c.nulls == null)
            return null;
        int i = c.nulls.nextSetBit(off);
        if (i < 0 || i >= off + n)
            return null;
        byte[] v = new byte[(n + 7) / 8];
        Arrays.fill(v, (byte) 0xff);
        if ((n & 7) != 0)
            v[v.length - 1] = (byte) ((1 << (n & 7)) - 1);
        for (; i >= 0 && i < off + n; i = c.nulls.nextSetBit(i + 1)) {
            int j = i - off;
            v[j >>> 3] &= (byte) ~(1 << (j & 7));
        }
        return v;
    }

    /** Reads the validity bitmap of {@code n} rows at the buffer's position into the nulls of {@code c} from
     * index {@code at}, and advances. */
    static void getValidity(ByteBuffer b, Column c, int at, int n) {
        for (int j = 0, bytes = (n + 7) / 8; j < bytes; j++) {
            int x = b.get() & 0xff;
            if (x == 0xff)
                continue;
            for (int k = 0; k < 8 && j * 8 + k < n; k++)
                if ((x & (1 << k)) == 0)
                    c.setNull(at + j * 8 + k);
        }
    }
}
