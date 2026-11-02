package ai.intellistream.tsdb;

/**
 * The packed body of an INSERT_VALUES frame (flags &amp; TSDB_PACKED, protocol version 3), byte for byte as
 * {@code client/tsdb_pack.c}:
 *
 * <pre>
 *   timestamps   zigzag deltas from the previous row (the first from 0), LEB128 varints
 *   values       byte planes: byte 0 of every value, then byte 1, ... (bool: one plane)
 *   valid        the validity bitmap, unchanged (with TSDB_VALID)
 *   series       zigzag deltas, LEB128 varints (with TSDB_MANY_SERIES)
 * </pre>
 *
 * The client compresses the result as one zstd frame. Regular clocks become one-byte deltas and the high bytes
 * of sensor values form long runs, which is what makes the frame several times smaller than the plain arrays.
 */
final class Pack {
    private Pack() {
    }

    /** The largest reshaped body of n rows, as {@code tsdb_pack_raw_bound}. */
    static long rawBound(int n, ValueType type, boolean valid, boolean many) {
        return (long) n * (10 + type.width) + (valid ? (n + 7) / 8 : 0) + (many ? (long) n * 5 : 0);
    }

    /**
     * Reshapes rows {@code [off, off + n)} into {@code out} (at least {@link #rawBound} bytes) and returns the
     * length written.
     *
     * @param valid the frame's validity bitmap (bit i for row off + i), or null
     * @param series the series id of every row (indexed like ts, each below 2^32: protocol 10), or null
     */
    static int reshape(long[] ts, Column col, long[] series, int off, int n, byte[] valid, byte[] out) {
        int o = 0;
        long prev = 0;
        for (int i = off, end = off + n; i < end; i++) {
            long t = ts[i];
            o = putVarint(out, o, zigzag(t - prev)); /* wraps like the C code's unsigned subtraction */
            prev = t;
        }
        o = planes(col, off, n, out, o);
        if (valid != null) {
            System.arraycopy(valid, 0, out, o, valid.length);
            o += valid.length;
        }
        if (series != null) {
            long ps = 0;
            for (int i = off, end = off + n; i < end; i++) {
                long s = series[i];
                o = putVarint(out, o, zigzag(s - ps));
                ps = s;
            }
        }
        return o;
    }

    /** Byte planes of the little-endian wire values: plane b holds byte b of every value. */
    private static int planes(Column c, int off, int n, byte[] out, int o) {
        switch (c.type()) {
        case FLOAT32: {
            float[] v = c.floats;
            for (int i = 0; i < n; i++) {
                int x = Float.floatToRawIntBits(v[off + i]); /* raw: the null NaN payload must survive */
                out[o + i] = (byte) x;
                out[o + n + i] = (byte) (x >>> 8);
                out[o + 2 * n + i] = (byte) (x >>> 16);
                out[o + 3 * n + i] = (byte) (x >>> 24);
            }
            return o + 4 * n;
        }
        case STATE: {
            int[] v = c.states;
            for (int i = 0; i < n; i++) {
                int x = v[off + i];
                out[o + i] = (byte) x;
                out[o + n + i] = (byte) (x >>> 8);
                out[o + 2 * n + i] = (byte) (x >>> 16);
                out[o + 3 * n + i] = (byte) (x >>> 24);
            }
            return o + 4 * n;
        }
        case FLOAT64: {
            double[] v = c.doubles;
            for (int i = 0; i < n; i++)
                plane8(out, o, n, i, Double.doubleToRawLongBits(v[off + i]));
            return o + 8 * n;
        }
        case INT64: {
            long[] v = c.longs;
            for (int i = 0; i < n; i++)
                plane8(out, o, n, i, v[off + i]);
            return o + 8 * n;
        }
        default: { /* BOOL: one plane */
            boolean[] v = c.bools;
            for (int i = 0; i < n; i++)
                out[o + i] = (byte) (v[off + i] ? 1 : 0);
            return o + n;
        }
        }
    }

    private static void plane8(byte[] out, int o, int n, int i, long x) {
        for (int b = 0; b < 8; b++, o += n)
            out[o + i] = (byte) (x >>> (8 * b));
    }

    static long zigzag(long x) {
        return (x << 1) ^ (x >> 63);
    }

    static int putVarint(byte[] out, int o, long x) {
        while (Long.compareUnsigned(x, 0x80) >= 0) {
            out[o++] = (byte) (x | 0x80);
            x >>>= 7;
        }
        out[o++] = (byte) x;
        return o;
    }
}
