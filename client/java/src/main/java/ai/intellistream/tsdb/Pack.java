// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

/**
 * The packed body of an INSERT_VALUES frame (flags &amp; TSDB_PACKED, protocol version 3), byte for byte as
 * {@code client/tsdb_pack.c}:
 *
 * <pre>
 *   timestamps   zigzag deltas from the previous row (the first from 0), LEB128 varints
 *   values       byte planes: byte 0 of every value, then byte 1, ... (bool: one plane)
 *   valid        the validity bitmap, unchanged (with TSDB_VALID)
 *   series       zigzag varints of the wrapping 64-bit difference from the previous id (the first from 0), with
 *                TSDB_MANY_SERIES: ids are u64 (protocol 11), a delta takes up to 10 bytes
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
        return (long) n * (10 + type.width) + (valid ? (n + 7) / 8 : 0) + (many ? (long) n * 10 : 0);
    }

    /**
     * Reshapes rows {@code [off, off + n)} into {@code out} (at least {@link #rawBound} bytes) and returns the
     * length written.
     *
     * @param valid the frame's validity bitmap (bit i for row off + i), or null
     * @param series the series id of every row (indexed like ts; any u64, the differences wrap), or null
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
                o = putVarint(out, o, zigzag(s - ps)); /* wraps like the C code's unsigned subtraction */
                ps = s;
            }
        }
        return o;
    }

    /** Tile width for L1D-resident byte-plane transpose: 1024 values * 8 planes = 8 KiB. */
    private static final int TILE = 1024;

    /** Byte planes of the little-endian wire values: plane b holds byte b of every value. */
    private static int planes(Column c, int off, int n, byte[] out, int o) {
        if (n == 0)
            return o;
        int tileLen = Math.min(n, TILE);
        switch (c.type()) {
        case FLOAT32: {
            float[] v = c.floats;
            byte[] p0 = new byte[tileLen], p1 = new byte[tileLen], p2 = new byte[tileLen], p3 = new byte[tileLen];
            for (int i0 = 0; i0 < n; i0 += TILE) {
                int m = Math.min(TILE, n - i0);
                int base = off + i0;
                for (int j = 0; j < m; j++) {
                    int x = Float.floatToRawIntBits(v[base + j]); /* raw: the null NaN payload must survive */
                    p0[j] = (byte) x;
                    p1[j] = (byte) (x >>> 8);
                    p2[j] = (byte) (x >>> 16);
                    p3[j] = (byte) (x >>> 24);
                }
                System.arraycopy(p0, 0, out, o + i0, m);
                System.arraycopy(p1, 0, out, o + n + i0, m);
                System.arraycopy(p2, 0, out, o + 2 * n + i0, m);
                System.arraycopy(p3, 0, out, o + 3 * n + i0, m);
            }
            return o + 4 * n;
        }
        case STATE: {
            int[] v = c.states;
            byte[] p0 = new byte[tileLen], p1 = new byte[tileLen], p2 = new byte[tileLen], p3 = new byte[tileLen];
            for (int i0 = 0; i0 < n; i0 += TILE) {
                int m = Math.min(TILE, n - i0);
                int base = off + i0;
                for (int j = 0; j < m; j++) {
                    int x = v[base + j];
                    p0[j] = (byte) x;
                    p1[j] = (byte) (x >>> 8);
                    p2[j] = (byte) (x >>> 16);
                    p3[j] = (byte) (x >>> 24);
                }
                System.arraycopy(p0, 0, out, o + i0, m);
                System.arraycopy(p1, 0, out, o + n + i0, m);
                System.arraycopy(p2, 0, out, o + 2 * n + i0, m);
                System.arraycopy(p3, 0, out, o + 3 * n + i0, m);
            }
            return o + 4 * n;
        }
        case FLOAT64: {
            double[] v = c.doubles;
            byte[] p0 = new byte[tileLen], p1 = new byte[tileLen], p2 = new byte[tileLen], p3 = new byte[tileLen];
            byte[] p4 = new byte[tileLen], p5 = new byte[tileLen], p6 = new byte[tileLen], p7 = new byte[tileLen];
            for (int i0 = 0; i0 < n; i0 += TILE) {
                int m = Math.min(TILE, n - i0);
                int base = off + i0;
                for (int j = 0; j < m; j++) {
                    long x = Double.doubleToRawLongBits(v[base + j]); /* raw: the null NaN payload must survive */
                    p0[j] = (byte) x;
                    p1[j] = (byte) (x >>> 8);
                    p2[j] = (byte) (x >>> 16);
                    p3[j] = (byte) (x >>> 24);
                    p4[j] = (byte) (x >>> 32);
                    p5[j] = (byte) (x >>> 40);
                    p6[j] = (byte) (x >>> 48);
                    p7[j] = (byte) (x >>> 56);
                }
                System.arraycopy(p0, 0, out, o + i0, m);
                System.arraycopy(p1, 0, out, o + n + i0, m);
                System.arraycopy(p2, 0, out, o + 2 * n + i0, m);
                System.arraycopy(p3, 0, out, o + 3 * n + i0, m);
                System.arraycopy(p4, 0, out, o + 4 * n + i0, m);
                System.arraycopy(p5, 0, out, o + 5 * n + i0, m);
                System.arraycopy(p6, 0, out, o + 6 * n + i0, m);
                System.arraycopy(p7, 0, out, o + 7 * n + i0, m);
            }
            return o + 8 * n;
        }
        case INT64: {
            long[] v = c.longs;
            byte[] p0 = new byte[tileLen], p1 = new byte[tileLen], p2 = new byte[tileLen], p3 = new byte[tileLen];
            byte[] p4 = new byte[tileLen], p5 = new byte[tileLen], p6 = new byte[tileLen], p7 = new byte[tileLen];
            for (int i0 = 0; i0 < n; i0 += TILE) {
                int m = Math.min(TILE, n - i0);
                int base = off + i0;
                for (int j = 0; j < m; j++) {
                    long x = v[base + j];
                    p0[j] = (byte) x;
                    p1[j] = (byte) (x >>> 8);
                    p2[j] = (byte) (x >>> 16);
                    p3[j] = (byte) (x >>> 24);
                    p4[j] = (byte) (x >>> 32);
                    p5[j] = (byte) (x >>> 40);
                    p6[j] = (byte) (x >>> 48);
                    p7[j] = (byte) (x >>> 56);
                }
                System.arraycopy(p0, 0, out, o + i0, m);
                System.arraycopy(p1, 0, out, o + n + i0, m);
                System.arraycopy(p2, 0, out, o + 2 * n + i0, m);
                System.arraycopy(p3, 0, out, o + 3 * n + i0, m);
                System.arraycopy(p4, 0, out, o + 4 * n + i0, m);
                System.arraycopy(p5, 0, out, o + 5 * n + i0, m);
                System.arraycopy(p6, 0, out, o + 6 * n + i0, m);
                System.arraycopy(p7, 0, out, o + 7 * n + i0, m);
            }
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
