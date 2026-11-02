package ai.intellistream.tsdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.BitSet;

import org.junit.jupiter.api.Test;

/** The packed body, byte for byte against client/tsdb_pack.c (the expected bytes come from tsdb_pack_reshape). */
class PackTest {
    static final long[] TS = {1000, 2000, 1999, -5, Long.MAX_VALUE};

    static byte[] reshape(long[] ts, Column col, long[] series, int off, int n) {
        byte[] valid = Values.validity(col, off, n);
        byte[] out = new byte[(int) Pack.rawBound(n, col.type(), valid != null, series != null)];
        return Arrays.copyOf(out, Pack.reshape(ts, col, series, off, n, valid, out));
    }

    static byte[] bytes(int... b) {
        byte[] r = new byte[b.length];
        for (int i = 0; i < b.length; i++)
            r[i] = (byte) b[i];
        return r;
    }

    @Test
    void float32ManySeriesWithNulls() {
        BitSet nulls = new BitSet();
        nulls.set(1);
        Column col = Column.ofFloats(1.5f, Float.intBitsToFloat(0x7fc01001), -2.0f, 0.0f, 3.25f).withNulls(nulls);
        long[] series = {3, 3, 1, 0x80000000L, 0};
        assertArrayEquals(bytes(-48, 15, -48, 15, 1, -89, 31, -9, -1, -1, -1, -1, -1, -1, -1, -1, 1, 0, 1, 0, 0, 0,
                0, 16, 0, 0, 0, -64, -64, 0, 0, 80, 63, 127, -64, 0, 64, 29, 6, 0, 3, -2, -1, -1, -1, 15, -1, -1, -1,
                -1, 15), reshape(TS, col, series, 0, 5));
    }

    @Test
    void otherTypes() {
        assertArrayEquals(bytes(-48, 15, -48, 15, 1, -1, 1, 2, -1, 0, 1, -1, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0, -1,
                0, 0, -1, -128, 0), reshape(TS, Column.ofLongs(-1, Long.MIN_VALUE + 1, 258), null, 0, 3));
        assertArrayEquals(bytes(-48, 15, -48, 15, 1, 1, 0, 1),
                reshape(TS, Column.ofBools(true, false, true), null, 0, 3));
        assertArrayEquals(bytes(-48, 15, -48, 15, -102, 0, -103, 0, -103, 0, -103, 0, -103, 0, -103, 0, -71, 0, 63,
                -128), reshape(TS, Column.ofDoubles(0.1, -0.0), null, 0, 2));
        assertArrayEquals(bytes(-48, 15, -48, 15, 7, 0, 0, 0, 0, 1, 0, 0),
                reshape(TS, Column.ofStates(7, 65536), null, 0, 2));
    }

    @Test
    void sliceStartsFromZero() {
        /* rows 2..3 of a column: the first delta is from 0 again, the bitmap and planes are the slice's */
        BitSet nulls = new BitSet();
        nulls.set(3);
        Column col = Column.ofFloats(9, 9, -2.0f, 0.0f, 9).withNulls(nulls);
        byte[] got = reshape(TS, col, new long[] {9, 9, 1, 0x80000000L, 9}, 2, 2);
        byte[] want = reshape(new long[] {1999, -5}, Column.ofFloats(-2.0f, 0.0f).withNulls(BitSet.valueOf(
                new long[] {2})), new long[] {1, 0x80000000L}, 0, 2);
        assertArrayEquals(want, got);
        assertEquals(1999 * 2, Pack.zigzag(1999));
    }

    @Test
    void varints() {
        byte[] b = new byte[10];
        assertEquals(1, Pack.putVarint(b, 0, 127));
        assertEquals(2, Pack.putVarint(b, 0, 128));
        assertArrayEquals(bytes(-128, 1), Arrays.copyOf(b, 2));
        assertEquals(10, Pack.putVarint(b, 0, -1L)); /* the largest: ten bytes */
        assertEquals(-1L, Pack.zigzag(Long.MIN_VALUE));
        assertEquals(1L, Pack.zigzag(-1));
    }

    /** Ids in the packed series column: reshape with ids only and read the varints back (wrapping 64-bit sums). */
    static long[] ids(long[] series) {
        int n = series.length;
        long[] ts = new long[n]; /* all zero: one byte each */
        Column col = Column.ofBools(new boolean[n]); /* one plane: n bytes */
        byte[] b = reshape(ts, col, series, 0, n);
        int o = n + n; /* the timestamp varints, then the bool plane */
        long[] got = new long[n];
        long prev = 0;
        for (int i = 0; i < n; i++) {
            long z = 0;
            int shift = 0;
            for (;;) {
                int x = b[o++] & 0xff;
                z |= (long) (x & 0x7f) << shift;
                if (x < 0x80)
                    break;
                shift += 7;
            }
            prev += (z >>> 1) ^ -(z & 1);
            got[i] = prev;
        }
        assertEquals(b.length, o, "the series column ends the body");
        return got;
    }

    @Test
    void idsAcrossTwoToTheThirtyTwoBothWays() {
        long wide = 0xFFFF_FFFF_0000_0001L;
        long[][] cases = {
            {0, 0x1_0000_0005L, 0, wide, -2L, 0, Long.MIN_VALUE, Long.MAX_VALUE, -2L, 5, 0xFFFF_FFFFL, 0x1_0000_0000L},
            {-2L}, {0xFFFF_FFFFL, 0x1_0000_0000L, 0xFFFF_FFFFL}, {Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE},
        };
        for (long[] c : cases)
            assertArrayEquals(c, ids(c), Arrays.toString(c));
        /* 0 -> UINT64_MAX - 1 is the delta -2: one zigzag byte; and back, +2 */
        byte[] b = reshape(new long[2], Column.ofBools(false, false), new long[] {-2L, 0}, 0, 2);
        assertArrayEquals(bytes(0, 0, 0, 0, 3, 4), b);
        /* a delta of 2^63 needs the full ten bytes: the bound has room for ten a row */
        assertEquals(10, reshape(new long[1], Column.ofBools(false), new long[] {Long.MIN_VALUE}, 0, 1).length - 2);
        assertEquals(1 * (10 + 1) + 10, Pack.rawBound(1, ValueType.BOOL, false, true));
        assertEquals(1 * (10 + 4) + (1 + 7) / 8 + 10, Pack.rawBound(1, ValueType.FLOAT32, true, true));
    }
}
