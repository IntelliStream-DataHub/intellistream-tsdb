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
}
