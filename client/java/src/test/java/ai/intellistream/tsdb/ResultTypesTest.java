package ai.intellistream.tsdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/** The public constructors other implementations (the embedded database) build results with, and the enum codes. */
class ResultTypesTest {
    @Test
    void pointsRefuseArraysOfDifferentLengths() {
        assertEquals(2, new Points(new long[] {1, 2}, new float[] {1, 2}).size());
        assertThrows(IllegalArgumentException.class, () -> new Points(new long[] {1, 2}, new float[] {1}));
        assertThrows(NullPointerException.class, () -> new Points(null, new float[0]));
        assertThrows(NullPointerException.class, () -> new Points(new long[0], null));
    }

    @Test
    void valuePointsRefuseAColumnOfAnotherSize() {
        assertEquals(2, new ValuePoints(new long[] {1, 2}, Column.ofDoubles(1, 2)).size());
        assertThrows(IllegalArgumentException.class, () -> new ValuePoints(new long[] {1, 2}, Column.ofLongs(7)));
        assertThrows(NullPointerException.class, () -> new ValuePoints(new long[0], null));
    }

    @Test
    void codesAreTheDeclaredOnesNotTheOrdinals() {
        /* include/tsdb.h tsdb_type and tsdb_unit; the embedded binding passes these to C */
        assertEquals(0, ValueType.FLOAT32.code());
        assertEquals(1, ValueType.FLOAT64.code());
        assertEquals(2, ValueType.INT64.code());
        assertEquals(3, ValueType.BOOL.code());
        assertEquals(4, ValueType.STATE.code());
        assertEquals(1, Unit.SECOND.code());
        assertEquals(8, Unit.YEAR.code());
    }
}
