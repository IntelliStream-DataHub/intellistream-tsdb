// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb.pulsar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.intellistream.tsdb.ValueType;
import org.junit.jupiter.api.Test;

/** The series cache stays within its bounds whatever names the topic carries. */
class SeriesCacheTest {
    static String name(char c, int i, int len) {
        return (c + Integer.toString(i) + "-" + "z".repeat(len)).substring(0, len);
    }

    @Test
    void boundedByCount() {
        SeriesCache c = new SeriesCache(100, Long.MAX_VALUE, 10);
        for (int i = 0; i < 10_000; i++)
            c.put("s" + i, new SeriesCache.Entry(i, ValueType.FLOAT64));
        assertEquals(100, c.size());
        assertNotNull(c.get("s9999"));
        assertNull(c.get("s0"));
    }

    @Test
    void boundedByBytesOfLongNames() {
        long max = 1 << 20;
        SeriesCache c = new SeriesCache(100_000, max, 10);
        for (int i = 0; i < 2_000; i++) { /* 2000 names of 65535 chars: about 256 MB unbounded */
            c.put(name('s', i, 65_535), new SeriesCache.Entry(i, ValueType.FLOAT64));
            assertTrue(c.bytes() <= max, "bytes " + c.bytes());
        }
        assertTrue(c.size() >= 7 && c.size() <= 8, "size " + c.size());
        assertNotNull(c.get(name('s', 1_999, 65_535)));
    }

    @Test
    void statesBoundedPerSeriesAndInBytes() {
        long max = 1 << 20;
        SeriesCache c = new SeriesCache(100_000, max, 50);
        SeriesCache.Entry mode = new SeriesCache.Entry(1, ValueType.STATE);
        c.put("mode", mode);
        for (int i = 0; i < 10_000; i++) { /* every record a new short state name */
            c.putState(mode, "st" + i, i);
            assertTrue(mode.states.size() <= 50);
        }
        assertEquals(9_999, c.state(mode, "st9999"));
        assertNull(c.state(mode, "st0"));
        for (int i = 0; i < 1_000; i++) { /* new long state names */
            c.putState(mode, name('x', i, 60_000), i);
            assertTrue(c.bytes() <= max, "bytes " + c.bytes());
        }
        assertNotNull(c.get("mode"), "the series in use stays; its old states go");
        assertEquals(999, c.state(mode, name('x', 999, 60_000)));
        long sum = 0;
        for (String s : mode.states.keySet())
            sum += SeriesCache.weight(s);
        assertEquals(c.bytes(), SeriesCache.weight("mode") + sum);
    }

    @Test
    void putAgainReplacesTheOldWeight() {
        SeriesCache c = new SeriesCache(100, Long.MAX_VALUE, 10);
        SeriesCache.Entry a = new SeriesCache.Entry(1, ValueType.STATE);
        c.put("mode", a);
        c.putState(a, "on", 1);
        c.put("mode", new SeriesCache.Entry(1, ValueType.STATE));
        assertEquals(SeriesCache.weight("mode"), c.bytes());
        assertEquals(1, c.size());
    }

    @Test
    void anEvictedEntryInUseGrowsOutsideTheCount() {
        SeriesCache c = new SeriesCache(1, Long.MAX_VALUE, 10);
        SeriesCache.Entry a = new SeriesCache.Entry(1, ValueType.STATE);
        c.put("a", a);
        c.put("b", new SeriesCache.Entry(2, ValueType.STATE)); /* evicts a; a batch may still hold it */
        long b = c.bytes();
        assertEquals(SeriesCache.weight("b"), b);
        c.putState(a, "on", 1);
        assertEquals(b, c.bytes());
        assertNull(c.get("a"));
    }

    @Test
    void manySeriesWithStatesShareTheByteBound() {
        long max = 4 << 20;
        SeriesCache c = new SeriesCache(100_000, max, 1_000);
        for (int s = 0; s < 500; s++) {
            SeriesCache.Entry e = new SeriesCache.Entry(s, ValueType.STATE);
            c.put("m" + s, e);
            for (int i = 0; i < 20; i++)
                c.putState(e, name('v', i, 10_000), i);
            assertTrue(c.bytes() <= max, "bytes " + c.bytes());
        }
        assertTrue(c.size() < 500);
        c.clear();
        assertEquals(0, c.bytes());
        assertEquals(0, c.size());
    }
}
