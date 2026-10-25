package ai.intellistream.tsdb.pulsar;

import ai.intellistream.tsdb.ValueType;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Bounded LRU of series names to id and type, with the state codes of state series. One per connection (shard):
 * only that shard's thread uses it, so it needs no lock.
 *
 * <p>Names come from topic data, so the cache is bounded three ways: by the number of series, by the number of state
 * names per series, and by the estimated heap the names take (series and state names together, up to 64 KiB of
 * UTF-8 each). Anything evicted is asked from the server again when it comes back: a state is created once and keeps
 * its code, so asking again gives the same answer.
 */
final class SeriesCache {
    /** Rough heap of a cached string beyond its characters: the String, its array and the map node. */
    static final long STRING_OVERHEAD = 96;

    /** A resolved series. */
    static final class Entry {
        final int id;
        final ValueType type;
        /** State names to codes (LRU), for state series; filled on first use through {@link SeriesCache#putState}. */
        LinkedHashMap<String, Integer> states;
        /** Estimated heap of the entry: its name and its state names. */
        long bytes;
        /** Whether the entry is counted in a cache (an evicted one may still be in use by a batch). */
        boolean cached;

        Entry(int id, ValueType type) {
            this.id = id;
            this.type = type;
        }
    }

    private final LinkedHashMap<String, Entry> map = new LinkedHashMap<>(16, 0.75f, true);
    private final int capacity, statesPerSeries;
    private final long maxBytes;
    private long bytes;

    SeriesCache(int capacity) {
        this(capacity, Long.MAX_VALUE, Integer.MAX_VALUE);
    }

    SeriesCache(int capacity, long maxBytes, int statesPerSeries) {
        this.capacity = capacity;
        this.maxBytes = maxBytes;
        this.statesPerSeries = statesPerSeries;
    }

    static long weight(String s) {
        return STRING_OVERHEAD + 2L * s.length();
    }

    Entry get(String name) {
        return map.get(name);
    }

    void put(String name, Entry e) {
        Entry old = map.put(name, e);
        if (old != null) {
            old.cached = false;
            bytes -= old.bytes;
        }
        e.cached = true;
        e.bytes = weight(name) + stateBytes(e);
        bytes += e.bytes;
        trim(e);
    }

    /** The code of a state name of a series, or null if not cached. */
    Integer state(Entry e, String name) {
        return e.states == null ? null : e.states.get(name);
    }

    void putState(Entry e, String name, int code) {
        if (e.states == null)
            e.states = new LinkedHashMap<>(16, 0.75f, true);
        if (e.states.put(name, code) == null)
            grow(e, weight(name));
        while (e.states.size() > statesPerSeries)
            dropEldestState(e);
        trim(e);
    }

    private static long stateBytes(Entry e) {
        long b = 0;
        if (e.states != null)
            for (String s : e.states.keySet())
                b += weight(s);
        return b;
    }

    private void grow(Entry e, long d) {
        e.bytes += d;
        if (e.cached)
            bytes += d;
    }

    private void dropEldestState(Entry e) {
        Iterator<String> it = e.states.keySet().iterator();
        String s = it.next();
        it.remove();
        grow(e, -weight(s));
    }

    /** Evicts least recently used series, then the states of keep itself, until within the bounds. */
    private void trim(Entry keep) {
        Iterator<Map.Entry<String, Entry>> it = map.entrySet().iterator();
        while ((map.size() > capacity || bytes > maxBytes) && it.hasNext()) {
            Map.Entry<String, Entry> eldest = it.next();
            if (eldest.getValue() == keep && map.size() <= capacity)
                continue; /* the one in use goes last */
            it.remove();
            eldest.getValue().cached = false;
            bytes -= eldest.getValue().bytes;
        }
        while (bytes > maxBytes && keep.cached && keep.states != null && !keep.states.isEmpty())
            dropEldestState(keep);
    }

    void clear() {
        for (Entry e : map.values())
            e.cached = false;
        map.clear();
        bytes = 0;
    }

    int size() {
        return map.size();
    }

    /** The estimated heap of the cached names, as counted against the byte bound. */
    long bytes() {
        return bytes;
    }
}
