package ai.intellistream.tsdb.pulsar;

import ai.intellistream.tsdb.ValueType;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Bounded LRU of series names to id and type, with the state codes of state series. One per connection (shard):
 * only that shard's thread uses it, so it needs no lock.
 */
final class SeriesCache {
    /** A resolved series. */
    static final class Entry {
        final int id;
        final ValueType type;
        /** State names to codes, for state series; filled on first use. */
        Map<String, Integer> states;

        Entry(int id, ValueType type) {
            this.id = id;
            this.type = type;
        }
    }

    private final LinkedHashMap<String, Entry> map;

    SeriesCache(int capacity) {
        this.map = new LinkedHashMap<>(16, 0.75f, true) {
            private static final long serialVersionUID = 1L;

            @Override
            protected boolean removeEldestEntry(Map.Entry<String, SeriesCache.Entry> eldest) {
                return size() > capacity;
            }
        };
    }

    Entry get(String name) {
        return map.get(name);
    }

    void put(String name, Entry e) {
        map.put(name, e);
    }

    void clear() {
        map.clear();
    }

    int size() {
        return map.size();
    }
}
