// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb.pulsar;

import ai.intellistream.tsdb.ValueType;

/**
 * One point of a record: a value of a series at a time.
 *
 * <p>{@code value} is {@code null}, a {@link Number}, a {@link Boolean} or a {@link String} (the name of a state);
 * it is converted to the series' type when the series is known, in {@link Shard}.
 */
final class Point {
    final String series;
    final long ts;
    final Object value;
    /** The type the record asks for, or {@code null}. */
    final ValueType type;

    Point(String series, long ts, Object value, ValueType type) {
        this.series = series;
        this.ts = ts;
        this.value = value;
        this.type = type;
    }

    /** A series name short enough for a log line or a dead-letter reason: a refused name can be megabytes long. */
    static String brief(String series) {
        return series.length() <= 200 ? series : series.substring(0, 200) + "... (" + series.length() + " chars)";
    }

    /**
     * Any producer-supplied value cut the same way, for reasons and logs: a dead letter carries the reason next to
     * the original payload, so an uncut 3 MB field would make it too large for Pulsar to ever take.
     */
    static String clip(Object v) {
        return brief(String.valueOf(v));
    }

    /** Rough memory taken while in flight, for the byte limits. */
    long size() {
        return 48 + 2L * series.length() + (value instanceof String ? 2L * ((String) value).length() : 0);
    }
}
