package ai.intellistream.tsdb;

import java.util.Objects;

/**
 * Options of the time-weighted average queries: the method, the longest gap a segment may span, the order and the
 * most buckets to return. Immutable; each method returns a changed copy.
 *
 * <pre>{@code
 * TwaOptions o = TwaOptions.linear().maxGap(60_000).order(Order.DESC).limit(10);
 * }</pre>
 *
 * @param method step or linear
 * @param maxGapMs a segment longer than this covers nothing (the value is stale); 0 for no limit
 * @param order oldest or newest bucket first
 * @param limit the most buckets to return; 0 for all
 */
public record TwaOptions(TwaMethod method, long maxGapMs, Order order, long limit) {
    /**
     * Checks the options.
     *
     * @throws IllegalArgumentException for a negative gap or limit
     */
    public TwaOptions {
        Objects.requireNonNull(method, "method");
        Objects.requireNonNull(order, "order");
        if (maxGapMs < 0)
            throw new IllegalArgumentException("negative maxGapMs");
        if (limit < 0)
            throw new IllegalArgumentException("negative limit");
    }

    /** @return step, no gap limit, oldest first, every bucket */
    public static TwaOptions step() {
        return new TwaOptions(TwaMethod.STEP, 0, Order.ASC, 0);
    }

    /** @return linear, no gap limit, oldest first, every bucket */
    public static TwaOptions linear() {
        return new TwaOptions(TwaMethod.LINEAR, 0, Order.ASC, 0);
    }

    /**
     * @param ms the longest gap between readings a segment may span; 0 for no limit
     * @return these options with that gap
     */
    public TwaOptions maxGap(long ms) {
        return new TwaOptions(method, ms, order, limit);
    }

    /**
     * @param o oldest or newest bucket first
     * @return these options in that order
     */
    public TwaOptions order(Order o) {
        return new TwaOptions(method, maxGapMs, o, limit);
    }

    /**
     * @param n the most buckets to return; 0 for all
     * @return these options with that limit
     */
    public TwaOptions limit(long n) {
        return new TwaOptions(method, maxGapMs, order, n);
    }
}
