package ai.intellistream.tsdb.pulsar;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff with a cap and jitter. The jitter keeps several sink instances from retrying in lockstep
 * against a server that just came back. Not thread safe: one per shard.
 */
final class Backoff {
    private final long initialMs;
    private final long maxMs;
    private final double jitter;
    private long nextMs;

    Backoff(long initialMs, long maxMs, double jitter) {
        this.initialMs = initialMs;
        this.maxMs = maxMs;
        this.jitter = jitter;
        this.nextMs = initialMs;
    }

    /** The delay before the next attempt; doubles the one after, up to the cap. */
    long next() {
        long base = nextMs;
        nextMs = Math.min(maxMs, nextMs * 2);
        long spread = (long) (base * jitter);
        long d = base - spread + (spread > 0 ? ThreadLocalRandom.current().nextLong(2 * spread + 1) : 0);
        return Math.max(1, Math.min(maxMs, d));
    }

    /** The delay {@link #next()} would start from, without jitter; 0 after a success. */
    long current() {
        return nextMs == initialMs ? 0 : nextMs / 2;
    }

    void reset() {
        nextMs = initialMs;
    }
}
