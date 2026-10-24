package ai.intellistream.tsdb.pulsar;

/**
 * Bounds the records (and their bytes) received but not yet acknowledged.
 *
 * <p>{@link #acquire} blocks the thread that calls the sink's {@code write}: the Pulsar runtime then stops pulling
 * messages, and they stay in the topic while tsdb is down or slow. A record larger than the byte limit on its own
 * is let through when nothing else is in flight, or it could never be written.
 */
final class InFlightLimiter {
    private final long maxCount;
    private final long maxBytes;
    private long count;
    private long bytes;
    private boolean shutdown;

    InFlightLimiter(long maxCount, long maxBytes) {
        this.maxCount = maxCount;
        this.maxBytes = maxBytes;
    }

    /** Takes room for one record of {@code size} bytes; false (and no room taken) once {@link #shutdown}. */
    synchronized boolean acquire(long size) throws InterruptedException {
        while (!shutdown && count > 0 && (count + 1 > maxCount || bytes + size > maxBytes))
            wait();
        if (shutdown)
            return false;
        count++;
        bytes += size;
        return true;
    }

    /** Wakes and refuses every later {@link #acquire}: the sink is closing. */
    synchronized void shutdown() {
        shutdown = true;
        notifyAll();
    }

    synchronized void release(long size) {
        count--;
        bytes -= size;
        notifyAll();
    }

    synchronized long count() {
        return count;
    }

    synchronized long bytes() {
        return bytes;
    }

    /** Waits until nothing is in flight or the deadline (System.nanoTime) passes; returns whether it drained. */
    synchronized boolean awaitEmpty(long deadline) throws InterruptedException {
        while (count > 0) {
            long left = deadline - System.nanoTime();
            if (left <= 0)
                return false;
            wait(Math.max(1, left / 1_000_000));
        }
        return true;
    }
}
