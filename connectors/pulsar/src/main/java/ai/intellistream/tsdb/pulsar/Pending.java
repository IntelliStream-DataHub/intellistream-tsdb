package ai.intellistream.tsdb.pulsar;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.functions.api.Record;

/**
 * A record between {@code write} and its acknowledgement.
 *
 * <p>Its points may belong to several shards (a batch message over many series); each shard reports its part done
 * after tsdb acknowledged the insert holding it, or after deciding the part is poison. The record is acknowledged
 * when the last part is done, or dead-lettered if any part was poison. A record whose part is still waiting is never
 * acknowledged, whatever happens to the others.
 */
final class Pending {
    final Record<?> record;
    final long size;
    private final AtomicInteger parts = new AtomicInteger();
    private volatile String poison;
    private volatile int poisonStatus;

    Pending(Record<?> record, long size) {
        this.record = record;
        this.size = size;
    }

    void parts(int n) {
        parts.set(n);
    }

    /** Longest reason kept: it travels as a message property of the dead letter and goes to the log. */
    static final int MAX_REASON = 1000;

    /** Marks the record poison; the first reason wins. Reasons are cut to {@link #MAX_REASON} characters. */
    synchronized void poison(String reason, int status) {
        if (poison == null) {
            poison = reason.length() <= MAX_REASON ? reason : reason.substring(0, MAX_REASON) + "...";
            poisonStatus = status;
        }
    }

    boolean isPoison() {
        return poison != null;
    }

    String reason() {
        return poison;
    }

    int status() {
        return poisonStatus;
    }

    /** One part is done; true for the caller that completed the last one, which then settles the record. */
    boolean partDone() {
        return parts.decrementAndGet() == 0;
    }
}
