package ai.intellistream.tsdb.pulsar;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.pulsar.functions.api.Record;

/** Collects dead letters; can be made to fail, as an unreachable dead-letter topic would. */
final class CapturingDeadLetter implements DeadLetter {
    final List<Record<?>> records = new CopyOnWriteArrayList<>();
    final List<String> reasons = new CopyOnWriteArrayList<>();
    final List<Integer> statuses = new CopyOnWriteArrayList<>();
    volatile int failNext;

    @Override
    public boolean settles() {
        return false;
    }

    @Override
    public void send(Record<?> r, String reason, int status) throws Exception {
        if (failNext > 0) {
            failNext--;
            throw new Exception("dead-letter topic unavailable");
        }
        records.add(r);
        reasons.add(reason);
        statuses.add(status);
    }

    @Override
    public void close() {
    }
}
