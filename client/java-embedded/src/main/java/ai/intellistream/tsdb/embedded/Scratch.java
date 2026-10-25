package ai.intellistream.tsdb.embedded;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Native memory for the copies of the caller's arrays that an insert hands the engine, reused from call to call. A
 * fresh arena per call made every insert of a million rows malloc 16 MB, fault it in (the kernel zeroes each page)
 * and free it again; with 16 threads loading, that page work cost more than the copying.
 *
 * <p>A bump allocator over one block. What does not fit goes to a confined arena of the call, and the block grows to
 * what the call needed (up to {@link #MAX}) when the call ends, so the next one fits. Not zeroed: callers set every
 * byte they hand over. Used by one thread at a time; {@link Pool} hands them out.
 */
final class Scratch implements SegmentAllocator {
    /** The largest block kept: 2M float64 rows of one series, 1.4M of many. */
    static final long MAX = 32L << 20;

    private Arena arena;                             /* owns block; shared, so the next call may be on another thread */
    private MemorySegment block = MemorySegment.NULL;
    private long need;                               /* bytes this call asked for, aligned as in the block */
    private Arena spill;                             /* this call's allocations beyond the block */

    @Override
    public MemorySegment allocate(long byteSize, long byteAlignment) {
        long at = (need + byteAlignment - 1) & -byteAlignment; /* the block is 64-byte aligned */
        need = at + byteSize;
        if (spill == null && need <= block.byteSize())
            return block.asSlice(at, byteSize);
        if (spill == null)
            spill = Arena.ofConfined();
        return spill.allocate(byteSize, byteAlignment);
    }

    /** Ends a call (on its thread): frees what spilled and grows the block to fit such a call next time. */
    void reset() {
        if (spill != null) {
            spill.close();
            spill = null;
            if (need > block.byteSize() && need <= MAX) {
                free();
                arena = Arena.ofShared();
                block = arena.allocate((need + 0xfffff) & ~0xfffffL, 64); /* whole MiB */
            }
        }
        need = 0;
    }

    private void free() {
        if (arena != null)
            arena.close();
        arena = null;
        block = MemorySegment.NULL;
    }

    /** The scratch blocks of one database: one per thread inserting at once, at most {@code KEEP} kept between calls. */
    static final class Pool {
        private static final int KEEP = 64;
        private final ConcurrentLinkedQueue<Scratch> free = new ConcurrentLinkedQueue<>();
        private final AtomicInteger kept = new AtomicInteger();

        Scratch take() {
            Scratch s = free.poll();
            if (s == null)
                return new Scratch();
            kept.decrementAndGet();
            return s;
        }

        /** Returns {@code s} after its call, on the call's thread. */
        void give(Scratch s) {
            s.reset();
            if (kept.incrementAndGet() <= KEEP) {
                free.offer(s);
            } else {
                kept.decrementAndGet();
                s.free();
            }
        }

        /** Frees the blocks kept; only when no call holds one (the database's write lock). */
        void clear() {
            for (Scratch s; (s = free.poll()) != null;) {
                kept.decrementAndGet();
                s.free();
            }
        }
    }
}
