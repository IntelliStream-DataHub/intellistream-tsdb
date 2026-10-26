package ai.intellistream.tsdb.embedded;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Native memory for the copies of the caller's arrays that an insert hands the engine, reused from call to call. A
 * fresh arena per call made every insert of a million rows malloc 16 MB, fault it in (the kernel zeroes each page)
 * and free it again; with 16 threads loading, that page work cost more than the copying.
 *
 * <p>A bump allocator over one block. What does not fit goes to a confined arena of the call (what every call did
 * before), and when the call ends {@link Pool#give} may grow the block to what the call needed (up to {@link #MAX}),
 * so the next one fits. Not zeroed: callers set every byte they hand over. Used by one thread at a time.
 */
final class Scratch implements SegmentAllocator {
    /** The largest block kept: 2M float64 rows of one series, about 1.66M of many (2.1M float32). */
    static final long MAX = 32L << 20;

    /** Tests only: runs before a block is allocated, to make the allocation fail as malloc can. */
    static volatile Runnable growHook;

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

    long size() {
        return block.byteSize();
    }

    /**
     * Ends a call (on its thread): frees what spilled.
     *
     * @return the block size that would have held the whole call (whole MiB), or 0 if the block did
     */
    long reset() {
        long want = 0;
        if (spill != null) {
            spill.close();
            spill = null;
            want = (need + 0xfffff) & ~0xfffffL;
        }
        need = 0;
        return want;
    }

    /** Replaces the block by one of {@code bytes}; false, and no block, if that cannot be allocated. */
    boolean grow(long bytes) {
        free();
        try {
            Runnable h = growHook;
            if (h != null)
                h.run();
            Arena a = Arena.ofShared();
            try {
                block = a.allocate(bytes, 64);
            } catch (Throwable t) {
                a.close();
                throw t;
            }
            arena = a;
            return true;
        } catch (OutOfMemoryError | RuntimeException e) {
            return false; /* the insert went through; only the cache is lost */
        }
    }

    void free() {
        if (arena != null)
            arena.close();
        arena = null;
        block = MemorySegment.NULL;
    }

    /**
     * The scratch blocks of one database: one per thread inserting at once, at most {@code limit} bytes of blocks in
     * all, kept until close. A block grows only when the limit leaves room for it, so a call beyond the limit costs
     * what every call did before (a confined arena), never a block allocated and freed again. The block returned
     * last is handed out first, so a few threads keep reusing the same warm blocks.
     */
    static final class Pool {
        private final long limit;
        private final ConcurrentLinkedDeque<Scratch> free = new ConcurrentLinkedDeque<>();
        private final AtomicLong bytes = new AtomicLong(); /* of every block the pool owns, in use or free */

        Pool(long limit) {
            this.limit = limit;
        }

        Scratch take() {
            Scratch s = free.pollFirst();
            return s != null ? s : new Scratch();
        }

        /** Returns {@code s} after its call, on the call's thread. A failed growth drops the block, never throws. */
        void give(Scratch s) {
            long want = s.reset();
            long have = s.size();
            if (want > have && want <= MAX && reserve(want - have)) {
                boolean grown = false;
                try {
                    grown = s.grow(want);
                } finally {
                    if (!grown)
                        bytes.addAndGet(-want); /* the old block is gone as well */
                }
                if (!grown)
                    return;
            }
            if (s.size() > 0) /* a scratch without a block is not worth keeping */
                free.addFirst(s);
        }

        private boolean reserve(long delta) {
            for (long b = bytes.get();; b = bytes.get()) {
                if (b + delta > limit)
                    return false;
                if (bytes.compareAndSet(b, b + delta))
                    return true;
            }
        }

        /** Frees the blocks kept; only when no call holds one (the database's write lock). */
        void clear() {
            for (Scratch s; (s = free.pollFirst()) != null;) {
                bytes.addAndGet(-s.size());
                s.free();
            }
        }

        /** Bytes of the blocks the pool owns (tests). */
        long bytes() {
            return bytes.get();
        }

        /** Scratches free to take (tests). */
        int kept() {
            return free.size();
        }
    }
}
