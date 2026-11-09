// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

import com.github.luben.zstd.ZstdCompressCtx;

/**
 * zstd for packed inserts, through zstd-jni (the native library, as the C client uses it). Only this class
 * refers to zstd-jni, so the client still loads, and sends plain arrays, when the library or its native code is
 * missing: {@link #create()} returns null then.
 */
final class Compressor implements AutoCloseable {
    /** The levels zstd accepts ({@code ZSTD_minCLevel}, {@code ZSTD_maxCLevel}). */
    static final int MIN_LEVEL = -(1 << 17), MAX_LEVEL = 22;

    private final ZstdCompressCtx ctx;
    private int level = Integer.MIN_VALUE;
    private byte[] buf = new byte[0];

    private Compressor(ZstdCompressCtx ctx) {
        this.ctx = ctx;
    }

    /** A compressor, or null when zstd-jni cannot be loaded. */
    static Compressor create() {
        try {
            return new Compressor(new ZstdCompressCtx());
        } catch (LinkageError e) { /* NoClassDefFoundError, UnsatisfiedLinkError */
            return null;
        }
    }

    /** Compresses {@code src[0, len)} into one zstd frame; returns its length, the bytes are in {@link #out()}. */
    int compress(byte[] src, int len, int level) {
        if (level != this.level) {
            ctx.setLevel(level);
            this.level = level;
        }
        long bound = com.github.luben.zstd.Zstd.compressBound(len);
        if (buf.length < bound)
            buf = new byte[(int) Math.min(bound, Integer.MAX_VALUE - 8)];
        return ctx.compressByteArray(buf, 0, buf.length, src, 0, len);
    }

    byte[] out() {
        return buf;
    }

    @Override
    public void close() {
        ctx.close();
    }
}
