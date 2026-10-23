/* Bit packing shared by the ALP-style codecs: values of w bits, LSB first. */
#ifndef TSDB_BITPACK_H
#define TSDB_BITPACK_H

#include <stdint.h>
#include <string.h>


static inline int bits_for(uint32_t x) { return x ? 32 - __builtin_clz(x) : 0; }

static inline size_t packed_bytes(size_t n, int w) { return (n * (size_t)w + 7) / 8; }

/* Streams values of w bits into out, LSB first; writes packed_bytes(n, w)
 * bytes plus up to 3 of padding. */
static inline void pack(const uint32_t *in, size_t n, int w, uint8_t *out)
{
    if (!w)
        return;
    uint64_t acc = 0;
    int bits = 0;
    for (size_t i = 0; i < n; i++) {
        acc |= (uint64_t)in[i] << bits; /* bits < 32 and w <= 32: fits */
        bits += w;
        if (bits >= 32) {
            uint32_t word = (uint32_t)acc;
            memcpy(out, &word, 4);
            out += 4;
            acc >>= 32;
            bits -= 32;
        }
    }
    if (bits > 0) {
        uint32_t word = (uint32_t)acc;
        memcpy(out, &word, 4);
    }
}

/* One 8-byte load per value; with w a constant the compiler unrolls and vectorizes. */
#define UNPACK_W(W)                                                                     \
    static inline void unpack_##W(const uint8_t *in, size_t n, uint32_t *out)                 \
    {                                                                                   \
        const uint64_t mask = (W) == 32 ? 0xffffffffull : ((1ull << (W)) - 1);          \
        for (size_t i = 0; i < n; i++) {                                                \
            uint64_t bit = (uint64_t)i * (W), x;                                        \
            memcpy(&x, in + (bit >> 3), 8);                                             \
            out[i] = (uint32_t)((x >> (bit & 7)) & mask);                               \
        }                                                                               \
    }
UNPACK_W(1) UNPACK_W(2) UNPACK_W(3) UNPACK_W(4) UNPACK_W(5) UNPACK_W(6) UNPACK_W(7) UNPACK_W(8)
UNPACK_W(9) UNPACK_W(10) UNPACK_W(11) UNPACK_W(12) UNPACK_W(13) UNPACK_W(14) UNPACK_W(15) UNPACK_W(16)
UNPACK_W(17) UNPACK_W(18) UNPACK_W(19) UNPACK_W(20) UNPACK_W(21) UNPACK_W(22) UNPACK_W(23) UNPACK_W(24)
UNPACK_W(25) UNPACK_W(26) UNPACK_W(27) UNPACK_W(28) UNPACK_W(29) UNPACK_W(30) UNPACK_W(31) UNPACK_W(32)

static void (*const unpack_fn[33])(const uint8_t *, size_t, uint32_t *) = {
    NULL,       unpack_1,  unpack_2,  unpack_3,  unpack_4,  unpack_5,  unpack_6,  unpack_7,  unpack_8,
    unpack_9,   unpack_10, unpack_11, unpack_12, unpack_13, unpack_14, unpack_15, unpack_16, unpack_17,
    unpack_18,  unpack_19, unpack_20, unpack_21, unpack_22, unpack_23, unpack_24, unpack_25, unpack_26,
    unpack_27,  unpack_28, unpack_29, unpack_30, unpack_31, unpack_32};

static inline void unpack(const uint8_t *in, size_t n, int w, uint32_t *out)
{
    if (w == 0)
        memset(out, 0, n * sizeof *out);
    else
        unpack_fn[w](in, n, out);
}

#endif
