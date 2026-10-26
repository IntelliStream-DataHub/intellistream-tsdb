/*
 * CPU levels: the functions compiled once per x86-64 microarchitecture level.
 *
 * A portable build (`make dist`) targets x86-64-v2 so that one binary runs on
 * CPUs as old as Sandy Bridge. The codecs and the value folds are where that
 * costs speed: bit-packing, the XOR codecs' leading/trailing zero counts,
 * zigzag deltas and the int64 <-> double conversions of the decimal codecs
 * want BMI1/2, LZCNT, AVX2 and AVX-512DQ/VL, which v2 lacks. So the files
 * that hold them (codec.c, codec_alp.c, codec_typed.c, codec_xor.c, values.c)
 * are compiled three times, for the baseline and with -march=x86-64-v3 and
 * -march=x86-64-v4, and cpu.c picks one set at startup.
 *
 * Why whole files rather than GCC target_clones or ifunc: the hot loops are in
 * static inline helpers (bits.h, bitpack.h) inlined into dozens of functions;
 * compiling the file for the level gives every one of them the instructions
 * without marking each. ifunc resolvers run before main, where TSDB_CPU cannot
 * be read reliably, and cannot be switched by a test that compares levels.
 *
 * Every function those files export is listed here, once. In a per-level
 * compile (TSDB_LV defined, e.g. v3) the pragmas below give each one the
 * symbol name_v3; cpu.c defines the plain names as calls through the table of
 * the level in use. The linker checks the list: a missing entry is a duplicate
 * or undefined symbol. Without CPU levels (the default `make`, -march=native)
 * none of this applies and the functions keep their names.
 *
 * All levels must produce byte-identical blocks and identical values and
 * statistics: parts written on one CPU are read on another. Hence
 * -ffp-contract=off everywhere (v3 has FMA, and contracted a*b+c rounds
 * differently), and no libm function that glibc picks by CPU (codec.c's
 * log2_portable); tests/test_levels.c compares every level this CPU has.
 * Nothing in those files may run at startup (constructors): the v3/v4 copies
 * would run on every CPU, before cpu.c's check. tools/check-isa.sh enforces
 * both on the objects.
 */
#ifndef TSDB_LEVELS_H
#define TSDB_LEVELS_H

/* X(return type, name, parameters, arguments); XV for functions returning void */
#define TSDB_LEVEL_FNS(X, XV)                                                                                 \
    X(uint64_t, xxh64, (const void *data, size_t len, uint64_t seed), (data, len, seed))                     \
    X(size_t, dd_bound, (size_t n), (n))                                                                      \
    X(size_t, gorilla_bound, (size_t n), (n))                                                                 \
    X(size_t, dd_encode, (const int64_t *in, size_t n, uint8_t *out), (in, n, out))                           \
    X(int, dd_decode, (const uint8_t *in, size_t len, size_t n, int64_t *out), (in, len, n, out))             \
    X(size_t, gorilla_encode, (const uint32_t *in, size_t n, uint8_t *out), (in, n, out))                     \
    X(int, gorilla_decode, (const uint8_t *in, size_t len, size_t n, uint32_t *out), (in, len, n, out))       \
    X(size_t, chimp_bound, (size_t n), (n))                                                                   \
    X(size_t, chimp_encode, (const uint32_t *in, size_t n, uint8_t *out), (in, n, out))                       \
    X(int, chimp_decode, (const uint8_t *in, size_t len, size_t n, uint32_t *out), (in, len, n, out))         \
    X(size_t, chimp128_bound, (size_t n), (n))                                                                \
    X(size_t, chimp128_encode, (const uint32_t *in, size_t n, uint8_t *out), (in, n, out))                    \
    X(int, chimp128_decode, (const uint8_t *in, size_t len, size_t n, uint32_t *out), (in, len, n, out))      \
    X(size_t, patas_bound, (size_t n), (n))                                                                   \
    X(size_t, patas_encode, (const uint32_t *in, size_t n, uint8_t *out), (in, n, out))                       \
    X(int, patas_decode, (const uint8_t *in, size_t len, size_t n, uint32_t *out), (in, len, n, out))         \
    X(size_t, delta32_encode, (const uint32_t *in, size_t n, uint8_t *out), (in, n, out))                     \
    X(int, delta32_decode, (const uint8_t *in, size_t len, size_t n, uint32_t *out), (in, len, n, out))       \
    X(size_t, bytesplit_encode, (const uint32_t *in, size_t n, uint8_t *out), (in, n, out))                   \
    X(int, bytesplit_decode, (const uint8_t *in, size_t len, size_t n, uint32_t *out), (in, len, n, out))     \
    X(size_t, tsfor_bound, (size_t n), (n))                                                                   \
    X(size_t, ts_grid_encode, (const int64_t *ts, size_t n, uint8_t *out), (ts, n, out))                      \
    X(int, ts_grid_decode, (const uint8_t *in, size_t len, size_t n, int64_t *out), (in, len, n, out))        \
    X(size_t, ts_delta_encode, (const int64_t *ts, size_t n, uint8_t *out), (ts, n, out))                     \
    X(int, ts_delta_decode, (const uint8_t *in, size_t len, size_t n, int64_t *out), (in, len, n, out))       \
    X(size_t, decimal_bp_bound, (size_t n), (n))                                                              \
    X(size_t, decimal_bp_encode, (const float *in, size_t n, uint8_t *out), (in, n, out))                     \
    X(int, decimal_bp_decode, (const uint8_t *in, size_t len, size_t n, float *out), (in, len, n, out))       \
    X(size_t, alp_bound, (size_t n), (n))                                                                     \
    X(size_t, alp_encode, (const float *in, size_t n, uint8_t *out), (in, n, out))                            \
    X(int, alp_decode, (const uint8_t *in, size_t len, size_t n, float *out), (in, len, n, out))              \
    X(size_t, alprd_bound, (size_t n), (n))                                                                   \
    X(size_t, alprd_encode, (const uint32_t *in, size_t n, uint8_t *out), (in, n, out))                       \
    X(int, alprd_decode, (const uint8_t *in, size_t len, size_t n, uint32_t *out), (in, len, n, out))         \
    X(size_t, alprd64_bound, (size_t n), (n))                                                                 \
    X(size_t, alprd64_encode, (const uint64_t *in, size_t n, uint8_t *out), (in, n, out))                     \
    X(int, alprd64_decode, (const uint8_t *in, size_t len, size_t n, uint64_t *out), (in, len, n, out))       \
    X(size_t, delta_bp32_bound, (size_t n), (n))                                                              \
    X(size_t, delta_bp32_encode, (const uint32_t *in, size_t n, uint8_t *out), (in, n, out))                  \
    X(int, delta_bp32_decode, (const uint8_t *in, size_t len, size_t n, uint32_t *out), (in, len, n, out))    \
    X(size_t, delta_bp64_bound, (size_t n), (n))                                                              \
    X(size_t, delta_bp64_encode, (const uint64_t *in, size_t n, uint8_t *out), (in, n, out))                  \
    X(int, delta_bp64_decode, (const uint8_t *in, size_t len, size_t n, uint64_t *out), (in, len, n, out))    \
    X(size_t, decimal_bound, (size_t n), (n))                                                                 \
    X(size_t, decimal_encode, (const float *in, size_t n, uint8_t *out), (in, n, out))                        \
    X(int, decimal_decode, (const uint8_t *in, size_t len, size_t n, float *out), (in, len, n, out))          \
    X(codec_ctx *, codec_new, (void), ())                                                                     \
    XV(codec_free, (codec_ctx * c), (c))                                                                      \
    X(size_t, block_bound, (size_t n), (n))                                                                   \
    X(size_t, block_encode_ts, (codec_ctx * c, int level, const int64_t *ts, size_t n, uint8_t *out),         \
      (c, level, ts, n, out))                                                                                 \
    X(size_t, block_encode_f32, (codec_ctx * c, int level, int value_codec, const float *v, size_t n,          \
                                 uint8_t *out), (c, level, value_codec, v, n, out))                           \
    X(int, block_decode_ts, (codec_ctx * c, const uint8_t *blk, size_t len, size_t n, int64_t *out),          \
      (c, blk, len, n, out))                                                                                  \
    X(int, block_decode_f32, (codec_ctx * c, const uint8_t *blk, size_t len, size_t n, float *out),           \
      (c, blk, len, n, out))                                                                                  \
    X(size_t, block_encode_vals, (codec_ctx * c, int level, int value_codec, int type, const void *v,         \
                                  size_t n, uint8_t *out), (c, level, value_codec, type, v, n, out))          \
    X(int, block_decode_vals, (codec_ctx * c, int type, const uint8_t *blk, size_t len, size_t n, void *out), \
      (c, type, blk, len, n, out))                                                                            \
    X(int, codec_scratch_reserve, (codec_ctx * c, size_t need), (c, need))                                    \
    X(size_t, block_wrap, (codec_ctx * c, int level, int kind, size_t inner, uint8_t *out),                   \
      (c, level, kind, inner, out))                                                                           \
    X(size_t, block_wrap_src, (codec_ctx * c, int level, int kind, const uint8_t *src, size_t inner,          \
                               uint8_t *out), (c, level, kind, src, inner, out))                              \
    X(int, codec_alt_reserve, (codec_ctx * c, size_t need), (c, need))                                        \
    X(int, codec_work_reserve, (codec_ctx * c, size_t need), (c, need))                                       \
    X(size_t, decimal_bp_encode_keep, (const float *in, size_t n, uint8_t *out, int64_t *keep, size_t *nexc),  \
      (in, n, out, keep, nexc))                                                                               \
    X(size_t, f64_decimal_encode_keep, (const double *in, size_t n, uint8_t *out, int64_t *keep,               \
                                        size_t *nexc), (in, n, out, keep, nexc))                              \
    X(int, block_open, (codec_ctx * c, const uint8_t *blk, size_t len, size_t n, int *kind,                   \
                        const uint8_t **p, size_t *plen), (c, blk, len, n, kind, p, plen))                    \
    X(size_t, auto_pick, (codec_ctx * c, int level, const int *kinds, size_t nk, codec_kind_fn enc,             \
                          const void *v, size_t n, size_t width, int pre_kind, size_t pre_inner, size_t have,       \
                          double all_bits, double surprise, uint8_t *out),                                          \
      (c, level, kinds, nk, enc, v, n, width, pre_kind, pre_inner, have, all_bits, surprise, out))                  \
    X(size_t, dict_block, (codec_ctx * c, int level, const void *v, size_t n, int width, uint8_t *out),          \
      (c, level, v, n, width, out))                                                                           \
    X(size_t, int_bp_bound, (size_t n), (n))                                                                  \
    X(size_t, int_bp_encode, (const int64_t *in, size_t n, uint8_t *out), (in, n, out))                       \
    X(int, int_bp_decode, (const uint8_t *in, size_t len, size_t n, int64_t *out), (in, len, n, out))         \
    X(size_t, dict_bound, (size_t n), (n))                                                                    \
    X(size_t, dict_encode, (const void *v, size_t n, int width, uint8_t *out), (v, n, width, out))            \
    X(int, dict_decode, (const uint8_t *in, size_t len, size_t n, int width, void *out),                      \
      (in, len, n, width, out))                                                                               \
    X(size_t, fixed_or_dict, (codec_ctx * c, int level, int pre_kind, size_t pre_inner, const int64_t *q,      \
                              size_t nexc, const void *v, size_t n, int width, uint8_t *out),                 \
      (c, level, pre_kind, pre_inner, q, nexc, v, n, width, out))                                             \
    X(size_t, f64_decimal_bound, (size_t n), (n))                                                             \
    X(size_t, f64_decimal_encode, (const double *in, size_t n, uint8_t *out), (in, n, out))                   \
    X(int, f64_decimal_decode, (const uint8_t *in, size_t len, size_t n, double *out), (in, len, n, out))     \
    X(size_t, xor64_bound, (size_t n), (n))                                                                   \
    X(size_t, xor64_encode, (const uint64_t *in, size_t n, uint8_t *out), (in, n, out))                       \
    X(int, xor64_decode, (const uint8_t *in, size_t len, size_t n, uint64_t *out), (in, len, n, out))         \
    XV(agg_fold, (int type, const void *v, size_t n, agg_state *a, agg_dev *x), (type, v, n, a, x))          \
    XV(agg_fold_hint, (int type, const void *v, size_t n, bool may_be_null, agg_state *a, agg_dev *x),        \
       (type, v, n, may_be_null, a, x))                                                                       \
    XV(granule_stats, (int type, const void *v, size_t n, struct granule *g), (type, v, n, g))                \
    XV(agg_from_granule, (int type, const struct granule *g, agg_state *a, agg_dev *x), (type, g, a, x))

/* CPU levels; CPU_BASE is whatever the build's ARCH targets (x86-64-v2 in `make dist`). */
enum { CPU_BASE, CPU_V3, CPU_V4, CPU_LEVELS };

#if defined(TSDB_LEVELS) && defined(TSDB_LV)
#define LV_CAT2(a, b) a##_##b
#define LV_CAT(a, b) LV_CAT2(a, b)
#define LV_PRAGMA(x) _Pragma(#x)
#define LV_RENAME2(from, to) LV_PRAGMA(redefine_extname from to)
#define LV_RENAME(r, f, p, a) LV_RENAME2(f, LV_CAT(f, TSDB_LV))
#define LV_RENAME_V(f, p, a) LV_RENAME2(f, LV_CAT(f, TSDB_LV))
TSDB_LEVEL_FNS(LV_RENAME, LV_RENAME_V)
#endif

#endif
