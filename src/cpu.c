/*
 * CPU levels: which compile of the codecs runs (see levels.h for why they are
 * compiled per level), and the cap for the aggregation kernels of simd.c.
 *
 * The level is the highest x86-64 microarchitecture level that both this CPU
 * and this build have, lowered by TSDB_CPU=v2|v3|v4 (tests, and to try a
 * portable binary the way an older CPU runs it). A level the CPU lacks is never
 * used, whatever TSDB_CPU says: it would end in SIGILL.
 *
 * The plain-named functions of TSDB_LEVEL_FNS are defined here as calls
 * through the table of the level in use. The table starts at the baseline,
 * which is correct on every CPU this build runs on, so a call that comes
 * before the constructor below (another constructor) is still right; all
 * levels give identical results anyway, only speed differs.
 */
#include "internal.h"

#include <stdlib.h>

#if defined(TSDB_LEVELS) && !(defined(__x86_64__) || defined(__i386__))
#error "CPU levels are x86-64 microarchitecture levels"
#endif

static int level_max = CPU_BASE, level_now = CPU_BASE;

#ifdef TSDB_LEVELS
#define CPU_CAT2(a, b) a##_##b
#define CPU_CAT(a, b) CPU_CAT2(a, b)
#define LV_SLOT(r, f, p, a) r(*f) p;
#define LV_SLOT_V(f, p, a) void(*f) p;
#define LV_INIT(r, f, p, a) .f = CPU_CAT(f, LV),
#define LV_INIT_V(f, p, a) .f = CPU_CAT(f, LV),

struct lv_fns {
    TSDB_LEVEL_FNS(LV_SLOT, LV_SLOT_V)
};

#define LV_DECL(r, f, p, a) r CPU_CAT(f, LV) p;
#define LV_DECL_V(f, p, a) void CPU_CAT(f, LV) p;

#define LV base
TSDB_LEVEL_FNS(LV_DECL, LV_DECL_V)
static const struct lv_fns lv_base = {TSDB_LEVEL_FNS(LV_INIT, LV_INIT_V)};
#undef LV
#ifdef TSDB_HAVE_v3
#define LV v3
TSDB_LEVEL_FNS(LV_DECL, LV_DECL_V)
static const struct lv_fns lv_v3 = {TSDB_LEVEL_FNS(LV_INIT, LV_INIT_V)};
#undef LV
#endif
#ifdef TSDB_HAVE_v4
#define LV v4
TSDB_LEVEL_FNS(LV_DECL, LV_DECL_V)
static const struct lv_fns lv_v4 = {TSDB_LEVEL_FNS(LV_INIT, LV_INIT_V)};
#undef LV
#endif

static const struct lv_fns *const lv_table[CPU_LEVELS] = {
    &lv_base,
#ifdef TSDB_HAVE_v3
    &lv_v3,
#else
    NULL,
#endif
#ifdef TSDB_HAVE_v4
    &lv_v4,
#else
    NULL,
#endif
};

static const struct lv_fns *lv = &lv_base;

#define LV_CALL(r, f, p, a) r f p { return lv->f a; }
#define LV_CALL_V(f, p, a) void f p { lv->f a; }
TSDB_LEVEL_FNS(LV_CALL, LV_CALL_V)

static bool built(int level) { return lv_table[level] != NULL; }
#else
/* One compile of the codecs; the levels only cap the kernels of simd.c. */
static bool built(int level) { (void)level; return true; }
#endif

const char *cpu_level_name(int level)
{
    switch (level) {
#ifdef __AVX2__
    case CPU_BASE: return "base";
#else
    case CPU_BASE: return "v2";
#endif
    case CPU_V3: return "v3";
    case CPU_V4: return "v4";
    default: return "?";
    }
}

static int hw_level(void)
{
#if defined(__x86_64__) || defined(__i386__)
    __builtin_cpu_init();
    if (__builtin_cpu_supports("x86-64-v4"))
        return CPU_V4;
    if (__builtin_cpu_supports("x86-64-v3"))
        return CPU_V3;
#endif
    return CPU_BASE;
}

int cpu_level(void) { return level_now; }
int cpu_level_max(void) { return level_max; }

int cpu_set_level(int level)
{
    if (level < 0 || level > level_max || !built(level))
        return -1;
    level_now = level;
#ifdef TSDB_LEVELS
    lv = lv_table[level];
#endif
    simd_select(level);
    return 0;
}

__attribute__((constructor)) static void cpu_pick(void)
{
    int hw = hw_level();
    for (int l = CPU_BASE; l <= hw; l++)
        if (built(l))
            level_max = l;
    int want = level_max;
    const char *env = getenv("TSDB_CPU");
    if (env && *env) {
        if (!strcmp(env, "v2") || !strcmp(env, "base"))
            want = CPU_BASE;
        else if (!strcmp(env, "v3"))
            want = CPU_V3;
        else if (!strcmp(env, "v4"))
            want = CPU_V4;
        if (want > level_max)
            want = level_max;
        while (want > CPU_BASE && !built(want))
            want--;
    }
    level_now = want;
#ifdef TSDB_LEVELS
    lv = lv_table[want];
#endif
}
