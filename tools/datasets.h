/*
 * Synthetic but physically plausible sensor streams, shared by the benchmarks.
 * Each kind has its own sample interval, clock jitter and value resolution.
 */
#ifndef TSDB_TOOLS_DATASETS_H
#define TSDB_TOOLS_DATASETS_H

#include <math.h>
#include <stdint.h>
#include <string.h>

typedef enum {
    DS_HDD_TEMP,   /* SMART drive temperature: very stable, 0.1 degC */
    DS_CPU_TEMP,   /* idle ~22 degC with several spikes a day to ~100 degC, 0.1 degC */
    DS_TANK_BAR,   /* tank level pressure, 0.001 bar */
    DS_TANK_PSI,   /* the same tank converted to psi, unrounded */
    DS_VIBRATION,  /* 1 kHz accelerometer through a 16-bit +-4 g ADC */
    DS_FLOW_RATE,  /* pipeline flow rate, 0.01 m3/h, with shutdowns */
    DS_FLOW_TOTAL, /* pipeline totalizer, m3 at 0.001 (beyond float32 precision) */
    DS_RANDOM,     /* uniform random floats: the worst case */
    DS_KINDS
} ds_kind;

static const struct {
    const char *name;
    int64_t step_ms;
    int jitter_ms;
} ds_info[DS_KINDS] = {
    {"hdd_temp", 60000, 0},  {"cpu_temp", 1000, 20}, {"tank_bar", 10000, 0}, {"tank_psi", 10000, 0},
    {"vibration", 1, 0},     {"flow_rate", 1000, 5}, {"flow_total", 1000, 5}, {"random", 1000, 0},
};

typedef struct ds_state {
    ds_kind kind;
    uint64_t rng;
    int64_t t0, i;
    int64_t stamp_ms; /* if set, timestamps use this interval; values still follow the kind's own */
    double x, y, z;  /* model state */
    double hold;     /* samples left in the current episode (spike, shutdown, fill) */
    int phase;
} ds_state;

static inline uint64_t ds_mix(uint64_t x)
{
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdull;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ull;
    return x ^ (x >> 33);
}

static inline double ds_u(ds_state *s) /* uniform [0, 1) */
{
    s->rng = ds_mix(s->rng + 0x9e3779b97f4a7c15ull);
    return (double)(s->rng >> 11) / 9007199254740992.0;
}

static inline double ds_n(ds_state *s) /* standard normal */
{
    double u = ds_u(s) + 1e-12, v = ds_u(s);
    return sqrt(-2 * log(u)) * cos(2 * M_PI * v);
}

static inline double ds_round(double x, double unit) { return round(x / unit) * unit; }

static inline void ds_init(ds_state *s, ds_kind kind, uint32_t series, int64_t t0)
{
    memset(s, 0, sizeof *s);
    s->kind = kind;
    s->rng = ds_mix(((uint64_t)kind << 32) ^ (series + 1));
    s->t0 = t0;
    switch (kind) {
    case DS_HDD_TEMP: s->x = 33.0 + (series % 7) * 0.8; break;
    case DS_CPU_TEMP: s->x = 22.0; break;
    case DS_TANK_BAR:
    case DS_TANK_PSI: s->x = 2.0 + (series % 5); s->y = 0.0008; break; /* level m, fill rate m/sample */
    case DS_VIBRATION: s->y = ds_u(s) * 2 * M_PI; break;
    case DS_FLOW_RATE: s->x = 240.0 + (series % 9) * 5; break;
    case DS_FLOW_TOTAL: s->x = 12000.0 + series * 1000.0; s->z = 240.0 + (series % 9) * 5; break;
    default: break;
    }
}

/* The next sample; the value in double precision, as a float64 series would store it. */
static inline void ds_next_f64(ds_state *s, int64_t *ts_out, double *v_out)
{
    int64_t step = ds_info[s->kind].step_ms;
    int jit = ds_info[s->kind].jitter_ms;
    int64_t ts = s->t0 + s->i * (s->stamp_ms ? s->stamp_ms : step);
    if (jit)
        ts += (int64_t)(ds_u(s) * (2 * jit + 1)) - jit;
    double tsec = (double)(s->i * step) / 1000.0, v = 0;
    switch (s->kind) {
    case DS_HDD_TEMP: {
        /* follows the room slowly; the sensor value moves 0.1 at a time */
        double target = 35.0 + 2.5 * sin(2 * M_PI * tsec / 86400.0) + (s->x - 35.0) + 0.3 * ds_n(s);
        if (ds_u(s) < 0.15) {
            double cur = s->y ? s->y : s->x;
            if (target > cur + 0.1)
                cur += 0.1;
            else if (target < cur - 0.1)
                cur -= 0.1;
            s->y = cur;
        }
        v = ds_round(s->y ? s->y : s->x, 0.1);
        break;
    }
    case DS_CPU_TEMP: {
        if (s->phase == 0 && ds_u(s) < 6.0 / 86400.0) { /* about six load spikes a day */
            s->phase = 1;
            s->hold = 60 + ds_u(s) * 240;
            s->z = 95 + ds_u(s) * 5;
        }
        double idle = 22.0 + 1.5 * sin(2 * M_PI * tsec / 86400.0);
        if (s->phase == 1) { /* heating up fast */
            s->x += (s->z - s->x) * 0.25;
            if (s->x > s->z - 1)
                s->phase = 2;
        } else if (s->phase == 2) { /* under load */
            s->x = s->z + ds_n(s) * 0.8;
            if (--s->hold <= 0)
                s->phase = 3;
        } else { /* cooling down, then idle noise */
            s->x += (idle - s->x) * 0.05 + ds_n(s) * 0.15;
            if (s->phase == 3 && s->x < idle + 1)
                s->phase = 0;
        }
        v = ds_round(fmin(fmax(s->x, 20.0), 100.0), 0.1);
        break;
    }
    case DS_TANK_BAR:
    case DS_TANK_PSI: {
        /* fill to ~9.5 m, drain to ~0.5 m, rest in between */
        if (s->hold > 0) {
            s->hold--;
        } else {
            s->x += s->y;
            if (s->x > 9.5 || s->x < 0.5) {
                s->y = -s->y;
                s->hold = 200 + ds_u(s) * 1000;
            }
        }
        double bar = s->x * 0.0980665 * 0.85 + 0.0004 * ds_n(s); /* oil, rho ~ 850 kg/m3 */
        v = s->kind == DS_TANK_BAR ? ds_round(bar, 0.001) : bar * 14.5037738;
        break;
    }
    case DS_VIBRATION: {
        double g = 0.8 * sin(2 * M_PI * 50 * tsec + s->y) + 0.3 * sin(2 * M_PI * 150 * tsec + 2 * s->y) +
                   0.1 * sin(2 * M_PI * 337 * tsec) + 0.05 * ds_n(s);
        v = round(g * 8192.0) / 8192.0; /* int16 counts at 4 g full scale */
        break;
    }
    case DS_FLOW_RATE: {
        if (s->phase == 0 && ds_u(s) < 1.0 / 86400.0) { /* a shutdown about once a day */
            s->phase = 1;
            s->hold = 1800 + ds_u(s) * 1800;
        }
        if (s->phase == 1) {
            v = 0;
            if (--s->hold <= 0)
                s->phase = 0;
        } else {
            s->x += (245 - s->x) * 0.001 + 0.2 * ds_n(s);
            v = ds_round(fmax(s->x + 1.5 * ds_n(s), 0), 0.01);
        }
        break;
    }
    case DS_FLOW_TOTAL: {
        s->z += (245 - s->z) * 0.001 + 0.2 * ds_n(s);
        s->x += s->z / 3600.0;
        v = ds_round(s->x, 0.001);
        break;
    }
    default:
        v = ds_u(s) * 200 - 100;
        break;
    }
    s->i++;
    *ts_out = ts;
    *v_out = v;
}

static inline void ds_next(ds_state *s, int64_t *ts_out, float *v_out)
{
    double v;
    ds_next_f64(s, ts_out, &v);
    *v_out = (float)v;
}

/* "mixed" assigns kinds round-robin over the series. */
static inline int ds_parse(const char *name)
{
    for (int k = 0; k < DS_KINDS; k++)
        if (strcmp(name, ds_info[k].name) == 0)
            return k;
    return -1;
}

#endif
