/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * R9 pure placement planner (server/place.h; tracks r9-plan and r9-return).
 *
 * Implements the output buffer helpers and default R9 settings; `place_plan` is a no-op stub until r9-plan
 * implements re-placement after OUT/fault/drain/replace with E9 rack rules and E13 endangered-first ordering,
 * and r9-return adds return-during-rebuild rules (D8-D13, D20).
 */
#include "place.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void place_settings_default(struct place_settings *s)
{
    if (!s)
        return;
    *s = (struct place_settings){
        .rebuild_after_s = 1800,
        .opening_hold_s = 3600,
        .failstop_out_s = 60,
        .out_guard_pct = 25,
        .out_guard_hold_s = 7200,
        .repl_retain_down_gb = 64,
        .young_high_pct = 90,
        .balance_mb_s = 0,
        .rebuild_concurrency = 4,
    };
}

void place_out_init(struct place_out *out)
{
    if (!out)
        return;
    memset(out, 0, sizeof *out);
}

void place_out_free(struct place_out *out)
{
    if (!out)
        return;
    free(out->props);
    free(out->logs);
    memset(out, 0, sizeof *out);
}

int place_out_push(struct place_out *out, const struct place_prop *p)
{
    if (!out || !p)
        return -1;
    if (out->nprops == out->cap_props) {
        uint32_t ncap = out->cap_props ? out->cap_props * 2 : 16;
        struct place_prop *np = realloc(out->props, (size_t)ncap * sizeof *np);
        if (!np)
            return -1;
        out->props = np;
        out->cap_props = ncap;
    }
    out->props[out->nprops++] = *p;
    return 0;
}

int place_out_log(struct place_out *out, const char *fmt, ...)
{
    if (!out || !fmt)
        return -1;
    if (out->nlogs == out->cap_logs) {
        uint32_t ncap = out->cap_logs ? out->cap_logs * 2 : 8;
        struct place_log *nl = realloc(out->logs, (size_t)ncap * sizeof *nl);
        if (!nl)
            return -1;
        out->logs = nl;
        out->cap_logs = ncap;
    }
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(out->logs[out->nlogs].msg, sizeof out->logs[out->nlogs].msg, fmt, ap);
    va_end(ap);
    out->nlogs++;
    return 0;
}

void place_plan(const struct place_in *in, struct place_out *out)
{
    /* Stub until r9-plan and r9-return fill this in. */
    (void)in;
    (void)out;
}
