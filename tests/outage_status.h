/* SPDX-License-Identifier: AGPL-3.0-only */
/* Waiting for the state a test asserts on, instead of a fixed sleep: after outagefs `hang`, tsdbd declares the storage
 * domain DOWN after its probe timeouts (about 2.5 s by default, longer on a loaded machine). The test asks STATUS
 * ("domain.D.state=down") and goes on the moment it holds, or fails with the last value seen after the timeout.
 * Included by test_unavailable.c and outage_server.c (which link the C client). */
#ifndef OUTAGE_STATUS_H
#define OUTAGE_STATUS_H

#include "tsdb_client.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static inline double os_now_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec * 1e3 + t.tv_nsec / 1e6;
}

/* The value of "key=" in a STATUS reply, into val ("" when the key is not there or STATUS failed). */
static inline void os_status_value(tsdbc *c, const char *key, char *val, size_t cap)
{
    char *text = NULL;
    val[0] = 0;
    if (tsdbc_status(c, &text) != 0 || !text)
        return;
    size_t kl = strlen(key);
    for (char *l = text; l && *l;) {
        char *nl = strchr(l, '\n');
        if (strncmp(l, key, kl) == 0 && l[kl] == '=') {
            size_t n = nl ? (size_t)(nl - l) - kl - 1 : strlen(l + kl + 1);
            snprintf(val, cap, "%.*s", (int)(n < cap - 1 ? n : cap - 1), l + kl + 1);
            break;
        }
        l = nl ? nl + 1 : NULL;
    }
    free(text);
}

/* Waits until the storage domain of volume vol is DOWN (STATUS domain.D.state, D from TOPOLOGY). Returns the ms it
 * took, or -1 (reason on stderr): vol not in a domain, STATUS not answering, or still not down after timeout_ms. */
static inline double os_await_domain_down(tsdbc *c, unsigned vol, int timeout_ms)
{
    tsdbc_topo *topo = NULL;
    char key[48] = "", val[64] = "";
    if (tsdbc_topology(c, &topo) != 0 || !topo) {
        fprintf(stderr, "os_await_domain_down: TOPOLOGY failed\n");
        return -1;
    }
    for (size_t i = 0; i < topo->nvols; i++)
        if (topo->vols[i].vol == vol && topo->vols[i].domain != 0xffff)
            snprintf(key, sizeof key, "domain.%u.state", topo->vols[i].domain);
    tsdbc_topology_free(topo);
    if (!key[0]) {
        fprintf(stderr, "os_await_domain_down: volume %u is in no storage domain\n", vol);
        return -1;
    }
    double t0 = os_now_ms();
    while (os_now_ms() - t0 < timeout_ms) {
        os_status_value(c, key, val, sizeof val);
        if (!strcmp(val, "down"))
            return os_now_ms() - t0;
        struct timespec d = {0, 50 * 1000000L};
        nanosleep(&d, NULL);
    }
    fprintf(stderr, "os_await_domain_down: %s did not become down in %d ms (last: '%s')\n", key, timeout_ms, val);
    return -1;
}

#endif
