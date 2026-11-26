/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * RDMA verbs device discovery for tests (docs/design-rdma.md §8, track rd-rxe): finds an active verbs device
 * (hardware InfiniBand HCA, Soft-RoCE rxe*, or Soft-iWARP siw*) without requiring root, or reports why none is
 * available so a test can skip cleanly (exit 0) unless TSDB_RDMA_REQUIRED=1 turns a skip into a failure.
 *
 * Environment knobs:
 *   TSDB_RDMA_DEV       optional "DEV" or "DEV:PORT" to pin a specific verbs device/port (e.g. rxe0:1, mlx4_0:1)
 *   TSDB_RDMA_REQUIRED  non-empty and != "0": tests fail instead of skipping when no verbs device is usable
 */
#ifndef TSDB_TESTS_RDMA_DEV_H
#define TSDB_TESTS_RDMA_DEV_H

#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "plrdma.h"

struct rdma_dev_info {
    char dev[64];   /* device name (e.g. "mlx4_0", "rxe0"), or "" if auto-IB */
    int port;       /* 1-based active port number */
    int link_layer; /* PLR_LL_IB or PLR_LL_ETH */
    char spec[80];  /* "DEV:PORT" suitable for --peer-rdma-device, or "" */
};

static inline int rdma_dev_required(void)
{
    const char *e = getenv("TSDB_RDMA_REQUIRED");
    return e && *e && strcmp(e, "0") != 0;
}

static inline int rdma_dev_probe_one(const struct plr_ops *ops, const char *name, int port, int want_ib,
                                     struct rdma_dev_info *out, char *err, size_t errlen)
{
    struct plr_dev *d = NULL;
    struct plr_port pi;
    memset(&pi, 0, sizeof pi);
    if (ops->dev_open(name, port, &d, &pi, err, errlen) != 0)
        return 0;
    ops->dev_close(d);
    if (want_ib && pi.link_layer != PLR_LL_IB)
        return 0;
    memset(out, 0, sizeof *out);
    if (name && *name) {
        snprintf(out->dev, sizeof out->dev, "%.63s", name);
        snprintf(out->spec, sizeof out->spec, "%.63s:%u", out->dev, (unsigned)pi.port);
    }
    out->port = (int)pi.port;
    out->link_layer = (int)pi.link_layer;
    return 1;
}

#ifdef __linux__
/* Scans /sys/class/infiniband for an active device matching `prefix` (NULL: any) and optionally IB-only. */
static inline int rdma_dev_scan_sysfs(const struct plr_ops *ops, const char *prefix, int want_ib,
                                      struct rdma_dev_info *out)
{
    DIR *dir = opendir("/sys/class/infiniband");
    if (!dir)
        return 0;
    char err[256];
    int found = 0;
    struct dirent *de;
    while (!found && (de = readdir(dir)) != NULL) {
        if (de->d_name[0] == '.')
            continue;
        if (prefix && strncmp(de->d_name, prefix, strlen(prefix)) != 0)
            continue;
        if (want_ib) {
            for (int p = 1; p <= 4 && !found; p++)
                found = rdma_dev_probe_one(ops, de->d_name, p, 1, out, err, sizeof err);
        } else {
            found = rdma_dev_probe_one(ops, de->d_name, 0, 0, out, err, sizeof err);
        }
    }
    closedir(dir);
    return found;
}
#endif

/*
 * Discovers an active RDMA verbs device. Returns 1 and fills *out on success; returns 0 and writes a human-readable
 * explanation into `reason` otherwise.
 */
static inline int rdma_dev_discover(struct rdma_dev_info *out, char *reason, size_t rlen)
{
    char err[256] = "";
    memset(out, 0, sizeof *out);
    const struct plr_ops *ops = plrdma_verbs(err, sizeof err);
    if (!ops) {
        snprintf(reason, rlen, "%s", err[0] ? err : "RDMA verbs not available");
        return 0;
    }
    const char *env = getenv("TSDB_RDMA_DEV");
    if (env && *env) {
        char name[64];
        int port = 0;
        const char *colon = strchr(env, ':');
        if (colon) {
            size_t nl = (size_t)(colon - env);
            if (nl == 0 || nl >= sizeof name) {
                snprintf(reason, rlen, "TSDB_RDMA_DEV=%s: invalid device name", env);
                return 0;
            }
            memcpy(name, env, nl);
            name[nl] = '\0';
            char *end = NULL;
            long p = strtol(colon + 1, &end, 10);
            if (!colon[1] || *end || p < 1 || p > 255) {
                snprintf(reason, rlen, "TSDB_RDMA_DEV=%s: invalid port", env);
                return 0;
            }
            port = (int)p;
        } else {
            if (strlen(env) >= sizeof name) {
                snprintf(reason, rlen, "TSDB_RDMA_DEV=%s: device name too long", env);
                return 0;
            }
            snprintf(name, sizeof name, "%s", env);
        }
        if (rdma_dev_probe_one(ops, name, port, 0, out, err, sizeof err))
            return 1;
        snprintf(reason, rlen, "TSDB_RDMA_DEV=%.63s: %.160s", env, err[0] ? err : "no active port");
        return 0;
    }
#ifdef __linux__
    /* Prefer a real InfiniBand HCA port (D10), then Soft-RoCE (rxe*), then Soft-iWARP (siw*). */
    if (rdma_dev_scan_sysfs(ops, NULL, 1, out))
        return 1;
    if (rdma_dev_probe_one(ops, NULL, 0, 1, out, err, sizeof err))
        return 1;
    if (rdma_dev_scan_sysfs(ops, "rxe", 0, out))
        return 1;
    if (rdma_dev_scan_sysfs(ops, "siw", 0, out))
        return 1;
#else
    if (rdma_dev_probe_one(ops, NULL, 0, 1, out, err, sizeof err))
        return 1;
#endif
    snprintf(reason, rlen, "no active RDMA verbs device (InfiniBand HCA, rxe*, or siw*) found");
    return 0;
}

#endif
