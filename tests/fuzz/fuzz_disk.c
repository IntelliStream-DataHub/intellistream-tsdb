/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The DISK metadata file parser (src/disk.c) on arbitrary bytes: disk_meta_parse must not read past the length it is
 * given, and whatever it accepts must encode back to the exact same bytes (one canonical encoding per state) and parse
 * back to the same values. A second pass fixes magic and checksum so that every structural check behind xxh64 is
 * reached directly.
 */
#include "fuzz.h"
#include "internal.h"
#include "disk.h"

int LLVMFuzzerInitialize(int *argc, char ***argv)
{
    (void)argc, (void)argv;
    return 0;
}

static void check_roundtrip(const uint8_t *p, size_t n)
{
    disk_meta d;
    if (disk_meta_parse(p, n, &d) != 0)
        return;
    if (n != DISK_META_BYTES || d.version != DISK_META_VERSION)
        abort();
    if (d.disk_class != DISK_CLASS_NVME && d.disk_class != DISK_CLASS_SSD && d.disk_class != DISK_CLASS_HDD)
        abort();
    uint8_t enc[DISK_META_BYTES];
    if (disk_meta_encode(&d, enc) != 0 || memcmp(enc, p, DISK_META_BYTES) != 0)
        abort();
    disk_meta d2;
    if (disk_meta_parse(enc, sizeof enc, &d2) != 0)
        abort();
    if (d2.version != d.version || d2.disk_class != d.disk_class || d2.sync_p50_us != d.sync_p50_us ||
        d2.node_id != d.node_id || d2.bound != d.bound || memcmp(d2.store_id, d.store_id, 16) != 0 ||
        memcmp(d2.cluster_uuid, d.cluster_uuid, 16) != 0)
        abort();
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    /* exact-size copy so ASan catches any out-of-bounds read */
    uint8_t *p = malloc(size ? size : 1);
    if (!p)
        return 0;
    if (size)
        memcpy(p, data, size);
    else
        p[0] = 0;
    check_roundtrip(p, size);

    /* second pass: fix magic and xxh64 to exercise all field checks behind the checksum */
    if (size >= DISK_META_BYTES) {
        uint8_t q[DISK_META_BYTES];
        memcpy(q, p, DISK_META_BYTES);
        memcpy(q, "TSDBDSK1", 8);
        uint64_t h = xxh64(q, 64, 0);
        for (int i = 0; i < 8; i++)
            q[64 + i] = (uint8_t)(h >> (8 * i));
        check_roundtrip(q, sizeof q);
    }

    free(p);
    return 0;
}

static void put_seed(const char *dir, const char *name, const disk_meta *d)
{
    uint8_t raw[DISK_META_BYTES];
    if (disk_meta_encode(d, raw) != 0) {
        fprintf(stderr, "fuzz_disk: cannot encode seed %s\n", name);
        exit(1);
    }
    fuzz_put(dir, name, raw, sizeof raw);
}

void fuzz_seeds(const char *dir)
{
    disk_meta d;
    memset(&d, 0, sizeof d);
    d.version = DISK_META_VERSION;
    d.disk_class = DISK_CLASS_NVME;
    for (int i = 0; i < 16; i++)
        d.store_id[i] = (uint8_t)(0x10 + i);
    d.sync_p50_us = 120;
    d.bound = false;
    put_seed(dir, "seed-nvme-unbound", &d);

    d.disk_class = DISK_CLASS_SSD;
    d.sync_p50_us = 450;
    for (int i = 0; i < 16; i++)
        d.cluster_uuid[i] = (uint8_t)(0xa0 + i);
    d.node_id = 1;
    d.bound = true;
    put_seed(dir, "seed-ssd-bound", &d);

    d.disk_class = DISK_CLASS_HDD;
    d.sync_p50_us = 8500;
    d.node_id = 42;
    put_seed(dir, "seed-hdd-bound", &d);

    uint8_t bad[DISK_META_BYTES];
    if (disk_meta_encode(&d, bad) == 0) {
        bad[12] ^= 1; /* corrupt class without updating checksum */
        fuzz_put(dir, "seed-corrupt-sum", bad, sizeof bad);
        fuzz_put(dir, "seed-truncated", bad, 40);
    }
}
