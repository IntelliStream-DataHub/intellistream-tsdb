/* SPDX-License-Identifier: AGPL-3.0-only */
/* DIVERGED (src/ecdiverge.c): the reader of the file's records (ecdiverge_parse, which the open and the CLI use) on
 * any bytes. Records are re-sealed (their offset-seeded checksum) unless bit 31 of their zero word is set (cleared,
 * left as it is), so that runs of valid records, damage between them and torn tails are all reached. Checks: no UB
 * or overread, the answer is 0 or TSDB_ERR_CORRUPT, never more records than whole ones, the same answer twice. */
#include "internal.h"
#include "ecdiverge.h"
#include "fuzz.h"
#define DIV_REC 48

static void reseal(uint8_t *b, size_t size)
{
    for (size_t q = 0; q + DIV_REC <= size; q += DIV_REC) {
        uint32_t z;
        memcpy(&z, b + q + 44, 4);
        bool keep = z >> 31;
        z &= 0x7fffffffu;
        memcpy(b + q + 44, &z, 4);
        if (!keep) {
            uint32_t c = (uint32_t)xxh64(b + q + 4, DIV_REC - 4, q);
            memcpy(b + q, &c, 4);
        }
    }
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size > 1 << 16)
        return 0;
    uint8_t *b = malloc(size ? size : 1);
    if (!b)
        return 0;
    memcpy(b, data, size);
    reseal(b, size);
    size_t n = 0;
    int rc = ecdiverge_parse(b, size, &n);
    if (rc != 0 && rc != TSDB_ERR_CORRUPT)
        abort();
    if (n * DIV_REC > size)
        abort();
    size_t n2 = 0;
    if (ecdiverge_parse(b, size, &n2) != rc || n2 != n) /* the same bytes give the same answer */
        abort();
    free(b);
    return 0;
}
