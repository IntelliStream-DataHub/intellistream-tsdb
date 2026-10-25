/*
 * MANIFEST as a follower reads it (follow_refresh): any text, with a valid
 * checksum line appended (it is no authentication), then the part set it
 * names opened. The refresh interval is so long that a bad MANIFEST never
 * outlasts the follower's patience: each input is judged on its own.
 */
#include "template.h"

static const char *D;
static eng *db;
static snap tpl;

int LLVMFuzzerInitialize(int *argc, char ***argv)
{
    (void)argc, (void)argv;
    D = fuzz_dir("manifest");
    template_build(D, true, false, NULL);
    snap_take(&tpl, D);
    tsdb_options o;
    template_opts(&o);
    o.read_only = 1;
    o.follow = 1;
    o.refresh_ms = 1000000000;
    if (eng_open(D, &o, &db) != 0) {
        fprintf(stderr, "fuzz: cannot follow the template\n");
        exit(1);
    }
    atexit(fuzz_rm_at_exit);
    return 0;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (!D)
        LLVMFuzzerInitialize(NULL, NULL);
    char *b = malloc(size + 64);
    memcpy(b, data, size);
    size_t n = size;
    if (n && b[n - 1] != '\n')
        b[n++] = '\n';
    n += (size_t)sprintf(b + n, "checksum %016" PRIx64 "\n", xxh64(b, n, 0));
    fuzz_put(D, "MANIFEST", b, n);
    free(b);
    follow_refresh(db);
    db->manifest_bad_since = 0;
    return 0;
}

void fuzz_seeds(const char *dir)
{
    const snap_file *f = tpl_find(&tpl, "MANIFEST", "");
    if (!f)
        return;
    /* without its checksum line: the harness appends one */
    size_t n = f->n;
    while (n > 1 && f->d[n - 2] != '\n')
        n--;
    fuzz_put(dir, "seed-MANIFEST", f->d, n - 1);
}
