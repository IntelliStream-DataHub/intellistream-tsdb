/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Configuration files (tools/props.c, tools/cfg.c) on arbitrary bytes, against the real key table of tsdbd. The
 * parser must not read past its input; whatever it accepts must hand over keys without NUL and values that split
 * and unescape without a failure that has no message; and a configuration that loads must print as a file
 * (cfg_print_effective) that loads again to the same settings: the printout of --check-config is itself a valid
 * config.
 *
 * Input: the text of the file.
 */
#include "fuzz.h"

#include "../../server/tsdbd_keys.h"
#include "../../tools/props.h"

int LLVMFuzzerInitialize(int *argc, char ***argv)
{
    (void)argc, (void)argv;
    return 0;
}

static void same_sets(const struct cfg *a, const struct cfg *b)
{
    for (size_t t = 0; t < TSDBD_NKEYS; t++) {
        const struct cfg_key *k = &tsdbd_keys[t];
        size_t j = 0;
        for (size_t i = 0; i < a->n; i++) {
            if (a->s[i].k != k)
                continue;
            while (j < b->n && b->s[j].k != k)
                j++;
            if (j == b->n)
                abort(); /* a setting of the file is missing from its printout */
            if (a->s[i].bval != b->s[j].bval || (a->s[i].arg == NULL) != (b->s[j].arg == NULL) ||
                (a->s[i].arg && strcmp(a->s[i].arg, b->s[j].arg)))
                abort();
            j++;
        }
    }
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    /* an exact-size copy: a read past the end is the sanitizer's to find */
    char *p = malloc(size ? size : 1);
    if (!p)
        return 0;
    memcpy(p, data, size);
    char err[512];
    props pr;
    if (props_parse(p, size, &pr, err, sizeof err) == 0) {
        int last = 0;
        for (size_t i = 0; i < pr.n; i++) {
            if (!pr.v[i].key[0] || pr.v[i].line < last || pr.v[i].line < 1)
                abort();
            last = pr.v[i].line;
            char *v;
            if (props_value(pr.v[i].raw, &v, err, sizeof err) == 0)
                free(v);
            else if (!err[0])
                abort();
            char **items;
            size_t n;
            if (props_split(pr.v[i].raw, &items, &n, err, sizeof err) == 0) {
                for (size_t j = 0; j < n; j++)
                    if (!items[j][0])
                        abort();
                props_items_free(items, n);
            } else if (!err[0]) {
                abort();
            }
        }
        props_free(&pr);
    } else if (!err[0]) {
        abort();
    }

    struct cfg c, c2;
    if (cfg_load_text(p, size, "fuzz", tsdbd_keys, TSDBD_NKEYS, &c, err, sizeof err) == 0) {
        char *out = NULL;
        size_t outn = 0;
        FILE *f = open_memstream(&out, &outn);
        char *argv0[] = {"tsdbd"};
        if (!f)
            abort();
        cfg_print_effective(f, &c, tsdbd_keys, TSDBD_NKEYS, 1, argv0);
        fclose(f);
        if (cfg_load_text(out, outn, "printout", tsdbd_keys, TSDBD_NKEYS, &c2, err, sizeof err) != 0) {
            fprintf(stderr, "the printout does not load: %s\n%s\n", err, out);
            abort();
        }
        same_sets(&c, &c2);
        size_t na;
        char **a = cfg_args(&c2, 1, argv0, NULL, &na);
        free(a);
        cfg_free(&c2);
        free(out);
        cfg_free(&c);
    } else if (!err[0]) {
        abort();
    }
    free(p);
    return 0;
}

static void seed(const char *dir, const char *name, const char *text)
{
    fuzz_put(dir, name, text, strlen(text));
}

void fuzz_seeds(const char *dir)
{
    seed(dir, "seed-basic",
         "# tsdbd\nstorage.dir = /data/tsdb\nserver.port = 7070\nserver.io = epoll\nmetrics.port = auto\n"
         "health.domains = /mnt/nfs1, /mnt/nfs2\nserver.send.timeout = 5m\nstorage.fsync = true\n"
         "latest.cache = 1G\nserver.max.request.memory = 2G\n");
    seed(dir, "seed-separators", "storage.dir: /a\nserver.port 7071\n  server.workers\t=\t4\n! comment\n\n");
    seed(dir, "seed-continuation", "health.domains = /a,\\\n    /b,\\\n    /c\nstorage.dir = /x\\\n  y\n");
    seed(dir, "seed-escapes",
         "storage.dir = /d\\u00e9\\ta\\\\b\\,c\\ \ncluster.rack = r\\uD83D\\uDE00\nhealth.domains = a\\,b, c\\ , \\ d\n");
    seed(dir, "seed-crlf", "storage.dir = /a\r\nserver.port = 1\r\n# c\r\nserver.workers = 2\r");
    seed(dir, "seed-bom", "\xEF\xBB\xBF" "storage.dir = /a\n");
    seed(dir, "seed-errors", "storage.dir = /a\nstorage.dir = /b\nserver.port = x\nnope = 1\nserver.io = \\q\n");
    seed(dir, "seed-units",
         "wal.sync = 10ms\nmerge.idle = 1h\nflush.interval = 1500ms\nstorage.partition = 7d\nlatest.cache = 512M\n"
         "server.frame.timeout = -5\nauth.login.timeout = 1d\n");
    seed(dir, "seed-flags",
         "storage.read.only = true\nstorage.follow = false\nauth.enabled = false\nstats.enabled = false\n"
         "cluster.enabled = true\ncluster.read.barrier = off\n");
    seed(dir, "seed-latin1", "storage.dir = caf\xE9\n");
    seed(dir, "seed-trailing-backslash", "storage.dir = /a\\");
    seed(dir, "seed-empty", "");
}
