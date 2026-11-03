/* Volumes: series stored in directories of their own, routed by the root. */
#define _GNU_SOURCE
#include "tsdb.h"
#include "tsdb_calendar.h"

#include <dirent.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <limits.h>
#include <unistd.h>

#include "portable.h"

static int failures;
#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            failures++;                                                              \
        }                                                                            \
    } while (0)
#define OK(expr)                                                                                    \
    do {                                                                                            \
        int rc_ = (expr);                                                                           \
        if (rc_ != 0) {                                                                             \
            fprintf(stderr, "%s:%d: %s -> %s\n", __FILE__, __LINE__, #expr, tsdb_strerror(rc_)); \
            exit(1);                                                                                \
        }                                                                                           \
    } while (0)

#define NS 6

uint64_t xxh64(const void *data, size_t len, uint64_t seed); /* the library's, to forge a record that checks out */
#define T0 1700000000000LL
#define SPAN 20000

/* the expected value at each (series, time slot); NAN-free: 0 means unset */
static float want[NS][SPAN];

static uint64_t rng = 12345;
static uint64_t rnd(void)
{
    rng ^= rng << 13;
    rng ^= rng >> 7;
    rng ^= rng << 17;
    return rng;
}

static void check_all(tsdb *db, const tsdb_sid *ids)
{
    for (int s = 0; s < NS; s++) {
        tsdb_points p;
        OK(tsdb_query_points(db, ids[s], INT64_MIN, INT64_MAX, &p));
        size_t k = 0;
        bool ok = true;
        for (int i = 0; i < SPAN; i++) {
            if (want[s][i] == 0)
                continue;
            if (k >= p.n || p.ts[k] != T0 + (int64_t)i * 1000 || p.values[k] != want[s][i])
                ok = false;
            k++;
        }
        CHECK(ok && k == p.n);
        tsdb_bucket *b;
        size_t nb;
        OK(tsdb_query_agg(db, ids[s], INT64_MIN, INT64_MAX, 0, &b, &nb));
        CHECK(nb == (k ? 1u : 0u) && (!nb || b[0].count == k));
        free(b);
        tsdb_points_free(&p);
    }
}

typedef struct wjob {
    tsdb *db;
    tsdb_sid id;
    int s;
} wjob;

static void *vol_writer(void *arg)
{
    wjob *j = arg;
    for (int i = 0; i < 2000; i++) {
        float v = (float)(j->s * 1000 + i + 1);
        int slot = i * 7 % SPAN;
        if (tsdb_append(j->db, j->id, T0 + (int64_t)slot * 1000, v) == 0)
            want[j->s][slot] = v;
    }
    return NULL;
}

static void append_bytes(const char *path, const void *data, size_t n)
{
    int fd = open(path, O_WRONLY | O_APPEND);
    CHECK(fd >= 0 && write(fd, data, n) == (ssize_t)n);
    if (fd >= 0)
        close(fd);
}

static void flip_byte(const char *path, off_t off)
{
    int fd = open(path, O_RDWR);
    unsigned char c = 0;
    CHECK(fd >= 0 && pread(fd, &c, 1, off) == 1);
    c ^= 0x5a;
    CHECK(fd >= 0 && pwrite(fd, &c, 1, off) == 1);
    if (fd >= 0)
        close(fd);
}

static off_t file_size(const char *path)
{
    struct stat st;
    return stat(path, &st) == 0 ? st.st_size : -1;
}

static bool exists(const char *path)
{
    struct stat st;
    return lstat(path, &st) == 0;
}

/* Volume paths are checked wherever a volume is added, and a database opened
 * with create_volumes = 0 (tsdbd) never adds one for a path it is given. */
static void test_paths(const char *base)
{
    char top[512], root[600], vols[700], p[900], q[1200], other[600], cmd[700];
    snprintf(top, sizeof top, "%s/tsdb-volp-%d", base, (int)getpid());
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", top);
    if (system(cmd) != 0 || mkdir(top, 0755) != 0)
        exit(1);
    snprintf(root, sizeof root, "%s/root", top);
    snprintf(vols, sizeof vols, "%s/VOLUMES", root);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    CHECK(o.create_volumes == 1); /* embedded default */
    tsdb *db;
    tsdb_sid id;
    uint32_t v;
    OK(tsdb_open(root, &o, &db));
    OK(tsdb_series(db, "plain", &id));

    /* the audit's proof of concept: a newline split the VOLUMES line and the database never opened again */
    snprintf(p, sizeof p, "%s/vol\ny", top);
    CHECK(tsdb_series_at(db, "evil", p, &id) == TSDB_ERR_INVALID);
    CHECK(!exists(p));
    CHECK(file_size(vols) <= 0);
    const char *bad_chars[] = {"\t", "\r", "\x01", "\x1f", "\x7f"};
    for (size_t i = 0; i < sizeof bad_chars / sizeof *bad_chars; i++) {
        snprintf(p, sizeof p, "%s/v%sz", top, bad_chars[i]);
        CHECK(tsdb_volume_add(db, p, &v) == TSDB_ERR_INVALID);
        CHECK(tsdb_volume_add_dir(root, p, &v) == TSDB_ERR_INVALID);
        CHECK(!exists(p));
    }
    /* relative, ".", "..", the root itself, inside it, the file system root */
    snprintf(p, sizeof p, "%s/root/../va", top);
    CHECK(tsdb_volume_add(db, p, &v) == TSDB_ERR_INVALID);
    CHECK(!exists(p));
    snprintf(p, sizeof p, "%s/./va", top);
    CHECK(tsdb_volume_add(db, p, &v) == TSDB_ERR_INVALID);
    CHECK(tsdb_volume_add(db, "relative/va", &v) == TSDB_ERR_INVALID);
    CHECK(tsdb_volume_add(db, "", &v) == TSDB_ERR_INVALID);
    CHECK(tsdb_volume_add(db, NULL, &v) == TSDB_ERR_INVALID);
    CHECK(tsdb_volume_add(db, root, &v) == TSDB_ERR_INVALID);
    CHECK(tsdb_volume_add(db, top, &v) == TSDB_ERR_INVALID); /* contains the root */
    CHECK(tsdb_volume_add(db, "/", &v) == TSDB_ERR_INVALID);
    snprintf(p, sizeof p, "%s/inside", root);
    CHECK(tsdb_volume_add(db, p, &v) == TSDB_ERR_INVALID);
    CHECK(!exists(p)); /* refused: the directory it made is gone again */
    CHECK(file_size(vols) <= 0);

    /* a good one; then nested in it, containing it, and the same one again */
    char va[600];
    snprintf(va, sizeof va, "%s/va", top);
    OK(tsdb_volume_add(db, va, &v));
    CHECK(v == 1 && tsdb_volume_count(db) == 2);
    snprintf(p, sizeof p, "%s/nested", va);
    CHECK(tsdb_volume_add(db, p, &v) == TSDB_ERR_INVALID);
    CHECK(!exists(p));
    snprintf(p, sizeof p, "%s//va/", top); /* the same, spelled differently */
    OK(tsdb_volume_add(db, p, &v));
    CHECK(v == 1 && tsdb_volume_count(db) == 2);

    /* another database's root (the audit's second finding): refused, and nothing written into it */
    snprintf(other, sizeof other, "%s/other", top);
    tsdb *db2;
    OK(tsdb_open(other, &o, &db2));
    OK(tsdb_series(db2, "b.own", &id));
    OK(tsdb_close(db2));
    CHECK(tsdb_series_at(db, "x", other, &id) == TSDB_ERR_INVALID);
    CHECK(tsdb_volume_add_dir(root, other, &v) == TSDB_ERR_INVALID);
    snprintf(q, sizeof q, "%s/ROOT_ID", other);
    CHECK(!exists(q));
    OK(tsdb_open(other, &o, &db2));
    CHECK(tsdb_series_count(db2) == 1);
    OK(tsdb_close(db2));
    /* a directory with tsdb files of no database */
    snprintf(p, sizeof p, "%s/stray", top);
    CHECK(mkdir(p, 0755) == 0);
    snprintf(q, sizeof q, "%s/MANIFEST", p);
    int mfd = open(q, O_WRONLY | O_CREAT | O_CLOEXEC, 0644);
    CHECK(mfd >= 0);
    close(mfd);
    CHECK(tsdb_volume_add(db, p, &v) == TSDB_ERR_INVALID);
    snprintf(q, sizeof q, "%s/ROOT_ID", p);
    CHECK(!exists(q));
    OK(tsdb_close(db));
    OK(tsdb_open(root, &o, &db)); /* nothing above damaged the database */
    CHECK(tsdb_volume_count(db) == 2);
    OK(tsdb_close(db));

    /* create_volumes = 0, as in tsdbd: a path that is not a volume is NOTFOUND and creates nothing */
    o.create_volumes = 0;
    OK(tsdb_open(root, &o, &db));
    snprintf(p, sizeof p, "%s/not_a_volume", top);
    off_t before = file_size(vols);
    CHECK(tsdb_series_at(db, "n", p, &id) == TSDB_ERR_NOTFOUND);
    CHECK(tsdb_series_create(db, "n", TSDB_INT64, p, &id) == TSDB_ERR_NOTFOUND);
    CHECK(!exists(p) && file_size(vols) == before && tsdb_volume_count(db) == 2);
    CHECK(tsdb_series_lookup(db, "n", &id) == TSDB_ERR_NOTFOUND);
    snprintf(p, sizeof p, "%s/vol\ny", top);
    CHECK(tsdb_series_at(db, "evil", p, &id) == TSDB_ERR_INVALID);
    /* an existing volume is matched as before: spelled with extra slashes, or through a symlink */
    tsdb_sid on_a;
    snprintf(p, sizeof p, "%s//va/", top);
    OK(tsdb_series_at(db, "on.a", p, &on_a));
    CHECK(strcmp(tsdb_series_path(db, on_a), va) == 0);
    snprintf(q, sizeof q, "%s/link_to_va", top);
    CHECK(symlink(va, q) == 0);
    OK(tsdb_series_create(db, "on.a2", TSDB_INT64, q, &id));
    CHECK(strcmp(tsdb_series_path(db, id), va) == 0);
    /* added while the database is open elsewhere (tsdb-cli while tsdbd runs): found when first named */
    char vb[600], vc[600];
    snprintf(vb, sizeof vb, "%s/vb", top);
    snprintf(vc, sizeof vc, "%s/vc", top);
    OK(tsdb_volume_add_dir(root, vb, &v));
    CHECK(v == 2 && tsdb_volume_count(db) == 2);
    tsdb_sid on_b;
    OK(tsdb_series_at(db, "on.b", vb, &on_b));
    CHECK(tsdb_volume_count(db) == 3 && strcmp(tsdb_series_path(db, on_b), vb) == 0);
    OK(tsdb_append(db, on_b, T0, 2.0));
    /* one added outside, then one by the open writer: the writer opens both, in order */
    OK(tsdb_volume_add_dir(root, vc, &v));
    CHECK(v == 3);
    char vd[600];
    snprintf(vd, sizeof vd, "%s/vd", top);
    OK(tsdb_volume_add(db, vd, &v));
    CHECK(v == 4 && tsdb_volume_count(db) == 5);
    CHECK(strcmp(tsdb_volume_path(db, 3), vc) == 0 && strcmp(tsdb_volume_path(db, 4), vd) == 0);
    OK(tsdb_close(db));
    /* a reader opens a volume no writer has opened yet (it holds META and ROOT_ID) */
    char ve[600];
    snprintf(ve, sizeof ve, "%s/ve", top);
    OK(tsdb_volume_add_dir(root, ve, &v));
    CHECK(v == 5);
    tsdb_options ro = o;
    ro.read_only = 1;
    OK(tsdb_open(root, &ro, &db));
    CHECK(tsdb_volume_count(db) == 6);
    OK(tsdb_close(db));
    OK(tsdb_open(root, &o, &db));
    CHECK(tsdb_volume_count(db) == 6);
    OK(tsdb_series_lookup(db, "on.b", &id));
    CHECK(id == on_b);
    tsdb_points pts;
    OK(tsdb_query_points(db, on_b, INT64_MIN, INT64_MAX, &pts));
    CHECK(pts.n == 1);
    tsdb_points_free(&pts);
    OK(tsdb_close(db));
    /* not a database */
    snprintf(p, sizeof p, "%s/none", top);
    CHECK(tsdb_volume_add_dir(p, vb, &v) == TSDB_ERR_NOTFOUND);
    CHECK(tsdb_volume_add_dir(va, vb, &v) == TSDB_ERR_NOTFOUND); /* a volume, not a root */

    if (system(cmd) != 0)
        exit(1);
    printf("  volume paths checked where volumes are added, none added with create_volumes = 0\n");
}

/* A fresh directory top = base/tsdb-<tag>-<pid> holding an empty database top/root. */
static void fresh(const char *base, const char *tag, char *top, size_t cap, char *root, size_t rcap)
{
    char cmd[1200];
    snprintf(top, cap, "%s/tsdb-%s-%d", base, tag, (int)getpid());
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", top);
    if (system(cmd) != 0 || mkdir(top, 0755) != 0)
        exit(1);
    char canon[PATH_MAX];
    if (!realpath(top, canon)) /* VOLUMES holds canonical paths, and the tests compare with them */
        exit(1);
    snprintf(top, cap, "%.500s", canon);
    snprintf(root, rcap, "%s/root", top);
    tsdb_options o;
    tsdb_options_init(&o);
    tsdb *db;
    OK(tsdb_open(root, &o, &db));
    OK(tsdb_close(db));
}

static void remove_tree(const char *top)
{
    char cmd[1200];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", top);
    if (system(cmd) != 0)
        exit(1);
}

static void write_file(const char *path, const char *text)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    CHECK(fd >= 0 && write(fd, text, strlen(text)) == (ssize_t)strlen(text));
    if (fd >= 0)
        close(fd);
}

static void read_text(const char *path, char *buf, size_t cap)
{
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    ssize_t n = fd >= 0 ? read(fd, buf, cap - 1) : -1;
    CHECK(n >= 0);
    buf[n > 0 ? n : 0] = '\0';
    if (fd >= 0)
        close(fd);
}

/* Writes the META of `dir` as `src`'s with its "volume=" line set to `vol` (-1: none). */
static void meta_with_volume(const char *src, const char *dir, long vol)
{
    char in[2048], out[2100], path[1200];
    snprintf(path, sizeof path, "%s/META", src);
    read_text(path, in, sizeof in);
    size_t n = 0;
    for (char *l = in, *next; *l; l = next) {
        next = strchr(l, '\n');
        next = next ? next + 1 : l + strlen(l);
        if (strncmp(l, "volume=", 7) != 0) {
            memcpy(out + n, l, (size_t)(next - l));
            n += (size_t)(next - l);
        }
    }
    out[n] = '\0';
    if (vol >= 0)
        snprintf(out + n, sizeof out - n, "volume=%ld\n", vol);
    snprintf(path, sizeof path, "%s/META", dir);
    write_file(path, out);
}

/* What an add refuses beyond the path's spelling: a directory that contains a
 * volume, another database's volume, a second path to (a copy of) one of ours,
 * a symlink to a name with a newline. Nothing is written into a refused one. */
static void test_refused_dirs(const char *base)
{
    char top[600], root[700], p[900], q[1000], id_txt[64] = {0};
    fresh(base, "volr", top, sizeof top, root, sizeof root);
    snprintf(p, sizeof p, "%s/ID", root);
    int fd = open(p, O_RDONLY);
    CHECK(fd >= 0 && read(fd, id_txt, sizeof id_txt - 1) > 0);
    close(fd);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.create_volumes = 0;
    tsdb *db;
    tsdb_sid id;
    uint32_t v;
    OK(tsdb_open(root, &o, &db));

    /* contains a volume, but not the root */
    snprintf(p, sizeof p, "%s/ns", top);
    CHECK(mkdir(p, 0755) == 0);
    snprintf(p, sizeof p, "%s/ns/x", top);
    CHECK(mkdir(p, 0755) == 0);
    snprintf(q, sizeof q, "%s/ns/x/v", top);
    OK(tsdb_volume_add(db, q, &v));
    CHECK(v == 1);
    CHECK(tsdb_volume_add(db, p, &v) == TSDB_ERR_INVALID);
    CHECK(tsdb_volume_add_dir(root, p, &v) == TSDB_ERR_INVALID);
    snprintf(q, sizeof q, "%s/ROOT_ID", p);
    CHECK(!exists(q));

    /* a volume of another database (its ROOT_ID) */
    snprintf(p, sizeof p, "%s/foreign", top);
    CHECK(mkdir(p, 0755) == 0);
    snprintf(q, sizeof q, "%s/ROOT_ID", p);
    write_file(q, strncmp(id_txt, "0123456789abcdef", 16) ? "0123456789abcdef\n" : "fedcba9876543210\n");
    CHECK(tsdb_volume_add(db, p, &v) == TSDB_ERR_INVALID);
    CHECK(tsdb_volume_add_dir(root, p, &v) == TSDB_ERR_INVALID);
    snprintf(q, sizeof q, "%s/META", p);
    CHECK(!exists(q));

    /* ours (our ROOT_ID, engine files), but not listed: the same volume under a second path, or a copy */
    snprintf(p, sizeof p, "%s/second", top);
    CHECK(mkdir(p, 0755) == 0);
    snprintf(q, sizeof q, "%s/ROOT_ID", p);
    write_file(q, id_txt);
    snprintf(q, sizeof q, "%s/MANIFEST", p);
    write_file(q, "");
    CHECK(tsdb_volume_add(db, p, &v) == TSDB_ERR_INVALID);
    CHECK(tsdb_volume_add_dir(root, p, &v) == TSDB_ERR_INVALID);
    /* ours without engine files is a crashed add: taken over */
    snprintf(p, sizeof p, "%s/crashed", top);
    CHECK(mkdir(p, 0755) == 0);
    snprintf(q, sizeof q, "%s/ROOT_ID", p);
    write_file(q, id_txt);
    OK(tsdb_volume_add_dir(root, p, &v));
    CHECK(v == 2);

    /* a symlink to a name with a newline: refused after realpath, VOLUMES unharmed */
    snprintf(p, sizeof p, "%s/nl\nname", top);
    CHECK(mkdir(p, 0755) == 0);
    snprintf(q, sizeof q, "%s/link_nl", top);
    CHECK(symlink(p, q) == 0);
    CHECK(tsdb_volume_add(db, q, &v) == TSDB_ERR_INVALID);
    CHECK(tsdb_volume_add_dir(root, q, &v) == TSDB_ERR_INVALID);
    CHECK(tsdb_series_at(db, "s", q, &id) == TSDB_ERR_NOTFOUND);
    snprintf(q, sizeof q, "%s/ROOT_ID", p);
    CHECK(!exists(q));
    OK(tsdb_close(db));
    OK(tsdb_open(root, &o, &db));
    CHECK(tsdb_volume_count(db) == 3);
    OK(tsdb_close(db));
    remove_tree(top);
    printf("  adds refuse a directory containing a volume, another database's volume, a copy of ours, a link to a "
           "bad name\n");
}

/* A volume may be a directory that already held the user's files (tsdb_volume_add_dir takes one with no engine
 * files): the cleanup at open removes the engine's own leftovers by their exact names and nothing else of theirs. */
static void test_user_files_in_volume(const char *base)
{
    char top[600], root[700], vol[800], p[1000];
    fresh(base, "volf", top, sizeof top, root, sizeof root);
    snprintf(vol, sizeof vol, "%s/data", top);
    CHECK(mkdir(vol, 0755) == 0);
    static const char *const users[] = {"tmp_notes.txt",  "tmp_x",       "tmp_1_2.part.bak", "tmp_1_x.part",
                                        "META.tmpfile",   "META.tmp.notes", "META.tmp.a.b.1", "txn_list",   "txn_notes.intent", "retired_old.csv",
                                        "retired_p1.part", "wal_cafe.log", "wal_0000000000000001.log.bak",
                                        "plan.part",      "p1_2.part"};
    size_t nu = sizeof users / sizeof *users;
    for (size_t i = 0; i < nu; i++) {
        snprintf(p, sizeof p, "%s/%s", vol, users[i]);
        write_file(p, "the user's\n");
    }
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    tsdb_sid id;
    uint32_t v;
    OK(tsdb_volume_add_dir(root, vol, &v));
    tsdb *db;
    OK(tsdb_open(root, &o, &db));
    OK(tsdb_series_at(db, "s", vol, &id));
    for (int i = 0; i < 100; i++)
        OK(tsdb_append(db, id, T0 + i * 1000, (float)i));
    OK(tsdb_close(db));
    /* what a crash leaves behind: a part writer's tmp file and a half-written META */
    char leftover[1000], meta_tmp[1000];
    snprintf(leftover, sizeof leftover, "%s/tmp_%d_7.part", vol, (int)getpid());
    write_file(leftover, "junk");
    snprintf(meta_tmp, sizeof meta_tmp, "%s/META.tmp", vol);
    write_file(meta_tmp, "junk");
    char add_tmp[1000]; /* and a volume add's own (meta_tmp_make) */
    snprintf(add_tmp, sizeof add_tmp, "%s/META.tmp.host-1.%d.%d", vol, 4242, 4243);
    write_file(add_tmp, "junk");
    OK(tsdb_open(root, &o, &db));
    tsdb_points pts;
    OK(tsdb_query_points(db, id, INT64_MIN, INT64_MAX, &pts));
    CHECK(pts.n == 100);
    tsdb_points_free(&pts);
    OK(tsdb_close(db));
    CHECK(!exists(leftover) && !exists(meta_tmp) && !exists(add_tmp));
    for (size_t i = 0; i < nu; i++) {
        snprintf(p, sizeof p, "%s/%s", vol, users[i]);
        CHECK(exists(p) && file_size(p) == 11);
        if (!exists(p))
            fprintf(stderr, "  %s was deleted\n", users[i]);
    }
    remove_tree(top);
    printf("  a volume directory's own files (tmp_notes.txt, txn_list, retired_old.csv, wal_cafe.log...) survive "
           "the open; the engine's leftovers go\n");
}

/* VOLUMES changed under a running writer: a torn tail (an add killed in its
 * write) is cut, so volumes still come and go as before; a line rewritten to
 * name another directory is damage, reported once per change. */
static void test_volumes_under_writer(const char *base)
{
    char top[600], root[700], vols[800], v1[800], v2[800], v3[800], w[800], line[1000];
    fresh(base, "volw", top, sizeof top, root, sizeof root);
    snprintf(vols, sizeof vols, "%s/VOLUMES", root);
    snprintf(v1, sizeof v1, "%s/v1", top);
    snprintf(v2, sizeof v2, "%s/v2", top);
    snprintf(v3, sizeof v3, "%s/v3", top);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.create_volumes = 0;
    tsdb *db;
    tsdb_sid id;
    uint32_t v;
    OK(tsdb_open(root, &o, &db));
    OK(tsdb_volume_add_dir(root, v1, &v));
    off_t good = file_size(vols);
    append_bytes(vols, "1234abcd /half", 14);
    OK(tsdb_series_at(db, "on.v1", v1, &id)); /* the writer reads VOLUMES, and cuts the tail */
    CHECK(tsdb_volume_count(db) == 2 && file_size(vols) == good);
    append_bytes(vols, "1234abcd /half", 14);
    OK(tsdb_volume_add_dir(root, v2, &v)); /* an add cuts it too */
    CHECK(v == 2);
    OK(tsdb_series_at(db, "on.v2", v2, &id));
    CHECK(tsdb_volume_count(db) == 3);
    snprintf(w, sizeof w, "%s/nowhere", top);
    CHECK(tsdb_series_at(db, "x", w, &id) == TSDB_ERR_NOTFOUND);
    OK(tsdb_close(db));

    /* line 1 rewritten (with a check that passes) to name another directory, while a writer runs */
    OK(tsdb_open(root, &o, &db));
    char *txt = NULL;
    size_t len = 0;
    FILE *f = fopen(vols, "r");
    CHECK(f && getdelim(&txt, &len, '\0', f) > 0);
    if (f)
        fclose(f);
    char *rest = txt ? strchr(txt, '\n') : NULL;
    snprintf(w, sizeof w, "%s/v9", top);
    snprintf(line, sizeof line, "%08x %s%s", (uint32_t)xxh64(w, strlen(w), 1), w, rest ? rest : "\n");
    write_file(vols, line);
    free(txt);
    OK(tsdb_volume_add_dir(root, v3, &v));
    CHECK(v == 3);
    CHECK(tsdb_series_at(db, "on.v3", v3, &id) == TSDB_ERR_CORRUPT);
    CHECK(tsdb_series_at(db, "on.v3", v3, &id) == TSDB_ERR_CORRUPT); /* not read again, but still refused */
    CHECK(tsdb_series_lookup(db, "on.v3", &id) == TSDB_ERR_NOTFOUND);
    OK(tsdb_close(db));
    remove_tree(top);
    printf("  a torn VOLUMES tail under a running writer is cut; a rewritten line is damage\n");
}

/* Adds from several processes at once, while a writer has the database open:
 * the lock on VOLUMES keeps their lines whole and numbered, so none is lost. */
static void test_concurrent_adds(const char *base)
{
    enum { PROCS = 8, EACH = 15 };
    char top[600], root[700], p[800];
    fresh(base, "volc", top, sizeof top, root, sizeof root);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.create_volumes = 0;
    tsdb *db;
    tsdb_sid id;
    uint32_t v;
    OK(tsdb_open(root, &o, &db));
    pid_t kids[PROCS];
    for (int k = 0; k < PROCS; k++) {
        if ((kids[k] = fork()) == 0) {
            int bad = 0;
            for (int i = 0; i < EACH; i++) {
                snprintf(p, sizeof p, "%s/c%d_%d", top, k, i);
                bad += tsdb_volume_add_dir(root, p, &v) != 0;
            }
            _exit(bad);
        }
        CHECK(kids[k] > 0);
    }
    for (int k = 0; k < PROCS; k++) {
        int st = -1;
        CHECK(kids[k] > 0 && waitpid(kids[k], &st, 0) == kids[k] && WIFEXITED(st) && WEXITSTATUS(st) == 0);
    }
    for (int k = 0; k < PROCS; k++)
        for (int i = 0; i < EACH; i++) {
            char name[32];
            snprintf(p, sizeof p, "%s/c%d_%d", top, k, i);
            snprintf(name, sizeof name, "s%d_%d", k, i);
            CHECK(tsdb_series_at(db, name, p, &id) == 0);
        }
    CHECK(tsdb_volume_count(db) == 1 + PROCS * EACH);
    OK(tsdb_close(db));
    OK(tsdb_open(root, &o, &db));
    CHECK(tsdb_volume_count(db) == 1 + PROCS * EACH && tsdb_series_count(db) == PROCS * EACH);
    OK(tsdb_close(db));
    remove_tree(top);
    printf("  %d processes added %d volumes at once, none lost\n", PROCS, PROCS * EACH);
}

struct opener {
    const char *root;
    tsdb_options o;
    tsdb *db;
    int rc;
    volatile int done;
};

static void *open_thread(void *arg)
{
    struct opener *x = arg;
    x->rc = tsdb_open(x->root, &x->o, &x->db);
    __atomic_store_n(&x->done, 1, __ATOMIC_RELEASE);
    return NULL;
}

/* A writer opening while an add is half way through its line waits for it,
 * rather than cutting the line as a torn tail. */
static void test_open_waits_for_add(const char *base)
{
    char top[600], root[700], vols[800], v1[800], *txt = NULL;
    size_t len = 0;
    fresh(base, "volo", top, sizeof top, root, sizeof root);
    snprintf(vols, sizeof vols, "%s/VOLUMES", root);
    snprintf(v1, sizeof v1, "%s/v1", top);
    uint32_t v;
    OK(tsdb_volume_add_dir(root, v1, &v));
    FILE *f = fopen(vols, "r");
    CHECK(f && getdelim(&txt, &len, '\0', f) > 0);
    if (f)
        fclose(f);
    size_t n = txt ? strlen(txt) : 0;
    int fd = open(vols, O_RDWR | O_TRUNC);
    struct flock fl = {.l_type = F_WRLCK, .l_whence = SEEK_SET};
    CHECK(fd >= 0 && fcntl(fd, F_OFD_SETLK, &fl) == 0); /* as an add holds it */
    CHECK(n > 4 && write(fd, txt, n / 2) == (ssize_t)(n / 2));
    struct opener x = {.root = root};
    tsdb_options_init(&x.o);
    x.o.fsync = 0;
    pthread_t th;
    CHECK(pthread_create(&th, NULL, open_thread, &x) == 0);
    usleep(300000);
    CHECK(!__atomic_load_n(&x.done, __ATOMIC_ACQUIRE));
    CHECK(write(fd, txt + n / 2, n - n / 2) == (ssize_t)(n - n / 2));
    close(fd); /* and the lock */
    pthread_join(th, NULL);
    CHECK(x.rc == 0);
    if (x.rc == 0) {
        CHECK(tsdb_volume_count(x.db) == 2);
        OK(tsdb_close(x.db));
    }
    free(txt);
    remove_tree(top);
    printf("  a writer's open waits for an add in progress\n");
}

/* A volume added a second time under another path (an NFS export mounted twice, a bind mount; here a rename and a
 * symlink to the new name): its META names line 1, which is this very directory, so the add is refused and META
 * keeps its number. It used to be rewritten to volume 2, and every later open failed. When line 1 cannot be
 * checked (its path is gone), the add is refused too. */
static void test_readd_other_path(const char *base)
{
    char top[600], root[700], a[800], b[800], meta[900], mtext[2048];
    fresh(base, "volre", top, sizeof top, root, sizeof root);
    snprintf(a, sizeof a, "%s/a", top);
    snprintf(b, sizeof b, "%s/b", top);
    snprintf(meta, sizeof meta, "%s/META", b);
    uint32_t v = 0;
    OK(tsdb_volume_add_dir(root, a, &v));
    CHECK(v == 1);
    CHECK(rename(a, b) == 0 && symlink(b, a) == 0); /* two paths to one directory */
    v = 0;
    CHECK(tsdb_volume_add_dir(root, b, &v) == TSDB_ERR_INVALID && v == 0);
    read_text(meta, mtext, sizeof mtext);
    CHECK(strstr(mtext, "\nvolume=1\n") != NULL);
    CHECK(unlink(a) == 0); /* line 1's path is gone: it cannot be told apart */
    CHECK(tsdb_volume_add_dir(root, b, &v) == TSDB_ERR_INVALID && v == 0);
    read_text(meta, mtext, sizeof mtext);
    CHECK(strstr(mtext, "\nvolume=1\n") != NULL);
    CHECK(rename(b, a) == 0);
    tsdb_options o;
    tsdb_options_init(&o);
    tsdb *db = NULL;
    CHECK(tsdb_open(root, &o, &db) == 0); /* the old add made every open fail */
    if (db) {
        CHECK(tsdb_volume_count(db) == 2);
        OK(tsdb_close(db));
    }
    remove_tree(top);
    printf("  a volume added again under a second path is refused, and its META keeps its number\n");
}

struct adder {
    tsdb *db;
    const char *path;
    int rc, done;
    uint32_t index;
    bool fifo_meta;  /* first make the add's own META temporary a FIFO (its name has this thread's id) */
    int planted;
    char fifo[1200];
};

static void *add_thread(void *arg)
{
    struct adder *x = arg;
    if (x->fifo_meta) { /* META.tmp.HOST.PID.TID, as meta_tmp_make (db.c) names it */
        char host[65] = "";
        if (gethostname(host, sizeof host - 1) != 0 || !host[0])
            snprintf(host, sizeof host, "localhost");
        for (char *c = host; *c; c++)
            if (!((*c >= 'a' && *c <= 'z') || (*c >= 'A' && *c <= 'Z') || (*c >= '0' && *c <= '9') || *c == '-' ||
                  *c == '_'))
                *c = '_';
        snprintf(x->fifo, sizeof x->fifo, "%s/META.tmp.%s.%ld.%ld", x->path, host, (long)getpid(), tsdb_gettid());
        CHECK(mkfifo(x->fifo, 0644) == 0);
        __atomic_store_n(&x->planted, 1, __ATOMIC_RELEASE);
    }
    x->rc = tsdb_volume_add(x->db, x->path, &x->index);
    __atomic_store_n(&x->done, 1, __ATOMIC_RELEASE);
    return NULL;
}

/* A new volume's META is written while the VOLUMES lock is free: its file system may hang (here the add's META
 * temporary is a FIFO nobody reads yet). And when that write fails (an fsync of a FIFO does), the add fails alone:
 * nothing relies on that META, so the engine is not failed and the add can be tried again. */
static void test_new_meta_write(const char *base)
{
    char top[600], root[700], v1[800], rid[900], vols[800], msg[256];
    fresh(base, "volmeta", top, sizeof top, root, sizeof root);
    snprintf(v1, sizeof v1, "%s/v1", top);
    snprintf(rid, sizeof rid, "%s/ROOT_ID", v1);
    snprintf(vols, sizeof vols, "%s/VOLUMES", root);
    CHECK(mkdir(v1, 0755) == 0);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 1;
    tsdb *db = NULL;
    OK(tsdb_open(root, &o, &db));
    tsdb_sid s;
    OK(tsdb_series(db, "before", &s));
    struct adder x = {.db = db, .path = v1, .fifo_meta = true};
    const char *fifo = x.fifo;
    pthread_t th;
    CHECK(pthread_create(&th, NULL, add_thread, &x) == 0);
    for (int i = 0; i < 1000 && !__atomic_load_n(&x.planted, __ATOMIC_ACQUIRE); i++)
        usleep(10000);
    for (int i = 0; i < 1000 && !exists(rid); i++)
        usleep(10000);
    usleep(300000); /* ROOT_ID is written right before META: the add is waiting for the FIFO now */
    CHECK(!__atomic_load_n(&x.done, __ATOMIC_ACQUIRE));
    int fd = open(vols, O_RDWR | O_CLOEXEC);
    struct flock fl = {.l_type = F_WRLCK, .l_whence = SEEK_SET};
    bool free_lock = fd >= 0 && fcntl(fd, F_OFD_SETLK, &fl) == 0;
    if (!free_lock)
        fprintf(stderr, "  the VOLUMES lock is held while a new volume's META is written\n");
    CHECK(free_lock);
    if (fd >= 0)
        close(fd);
    int r = open(fifo, O_RDONLY | O_CLOEXEC);
    char buf[4096];
    while (r >= 0 && read(r, buf, sizeof buf) > 0)
        ;
    if (r >= 0)
        close(r);
    pthread_join(th, NULL);
    CHECK(x.rc == TSDB_ERR_IO);
    CHECK(tsdb_failed(db, msg, sizeof msg) == 0);
    OK(tsdb_append(db, s, T0, 1));
    CHECK(!exists(fifo)); /* the failed add removed its temporary */
    x.index = 0;
    OK(tsdb_volume_add(db, v1, &x.index));
    CHECK(x.index == 1);
    OK(tsdb_close(db));
    OK(tsdb_open(root, &o, &db));
    if (db) {
        CHECK(tsdb_volume_count(db) == 2);
        OK(tsdb_close(db));
    }
    remove_tree(top);
    printf("  a new volume's META is written outside the VOLUMES lock, and its failure fails just the add\n");
}

typedef struct {
    tsdb *db;
    const char *name, *path;
    tsdb_sid id;
    int rc, done;
} maker;

static void *make_thread(void *arg)
{
    maker *m = arg;
    m->rc = tsdb_series_at(m->db, m->name, m->path, &m->id);
    __atomic_store_n(&m->done, 1, __ATOMIC_RELEASE);
    return NULL;
}

static bool wait_done(maker *m, int ms)
{
    for (int i = 0; i < ms / 10 && !__atomic_load_n(&m->done, __ATOMIC_ACQUIRE); i++)
        usleep(10000);
    return __atomic_load_n(&m->done, __ATOMIC_ACQUIRE);
}

/* A volume whose file system hangs (here its SERIES is a FIFO with a full pipe) holds up only the creation of the
 * name being appended there: a creation of another name in another volume and lookups go on, and a second creator
 * of the hung name waits for it and gets the same series. */
static void test_hung_volume(const char *base)
{
    char top[600], root[700], v1[800], fifo[900];
    fresh(base, "volhung", top, sizeof top, root, sizeof root);
    snprintf(v1, sizeof v1, "%s/v1", top);
    snprintf(fifo, sizeof fifo, "%s/SERIES", v1);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0; /* no fdatasync of the FIFO */
    tsdb *db = NULL;
    OK(tsdb_open(root, &o, &db));
    tsdb_sid before, s;
    OK(tsdb_series(db, "before", &before));
    uint32_t idx = 0;
    OK(tsdb_volume_add(db, v1, &idx));
    CHECK(idx == 1);
    OK(tsdb_close(db));
    CHECK(unlink(fifo) == 0 && mkfifo(fifo, 0644) == 0); /* an empty SERIES before */
    OK(tsdb_open(root, &o, &db));
    int w = open(fifo, O_WRONLY | O_NONBLOCK | O_CLOEXEC); /* the engine holds it open for reading and writing */
    CHECK(w >= 0);
    char buf[4096] = {0};
    while (w >= 0 && write(w, buf, sizeof buf) > 0)
        ;
    while (w >= 0 && write(w, buf, 1) > 0)
        ;
    maker a = {.db = db, .name = "hung", .path = v1, .rc = -1};
    maker b = {.db = db, .name = "other", .path = NULL, .rc = -1};
    maker c = {.db = db, .name = "hung", .path = NULL, .rc = -1};
    pthread_t ta, tb, tc;
    CHECK(pthread_create(&ta, NULL, make_thread, &a) == 0);
    usleep(300000);
    CHECK(!__atomic_load_n(&a.done, __ATOMIC_ACQUIRE)); /* its append waits for room in the pipe */
    CHECK(pthread_create(&tb, NULL, make_thread, &b) == 0);
    CHECK(pthread_create(&tc, NULL, make_thread, &c) == 0);
    bool b_done = wait_done(&b, 3000);
    if (!b_done)
        fprintf(stderr, "  creating a series in volume 0 waits for an append to volume 1\n");
    CHECK(b_done);
    CHECK(tsdb_series_lookup(db, "before", &s) == 0 && s == before);
    CHECK(!__atomic_load_n(&c.done, __ATOMIC_ACQUIRE)); /* the same name waits for the first creation */
    int r = open(fifo, O_RDONLY | O_NONBLOCK | O_CLOEXEC);
    CHECK(r >= 0);
    for (int i = 0; i < 1000 && r >= 0 &&
                    !(__atomic_load_n(&a.done, __ATOMIC_ACQUIRE) && __atomic_load_n(&c.done, __ATOMIC_ACQUIRE));
         i++) {
        while (read(r, buf, sizeof buf) > 0)
            ;
        usleep(10000);
    }
    pthread_join(ta, NULL);
    pthread_join(tb, NULL);
    pthread_join(tc, NULL);
    if (r >= 0)
        close(r);
    if (w >= 0)
        close(w);
    CHECK(a.rc == 0 && b.rc == 0 && c.rc == 0);
    CHECK(TSDB_SID_VOLUME(a.id) == 1 && TSDB_SID_VOLUME(b.id) == 0);
    CHECK(c.id == a.id); /* one series, not a second one in volume 0 */
    CHECK(tsdb_volume_series_count(db, 1) == 1 && tsdb_series_count(db) == 3);
    OK(tsdb_close(db)); /* not opened again: the FIFO kept no names */
    remove_tree(top);
    printf("  a hung volume holds up only the name being created in it\n");
}

typedef struct {
    tsdb *db;
    const char *name;
    tsdb_sid id;
    int rc, done;
} looker;

static void *lookup_thread(void *arg)
{
    looker *l = arg;
    l->rc = tsdb_series_lookup(l->db, l->name, &l->id);
    __atomic_store_n(&l->done, 1, __ATOMIC_RELEASE);
    return NULL;
}

/* A writer opening a volume an operator added while it runs (tsdb-cli), on a file system that hangs (here the
 * volume's META is a FIFO nobody writes), holds up nothing in the volumes it has open: lookups and creations there
 * go on. The open, once it fails, is tried again by the next request that names the volume. */
static void test_hung_new_volume(const char *base)
{
    char top[600], root[700], v1[800], meta[900], saved[4096];
    fresh(base, "volnew", top, sizeof top, root, sizeof root);
    snprintf(v1, sizeof v1, "%s/v1", top);
    snprintf(meta, sizeof meta, "%s/META", v1);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.create_volumes = 0;
    tsdb *db = NULL;
    OK(tsdb_open(root, &o, &db));
    tsdb_sid before, s;
    OK(tsdb_series(db, "before", &before));
    uint32_t idx = 0;
    OK(tsdb_volume_add_dir(root, v1, &idx)); /* as tsdb-cli does, beside the running writer */
    CHECK(idx == 1);
    ssize_t n = -1;
    int fd = open(meta, O_RDONLY | O_CLOEXEC);
    if (fd >= 0) {
        n = read(fd, saved, sizeof saved);
        close(fd);
    }
    CHECK(n > 0);
    CHECK(unlink(meta) == 0 && mkfifo(meta, 0644) == 0);
    maker a = {.db = db, .name = "x", .path = v1, .rc = -1};
    maker b = {.db = db, .name = "other", .path = NULL, .rc = -1};
    pthread_t ta, tb;
    CHECK(pthread_create(&ta, NULL, make_thread, &a) == 0);
    usleep(300000);
    CHECK(!__atomic_load_n(&a.done, __ATOMIC_ACQUIRE)); /* opening the new volume: its META does not answer */
    looker l = {.db = db, .name = "before", .rc = 1};
    pthread_t tl;
    CHECK(pthread_create(&tl, NULL, lookup_thread, &l) == 0);
    CHECK(pthread_create(&tb, NULL, make_thread, &b) == 0);
    bool b_done = wait_done(&b, 3000);
    bool l_done = __atomic_load_n(&l.done, __ATOMIC_ACQUIRE);
    if (!b_done || !l_done)
        fprintf(stderr, "  opening a new volume on a hung file system holds up the volumes already open\n");
    CHECK(b_done && l_done);
    int w = -1;
    for (int i = 0; i < 500 && w < 0; i++) /* the open of the FIFO for reading is waiting for us */
        if ((w = open(meta, O_WRONLY | O_NONBLOCK | O_CLOEXEC)) < 0)
            usleep(10000);
    CHECK(w >= 0);
    if (w >= 0)
        close(w); /* nothing written: META reads as broken, the request fails */
    pthread_join(ta, NULL);
    pthread_join(tb, NULL);
    pthread_join(tl, NULL);
    CHECK(a.rc != 0);
    CHECK(b.rc == 0 && TSDB_SID_VOLUME(b.id) == 0);
    CHECK(l.rc == 0 && l.id == before);
    CHECK(tsdb_volume_count(db) == 1);
    CHECK(unlink(meta) == 0);
    fd = open(meta, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    CHECK(fd >= 0 && n > 0 && write(fd, saved, (size_t)n) == n);
    if (fd >= 0)
        close(fd);
    OK(tsdb_series_at(db, "x", v1, &s)); /* tried again */
    CHECK(s == TSDB_SID(1, 0) && tsdb_volume_count(db) == 2);
    OK(tsdb_close(db));
    remove_tree(top);
    printf("  a new volume on a hung file system holds up only the requests that name it\n");
}

/* The dictionaries' test hook (src/internal.h): here it holds a follower's read of SERIES, as a hung NFS server
 * would. */
void tsdb_test_dict_fault(tsdb *t, int (*fn)(void *arg, int point, const char *name), void *arg);
static pthread_mutex_t rl_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t rl_cv = PTHREAD_COND_INITIALIZER;
static int rl_held, rl_go;

static int reload_hold(void *arg, int point, const char *name)
{
    (void)arg;
    if (point != 3 || strcmp(name, "SERIES") != 0) /* DICT_AT_RELOAD */
        return 0;
    pthread_mutex_lock(&rl_mu);
    rl_held++;
    pthread_cond_broadcast(&rl_cv);
    while (!rl_go)
        pthread_cond_wait(&rl_cv, &rl_mu);
    pthread_mutex_unlock(&rl_mu);
    return 0;
}

/* A follower reading a SERIES that grew (on a hung file system, the read may take forever) holds no lock that a
 * lookup of a name it already knows needs. */
static void test_hung_reload(const char *base)
{
    char top[600], root[700];
    fresh(base, "volreload", top, sizeof top, root, sizeof root);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    tsdb *w = NULL, *f = NULL;
    OK(tsdb_open(root, &o, &w));
    tsdb_sid a, b, fa;
    OK(tsdb_series(w, "a", &a));
    tsdb_options ro = o;
    ro.read_only = 1;
    OK(tsdb_open(root, &ro, &f));
    OK(tsdb_series_lookup(f, "a", &fa));
    CHECK(fa == a);
    OK(tsdb_series(w, "b", &b)); /* SERIES grows */
    rl_held = rl_go = 0;
    tsdb_test_dict_fault(f, reload_hold, NULL);
    looker lb = {.db = f, .name = "b", .rc = 1}, la = {.db = f, .name = "a", .rc = 1};
    pthread_t tb, ta;
    CHECK(pthread_create(&tb, NULL, lookup_thread, &lb) == 0); /* a miss: reads SERIES again, and hangs there */
    pthread_mutex_lock(&rl_mu);
    while (!rl_held)
        pthread_cond_wait(&rl_cv, &rl_mu);
    pthread_mutex_unlock(&rl_mu);
    CHECK(pthread_create(&ta, NULL, lookup_thread, &la) == 0);
    bool done = false;
    for (int i = 0; i < 300 && !(done = __atomic_load_n(&la.done, __ATOMIC_ACQUIRE)); i++)
        usleep(10000);
    if (!done)
        fprintf(stderr, "  a lookup of a known name waits for a follower's read of SERIES\n");
    CHECK(done);
    pthread_mutex_lock(&rl_mu);
    rl_go = 1;
    pthread_cond_broadcast(&rl_cv);
    pthread_mutex_unlock(&rl_mu);
    pthread_join(ta, NULL);
    pthread_join(tb, NULL);
    CHECK(la.rc == 0 && la.id == a);
    CHECK(lb.rc == 0 && lb.id == b);
    tsdb_test_dict_fault(f, NULL, NULL);
    OK(tsdb_close(f));
    OK(tsdb_close(w));
    remove_tree(top);
    printf("  a follower's read of SERIES holds up no lookup of a known name\n");
}

/* Threads of this process in an open system call now (here: waiting for a FIFO's writer); -1: /proc does not say. */
static int threads_in_open(void)
{
    DIR *d = opendir("/proc/self/task");
    if (!d)
        return -1;
    int n = 0, seen = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (de->d_name[0] == '.')
            continue;
        char p[300], buf[64];
        snprintf(p, sizeof p, "/proc/self/task/%s/syscall", de->d_name);
        int fd = open(p, O_RDONLY | O_CLOEXEC);
        ssize_t r = fd < 0 ? -1 : read(fd, buf, sizeof buf - 1);
        if (fd >= 0)
            close(fd);
        if (r <= 0)
            continue;
        buf[r] = '\0';
        seen++;
        long nr = strtol(buf, NULL, 10);
        bool in_open = buf[0] >= '0' && buf[0] <= '9' && nr == SYS_openat;
#ifdef SYS_open
        in_open = in_open || (buf[0] >= '0' && buf[0] <= '9' && nr == SYS_open);
#endif
        n += in_open;
    }
    closedir(d);
    return seen ? n : -1;
}

/* The fewest over a few looks: another thread may pass through an open. */
static int threads_in_open_min(void)
{
    int m = -1;
    for (int i = 0; i < 5; i++) {
        int c = threads_in_open();
        if (c >= 0 && (m < 0 || c < m))
            m = c;
        usleep(20000);
    }
    return m;
}

#define HERD 8

/* A follower's threads that want a volume its writer just added, on a file system that hangs (here the volume's
 * META is a FIFO nobody writes): one opens it, and the others wait for that open rather than each run an open of
 * their own. A lookup of a name the writer just created in a volume the follower has open completes meanwhile. */
static void test_follower_hung_new_volume(const char *base)
{
    char top[600], root[700], v1[800], meta[900], fifo[900], good[900], saved[4096];
    fresh(base, "volfnew", top, sizeof top, root, sizeof root);
    snprintf(v1, sizeof v1, "%s/v1", top);
    snprintf(meta, sizeof meta, "%s/META", v1);
    snprintf(fifo, sizeof fifo, "%s/fifo", top); /* a second name: the FIFO is answered after META is fixed */
    snprintf(good, sizeof good, "%s/META.good", v1);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    tsdb *w = NULL, *f = NULL;
    OK(tsdb_open(root, &o, &w));
    tsdb_sid before, x, fr, fb = 0;
    OK(tsdb_series(w, "before", &before));
    tsdb_options ro = o;
    ro.read_only = 1;
    OK(tsdb_open(root, &ro, &f));
    OK(tsdb_series_lookup(f, "before", &fb));
    CHECK(fb == before);
    uint32_t idx = 0;
    OK(tsdb_volume_add(w, v1, &idx));
    CHECK(idx == 1);
    OK(tsdb_series_at(w, "x", v1, &x));
    ssize_t n = -1;
    int fd = open(meta, O_RDONLY | O_CLOEXEC);
    if (fd >= 0) {
        n = read(fd, saved, sizeof saved);
        close(fd);
    }
    CHECK(n > 0);
    CHECK(unlink(meta) == 0 && mkfifo(meta, 0644) == 0 && link(meta, fifo) == 0);
    looker herd[HERD];
    pthread_t th[HERD];
    for (int i = 0; i < HERD; i++)
        herd[i] = (looker){.db = f, .name = "x", .rc = 1};
    CHECK(pthread_create(&th[0], NULL, lookup_thread, &herd[0]) == 0);
    int in_open = -1;
    for (int i = 0; i < 300 && in_open != 1; i++) /* the first opener reaches the FIFO */
        if ((in_open = threads_in_open()) != 1)
            usleep(10000);
    CHECK(!__atomic_load_n(&herd[0].done, __ATOMIC_ACQUIRE)); /* opening v1: its META does not answer */
    OK(tsdb_series(w, "fresh", &fr)); /* in volume 0, after the follower read its SERIES */
    looker lf = {.db = f, .name = "fresh", .rc = 1};
    pthread_t tf;
    CHECK(pthread_create(&tf, NULL, lookup_thread, &lf) == 0);
    for (int i = 1; i < HERD; i++)
        CHECK(pthread_create(&th[i], NULL, lookup_thread, &herd[i]) == 0);
    bool f_done = false;
    for (int i = 0; i < 300 && !(f_done = __atomic_load_n(&lf.done, __ATOMIC_ACQUIRE)); i++)
        usleep(10000);
    if (!f_done)
        fprintf(stderr, "  a follower's open of a hung new volume hides a name new in a volume it has open\n");
    CHECK(f_done);
    usleep(200000); /* the others: waiting for the opener, or each in an open of its own */
    if (in_open == 1) {
        int opening = threads_in_open_min();
        if (opening != 1)
            fprintf(stderr, "  %d threads of a follower open one new volume at once\n", opening);
        CHECK(opening == 1);
    } else {
        printf("  (/proc/self/task/*/syscall says nothing here: the opens are not counted)\n");
    }
    fd = open(good, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    CHECK(fd >= 0 && n > 0 && write(fd, saved, (size_t)n) == n);
    if (fd >= 0)
        close(fd);
    CHECK(rename(good, meta) == 0); /* what the next opener reads */
    int wfd = -1;
    for (int i = 0; i < 500 && wfd < 0; i++)
        if ((wfd = open(fifo, O_WRONLY | O_NONBLOCK | O_CLOEXEC)) < 0)
            usleep(10000);
    CHECK(wfd >= 0);
    if (wfd >= 0)
        close(wfd); /* nothing written: that open fails, and one of those waiting opens v1 */
    for (int i = 0; i < HERD; i++)
        pthread_join(th[i], NULL);
    pthread_join(tf, NULL);
    CHECK(lf.rc == 0 && lf.id == fr);
    int found = 0;
    for (int i = 1; i < HERD; i++)
        found += herd[i].rc == 0 && herd[i].id == x;
    if (found != HERD - 1)
        fprintf(stderr, "  %d of the %d lookups that waited for a new volume found the name\n", found, HERD - 1);
    CHECK(found == HERD - 1);
    tsdb_sid sx = 0;
    OK(tsdb_series_lookup(f, "x", &sx));
    CHECK(sx == x);
    CHECK(tsdb_volume_count(f) == 2 && tsdb_volume_count(w) == 2);
    CHECK(tsdb_volume_count(f) == 2 && strcmp(tsdb_volume_path(f, 1), tsdb_volume_path(w, 1)) == 0);
    unlink(fifo);
    OK(tsdb_close(f));
    OK(tsdb_close(w));
    remove_tree(top);
    printf("  a follower opens a new volume on a hung file system once, and it hides no name in the others\n");
}

/* A follower's threads racing for volumes its writer adds one at a time: half look the new name up, half query the
 * id (route). Every one gets the right series, and the follower lists each volume once, in the writer's order. */
#define RACERS 16
#define RACE_ROUNDS 6
static tsdb *race_f;
static char race_names[RACE_ROUNDS][64];
static tsdb_sid race_want[RACE_ROUNDS];
static atomic_int race_round, race_failures, race_done;

static void *racer(void *arg)
{
    long me = (long)arg;
    for (int r = 0; r < RACE_ROUNDS; r++) {
        while (atomic_load(&race_round) < r)
            usleep(100);
        int rc = TSDB_ERR_NOTFOUND;
        if (me & 1) {
            tsdb_points p = {0};
            for (int k = 0; k < 500 && (rc || p.n == 0); k++) {
                tsdb_points_free(&p);
                if ((rc = tsdb_query_points(race_f, race_want[r], INT64_MIN, INT64_MAX, &p)) != 0 || !p.n)
                    usleep(2000);
            }
            if (rc || p.n != 1 || p.values[0] != (float)r) {
                fprintf(stderr, "  racer %ld, volume %d: query rc %d, %zu rows\n", me, r + 1, rc, p.n);
                atomic_fetch_add(&race_failures, 1);
            }
            tsdb_points_free(&p);
        } else {
            tsdb_sid id = 0;
            for (int k = 0; k < 500 && rc; k++)
                if ((rc = tsdb_series_lookup(race_f, race_names[r], &id)) != 0)
                    usleep(2000);
            if (rc || id != race_want[r]) {
                fprintf(stderr, "  racer %ld, volume %d: lookup rc %d, id %llx, want %llx\n", me, r + 1, rc,
                        (unsigned long long)id, (unsigned long long)race_want[r]);
                atomic_fetch_add(&race_failures, 1);
            }
        }
        atomic_fetch_add(&race_done, 1);
    }
    return NULL;
}

static void test_follower_open_race(const char *base)
{
    char top[600], root[700];
    fresh(base, "volrace", top, sizeof top, root, sizeof root);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    tsdb *w = NULL;
    OK(tsdb_open(root, &o, &w));
    tsdb_options fo = o;
    fo.follow = 1;
    fo.refresh_ms = 5;
    race_f = NULL;
    OK(tsdb_open(root, &fo, &race_f));
    if (!w || !race_f) {
        tsdb_close(race_f);
        tsdb_close(w);
        return;
    }
    atomic_store(&race_round, -1);
    atomic_store(&race_failures, 0);
    atomic_store(&race_done, 0);
    pthread_t th[RACERS];
    int started = 0;
    for (long i = 0; i < RACERS; i++)
        if (pthread_create(&th[started], NULL, racer, (void *)i) == 0)
            started++;
    CHECK(started == RACERS);
    for (int r = 0; r < RACE_ROUNDS; r++) {
        char v[800];
        snprintf(v, sizeof v, "%s/vol%d", top, r);
        uint32_t idx = 0;
        OK(tsdb_volume_add(w, v, &idx));
        CHECK(idx == (uint32_t)r + 1);
        snprintf(race_names[r], sizeof race_names[r], "race.%d", r);
        OK(tsdb_series_at(w, race_names[r], v, &race_want[r]));
        CHECK(TSDB_SID_VOLUME(race_want[r]) == (uint32_t)r + 1);
        int64_t ts = 1000;
        float val = (float)r;
        OK(tsdb_insert_series(w, race_want[r], &ts, &val, 1));
        OK(tsdb_flush(w));
        atomic_store(&race_round, r); /* every racer takes this volume on at once */
        while (atomic_load(&race_done) < started * (r + 1))
            usleep(1000);
    }
    for (int i = 0; i < started; i++)
        pthread_join(th[i], NULL);
    CHECK(atomic_load(&race_failures) == 0);
    uint32_t nf = tsdb_volume_count(race_f), nw = tsdb_volume_count(w);
    if (nf != nw)
        fprintf(stderr, "  the follower lists %u volumes, the writer %u\n", nf, nw);
    CHECK(nf == nw && nw == RACE_ROUNDS + 1);
    for (uint32_t i = 0; i < nf && i < nw; i++)
        CHECK(strcmp(tsdb_volume_path(race_f, i), tsdb_volume_path(w, i)) == 0);
    for (int r = 0; r < RACE_ROUNDS; r++) {
        const char *name = tsdb_series_name(race_f, race_want[r]);
        CHECK(name && strcmp(name, race_names[r]) == 0);
    }
    OK(tsdb_close(race_f));
    OK(tsdb_close(w));
    remove_tree(top);
    printf("  a follower's threads racing for new volumes open each once, in order, with the right series\n");
}

/* A volume's META that names another number for a moment (an add of the same directory under another path writes
 * its number there and puts it back, volume_add) fails the requests that come meanwhile, not every later one until
 * VOLUMES changes: the writer reads that META again before it answers with the damage it found. */
static void test_meta_passing_mismatch(const char *base)
{
    char top[600], root[700], v1[800];
    fresh(base, "volmeta", top, sizeof top, root, sizeof root);
    snprintf(v1, sizeof v1, "%s/v1", top);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.create_volumes = 0;
    tsdb *db = NULL;
    OK(tsdb_open(root, &o, &db));
    uint32_t idx = 0;
    OK(tsdb_volume_add_dir(root, v1, &idx)); /* as tsdb-cli does, beside the running writer */
    CHECK(idx == 1);
    meta_with_volume(v1, v1, 5); /* what the other add writes */
    tsdb_sid s = 0;
    CHECK(tsdb_series_at(db, "x", v1, &s) == TSDB_ERR_CORRUPT);
    CHECK(tsdb_series_at(db, "x", v1, &s) == TSDB_ERR_CORRUPT); /* still another number: still damage */
    meta_with_volume(v1, v1, 1);                                  /* put back */
    int rc = tsdb_series_at(db, "x", v1, &s);
    if (rc)
        fprintf(stderr, "  a META that named another number for a moment refuses its volume until VOLUMES changes\n");
    CHECK(rc == 0 && s == TSDB_SID(1, 0) && tsdb_volume_count(db) == 2);
    OK(tsdb_close(db));
    remove_tree(top);
    printf("  a META that names another number for a moment does not refuse its volume for good\n");
}

static int fail_add_of_b(void *arg, int point, const char *name)
{
    (void)arg;
    return point == 1 && strcmp(name, "b") == 0; /* DICT_AT_ADD: as if memory ran out */
}

/* A follower routing an id it does not know reads SERIES again. A record after the id's that cannot be added (here
 * no memory) is for the refresh to report: it does not hide the id, which was loaded before it. */
static void test_route_past_failed_record(const char *base)
{
    char top[600], root[700];
    fresh(base, "volroute", top, sizeof top, root, sizeof root);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    tsdb *w = NULL, *f = NULL;
    OK(tsdb_open(root, &o, &w));
    tsdb_options ro = o;
    ro.read_only = 1;
    OK(tsdb_open(root, &ro, &f));
    tsdb_sid a = 0, b = 0;
    OK(tsdb_series(w, "a", &a));
    OK(tsdb_series(w, "b", &b));
    tsdb_test_dict_fault(f, fail_add_of_b, NULL);
    tsdb_type ty;
    int rc = tsdb_series_type(f, a, &ty);
    if (rc)
        fprintf(stderr, "  a record that failed after an id's hides the id from a follower\n");
    CHECK(rc == 0);
    CHECK(tsdb_series_type(f, b, &ty) != 0); /* not loaded */
    tsdb_test_dict_fault(f, NULL, NULL);
    OK(tsdb_series_type(f, b, &ty));
    OK(tsdb_close(f));
    OK(tsdb_close(w));
    remove_tree(top);
    printf("  a follower routes an id loaded before a record that failed\n");
}

/* A follower asked about a series its writer created a moment ago reads SERIES again. STATES (state names) has
 * nothing to do with it: a STATES that cannot be read (here a directory took its name) must not hide the series. */
static void test_reload_states_fails(const char *base)
{
    char top[600], root[700], states[800], aside[800];
    fresh(base, "volstates", top, sizeof top, root, sizeof root);
    snprintf(states, sizeof states, "%s/STATES", root);
    snprintf(aside, sizeof aside, "%s/STATES.aside", root);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    tsdb *w = NULL, *f = NULL;
    OK(tsdb_open(root, &o, &w));
    tsdb_sid a, n;
    OK(tsdb_series(w, "a", &a));
    tsdb_options ro = o;
    ro.read_only = 1;
    OK(tsdb_open(root, &ro, &f));
    CHECK(rename(states, aside) == 0 && mkdir(states, 0755) == 0);
    OK(tsdb_series_create(w, "n", TSDB_INT64, NULL, &n));
    tsdb_type ty = TSDB_FLOAT32;
    int rc = tsdb_series_type(f, n, &ty);
    if (rc != 0)
        fprintf(stderr, "  a follower misses a new series when STATES cannot be read\n");
    CHECK(rc == 0 && ty == TSDB_INT64);
    CHECK(tsdb_series_name(f, n) && strcmp(tsdb_series_name(f, n), "n") == 0);
    OK(tsdb_close(f));
    OK(tsdb_close(w));
    remove_tree(top);
    printf("  a follower finds a new series when STATES cannot be read\n");
}

static void put_byte(const char *path, off_t off, char c)
{
    int fd = open(path, O_WRONLY | O_CLOEXEC);
    CHECK(fd >= 0 && pwrite(fd, &c, 1, off) == 1);
    if (fd >= 0)
        close(fd);
}

static bool same_text(const char *path, const char *want, size_t len)
{
    char buf[4096];
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    ssize_t n = fd >= 0 ? read(fd, buf, sizeof buf) : -1;
    if (fd >= 0)
        close(fd);
    return n == (ssize_t)len && memcmp(buf, want, len) == 0;
}

/* One damaged byte in VOLUMES (the 0.9.0 review, secrel F2). Lines were split with strchr, which stops at a NUL, and
 * a bad line was damage only when the one line after it passed its check under the bad line's number + 1: a NUL in
 * line 2, a newline lost between lines 1 and 2, two bad lines or a bad last line were taken for a torn tail. The open
 * cut every line from there and succeeded, those volumes' series were gone, and the next add handed a cut volume's
 * number out again, so ids clients hold named the new volume's series. A torn append is the last line without its
 * newline; only a tail with no newline at all is cut now, and any other damage fails the writer's open, a reader's
 * and an add with TSDB_ERR_CORRUPT, the file as it was. */
static void test_volumes_damaged_lines(const char *base)
{
    char top[600], root[700], vols[800], v[3][800], vd[800], name[32];
    fresh(base, "voldmg", top, sizeof top, root, sizeof root);
    snprintf(vols, sizeof vols, "%s/VOLUMES", root);
    snprintf(vd, sizeof vd, "%s/vd", top);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.create_volumes = 0;
    tsdb_options ro = o;
    ro.read_only = 1;
    tsdb *db;
    tsdb_sid ids[3], id;
    uint32_t idx;
    for (int i = 0; i < 3; i++) {
        snprintf(v[i], sizeof v[i], "%s/v%d", top, i + 1);
        OK(tsdb_volume_add_dir(root, v[i], &idx));
        CHECK(idx == (uint32_t)i + 1);
    }
    OK(tsdb_open(root, &o, &db));
    for (int i = 0; i < 3; i++) {
        snprintf(name, sizeof name, "s%d", i + 1);
        OK(tsdb_series_at(db, name, v[i], &ids[i]));
        CHECK(ids[i] == TSDB_SID((uint32_t)i + 1, 0));
    }
    OK(tsdb_close(db));
    char good[4096];
    read_text(vols, good, sizeof good);
    size_t len = strlen(good);
    const char *nl1 = strchr(good, '\n'), *nl2 = nl1 ? strchr(nl1 + 1, '\n') : NULL;
    CHECK(nl1 && nl2 && good[len - 1] == '\n');
    if (!nl1 || !nl2)
        return;
    off_t l2 = nl1 + 1 - good, l3 = nl2 + 1 - good; /* where lines 2 and 3 start */
    static const struct {
        const char *what;
        int line;  /* 2: at l2 + at, 1: at l2 - 1 (line 1's newline), 3: at l3 + at */
        int at;
        char c;
        int line2; /* a second byte, or 0 */
    } cases[] = {
        {"a NUL inside line 2", 2, 8, '\0', 0},
        {"line 1's newline lost (two lines run together)", 1, 0, 'Z', 0},
        {"lines 2 and 3 damaged", 2, 3, 'x', 3},
        {"the last line damaged, its newline whole", 3, 12, 'q', 0},
        {"a NUL at the start of line 2", 2, 0, '\0', 0},
    };
    for (size_t k = 0; k < sizeof cases / sizeof *cases; k++) {
        write_file(vols, good);
        off_t at = cases[k].line == 1 ? l2 - 1 : (cases[k].line == 2 ? l2 : l3) + cases[k].at;
        put_byte(vols, at, cases[k].c);
        if (cases[k].line2)
            put_byte(vols, l3 + 3, 'x');
        char damaged[4096];
        int fd = open(vols, O_RDONLY | O_CLOEXEC);
        CHECK(fd >= 0 && read(fd, damaged, sizeof damaged) == (ssize_t)len);
        if (fd >= 0)
            close(fd);
        int w = tsdb_open(root, &o, &db), r = w;
        if (w == 0)
            tsdb_close(db);
        if (w != TSDB_ERR_CORRUPT)
            fprintf(stderr, "  %s: the writer's open gave %d, not TSDB_ERR_CORRUPT\n", cases[k].what, w);
        CHECK(w == TSDB_ERR_CORRUPT);
        CHECK(same_text(vols, damaged, len)); /* nothing cut */
        if ((r = tsdb_open(root, &ro, &db)) == 0)
            tsdb_close(db);
        CHECK(r == TSDB_ERR_CORRUPT);
        int a = tsdb_volume_add_dir(root, vd, &idx);
        if (a != TSDB_ERR_CORRUPT)
            fprintf(stderr, "  %s: an add gave %d (volume %u), not TSDB_ERR_CORRUPT\n", cases[k].what, a, idx);
        CHECK(a == TSDB_ERR_CORRUPT);
        CHECK(same_text(vols, damaged, len));
    }
    /* repaired: every volume and series is there again */
    write_file(vols, good);
    OK(tsdb_open(root, &o, &db));
    CHECK(tsdb_volume_count(db) == 4);
    for (int i = 0; i < 3; i++) {
        snprintf(name, sizeof name, "s%d", i + 1);
        OK(tsdb_series_lookup(db, name, &id));
        CHECK(id == ids[i]);
    }
    OK(tsdb_close(db));
    /* a torn append (no newline in the tail) is still cut, by the open and by an add, and also when it is zeros */
    for (int z = 0; z <= 1; z++) {
        append_bytes(vols, z ? "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0" : "1234abcd /half/v", 16);
        OK(tsdb_open(root, &o, &db));
        CHECK(tsdb_volume_count(db) == 4);
        OK(tsdb_close(db));
        CHECK(same_text(vols, good, len));
    }
    append_bytes(vols, "1234abcd /half/v", 16);
    if (tsdb_open(root, &ro, &db) == 0) { /* a reader stops before it: a writer may be appending */
        CHECK(tsdb_volume_count(db) == 4);
        tsdb_close(db);
    } else {
        CHECK(!"a reader refused a torn tail");
    }
    OK(tsdb_volume_add_dir(root, vd, &idx));
    CHECK(idx == 4);
    OK(tsdb_open(root, &o, &db));
    OK(tsdb_series_at(db, "s4", vd, &id));
    CHECK(id == TSDB_SID(4, 0));
    for (int i = 0; i < 3; i++) {
        snprintf(name, sizeof name, "s%d", i + 1);
        CHECK(tsdb_series_lookup(db, name, &id) == 0 && id == ids[i]);
    }
    OK(tsdb_close(db));
    remove_tree(top);
    printf("  damage in VOLUMES fails the open, a reader's and an add; only a torn tail without a newline is cut\n");
}

/* A reader that saw VOLUMES end in a torn line (here the zeros a power loss leaves) must notice when an add cuts it
 * and appends a line of the very same length: the size alone does not change. */
static void test_volumes_same_size(const char *base)
{
    char top[600], root[700], v1[800], vols[800];
    fresh(base, "volsize", top, sizeof top, root, sizeof root);
    snprintf(v1, sizeof v1, "%s/v1", top);
    snprintf(vols, sizeof vols, "%s/VOLUMES", root);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.create_volumes = 0;
    tsdb *w = NULL, *f = NULL;
    OK(tsdb_open(root, &o, &w));
    tsdb_sid a, x, fx;
    OK(tsdb_series(w, "a", &a));
    char zeros[1024] = {0};
    size_t line_len = 9 + strlen(v1) + 1; /* "%08x " path "\n" */
    int fd = open(vols, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    CHECK(fd >= 0 && write(fd, zeros, line_len) == (ssize_t)line_len);
    if (fd >= 0)
        close(fd);
    tsdb_options ro = o;
    ro.read_only = 1;
    OK(tsdb_open(root, &ro, &f));
    CHECK(tsdb_series_lookup(f, "x", &fx) == TSDB_ERR_NOTFOUND); /* reads VOLUMES: the torn line */
    struct stat before, after;
    CHECK(stat(vols, &before) == 0);
    usleep(20000);
    uint32_t idx = 0;
    OK(tsdb_volume_add_dir(root, v1, &idx)); /* cuts the zeros, appends its line */
    CHECK(idx == 1);
    CHECK(stat(vols, &after) == 0 && after.st_size == before.st_size);
    OK(tsdb_series_at(w, "x", v1, &x));
    CHECK(x == TSDB_SID(1, 0));
    int rc = tsdb_series_lookup(f, "x", &fx);
    if (rc != 0)
        fprintf(stderr, "  a reader misses a volume whose VOLUMES line replaced a torn one of the same size\n");
    CHECK(rc == 0 && fx == x);
    OK(tsdb_close(f));
    OK(tsdb_close(w));
    remove_tree(top);
    printf("  a volume line that replaces a torn one of the same size is seen\n");
}

/* Power lost at every step of "tsdb <dir> volume add" (the crashfs shim), and
 * just after it reported success: the database opens again each time, and
 * the volume it reported is there. A VOLUMES line whose directory entry was
 * not yet durable used to fail every later open. */
static void test_add_power_loss(const char *base, const char *argv0)
{
    char dir[PATH_MAX], cli[PATH_MAX + 16], shim[PATH_MAX + 32];
    snprintf(dir, sizeof dir, "%.*s", (int)(strrchr(argv0, '/') ? strrchr(argv0, '/') - argv0 : 1),
             strrchr(argv0, '/') ? argv0 : ".");
    snprintf(cli, sizeof cli, "%s/tsdb", dir);
    snprintf(shim, sizeof shim, "%s/libcrashfs.so", dir);
    char shim_abs[PATH_MAX];
    if (access(cli, X_OK) != 0 || !realpath(shim, shim_abs)) {
        printf("  power loss during volume add: skipped (no %s or %s)\n", cli, shim);
        return;
    }
    char top[600], root[700], p[800];
    fresh(base, "volpl", top, sizeof top, root, sizeof root);
    tsdb_options o;
    tsdb_options_init(&o);
    o.create_volumes = 0;
    int crashes = 0, finished = 0;
    for (int k = 0; k < 200 && !finished; k++) {
        snprintf(p, sizeof p, "%s/v%d", top, k);
        char warm[16];
        snprintf(warm, sizeof warm, "%d", k);
        pid_t pid = fork();
        if (pid == 0) {
            char asan[512];
            snprintf(asan, sizeof asan, "%s:verify_asan_link_order=0",
                     getenv("ASAN_OPTIONS") ? getenv("ASAN_OPTIONS") : "");
            setenv("ASAN_OPTIONS", asan, 1);
            setenv("LD_PRELOAD", shim_abs, 1);
            setenv("CRASHFS_ROOT", top, 1);
            setenv("CRASHFS_PROB", "1", 1);
            setenv("CRASHFS_WARMUP", warm, 1);
            setenv("CRASHFS_AT_EXIT", "1", 1);
            int null = open("/dev/null", O_WRONLY);
            if (null >= 0) {
                dup2(null, 1);
                dup2(null, 2);
            }
            execl(cli, "tsdb", root, "volume", "add", p, (char *)NULL);
            _exit(127);
        }
        int st = -1;
        CHECK(pid > 0 && waitpid(pid, &st, 0) == pid && WIFEXITED(st));
        int code = WIFEXITED(st) ? WEXITSTATUS(st) : -1;
        CHECK(code == 99 || code == 0);
        if (code != 99 && code != 0)
            break;
        crashes += code == 99;
        finished = code == 0;
        tsdb *db;
        int rc = tsdb_open(root, &o, &db);
        if (rc != 0)
            fprintf(stderr, "  open after a power loss at step %d of volume add: %s\n", k + 1, tsdb_strerror(rc));
        CHECK(rc == 0);
        if (rc == 0 && finished) {
            bool listed = false;
            for (uint32_t i = 0; i < tsdb_volume_count(db); i++)
                listed |= strcmp(tsdb_volume_path(db, i), p) == 0;
            CHECK(listed); /* it said so: it is on disk */
            tsdb_sid id;
            CHECK(tsdb_series_at(db, "after", p, &id) == 0);
        }
        if (rc == 0)
            OK(tsdb_close(db));
    }
    CHECK(finished && crashes > 3);
    remove_tree(top);
    printf("  power lost at each of %d steps of volume add and after it: the database opens\n", crashes);
}

/* tsdb_series_foreach hands out every series once; an id is 64 bits and an id with bits above the 32nd is unknown,
 * never truncated to the series whose low bits it carries. */
typedef struct visit {
    tsdb_sid seen[16];
    int calls;
    int stop_at; /* the call that returns stop_rc; 0: none */
    int stop_rc;
} visit;

static int visit_fn(void *ctx, tsdb_sid id)
{
    visit *v = ctx;
    if (v->calls < 16)
        v->seen[v->calls] = id;
    v->calls++;
    return v->stop_at && v->calls == v->stop_at ? v->stop_rc : 0;
}

typedef struct first_row {
    size_t n;
    float first;
} first_row;

static int first_row_fn(void *ctx, const int64_t *ts, const float *values, size_t n)
{
    first_row *r = ctx;
    if (!r->n && n)
        r->first = values[0];
    r->n += n;
    return 0;
}

static int count_values_fn(void *ctx, const int64_t *ts, const tsdb_values *values, size_t n)
{
    (void)ts, (void)values;
    ((first_row *)ctx)->n += n;
    return 0;
}

static void test_series_ids(const char *base)
{
    char top[600], root[700], v1[800];
    fresh(base, "sid", top, sizeof top, root, sizeof root);
    snprintf(v1, sizeof v1, "%s/v1", top);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    tsdb *db;
    OK(tsdb_open(root, &o, &db));
    CHECK(tsdb_abi_version() == TSDB_ABI_VERSION);

    enum { N = 6 };
    tsdb_sid ids[N];
    for (int i = 0; i < N; i++) { /* three in the root, three in a second volume */
        char name[32];
        snprintf(name, sizeof name, "%s.%d", i < 3 ? "root" : "vol", i);
        OK(tsdb_series_at(db, name, i < 3 ? NULL : v1, &ids[i]));
        int64_t t = T0;
        float x = (float)(i + 1);
        OK(tsdb_insert_series(db, ids[i], &t, &x, 1));
    }
    CHECK(tsdb_volume_count(db) == 2 && tsdb_series_count(db) == N);

    visit v = {0};
    CHECK(tsdb_series_foreach(db, visit_fn, &v) == 0 && v.calls == N);
    for (int i = 0; i < N; i++) {
        int times = 0;
        for (int k = 0; k < v.calls; k++)
            times += v.seen[k] == ids[i];
        CHECK(times == 1); /* every series exactly once */
    }

    visit stop = {.stop_at = 3, .stop_rc = 7};
    CHECK(tsdb_series_foreach(db, visit_fn, &stop) == 7 && stop.calls == 3); /* the first non-zero return ends it */

    CHECK(ids[0] == TSDB_SID(0, 0) && ids[3] == TSDB_SID(1, 0) && ids[5] == TSDB_SID(1, 2));
    CHECK(tsdb_volume_series_count(db, 0) == 3 && tsdb_volume_series_count(db, 1) == 3);

    /* ids that name no series, by every way an id can be wrong; each carries the low bits of a real one, so a
     * truncation or a lookup by the local index alone would reach it */
    const tsdb_sid bad_ids[] = {
        ((tsdb_sid)1 << 48) | ids[0], /* bits 48-63 */
        ((tsdb_sid)1 << 63) | ids[3],
        TSDB_SID_LOCAL(ids[1]),       /* volume field 0: below 2^32 */
        TSDB_SID_NONE,
        TSDB_SID(2, 0),               /* a volume the database does not have */
        TSDB_SID(0xfffe, 1),
        TSDB_SID(0, 3),               /* past the series of a volume */
        TSDB_SID(1, 3),
        TSDB_SID(1, UINT32_MAX),
    };
    for (size_t b = 0; b < sizeof bad_ids / sizeof *bad_ids; b++) {
        tsdb_sid bad = bad_ids[b];
        int64_t t = T0 + 1000;
        float x = 99;
        tsdb_type type;
        int cls;
        uint32_t code;
        int64_t limit;
        CHECK(tsdb_series_name(db, bad) == NULL);
        CHECK(tsdb_series_path(db, bad) == NULL);
        CHECK(tsdb_series_type(db, bad, &type) == TSDB_ERR_NOTFOUND);
        CHECK(tsdb_series_class(db, bad, &cls) == TSDB_ERR_NOTFOUND);
        CHECK(tsdb_insert(db, &bad, &t, &x, 1) == TSDB_ERR_INVALID);
        CHECK(tsdb_insert_series(db, bad, &t, &x, 1) == TSDB_ERR_INVALID);
        CHECK(tsdb_append(db, bad, t, x) == TSDB_ERR_INVALID);
        tsdb_values tv = {TSDB_FLOAT32, &x, NULL};
        CHECK(tsdb_insert_values(db, &bad, &t, &tv, 1) == TSDB_ERR_INVALID);
        CHECK(tsdb_insert_series_values(db, bad, &t, &tv, 1) == TSDB_ERR_INVALID);
        CHECK(tsdb_delete_range(db, bad, INT64_MIN, INT64_MAX) == TSDB_ERR_INVALID);
        CHECK(tsdb_replace_range(db, bad, INT64_MIN, INT64_MAX, &t, &tv, 1) == TSDB_ERR_INVALID);
        CHECK(tsdb_replace_range(db, bad, INT64_MIN, INT64_MAX, NULL, NULL, 0) == TSDB_ERR_INVALID);
        CHECK(tsdb_set_retention(db, bad, 1000) == TSDB_ERR_NOTFOUND);
        CHECK(tsdb_get_retention(db, bad, &limit) == TSDB_ERR_NOTFOUND);
        CHECK(tsdb_state(db, bad, "on", &code) == TSDB_ERR_NOTFOUND);
        CHECK(tsdb_state_lookup(db, bad, "on", &code) == TSDB_ERR_NOTFOUND);
        tsdb_sid mixed[2] = {ids[b % N], bad}; /* one unknown id fails the whole call */
        int64_t mt[2] = {T0 + 2000, T0 + 3000};
        float mv[2] = {98, 97};
        CHECK(tsdb_insert(db, mixed, mt, mv, 2) == TSDB_ERR_INVALID);
        tsdb_values mvv = {TSDB_FLOAT32, mv, NULL};
        CHECK(tsdb_insert_values(db, mixed, mt, &mvv, 2) == TSDB_ERR_INVALID);
        /* queries: a well formed id that is unknown answers nothing (a follower may not have seen it yet); one that no
         * volume can have by its bits (a truncated id) answers INVALID, so it does not look like an empty range */
        bool mal = b < 4;
        int want = mal ? TSDB_ERR_INVALID : 0;
        first_row r = {0};
        CHECK(tsdb_query_raw(db, bad, INT64_MIN, INT64_MAX, first_row_fn, &r) == want && r.n == 0);
        tsdb_points p = {0};
        CHECK(tsdb_query_points(db, bad, INT64_MIN, INT64_MAX, &p) == want && p.n == 0);
        tsdb_points_free(&p);
        CHECK(tsdb_query_values(db, bad, INT64_MIN, INT64_MAX, NULL, count_values_fn, &r) == want && r.n == 0);
        CHECK(tsdb_query_lttb(db, bad, INT64_MIN, INT64_MAX, 10, count_values_fn, &r) == want && r.n == 0);
        tsdb_bucket *bk = NULL;
        size_t nb = 1;
        CHECK(tsdb_query_agg(db, bad, INT64_MIN, INT64_MAX, 0, &bk, &nb) == want && nb == 0);
        free(bk);
        int64_t bounds[2] = {T0, T0 + 86400000};
        bk = NULL, nb = 1;
        CHECK(tsdb_query_bounds(db, bad, INT64_MIN, INT64_MAX, bounds, 2, NULL, &bk, &nb) == want && nb == 0);
        free(bk);
        tsdb_group zone = {.unit = TSDB_DAY, .count = 1, .timezone = "Europe/Oslo"};
        bk = NULL, nb = 1;
        CHECK(tsdb_query_group_local(db, bad, T0 - 86400000, T0 + 86400000, &zone, NULL, &bk, &nb) == want &&
              nb == 0);
        free(bk);
        tsdb_spread_bucket *sb = NULL;
        nb = 1;
        CHECK(tsdb_query_spread(db, bad, INT64_MIN, INT64_MAX, 0, NULL, &sb, &nb) == want && nb == 0);
        free(sb);
        tsdb_twa_bucket *tb = NULL;
        nb = 1;
        CHECK(tsdb_query_twa(db, bad, T0, T0 + 86400000, 3600000, NULL, &tb, &nb) == want && nb == 0);
        free(tb);
        tb = NULL, nb = 1;
        int twa_rc = tsdb_query_twa_group_local(db, bad, T0 - 86400000, T0 + 86400000, &zone, NULL, &tb, &nb);
        CHECK((mal ? twa_rc == TSDB_ERR_INVALID : twa_rc != TSDB_ERR_INVALID) && nb == 0);
        free(tb);
        tsdb_series_result *res = NULL;
        tsdb_multi_query q = {.kind = TSDB_MULTI_VALUES, .start = INT64_MIN, .end = INT64_MAX};
        CHECK(tsdb_query_multi_collect(db, &q, mixed, 2, NULL, &res) == 0);
        CHECK(res && res[0].n == 1 && res[0].status == 0 && res[1].n == 0 && res[1].status == want);
        if (res)
            tsdb_series_results_free(res, 2);
        tsdb_multi_query bq = {.kind = TSDB_MULTI_BUCKETS, .start = INT64_MIN, .end = INT64_MAX};
        res = NULL;
        CHECK(tsdb_query_multi_collect(db, &bq, mixed, 2, NULL, &res) == 0);
        CHECK(res && res[0].status == 0 && res[1].n == 0 && res[1].status == want);
        if (res)
            tsdb_series_results_free(res, 2);
    }
    /* none of it had an effect: no series, volume, state, retention or row came or went */
    CHECK(tsdb_volume_count(db) == 2 && tsdb_series_count(db) == N);
    CHECK(tsdb_volume_series_count(db, 0) == 3 && tsdb_volume_series_count(db, 1) == 3);
    for (int i = 0; i < N; i++) {
        first_row r = {0};
        OK(tsdb_query_raw(db, ids[i], INT64_MIN, INT64_MAX, first_row_fn, &r));
        CHECK(r.n == 1 && r.first == (float)(i + 1));
        int64_t limit = -1;
        uint32_t code;
        CHECK(tsdb_get_retention(db, ids[i], &limit) == 0 && limit == 0);
        CHECK(tsdb_state_lookup(db, ids[i], "on", &code) == TSDB_ERR_NOTFOUND);
    }
    OK(tsdb_close(db));
    OK(tsdb_open(root, &o, &db)); /* and nothing on disk either */
    CHECK(tsdb_volume_count(db) == 2 && tsdb_series_count(db) == N);
    for (int i = 0; i < N; i++) {
        first_row r = {0};
        OK(tsdb_query_raw(db, ids[i], INT64_MIN, INT64_MAX, first_row_fn, &r));
        CHECK(r.n == 1 && r.first == (float)(i + 1));
    }
    OK(tsdb_close(db));
    remove_tree(top);
    printf("  the abi version, tsdb_series_foreach, and 64-bit ids that are not truncated\n");
}

/* Names are unique in the whole database: creators of one name naming different volumes, at once, all get the
 * one series, which lives in one volume; a later creation with another path returns it too. */
typedef struct dupjob {
    tsdb *db;
    const char *path;
    const char *name;
    tsdb_sid id;
    int rc;
} dupjob;

static void *dup_creator(void *arg)
{
    dupjob *j = arg;
    j->rc = tsdb_series_at(j->db, j->name, j->path, &j->id);
    return NULL;
}

static void test_unique_names(const char *base)
{
    char top[600], root[700], vp[3][800];
    fresh(base, "uniq", top, sizeof top, root, sizeof root);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    tsdb *db;
    OK(tsdb_open(root, &o, &db));
    for (int v = 0; v < 3; v++) {
        snprintf(vp[v], sizeof vp[v], "%s/v%d", top, v);
        uint32_t idx;
        OK(tsdb_volume_add(db, vp[v], &idx));
        CHECK(idx == (uint32_t)v + 1);
    }
    enum { ROUNDS = 20, THREADS = 8 };
    for (int r = 0; r < ROUNDS; r++) {
        char name[32];
        snprintf(name, sizeof name, "dup.%d", r);
        pthread_t th[THREADS];
        dupjob j[THREADS];
        for (int k = 0; k < THREADS; k++) {
            j[k] = (dupjob){db, k % 4 ? vp[k % 4 - 1] : NULL, name, 0, -1};
            pthread_create(&th[k], NULL, dup_creator, &j[k]);
        }
        for (int k = 0; k < THREADS; k++)
            pthread_join(th[k], NULL);
        for (int k = 0; k < THREADS; k++)
            CHECK(j[k].rc == 0 && j[k].id == j[0].id);
        tsdb_sid again;
        OK(tsdb_series_at(db, name, vp[(TSDB_SID_VOLUME(j[0].id) + 1) % 3], &again)); /* another volume */
        CHECK(again == j[0].id);
    }
    CHECK(tsdb_series_count(db) == ROUNDS);
    uint64_t sum = 0;
    for (uint32_t v = 0; v < 4; v++)
        sum += tsdb_volume_series_count(db, v);
    CHECK(sum == ROUNDS);
    OK(tsdb_close(db));
    OK(tsdb_open(root, &o, &db));
    CHECK(tsdb_series_count(db) == ROUNDS);
    for (int r = 0; r < ROUNDS; r++) {
        char name[32];
        tsdb_sid id;
        snprintf(name, sizeof name, "dup.%d", r);
        OK(tsdb_series_lookup(db, name, &id));
        CHECK(strcmp(tsdb_series_name(db, id), name) == 0);
    }
    OK(tsdb_close(db));
    remove_tree(top);
    printf("  one name, created at once in different volumes: one series\n");
}

/* A name in two volumes cannot be created, but a SERIES copied by hand (or damage) can hold one: every call then
 * gives the same id, whatever volume it names, and the duplicate is reported once. */
static void test_duplicate_name(const char *base)
{
    char top[600], root[700], v1[800], cmd[2000], log[800], text[4096];
    fresh(base, "voldup", top, sizeof top, root, sizeof root);
    snprintf(v1, sizeof v1, "%s/v1", top);
    snprintf(log, sizeof log, "%s/stderr.txt", top);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    tsdb *db = NULL;
    OK(tsdb_open(root, &o, &db));
    tsdb_sid id, want = TSDB_SID(0, 0);
    uint32_t idx = 0;
    OK(tsdb_volume_add(db, v1, &idx));
    CHECK(idx == 1);
    OK(tsdb_series_at(db, "dup", v1, &id));
    CHECK(id == TSDB_SID(1, 0));
    OK(tsdb_close(db));
    /* the record is the first in both files, so it checks out at the same offset in the root's SERIES */
    snprintf(cmd, sizeof cmd, "cp '%s/SERIES' '%s/SERIES'", v1, root);
    CHECK(system(cmd) == 0);
    OK(tsdb_open(root, &o, &db));
    fflush(stderr);
    int saved = dup(2), fd = open(log, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    CHECK(saved >= 0 && fd >= 0 && dup2(fd, 2) == 2);
    CHECK(tsdb_series_lookup(db, "dup", &id) == 0 && id == want);
    CHECK(tsdb_series_at(db, "dup", v1, &id) == 0 && id == want);
    CHECK(tsdb_series(db, "dup", &id) == 0 && id == want);
    CHECK(tsdb_series_create(db, "dup", TSDB_FLOAT32, v1, &id) == 0 && id == want);
    CHECK(tsdb_series_at(db, "dup", v1, &id) == 0 && id == want);
    fflush(stderr);
    dup2(saved, 2);
    close(saved);
    close(fd);
    read_text(log, text, sizeof text);
    int reports = 0;
    for (const char *p = text; (p = strstr(p, "is in volume 0 and in volume 1")) != NULL; p++)
        reports++;
    if (reports != 1)
        fprintf(stderr, "  duplicate reported %d times: %s\n", reports, text);
    CHECK(reports == 1);
    OK(tsdb_close(db));
    remove_tree(top);
    printf("  a name in two volumes (a copied SERIES) gets one id from every call, reported once\n");
}

/* A writer killed after acknowledged appends to series in several volumes: the reopen replays each volume's WAL,
 * and every id names the same series with the same rows. */
static void test_wal_replay(const char *base)
{
    char top[600], root[700], vp[2][800];
    fresh(base, "walrep", top, sizeof top, root, sizeof root);
    for (int v = 0; v < 2; v++)
        snprintf(vp[v], sizeof vp[v], "%s/v%d", top, v);
    enum { SERIES = 6, ROWS = 300 };
    int fds[2];
    CHECK(pipe(fds) == 0);
    pid_t pid = fork();
    if (pid == 0) {
        close(fds[0]);
        tsdb_options o;
        tsdb_options_init(&o);
        tsdb *db;
        if (tsdb_open(root, &o, &db) != 0)
            _exit(2);
        tsdb_sid ids[SERIES];
        for (int s = 0; s < SERIES; s++) {
            char name[32];
            snprintf(name, sizeof name, "wal.%d", s);
            if (tsdb_series_at(db, name, s % 3 ? vp[s % 3 - 1] : NULL, &ids[s]) != 0)
                _exit(3);
        }
        for (int i = 0; i < ROWS; i++)
            for (int s = 0; s < SERIES; s++)
                if (tsdb_append(db, ids[s], T0 + (int64_t)i * 1000, (float)(s * 1000 + i)) != 0)
                    _exit(4);
        if (write(fds[1], ids, sizeof ids) != (ssize_t)sizeof ids)
            _exit(5);
        _exit(0); /* no close, no flush: the rows are only in the WALs */
    }
    close(fds[1]);
    tsdb_sid ids[SERIES];
    CHECK(read(fds[0], ids, sizeof ids) == (ssize_t)sizeof ids);
    close(fds[0]);
    int st = -1;
    CHECK(pid > 0 && waitpid(pid, &st, 0) == pid && WIFEXITED(st) && WEXITSTATUS(st) == 0);
    tsdb_options o;
    tsdb_options_init(&o);
    tsdb *db;
    OK(tsdb_open(root, &o, &db));
    CHECK(tsdb_volume_count(db) == 3 && tsdb_series_count(db) == SERIES);
    for (int s = 0; s < SERIES; s++) {
        char name[32];
        tsdb_sid id;
        snprintf(name, sizeof name, "wal.%d", s);
        OK(tsdb_series_lookup(db, name, &id));
        CHECK(id == ids[s] && TSDB_SID_VOLUME(id) == (uint32_t)(s % 3));
        tsdb_points p;
        OK(tsdb_query_points(db, id, INT64_MIN, INT64_MAX, &p));
        bool ok = p.n == ROWS;
        for (size_t i = 0; ok && i < p.n; i++)
            ok = p.ts[i] == T0 + (int64_t)i * 1000 && p.values[i] == (float)(s * 1000 + (int)i);
        CHECK(ok);
        tsdb_points_free(&p);
    }
    OK(tsdb_close(db));
    remove_tree(top);
    printf("  writer killed with rows in three volumes' WALs: replayed under the same ids\n");
}

#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
#define SANITIZED_MALLOC 1
#elif defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)
#define SANITIZED_MALLOC 1
#endif
#endif

static long minor_faults(void)
{
    struct rusage u;
    getrusage(RUSAGE_SELF, &u);
    return u.ru_minflt;
}

/* A large batch (here 9 M rows) written again and again must reuse the thread's buffers: the ids narrowed for the
 * engine, and a split by volume, went to a malloc per batch, which glibc serves from a fresh mmap above 32 MB, so
 * every batch faulted 4 bytes per row in again (and 16 when split); the 100-billion-row load ran 10% slower on CPU
 * and tripled its system time. After a first batch the engine itself faults a few pages per batch. Sanitizer
 * allocators keep freed memory in quarantine, so their own faults would decide this: skipped there. */
static void test_large_insert_reuses_buffers(const char *base)
{
#ifdef SANITIZED_MALLOC
    (void)base;
    printf("  large inserts reuse their buffers: skipped under a sanitizer\n");
#else
    char top[600], root[700], vp[800];
    fresh(base, "bigins", top, sizeof top, root, sizeof root);
    snprintf(vp, sizeof vp, "%s/v1", top);
    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.merge_threads = 0;
    tsdb *db;
    OK(tsdb_open(root, &o, &db));
    enum { SERIES = 100, PER = 90000 };
    size_t n = (size_t)SERIES * PER;
    tsdb_sid ids[2][SERIES];
    for (int s = 0; s < SERIES; s++) {
        char name[32];
        snprintf(name, sizeof name, "big.%d", s);
        OK(tsdb_series(db, name, &ids[0][s]));
        snprintf(name, sizeof name, "big.v1.%d", s);
        OK(tsdb_series_at(db, name, vp, &ids[1][s]));
    }
    tsdb_sid *id = malloc(n * sizeof *id);
    int64_t *ts = malloc(n * sizeof *ts);
    float *v = malloc(n * sizeof *v);
    if (!id || !ts || !v)
        exit(1);
    long limit = (long)(n * sizeof(uint32_t) / 4096 / 4); /* a quarter of what one per-batch id array faults */
    for (int split = 0; split < 2; split++) {
        long faults = 0;
        for (int b = 0; b < 2; b++) {
            for (size_t s = 0, k = 0; s < SERIES; s++)
                for (size_t i = 0; i < PER; i++, k++) {
                    id[k] = ids[split && (s & 1)][s]; /* split: odd series in the volume, even ones in the root */
                    ts[k] = T0 + (int64_t)(b * PER + i) * 1000;
                    v[k] = (float)(i % 1000);
                }
            long f0 = minor_faults();
            OK(tsdb_insert(db, id, ts, v, n));
            faults = minor_faults() - f0;
        }
        if (faults >= limit)
            fprintf(stderr, "  %s batch of %zu rows: %ld minor faults the second time (limit %ld)\n",
                    split ? "split" : "one-volume", n, faults, limit);
        CHECK(faults < limit);
    }
    tsdb_points p;
    OK(tsdb_query_points(db, ids[1][1], INT64_MIN, INT64_MAX, &p));
    CHECK(p.n == 2 * PER && p.ts[PER] == T0 + (int64_t)PER * 1000 && p.values[PER + 7] == 7);
    tsdb_points_free(&p);
    OK(tsdb_query_points(db, ids[0][2], INT64_MIN, INT64_MAX, &p));
    CHECK(p.n == 2 * PER);
    tsdb_points_free(&p);
    free(id);
    free(ts);
    free(v);
    OK(tsdb_close(db));
    remove_tree(top);
    printf("  large inserts (one volume and split) reuse their buffers: no page faults per batch\n");
#endif
}

int main(int argc, char **argv)
{
    tp_raise_nofile();
    const char *base = argc > 1 ? argv[1] : "/tmp";
    char *canon = realpath(base, NULL); /* canonical, as VOLUMES keeps paths (/tmp is /private/tmp on macOS) */
    if (canon)
        base = canon; /* kept to the end */
    char root[512], va[512], vb[512], other[512], cmd[2200];
    snprintf(root, sizeof root, "%s/tsdb-vol-%d/root", base, (int)getpid());
    snprintf(va, sizeof va, "%s/tsdb-vol-%d/mount_a/sensors", base, (int)getpid());
    snprintf(vb, sizeof vb, "%s/tsdb-vol-%d/mount_b/sensors", base, (int)getpid());
    snprintf(other, sizeof other, "%s/tsdb-vol-%d/other", base, (int)getpid());
    snprintf(cmd, sizeof cmd, "rm -rf '%s/tsdb-vol-%d'", base, (int)getpid());
    if (system(cmd) != 0)
        return 1;
    char mk[1200];
    snprintf(mk, sizeof mk, "mkdir -p '%s/tsdb-vol-%d/mount_a' '%s/tsdb-vol-%d/mount_b'", base, (int)getpid(), base,
             (int)getpid());
    if (system(mk) != 0)
        return 1;

    tsdb_options o;
    tsdb_options_init(&o);
    o.fsync = 0;
    o.granule_rows = 128;
    o.partition_ms = 3600000;
    o.wal_max_batch_rows = 500;
    tsdb *db;
    OK(tsdb_open(root, &o, &db));
    const char *where[NS] = {NULL, va, vb, va, NULL, vb};
    tsdb_sid ids[NS];
    char name[32];
    static const uint32_t vol_of[NS] = {0, 1, 2, 1, 0, 2}, local_of[NS] = {0, 0, 0, 1, 1, 1};
    for (int s = 0; s < NS; s++) {
        snprintf(name, sizeof name, "sensor.%d", s);
        OK(tsdb_series_at(db, name, where[s], &ids[s]));
        CHECK(ids[s] == TSDB_SID(vol_of[s], local_of[s])); /* the volume in bits 32-47, its own index below */
        CHECK(TSDB_SID_VOLUME(ids[s]) == vol_of[s] && TSDB_SID_LOCAL(ids[s]) == local_of[s]);
    }
    for (uint32_t v = 0; v < 3; v++)
        CHECK(tsdb_volume_series_count(db, v) == 2);
    CHECK(tsdb_volume_series_count(db, 3) == 0);
    CHECK(tsdb_volume_count(db) == 3);
    tsdb_sid again;
    OK(tsdb_series_at(db, "sensor.1", vb, &again)); /* exists: stays where it is */
    CHECK(again == ids[1] && strstr(tsdb_series_path(db, again), "mount_a") != NULL);
    CHECK(strcmp(tsdb_series_name(db, ids[5]), "sensor.5") == 0);

    /* batches mixing volumes, unsorted, with duplicates: last write wins */
    for (int round = 0; round < 30; round++) {
        size_t n = 1 + rnd() % 2000;
        tsdb_sid *s = malloc(n * sizeof *s);
        int64_t *t = malloc(n * sizeof *t);
        float *v = malloc(n * sizeof *v);
        for (size_t i = 0; i < n; i++) {
            int ser = (int)(rnd() % NS), slot = (int)(rnd() % SPAN);
            s[i] = ids[ser];
            t[i] = T0 + (int64_t)slot * 1000;
            v[i] = (float)(round * 10000 + (int)i + 1);
        }
        OK(tsdb_insert(db, s, t, v, n));
        for (size_t i = 0; i < n; i++) /* in order: a later duplicate replaces an earlier one */
            for (int ser = 0; ser < NS; ser++)
                if (s[i] == ids[ser])
                    want[ser][(t[i] - T0) / 1000] = v[i];
        free(s);
        free(t);
        free(v);
    }
    check_all(db, ids);

    /* parallel writers, each to its own series (its own volume's WAL) */
    pthread_t th[NS];
    wjob jobs[NS];
    for (int s = 0; s < NS; s++) {
        jobs[s] = (wjob){db, ids[s], s};
        pthread_create(&th[s], NULL, vol_writer, &jobs[s]);
    }
    for (int s = 0; s < NS; s++)
        pthread_join(th[s], NULL);
    check_all(db, ids);
    OK(tsdb_optimize(db));
    check_all(db, ids);
    tsdb_stats st;
    tsdb_get_stats(db, &st);
    printf("  %llu series in %u volumes, %llu parts\n", (unsigned long long)tsdb_series_count(db),
           tsdb_volume_count(db), (unsigned long long)st.parts);

    /* a follower of the root sees all volumes, and a volume created after it opened */
    tsdb_options fo = o;
    fo.follow = 1;
    fo.refresh_ms = 20;
    tsdb *f;
    OK(tsdb_open(root, &fo, &f));
    check_all(f, ids);
    char vc[600];
    snprintf(vc, sizeof vc, "%s/tsdb-vol-%d/mount_c", base, (int)getpid());
    tsdb_sid late;
    OK(tsdb_series_at(db, "late.on_c", vc, &late));
    int64_t lt = T0;
    float lv = 7;
    OK(tsdb_insert_series(db, late, &lt, &lv, 1));
    tsdb_sid seen = 0;
    int rc = TSDB_ERR_NOTFOUND;
    for (int k = 0; k < 100 && rc; k++) {
        rc = tsdb_series_lookup(f, "late.on_c", &seen);
        if (rc)
            usleep(20000);
    }
    CHECK(rc == 0 && seen == late);
    tsdb_points p = {0};
    for (int k = 0; k < 100 && p.n == 0; k++) {
        tsdb_points_free(&p);
        OK(tsdb_query_points(f, seen, INT64_MIN, INT64_MAX, &p));
        if (!p.n)
            usleep(20000);
    }
    CHECK(p.n == 1 && p.values[0] == 7);
    tsdb_points_free(&p);
    OK(tsdb_close(f));
    OK(tsdb_close(db));

    /* reopen: same ids, names, paths and data */
    OK(tsdb_open(root, &o, &db));
    CHECK(tsdb_series_count(db) == NS + 1 && tsdb_volume_count(db) == 4);
    for (int s = 0; s < NS; s++) {
        tsdb_sid id;
        snprintf(name, sizeof name, "sensor.%d", s);
        OK(tsdb_series_lookup(db, name, &id));
        CHECK(id == ids[s]);
    }
    check_all(db, ids);
    OK(tsdb_close(db));

    /* no CATALOG: creating a series is one append to its volume's SERIES; each volume's META says its number */
    char cat[600], mpath[700], mtext[2048], want_line[32];
    snprintf(cat, sizeof cat, "%s/CATALOG", root);
    CHECK(!exists(cat));
    const char *const vpaths[3] = {root, va, vb};
    for (int v = 0; v < 3; v++) {
        snprintf(mpath, sizeof mpath, "%s/META", vpaths[v]);
        read_text(mpath, mtext, sizeof mtext);
        snprintf(want_line, sizeof want_line, "\nvolume=%d\n", v);
        CHECK(strstr(mtext, want_line) != NULL);
    }

    /* a volume belongs to one database */
    tsdb *db2;
    OK(tsdb_open(other, &o, &db2));
    tsdb_sid x;
    CHECK(tsdb_series_at(db2, "steal", va, &x) == TSDB_ERR_INVALID);
    OK(tsdb_close(db2));

    /* two volume directories swapped (or one reached under another line's path): the ids clients hold would
     * reach other series, so the open fails, every time, before the volumes' engines read or write anything */
    char swap[700];
    snprintf(swap, sizeof swap, "%s.swap", va);
    CHECK(rename(va, swap) == 0 && rename(vb, va) == 0 && rename(swap, vb) == 0);
    CHECK(tsdb_open(root, &o, &db) == TSDB_ERR_CORRUPT);
    CHECK(tsdb_open(root, &o, &db) == TSDB_ERR_CORRUPT);
    o.read_only = 1;
    CHECK(tsdb_open(root, &o, &db) == TSDB_ERR_CORRUPT); /* readers too */
    o.read_only = 0;
    CHECK(rename(va, swap) == 0 && rename(vb, va) == 0 && rename(swap, vb) == 0);
    OK(tsdb_open(root, &o, &db));
    check_all(db, ids);
    OK(tsdb_close(db));

    /* a META edited to another number, or without one, or missing, fails the open as well */
    snprintf(mpath, sizeof mpath, "%s/META", va);
    char saved[2048];
    read_text(mpath, saved, sizeof saved);
    meta_with_volume(va, va, 2);
    CHECK(tsdb_open(root, &o, &db) == TSDB_ERR_CORRUPT);
    meta_with_volume(va, va, -1);
    CHECK(tsdb_open(root, &o, &db) == TSDB_ERR_CORRUPT);
    CHECK(unlink(mpath) == 0);
    CHECK(tsdb_open(root, &o, &db) == TSDB_ERR_CORRUPT);
    write_file(mpath, saved);
    /* a volume opened as a database: its META says volume 1, not a database directory; nothing is written there */
    CHECK(tsdb_open(va, &o, &db) == TSDB_ERR_CORRUPT);
    char vf[700];
    snprintf(vf, sizeof vf, "%s/VOLUMES", va);
    CHECK(!exists(vf));
    /* and the database directory itself must say 0 */
    snprintf(mpath, sizeof mpath, "%s/META", root);
    read_text(mpath, saved, sizeof saved);
    meta_with_volume(root, root, 1);
    CHECK(tsdb_open(root, &o, &db) == TSDB_ERR_CORRUPT);
    write_file(mpath, saved);
    OK(tsdb_open(root, &o, &db));
    check_all(db, ids);
    OK(tsdb_close(db));
    tsdb_sid back;

    /* VOLUMES: a torn last line is cut; a volume added after it is found again */
    char vols[600], vd[600];
    snprintf(vols, sizeof vols, "%s/VOLUMES", root);
    snprintf(vd, sizeof vd, "%s/tsdb-vol-%d/vol_d", base, (int)getpid());
    append_bytes(vols, "0123abcd /half/a/pa", 19);
    OK(tsdb_open(root, &o, &db));
    tsdb_sid on_d;
    OK(tsdb_series_at(db, "on.d", vd, &on_d));
    OK(tsdb_append(db, on_d, T0, 1.0));
    OK(tsdb_close(db));
    OK(tsdb_open(root, &o, &db));
    CHECK(tsdb_volume_count(db) == 5);
    OK(tsdb_series_lookup(db, "on.d", &back));
    CHECK(back == on_d);
    OK(tsdb_query_points(db, back, INT64_MIN, INT64_MAX, &p));
    CHECK(p.n == 1);
    tsdb_points_free(&p);
    OK(tsdb_close(db));

    /* a damaged volume line with good ones after it fails the open */
    flip_byte(vols, 12);
    CHECK(tsdb_open(root, &o, &db) == TSDB_ERR_CORRUPT);
    flip_byte(vols, 12);
    OK(tsdb_open(root, &o, &db));
    OK(tsdb_close(db));
    printf("  torn VOLUMES tails cut, damage before good lines fails the open\n");

    /* an add that crashed after the volume's META and before its VOLUMES line: the directory holds our ROOT_ID and
     * a META with a number no line gives it; the open ignores it, and the retried add completes it, under the
     * number it gets then (the second one's was taken by another add in between) */
    char crashed[2][600], rid[700], ridtext[64];
    snprintf(rid, sizeof rid, "%s/ROOT_ID", va);
    read_text(rid, ridtext, sizeof ridtext);
    for (int k = 0; k < 2; k++) {
        snprintf(crashed[k], sizeof crashed[k], "%s/tsdb-vol-%d/crashed%d", base, (int)getpid(), k);
        CHECK(mkdir(crashed[k], 0755) == 0);
        snprintf(rid, sizeof rid, "%s/ROOT_ID", crashed[k]);
        write_file(rid, ridtext);
        meta_with_volume(va, crashed[k], 5);
    }
    OK(tsdb_open(root, &o, &db));
    CHECK(tsdb_volume_count(db) == 5);
    tsdb_sid on_crashed[2];
    for (int k = 0; k < 2; k++) {
        char cname[32];
        snprintf(cname, sizeof cname, "on.crashed%d", k);
        OK(tsdb_series_at(db, cname, crashed[k], &on_crashed[k]));
        CHECK(on_crashed[k] == TSDB_SID(5 + k, 0));
        OK(tsdb_append(db, on_crashed[k], T0, (float)(k + 1)));
        snprintf(mpath, sizeof mpath, "%s/META", crashed[k]);
        read_text(mpath, mtext, sizeof mtext);
        snprintf(want_line, sizeof want_line, "\nvolume=%d\n", 5 + k);
        CHECK(strstr(mtext, want_line) != NULL && strstr(mtext, "volume=") == strstr(mtext, want_line) + 1);
    }
    CHECK(tsdb_volume_count(db) == 7);
    OK(tsdb_close(db));
    OK(tsdb_open(root, &o, &db));
    for (int k = 0; k < 2; k++) {
        OK(tsdb_query_points(db, on_crashed[k], INT64_MIN, INT64_MAX, &p));
        CHECK(p.n == 1 && p.values[0] == (float)(k + 1));
        tsdb_points_free(&p);
    }
    check_all(db, ids);
    OK(tsdb_close(db));
    printf("  volume META: swapped, edited or missing numbers fail the open; a crashed add is completed\n");

    test_paths(base);
    test_refused_dirs(base);
    test_user_files_in_volume(base);
    test_volumes_under_writer(base);
    test_concurrent_adds(base);
    test_open_waits_for_add(base);
    test_readd_other_path(base);
#ifdef __APPLE__
    printf("  (test_new_meta_write: skipped on macOS, it simulates a hung file system with a FIFO, which blocks differently there)\n");
    (void)test_new_meta_write;
#else
    test_new_meta_write(base);
#endif
#ifdef __APPLE__
    printf("  (test_hung_volume: skipped on macOS, it simulates a hung file system with a FIFO, which blocks differently there)\n");
    (void)test_hung_volume;
#else
    test_hung_volume(base);
#endif
    test_volumes_same_size(base);
    test_volumes_damaged_lines(base);
#ifdef __APPLE__
    printf("  (test_hung_reload: skipped on macOS, it simulates a hung file system with a FIFO, which blocks differently there)\n");
    (void)test_hung_reload;
#else
    test_hung_reload(base);
#endif
#ifdef __APPLE__
    printf("  (test_hung_new_volume: skipped on macOS, it simulates a hung file system with a FIFO, which blocks differently there)\n");
    (void)test_hung_new_volume;
#else
    test_hung_new_volume(base);
#endif
#ifdef __APPLE__
    printf("  (test_follower_hung_new_volume: skipped on macOS, it simulates a hung file system with a FIFO, which blocks differently there)\n");
    (void)test_follower_hung_new_volume;
#else
    test_follower_hung_new_volume(base);
#endif
    test_follower_open_race(base);
    test_meta_passing_mismatch(base);
    test_route_past_failed_record(base);
    test_reload_states_fails(base);
#ifdef __APPLE__
    printf("  (test_add_power_loss: skipped on macOS, it needs the crashfs LD_PRELOAD shim)\n");
    (void)test_add_power_loss;
#else
    test_add_power_loss(base, argv[0]);
#endif
    test_series_ids(base);
    test_unique_names(base);
    test_duplicate_name(base);
    test_wal_replay(base);
    test_large_insert_reuses_buffers(base);

    if (system(cmd) != 0)
        return 1;
    if (failures) {
        printf("FAILED: %d checks\n", failures);
        return 1;
    }
    printf("volume tests passed\n");
    return 0;
}
