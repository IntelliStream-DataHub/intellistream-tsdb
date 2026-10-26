/* Volumes: series stored in directories of their own, routed by the root. */
#define _GNU_SOURCE
#include "tsdb.h"

#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <limits.h>
#include <unistd.h>

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

static void check_all(tsdb *db, const uint32_t *ids)
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
    uint32_t id;
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
    uint32_t id, v;
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
    uint32_t on_a;
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
    uint32_t on_b;
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
    uint32_t v, id;
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
                                        "META.tmpfile",   "txn_list",    "txn_notes.intent", "retired_old.csv",
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
    uint32_t v, id;
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
    OK(tsdb_open(root, &o, &db));
    tsdb_points pts;
    OK(tsdb_query_points(db, id, INT64_MIN, INT64_MAX, &pts));
    CHECK(pts.n == 100);
    tsdb_points_free(&pts);
    OK(tsdb_close(db));
    CHECK(!exists(leftover) && !exists(meta_tmp));
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
    uint32_t v, id;
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
    uint32_t v, id;
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
            uint32_t id;
            CHECK(tsdb_series_at(db, "after", p, &id) == 0);
        }
        if (rc == 0)
            OK(tsdb_close(db));
    }
    CHECK(finished && crashes > 3);
    remove_tree(top);
    printf("  power lost at each of %d steps of volume add and after it: the database opens\n", crashes);
}

int main(int argc, char **argv)
{
    const char *base = argc > 1 ? argv[1] : "/tmp";
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
    uint32_t ids[NS];
    char name[32];
    for (int s = 0; s < NS; s++) {
        snprintf(name, sizeof name, "sensor.%d", s);
        OK(tsdb_series_at(db, name, where[s], &ids[s]));
        CHECK(ids[s] == (uint32_t)s);
    }
    CHECK(tsdb_volume_count(db) == 3);
    uint32_t again;
    OK(tsdb_series_at(db, "sensor.1", vb, &again)); /* exists: stays where it is */
    CHECK(again == ids[1] && strstr(tsdb_series_path(db, again), "mount_a") != NULL);
    CHECK(strcmp(tsdb_series_name(db, ids[5]), "sensor.5") == 0);

    /* batches mixing volumes, unsorted, with duplicates: last write wins */
    for (int round = 0; round < 30; round++) {
        size_t n = 1 + rnd() % 2000;
        uint32_t *s = malloc(n * sizeof *s);
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
    printf("  %u series in %u volumes, %llu parts\n", tsdb_series_count(db), tsdb_volume_count(db),
           (unsigned long long)st.parts);

    /* a follower of the root sees all volumes, and a volume created after it opened */
    tsdb_options fo = o;
    fo.follow = 1;
    fo.refresh_ms = 20;
    tsdb *f;
    OK(tsdb_open(root, &fo, &f));
    check_all(f, ids);
    char vc[600];
    snprintf(vc, sizeof vc, "%s/tsdb-vol-%d/mount_c", base, (int)getpid());
    uint32_t late;
    OK(tsdb_series_at(db, "late.on_c", vc, &late));
    int64_t lt = T0;
    float lv = 7;
    OK(tsdb_insert_series(db, late, &lt, &lv, 1));
    uint32_t seen = 0;
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
        uint32_t id;
        snprintf(name, sizeof name, "sensor.%d", s);
        OK(tsdb_series_lookup(db, name, &id));
        CHECK(id == ids[s]);
    }
    check_all(db, ids);
    OK(tsdb_close(db));

    /* crash between creating a series in its volume and recording it: adopted at open */
    char cat[600];
    snprintf(cat, sizeof cat, "%s/CATALOG", root);
    struct stat cs;
    CHECK(stat(cat, &cs) == 0 && cs.st_size == (NS + 1) * 16);
    CHECK(truncate(cat, cs.st_size - 16) == 0); /* "late.on_c" is no longer recorded */
    OK(tsdb_open(root, &o, &db));
    uint32_t back;
    OK(tsdb_series_lookup(db, "late.on_c", &back));
    CHECK(back == late);
    OK(tsdb_query_points(db, back, INT64_MIN, INT64_MAX, &p));
    CHECK(p.n == 1);
    tsdb_points_free(&p);

    /* a volume belongs to one database */
    tsdb *db2;
    OK(tsdb_open(other, &o, &db2));
    uint32_t x;
    CHECK(tsdb_series_at(db2, "steal", va, &x) == TSDB_ERR_INVALID);
    OK(tsdb_close(db2));
    OK(tsdb_close(db));

    /* CATALOG: a torn tail is cut, so the next record starts on a boundary */
    CHECK(stat(cat, &cs) == 0 && cs.st_size == (NS + 1) * 16);
    off_t full = cs.st_size;
    append_bytes(cat, "abc", 3);
    OK(tsdb_open(root, &o, &db));
    CHECK(stat(cat, &cs) == 0 && cs.st_size == full);
    uint32_t after_torn = 0;
    OK(tsdb_series(db, "after.torn", &after_torn));
    CHECK(after_torn == NS + 1);
    OK(tsdb_close(db));
    OK(tsdb_open(root, &o, &db));
    CHECK(tsdb_series_count(db) == NS + 2);
    OK(tsdb_series_lookup(db, "after.torn", &back));
    CHECK(back == after_torn);
    check_all(db, ids);
    OK(tsdb_close(db));
    full += 16;

    /* a run of zeros at the end (blocks allocated, data not written) is a torn tail too */
    static const char zeros[40];
    append_bytes(cat, zeros, sizeof zeros);
    OK(tsdb_open(root, &o, &db));
    CHECK(tsdb_series_count(db) == NS + 2);
    OK(tsdb_close(db));
    CHECK(stat(cat, &cs) == 0 && cs.st_size == full);

    /* a damaged record with good ones after it: the open fails instead of renumbering or re-adding series */
    flip_byte(cat, 16 + 1);
    CHECK(tsdb_open(root, &o, &db) == TSDB_ERR_CORRUPT);
    CHECK(tsdb_open(root, &o, &db) == TSDB_ERR_CORRUPT);
    CHECK(stat(cat, &cs) == 0 && cs.st_size == full); /* nothing appended behind the damage */
    flip_byte(cat, 16 + 1);

    /* a record that checks out but names a series its volume does not have: damage, every time */
    {
        uint32_t vol = 0, local = 9999;
        uint64_t gid = (uint64_t)full / 16;
        uint8_t key[16], rec[16];
        memcpy(key, &gid, 8);
        memcpy(key + 8, &vol, 4);
        memcpy(key + 12, &local, 4);
        uint64_t check = xxh64(key, 16, 0x434154414c4f47ull);
        memcpy(rec, &vol, 4);
        memcpy(rec + 4, &local, 4);
        memcpy(rec + 8, &check, 8);
        append_bytes(cat, rec, 16);
        CHECK(tsdb_open(root, &o, &db) == TSDB_ERR_CORRUPT);
        CHECK(tsdb_open(root, &o, &db) == TSDB_ERR_CORRUPT);
        CHECK(truncate(cat, full) == 0);
    }

    /* VOLUMES: a torn last line is cut; a volume added after it is found again */
    char vols[600], vd[600];
    snprintf(vols, sizeof vols, "%s/VOLUMES", root);
    snprintf(vd, sizeof vd, "%s/tsdb-vol-%d/vol_d", base, (int)getpid());
    append_bytes(vols, "0123abcd /half/a/pa", 19);
    OK(tsdb_open(root, &o, &db));
    uint32_t on_d;
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
    printf("  torn CATALOG and VOLUMES tails cut, damage before good records fails the open\n");

    test_paths(base);
    test_refused_dirs(base);
    test_user_files_in_volume(base);
    test_volumes_under_writer(base);
    test_concurrent_adds(base);
    test_open_waits_for_add(base);
    test_add_power_loss(base, argv[0]);

    if (system(cmd) != 0)
        return 1;
    if (failures) {
        printf("FAILED: %d checks\n", failures);
        return 1;
    }
    printf("volume tests passed\n");
    return 0;
}
