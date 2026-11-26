/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * `tsdb cluster user ...`: users and grants in the cluster's metadata group (S2-1; design-cluster.md C22). A user is
 * a blob of its compacted USERS records (server/users.h user_blob_*); the CLI reads it (USER_GET), changes it here and
 * puts it back conditional on the version it read (USER_PUT, through the Raft leader), so two operators editing one
 * user never lose a change: the second gets STALE, and the CLI reads again and retries. The password is read from a
 * file (or TSDB_PASSWORD, or the terminal) and only its SCRAM verifier leaves this process.
 *
 * A put or drop is answered once committed and applied on the leader; the reply names the members that had not
 * applied it after --wait-ms (default 10 s): they still log users in by what they had.
 */
#include "cluster.h"
#include "meta.h"
#include "users.h"
#include "tsdb.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int cluster_user_cli(int argc, char **argv);
int cluster_user_add_first(const char *addr, const uint8_t *secret, size_t slen, const char *name,
                           const char *pwfile);
int cluster_user_first_check(const char *name, const char *pwfile);
int cli_user_verifier(const char *file, const char *name, uint32_t iterations, user_verifier *v); /* users-cli.c */

static int uusage(void)
{
    fprintf(stderr,
            "usage: tsdb cluster user add PEER NAME [--superuser] [--password-file F] [--iterations N] [--wait-ms MS]\n"
            "       tsdb cluster user passwd PEER NAME [--password-file F] [--iterations N] [--wait-ms MS]\n"
            "       tsdb cluster user drop PEER NAME [--wait-ms MS]\n"
            "       tsdb cluster user grant|revoke PEER NAME SELECTOR RIGHTS [--wait-ms MS]\n"
            "       tsdb cluster user list PEER [NAME]\n"
            "       tsdb cluster user import PEER DIR... [--from-node DIR] [--wait-ms MS]\n"
            "  each with --secret-file F (the cluster's secret). SELECTOR: all, prefix:P, name:N, glob:G; RIGHTS:\n"
            "  read, insert, delete, admin, write, separated by commas. A change is answered once the group\n"
            "  committed it; members that had not applied it after --wait-ms (10000) are listed as lagging.\n"
            "  import reads the USERS of stopped servers' roots (DIR...) and puts the users that are the same in every\n"
            "  one (verifiers as they are: no password is needed); a user that differs between them stops the import\n"
            "  unless --from-node DIR (one of them) says whose copy wins.\n");
    return 2;
}

struct uctx {
    char target[META_ADDR_MAX + 1];
    const uint8_t *secret;
    size_t slen;
};

/* One admin op, following a NOTLEADER to the leader it names (once). 0 with *rc and the reply, or -1 (said). */
static int uadmin(struct uctx *x, int op, const uint8_t *req, size_t rl, int timeout_ms, int *rc, uint8_t **reply,
                  size_t *len)
{
    char err[1024];
    for (int hop = 0;; hop++) {
        *reply = NULL;
        *len = 0;
        if (cluster_admin(x->target, x->secret, x->slen, op, req, rl, timeout_ms, rc, reply, len, err, sizeof err)) {
            fprintf(stderr, "tsdb cluster user: %s\n", err);
            return -1;
        }
        if (*rc == CLUSTER_ERR_NOTLEADER && hop == 0 && *len > 0 && *len < sizeof x->target) {
            memcpy(x->target, *reply, *len);
            x->target[*len] = 0;
            free(*reply);
            continue;
        }
        return 0;
    }
}

/* q holds 2 + USER_NAME_MAX bytes: callers check the name first (user_name_valid) */
static size_t put_name(uint8_t *q, const char *name)
{
    size_t n = strlen(name);
    q[0] = (uint8_t)n;
    q[1] = (uint8_t)(n >> 8);
    memcpy(q + 2, name, n);
    return 2 + n;
}

static uint32_t rd32(const uint8_t *p) { return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24; }
static uint64_t rd64(const uint8_t *p) { return rd32(p) | (uint64_t)rd32(p + 4) << 32; }

/* The user NAME as the server's copy has it: 0 with *u (NULL when there is none) and *version, or -1 (said). */
static int uget(struct uctx *x, const char *name, user **u, uint64_t *version)
{
    uint8_t q[2 + USER_NAME_MAX], *rep;
    size_t ql = put_name(q, name), rl;
    int rc;
    *u = NULL;
    *version = 0;
    if (uadmin(x, CLUSTER_OP_USER_GET, q, ql, 30000, &rc, &rep, &rl) != 0)
        return -1;
    if (rc == META_ERR_NOTFOUND) {
        free(rep);
        return 0;
    }
    if (rc != 0 || rl < 12 + META_USER_UID_LEN) {
        fprintf(stderr, "tsdb cluster user: %s: %s\n", name, rc ? cluster_strerror(rc) : "a reply that does not decode");
        free(rep);
        return -1;
    }
    *version = rd64(rep);
    int d = user_blob_decode(rep + 12, rl - 12, u);
    free(rep);
    if (d != 0) {
        fprintf(stderr, "tsdb cluster user: %s: the stored record does not decode (%d)\n", name, d);
        return -1;
    }
    return 0;
}

/* USER_PUT (u != NULL) or USER_DEL; prints the outcome. 0, 1 (failed; rc in *rc), or -1 (said) */
static int uchange(struct uctx *x, const char *name, const user *u, uint64_t version, uint32_t wait_ms, int *rc)
{
    uint8_t *blob = NULL;
    size_t bl = 0;
    if (u && user_blob_encode(u, &blob, &bl) != 0) {
        fprintf(stderr, "tsdb cluster user: out of memory\n");
        return -1;
    }
    size_t cap = 2 + strlen(name) + 16 + bl;
    uint8_t *q = malloc(cap), *rep;
    if (!q) {
        free(blob);
        return -1;
    }
    size_t ql = put_name(q, name), rl;
    for (int j = 0; j < 8; j++)
        q[ql++] = (uint8_t)(version >> (8 * j));
    for (int j = 0; j < 4; j++)
        q[ql++] = (uint8_t)(wait_ms >> (8 * j));
    if (u) {
        for (int j = 0; j < 4; j++)
            q[ql++] = (uint8_t)(bl >> (8 * j));
        memcpy(q + ql, blob, bl);
        ql += bl;
    }
    free(blob);
    int r = uadmin(x, u ? CLUSTER_OP_USER_PUT : CLUSTER_OP_USER_DEL, q, ql, (int)wait_ms + 30000, rc, &rep, &rl);
    free(q);
    if (r != 0)
        return -1;
    if (*rc != 0) {
        free(rep);
        return 1;
    }
    if (rl < 16) {
        fprintf(stderr, "tsdb cluster user: a reply that does not decode\n");
        free(rep);
        return -1;
    }
    uint64_t index = rd64(rep);
    uint32_t uid = rd32(rep + 8), n = rd32(rep + 12);
    printf("%s user %s: uid %" PRIu32 ", index %" PRIu64 "\n", u ? "put" : "dropped", name, uid, index);
    for (uint32_t k = 0; k < n && 16 + 12 * (size_t)k + 12 <= rl; k++)
        printf("lagging: node %" PRIu32 " applied %" PRIu64 "\n", rd32(rep + 16 + 12 * k), rd64(rep + 20 + 12 * k));
    free(rep);
    return 0;
}

static int secret_of(const char *file, uint8_t *secret, size_t *slen)
{
    char err[1024];
    if (!file) {
        fprintf(stderr, "tsdb cluster: --secret-file is required (the cluster's secret, as tsdbd's "
                        "--cluster-secret-file)\n");
        return 2;
    }
    if (cluster_read_secret(file, secret, CLUSTER_SECRET_MAX, slen, err, sizeof err) != 0) {
        fprintf(stderr, "tsdb cluster: %s\n", err);
        return 1;
    }
    return 0;
}

static void print_user(const user *u, uint64_t version)
{
    char b[RIGHTS_TEXT_MAX];
    static const char *tag[] = {"all", "prefix:", "name:", "glob:"};
    printf("user %s\nuid %" PRIu32 "\nversion %" PRIu64 "\nsuperuser %s\npassword %s\n", u->name, u->uid, version,
           (u->flags & USER_SUPERUSER) ? "yes" : "no", u->has_verifier ? "yes" : "no");
    for (uint32_t j = 0; j < u->ng; j++)
        printf("grant %s%s %s\n", u->g[j].kind <= SEL_GLOB ? tag[u->g[j].kind] : "?", u->g[j].sel,
               rights_text(u->g[j].rights, b));
}

static int ulist(struct uctx *x, const char *name)
{
    if (name && !user_name_valid(name, strlen(name))) {
        fprintf(stderr, "tsdb cluster user: %s: not a valid user name\n", name);
        return 2;
    }
    if (name) {
        user *u;
        uint64_t version;
        if (uget(x, name, &u, &version) != 0)
            return 1;
        if (!u) {
            fprintf(stderr, "tsdb cluster user: %s: no such user\n", name);
            return 1;
        }
        print_user(u, version);
        user_release(u);
        return 0;
    }
    uint8_t q[2] = {0, 0}, *rep;
    size_t rl;
    int rc;
    if (uadmin(x, CLUSTER_OP_USER_GET, q, 2, 30000, &rc, &rep, &rl) != 0)
        return 1;
    if (rc != 0 || rl < 4) {
        fprintf(stderr, "tsdb cluster user list: %s\n", rc ? cluster_strerror(rc) : "a reply that does not decode");
        free(rep);
        return 1;
    }
    uint32_t n = rd32(rep);
    size_t off = 4;
    for (uint32_t i = 0; i < n && off + 2 <= rl; i++) {
        size_t nl = rep[off] | (size_t)rep[off + 1] << 8;
        if (off + 2 + nl + 12 > rl)
            break;
        printf("user %.*s uid %" PRIu32 " version %" PRIu64 "\n", (int)nl, (const char *)rep + off + 2,
               rd32(rep + off + 2 + nl), rd64(rep + off + 6 + nl));
        off += 2 + nl + 12;
    }
    free(rep);
    return 0;
}

/* add, passwd, drop, grant, revoke: read, change, put conditional on the version read; STALE reads again */
static int uedit(struct uctx *x, const char *verb, const char *name, int super, const char *pwfile,
                 uint32_t iterations, const char *seltext, const char *rtext, uint32_t wait_ms)
{
    if (!user_name_valid(name, strlen(name))) {
        fprintf(stderr, "tsdb cluster user: %s: not a valid user name\n", name);
        return 2;
    }
    uint8_t kind = 0, rights = 0;
    const char *sel = NULL;
    size_t sl = 0;
    if (seltext && (sel_parse(seltext, &kind, &sel, &sl) != 0 || rights_parse(rtext, &rights) != 0)) {
        fprintf(stderr, "tsdb cluster user: %s %s: not a selector and rights\n", seltext, rtext);
        return 2;
    }
    user_verifier v;
    int want_v = !strcmp(verb, "add") || !strcmp(verb, "passwd");
    if (want_v && cli_user_verifier(pwfile, name, iterations, &v) != 0)
        return 1;
    for (int attempt = 0; attempt < 8; attempt++) {
        user *u;
        uint64_t version;
        if (uget(x, name, &u, &version) != 0)
            return 1;
        int rc = 0, r;
        if (!strcmp(verb, "add")) {
            if (u) {
                fprintf(stderr, "tsdb cluster user add: %s exists\n", name);
                user_release(u);
                return 1;
            }
            user nu;
            memset(&nu, 0, sizeof nu);
            snprintf(nu.name, sizeof nu.name, "%s", name);
            nu.flags = super ? USER_SUPERUSER : 0;
            nu.has_verifier = true;
            nu.v = v;
            r = uchange(x, name, &nu, 0, wait_ms, &rc);
        } else if (!u) {
            fprintf(stderr, "tsdb cluster user %s: %s: no such user\n", verb, name);
            return 1;
        } else if (!strcmp(verb, "drop")) {
            r = uchange(x, name, NULL, 0, wait_ms, &rc);
        } else {
            if (!strcmp(verb, "passwd")) {
                u->v = v;
                u->has_verifier = true;
            } else {
                int g = user_change_grant(u, kind, sel, sl, rights, !strcmp(verb, "grant"));
                if (g != 0) {
                    fprintf(stderr, "tsdb cluster user %s: %s\n", verb,
                            g == TSDB_ERR_NOTFOUND ? "the user holds none of those rights there"
                                                   : "not a selector and rights it can hold");
                    user_release(u);
                    return 1;
                }
            }
            r = uchange(x, name, u, version, wait_ms, &rc);
        }
        user_release(u);
        if (r < 0)
            return 1;
        if (r == 0)
            return 0;
        if (rc == META_ERR_STALE || (rc == META_ERR_EXISTS && !strcmp(verb, "add")) ||
            (rc == META_ERR_NOTFOUND && strcmp(verb, "add")))
            continue; /* another change came between the read and the put: read again */
        fprintf(stderr, "tsdb cluster user %s: %s\n", verb, cluster_strerror(rc));
        return 1;
    }
    fprintf(stderr, "tsdb cluster user %s: %s kept changing; try again\n", verb, name);
    return 1;
}

/* `tsdb cluster init --superuser NAME --password-file F`, before INIT: the name, and a verifier from the file */
int cluster_user_first_check(const char *name, const char *pwfile)
{
    user_verifier v;
    if (!user_name_valid(name, strlen(name))) {
        fprintf(stderr, "tsdb cluster init: %s: not a valid user name\n", name);
        return 2;
    }
    if (!pwfile) {
        fprintf(stderr, "tsdb cluster init: --superuser needs --password-file\n");
        return 2;
    }
    return cli_user_verifier(pwfile, name, ITER_MIN, &v) != 0;
}

int cluster_user_add_first(const char *addr, const uint8_t *secret, size_t slen, const char *name, const char *pwfile)
{
    struct uctx x = {.secret = secret, .slen = slen};
    snprintf(x.target, sizeof x.target, "%s", addr);
    return uedit(&x, "add", name, 1, pwfile, ITER_MIN, NULL, NULL, 10000);
}

/* Two users hold the same facts: flags, verifier, grants (as sets: their order is their history's). */
static bool user_same(const user *a, const user *b)
{
    if (a->flags != b->flags || a->has_verifier != b->has_verifier || a->ng != b->ng)
        return false;
    if (a->has_verifier &&
        (a->v.iterations != b->v.iterations || a->v.salt_len != b->v.salt_len ||
         memcmp(a->v.salt, b->v.salt, a->v.salt_len) || memcmp(a->v.stored_key, b->v.stored_key, 32) ||
         memcmp(a->v.server_key, b->v.server_key, 32)))
        return false;
    for (uint32_t i = 0; i < a->ng; i++) {
        uint32_t j = 0;
        while (j < b->ng && (b->g[j].kind != a->g[i].kind || b->g[j].rights != a->g[i].rights ||
                             b->g[j].len != a->g[i].len || memcmp(b->g[j].sel, a->g[i].sel, a->g[i].len)))
            j++;
        if (j == b->ng)
            return false;
    }
    return true;
}

/* `import PEER DIR...`: every user of the USERS files given, put with version 0 (a create). A user that differs
 * between them (or is missing from one: that node may have dropped it) stops the whole import before anything is put,
 * unless --from-node names whose copy wins; a user the group already holds the same is skipped, so a rerun after a
 * failure is safe. TSDB_AUTH_MUTANT=importmerge (tests only): differing users go in from the first file. */
static int uimport(struct uctx *x, char **dirs, int nd, const char *from, uint32_t wait_ms)
{
    const char *m = getenv("TSDB_AUTH_MUTANT");
    bool merge = m && !strcmp(m, "importmerge");
    users *us = calloc((size_t)nd, sizeof *us);
    int fi = -1, ret = 0;
    if (!us)
        return 1;
    for (int i = 0; i < nd; i++) {
        if (from && !strcmp(from, dirs[i]))
            fi = i;
        int rc = users_open(&us[i], dirs[i], false);
        if (rc) {
            fprintf(stderr, "tsdb cluster user import: %s/USERS: %s\n", dirs[i], tsdb_strerror(rc));
            ret = 1;
        } else if (us[i].n == 0) {
            fprintf(stderr, "tsdb cluster user import: %s/USERS: no users (is it a server's root?)\n", dirs[i]);
            ret = 1;
        }
    }
    if (from && fi < 0) {
        fprintf(stderr, "tsdb cluster user import: --from-node %s is none of the directories given\n", from);
        ret = 2;
    }
    /* first pass: decide; nothing is put while a user differs without a winner */
    uint32_t differ = 0, nput = 0, same = 0, failed = 0;
    for (int pass = 0; pass < 2 && ret == 0; pass++) {
        for (int i = 0; i < nd && ret == 0; i++) {
            for (uint32_t k = 0; k < us[i].n; k++) {
                const user *u = us[i].u[k];
                bool seen = false, equal = true;
                for (int j = 0; j < i && !seen; j++)
                    seen = users_find(&us[j], u->name) != NULL;
                if (seen)
                    continue;
                for (int j = i + 1; j < nd; j++) {
                    const user *o = users_find(&us[j], u->name);
                    equal &= o && user_same(u, o);
                }
                if (i > 0) /* absent from every earlier one (seen is false) */
                    equal = false;
                const user *put = u;
                if (!equal && !merge) {
                    if (fi < 0) {
                        if (pass == 0) {
                            differ++;
                            fprintf(stderr, "user %s differs between the servers:", u->name);
                            for (int j = 0; j < nd; j++) {
                                const user *o = users_find(&us[j], u->name);
                                fprintf(stderr, " %s: %s", dirs[j],
                                        !o ? "absent" : o->flags & USER_SUPERUSER ? "superuser" : "present");
                                if (o)
                                    fprintf(stderr, " with %" PRIu32 " grants%s", o->ng,
                                            o->has_verifier ? "" : ", no password");
                                fputc(j + 1 < nd ? ',' : '\n', stderr);
                            }
                        }
                        continue;
                    }
                    put = users_find(&us[fi], u->name); /* the winner's copy; absent there: dropped there */
                }
                if (pass == 0 || !put)
                    continue;
                int rc = 0, r = uchange(x, put->name, put, 0, wait_ms, &rc);
                if (r == 1 && rc == META_ERR_EXISTS) {
                    user *g = NULL;
                    uint64_t version;
                    if (uget(x, put->name, &g, &version) == 0 && g && user_same(g, put)) {
                        printf("user %s: in the group already, the same\n", put->name);
                        same++;
                    } else {
                        fprintf(stderr, "user %s: the group holds another user of that name: not imported\n",
                                put->name);
                        failed++;
                    }
                    user_release(g);
                } else if (r != 0) {
                    if (r == 1)
                        fprintf(stderr, "user %s: %s\n", put->name, cluster_strerror(rc));
                    failed++;
                } else {
                    nput++;
                }
            }
        }
        if (pass == 0 && differ) {
            fprintf(stderr, "tsdb cluster user import: %" PRIu32 " users differ between the servers: nothing "
                            "imported; --from-node DIR says whose copy wins\n", differ);
            ret = 1;
        }
    }
    if (ret == 0) {
        printf("imported %" PRIu32 " users (%" PRIu32 " in the group already)%s\n", nput, same,
               failed ? "; some failed" : "");
        ret = failed ? 1 : 0;
    }
    for (int i = 0; i < nd; i++)
        users_close(&us[i]);
    free(us);
    return ret;
}

int cluster_user_cli(int argc, char **argv)
{
    /* argv: cluster user VERB PEER [NAME ...] */
    if (argc < 5)
        return uusage();
    const char *verb = argv[2], *peer = argv[3], *name = NULL, *seltext = NULL, *rtext = NULL;
    const char *secret_file = NULL, *pwfile = NULL;
    if (!strcmp(verb, "import")) {
        char **dirs = calloc((size_t)argc, sizeof *dirs);
        const char *from = NULL;
        long wms = 10000;
        int nd = 0;
        if (!dirs)
            return 1;
        for (int k = 4; k < argc; k++) {
            char *end;
            if (!strcmp(argv[k], "--secret-file") && k + 1 < argc)
                secret_file = argv[++k];
            else if (!strcmp(argv[k], "--from-node") && k + 1 < argc)
                from = argv[++k];
            else if (!strcmp(argv[k], "--wait-ms") && k + 1 < argc &&
                     (wms = strtol(argv[k + 1], &end, 10), k++, !*end && wms >= 0 && wms <= 3600000))
                ;
            else if (strncmp(argv[k], "--", 2))
                dirs[nd++] = argv[k];
            else
                return free(dirs), uusage();
        }
        if (nd == 0)
            return free(dirs), uusage();
        uint8_t sec[CLUSTER_SECRET_MAX];
        size_t sl;
        int s = secret_of(secret_file, sec, &sl);
        if (s)
            return free(dirs), s;
        struct uctx x = {.secret = sec, .slen = sl};
        snprintf(x.target, sizeof x.target, "%s", peer);
        int r = uimport(&x, dirs, nd, from, (uint32_t)wms);
        free(dirs);
        return r;
    }
    int super = 0, i = 4;
    long iterations = ITER_MIN, wait_ms = 10000;
    int edit = !strcmp(verb, "add") || !strcmp(verb, "passwd") || !strcmp(verb, "drop");
    int grant = !strcmp(verb, "grant") || !strcmp(verb, "revoke");
    if (!edit && !grant && strcmp(verb, "list"))
        return uusage();
    if (i < argc && strncmp(argv[i], "--", 2))
        name = argv[i++];
    if ((edit || grant) && !name)
        return uusage();
    if (grant) {
        if (i + 1 >= argc)
            return uusage();
        seltext = argv[i++];
        rtext = argv[i++];
    }
    for (; i < argc; i++) {
        char *end;
        if (!strcmp(argv[i], "--secret-file") && i + 1 < argc)
            secret_file = argv[++i];
        else if (!strcmp(argv[i], "--password-file") && i + 1 < argc && (!strcmp(verb, "add") || !strcmp(verb, "passwd")))
            pwfile = argv[++i];
        else if (!strcmp(argv[i], "--superuser") && !strcmp(verb, "add"))
            super = 1;
        else if (!strcmp(argv[i], "--iterations") && i + 1 < argc) {
            iterations = strtol(argv[++i], &end, 10);
            if (*end || iterations < ITER_MIN || iterations > (long)ITER_MAX)
                return uusage();
        } else if (!strcmp(argv[i], "--wait-ms") && i + 1 < argc) {
            wait_ms = strtol(argv[++i], &end, 10);
            if (*end || wait_ms < 0 || wait_ms > 3600000)
                return uusage();
        } else
            return uusage();
    }
    uint8_t secret[CLUSTER_SECRET_MAX];
    size_t slen;
    int s = secret_of(secret_file, secret, &slen);
    if (s)
        return s;
    struct uctx x = {.secret = secret, .slen = slen};
    snprintf(x.target, sizeof x.target, "%s", peer);
    if (!strcmp(verb, "list"))
        return ulist(&x, name);
    return uedit(&x, verb, name, super, pwfile, (uint32_t)iterations, seltext, rtext, (uint32_t)wait_ms);
}
