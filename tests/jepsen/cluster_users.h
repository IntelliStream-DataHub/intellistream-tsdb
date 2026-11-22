/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The users workload of jepsen_cluster.c (included by it, after its node table and clients): an acknowledged change of
 * a user's password, grant or existence must hold on every member, through kill -9, power loss, partitions and pauses
 * (design-auth.md 12, A30-A34; the Raft log in --state-dir replaces USERS, which was fail-stop on fdatasync).
 *
 * The run's tsdbd all start with a login (jepsen_cluster --auth, implied by the workload): the cluster is initialised
 * with a superuser "jroot" (password file in the run's directory, never argv). One admin thread changes JU_NU users
 * "ju0".."ju3" as jroot through a random member, over the client protocol: add (a password), passwd, grant and revoke
 * (read on the prefix "jpK."), drop (and so add again under the same name). Every password is made for one change
 * ("jpw_<user>_<change>"), so a login names the change whose password it uses. A change is acknowledged (the op
 * returned OK), definitely refused (INVALID, NOTFOUND, DENIED, UNAVAILABLE: A32 says a failed proposal changes
 * nothing) or indeterminate (anything else: the connection closed, a timeout, IO). One prober thread per member
 * logs in as a random user with the password of one of its latest changes, and sends one request on each session: the
 * lookup of the series "jpK.x" in the root volume, which is NOTFOUND (or found) inside the user's grants and DENIED
 * outside them; a session is kept a few seconds, so a grant revoked or a user dropped while it is open is seen.
 * Probes go on hot after every acknowledgement (a burst on the user just changed).
 *
 * What a change binds. A change C acknowledged at T (begun at B) binds member X, for a probe that begins at t > T, when
 * X holds it. X's process started (S) after T: the restart barrier (A31) must have made it learn C from the group,
 * unless a partition or a pause of X lay in [T, t]: a member cut off keeps the users it has (a restarted one, in the
 * minority, may hear a leader that never saw C). X's process started before T: X applied C when the acknowledgement
 * was fast (under 3.5 s: the reply waits for every member that reports, 5 s at most) and X was open for logins a second
 * before T (a probe saw it answer something but UNAVAILABLE), and no pause or partition of X lay in [B, T].
 *
 * The checks, over every probe, at the moment it ends (changes that began before it ended are all in the model):
 *  1 a login succeeds with the password of change A after a change that removes it (a later passwd or drop) was
 *    acknowledged and binds, or with the password of a change that was refused (an answer UNAVAILABLE that was not true);
 *  2 a login is refused (AUTH) with the password of an acknowledged change that binds, with no later passwd or drop begun;
 *  3 a request on a session is allowed after a revoke, a drop or an add (without a grant after it) acknowledged and
 *    binding, or on the session of a user dropped after its login, or with no grant ever made;
 *  4 a request is denied while a grant acknowledged and binding stands (no later revoke, drop or add begun, and no
 *    drop since the session's login: a dropped and added user is a new user, and its old sessions end);
 *  5 a member that took a login with a newer password refuses nothing older any more: an older password never logs in
 *    again on the member that saw the newer one work (an indeterminate change seen applied never reverts).
 * UNAVAILABLE and IO are never a violation (the barrier, a node going down). make jepsen-cluster-selftest plants the
 * server bugs dropnotcommitted (a user op answered before it commits, TSDB_CLUSTER_MUTANT) and loginbeforecatchup (the
 * restart barrier ignored, TSDB_AUTH_MUTANT), and --selftest-checker feeds the checks histories made here.
 */
#define JU_NU 4
#define JU_FAST_NS 3500000000LL
#define JU_OPEN_NS 1000000000LL
#define JU_SKIP 100000

enum { JK_ADD, JK_PASSWD, JK_DROP, JK_GRANT, JK_REVOKE };
enum { JS_INFLIGHT, JS_OK, JS_FAIL, JS_INDET };
static const char *const jk_name[] = {"add", "passwd", "drop", "grant", "revoke"};

struct jch {
    uint8_t kind, user, st;
    uint32_t pos; /* its place in the user's list */
    int64_t begin, end; /* end: INT64_MAX until it ends */
};
struct jdirty {
    int64_t from, until;
};
struct jmodel {
    pthread_mutex_t mu;
    struct jch *c;
    uint32_t n, cap;
    uint32_t *ux[JU_NU];
    uint32_t nux[JU_NU], capx[JU_NU];
    struct jdirty *d[MAXN];
    uint32_t nd[MAXN], capd[MAXN];
    int64_t open_for[MAXN], open_ns[MAXN]; /* the process (its start) that answered something but UNAVAILABLE, and when */
    uint32_t seen[MAXN][JU_NU];            /* 1 + the position of the newest password that logged in at the member */
    int64_t seen_te[MAXN][JU_NU];
    uint64_t ck_dead, ck_live, ck_deny, ck_allow, viol;
    int quiet; /* the selftest: no line per violation */
};

static void jm_init(struct jmodel *m)
{
    memset(m, 0, sizeof *m);
    pthread_mutex_init(&m->mu, NULL);
}
static void jm_free(struct jmodel *m)
{
    free(m->c);
    for (int u = 0; u < JU_NU; u++)
        free(m->ux[u]);
    for (int i = 0; i < MAXN; i++)
        free(m->d[i]);
}

/* a change has begun: its index (1-based, in the password) */
static uint32_t jm_add(struct jmodel *m, int kind, int user, int64_t begin)
{
    pthread_mutex_lock(&m->mu);
    if (m->n == m->cap) {
        m->cap = m->cap ? m->cap * 2 : 1024;
        m->c = realloc(m->c, m->cap * sizeof *m->c);
        if (!m->c)
            die(2, "out of memory");
    }
    if (m->nux[user] == m->capx[user]) {
        m->capx[user] = m->capx[user] ? m->capx[user] * 2 : 256;
        m->ux[user] = realloc(m->ux[user], m->capx[user] * sizeof *m->ux[user]);
        if (!m->ux[user])
            die(2, "out of memory");
    }
    struct jch *c = &m->c[m->n++];
    c->kind = (uint8_t)kind;
    c->user = (uint8_t)user;
    c->st = JS_INFLIGHT;
    c->begin = begin;
    c->end = INT64_MAX;
    c->pos = m->nux[user];
    m->ux[user][m->nux[user]++] = m->n - 1;
    uint32_t id = m->n;
    pthread_mutex_unlock(&m->mu);
    return id;
}
static void jm_end(struct jmodel *m, uint32_t id, int st, int64_t end)
{
    pthread_mutex_lock(&m->mu);
    m->c[id - 1].st = (uint8_t)st;
    if (st != JS_INDET)
        m->c[id - 1].end = end;
    pthread_mutex_unlock(&m->mu);
}
/* a pause or partition of member i (-1: every member) from `now` until jm_settled */
static void jm_fault(struct jmodel *m, int i, int64_t now)
{
    pthread_mutex_lock(&m->mu);
    for (int j = 0; j < MAXN; j++) {
        if (i >= 0 && j != i)
            continue;
        if (m->nd[j] == m->capd[j]) {
            m->capd[j] = m->capd[j] ? m->capd[j] * 2 : 64;
            m->d[j] = realloc(m->d[j], m->capd[j] * sizeof *m->d[j]);
            if (!m->d[j])
                die(2, "out of memory");
        }
        m->d[j][m->nd[j]++] = (struct jdirty){now, INT64_MAX};
    }
    pthread_mutex_unlock(&m->mu);
}
static void jm_settled(struct jmodel *m, int64_t now)
{
    pthread_mutex_lock(&m->mu);
    for (int j = 0; j < MAXN; j++)
        for (uint32_t k = m->nd[j]; k > 0 && m->d[j][k - 1].until == INT64_MAX; k--)
            m->d[j][k - 1].until = now;
    pthread_mutex_unlock(&m->mu);
}
/* members in `clean` (a bit mask) were marked by a fault that, once its cuts are set, turns out not to touch them: their
 * open intervals end at `now` (what stays is the moment between the mark and the cuts being set) */
static void jm_relax(struct jmodel *m, uint32_t clean, int64_t now)
{
    pthread_mutex_lock(&m->mu);
    for (int j = 0; j < MAXN; j++)
        if (clean >> j & 1)
            for (uint32_t k = m->nd[j]; k > 0 && m->d[j][k - 1].until == INT64_MAX; k--)
                m->d[j][k - 1].until = now;
    pthread_mutex_unlock(&m->mu);
}
/* The members a set of cuts leaves in step with the group: those in every largest set of members that reach each other
 * both ways and hold a majority (a member cut from its leader, or from a majority, lags: it keeps the users it has).
 * With no cut the mask is empty (a delay, say, lags everybody). */
static uint32_t jm_clean_set(const uint8_t in[MAXN][MAXN], int n)
{
    int maj = n / 2 + 1, best = 0, any = 0;
    uint32_t inter = 0;
    for (int a = 0; a < n; a++)
        for (int b = 0; b < n; b++)
            if (a != b && in[a][b])
                any = 1;
    if (!any)
        return 0;
    for (uint32_t mask = 1; mask < (1u << n); mask++) {
        int cnt = __builtin_popcount(mask), ok = cnt >= maj;
        for (int a = 0; ok && a < n; a++)
            for (int b = a + 1; ok && b < n; b++)
                if ((mask >> a & 1) && (mask >> b & 1) && (in[a][b] || in[b][a]))
                    ok = 0;
        if (!ok || cnt < best)
            continue;
        if (cnt > best) {
            best = cnt;
            inter = mask;
        } else {
            inter &= mask;
        }
    }
    return inter;
}
static void jm_open(struct jmodel *m, int x, int64_t s, int64_t te)
{
    pthread_mutex_lock(&m->mu);
    if (m->open_for[x] != s) {
        m->open_for[x] = s;
        m->open_ns[x] = te;
    }
    pthread_mutex_unlock(&m->mu);
}

static int jm_dirty(const struct jmodel *m, int x, int64_t a, int64_t b)
{
    for (uint32_t k = m->nd[x]; k > 0; k--) {
        const struct jdirty *d = &m->d[x][k - 1];
        if (d->from <= b && d->until >= a)
            return 1;
    }
    return 0;
}

/* does the acknowledged change c bind member x (its process started at s) for a probe that begins at tb? mu held */
static int jm_bound(const struct jmodel *m, const struct jch *c, int x, int64_t s, int64_t tb)
{
    if (c->st != JS_OK || c->end >= tb)
        return 0;
    if (s > c->end)
        return !jm_dirty(m, x, c->end, tb);
    if (c->end - c->begin > JU_FAST_NS)
        return 0;
    if (m->open_for[x] != s || m->open_ns[x] == 0 || m->open_ns[x] + JU_OPEN_NS > c->end)
        return 0;
    return !jm_dirty(m, x, c->begin, c->end);
}

static void jm_say(struct jmodel *m, const char *what, int x, int u, const struct jch *a, int64_t tb, int64_t te, int rc,
                   const struct jch *b)
{
    m->viol++;
    if (m->quiet || m->viol > 12) /* the first ones say it; the count at the end says how many */
        return;
    fprintf(stderr, "jepsen-cluster: FAIL: users: %s: member %d, user ju%d, the password or session of change %u (%s, "
            "begun %lld ms), the probe [%lld, %lld] ms answered %d", what, x, u, a ? a->pos : 0,
            a ? jk_name[a->kind] : "-", a ? (long long)(a->begin / 1000000) : 0LL, (long long)(tb / 1000000),
            (long long)(te / 1000000), rc);
    if (b)
        fprintf(stderr, "; the change %s begun %lld, acknowledged %lld ms (a change of the user, number %u)\n",
                jk_name[b->kind], (long long)(b->begin / 1000000), (long long)(b->end / 1000000), b->pos);
    else
        fputc('\n', stderr);
}

/* a login of user u at member x (process s) with the password of change a, began tb and ended te with rc (a tsdbc
 * status). 1: a violation. */
static int jm_eval_login(struct jmodel *m, int x, int64_t s, int u, uint32_t a, int64_t tb, int64_t te, int rc)
{
    pthread_mutex_lock(&m->mu);
    const struct jch *A = &m->c[a - 1];
    const struct jch *dead = NULL;
    int later_open = 0; /* a passwd or drop that may have applied by te */
    for (uint32_t k = A->pos + 1; k < m->nux[u]; k++) {
        const struct jch *B = &m->c[m->ux[u][k]];
        if (B->kind != JK_PASSWD && B->kind != JK_DROP)
            continue;
        if (B->st == JS_OK && !dead && jm_bound(m, B, x, s, tb))
            dead = B;
        if (B->st != JS_FAIL && B->begin < te)
            later_open = 1;
    }
    int live = A->st == JS_OK && !later_open && jm_bound(m, A, x, s, tb);
    int v = 0;
    if (rc == 0) {
        if (A->st == JS_FAIL || (dead && A->st != JS_INFLIGHT)) {
            jm_say(m, A->st == JS_FAIL ? "a login with the password of a refused change"
                                       : "a login with a password after the change that removed it was acknowledged",
                   x, u, A, tb, te, rc, dead);
            v = 1;
        }
        /* not across a fault of the member: a restarted one in the minority may hear a leader behind the newer change */
        if (m->seen[x][u] > A->pos + 1 && m->seen_te[x][u] < tb && !jm_dirty(m, x, m->seen_te[x][u], te)) {
            jm_say(m, "an older password logs in after a newer one did on the member", x, u, A, tb, te, rc, NULL);
            v = 1;
        }
        if (A->pos + 1 > m->seen[x][u]) {
            m->seen[x][u] = A->pos + 1;
            m->seen_te[x][u] = te;
        }
    } else if (rc == TSDB_ERR_AUTH && live) {
        jm_say(m, "a login with the password of an acknowledged change is refused", x, u, A, tb, te, rc, NULL);
        v = 1;
    }
    if (dead)
        m->ck_dead++;
    if (live)
        m->ck_live++;
    pthread_mutex_unlock(&m->mu);
    return v;
}

/* a request (the lookup of a series of the user's prefix) on a session of user u opened by change a, at member x */
static int jm_eval_req(struct jmodel *m, int x, int64_t s, int u, uint32_t a, int64_t tb, int64_t te, int rc)
{
    int allowed = rc == 0 || rc == TSDB_ERR_NOTFOUND, denied = rc == TSDB_ERR_DENIED || rc == TSDB_ERR_AUTH;
    if (!allowed && !denied)
        return 0;
    pthread_mutex_lock(&m->mu);
    const struct jch *A = &m->c[a - 1];
    int64_t last_grant = -1, last_remove = -1; /* positions of the newest grant and removal that may have applied */
    int dropped_since = 0;
    for (uint32_t k = 0; k < m->nux[u]; k++) {
        const struct jch *B = &m->c[m->ux[u][k]];
        if (B->begin >= te || B->st == JS_FAIL)
            continue;
        if (B->kind == JK_GRANT)
            last_grant = B->pos;
        else if (B->kind != JK_PASSWD)
            last_remove = B->pos; /* revoke, drop, add: the user holds no grant from here */
        if (B->kind == JK_DROP && B->pos > A->pos)
            dropped_since = 1;
    }
    const struct jch *bad = NULL, *dropb = NULL, *standing = NULL;
    for (uint32_t k = 0; k < m->nux[u]; k++) {
        const struct jch *B = &m->c[m->ux[u][k]];
        if (B->st != JS_OK || B->begin >= te || !jm_bound(m, B, x, s, tb))
            continue;
        if (B->kind != JK_GRANT && B->kind != JK_PASSWD && (int64_t)B->pos > last_grant)
            bad = B; /* a removal that no grant followed */
        if (B->kind == JK_DROP && B->pos > A->pos)
            dropb = B;
        if (B->kind == JK_GRANT && (int64_t)B->pos > last_remove)
            standing = B; /* a grant that nothing removed after */
    }
    int v = 0;
    if (allowed) {
        if (last_grant < 0) {
            jm_say(m, "a request is allowed for a user no grant was ever made to", x, u, A, tb, te, rc, NULL);
            v = 1;
        } else if (bad) {
            jm_say(m, "a request is allowed after the grant was removed and the change acknowledged", x, u, A, tb, te, rc,
                   bad);
            v = 1;
        } else if (dropb) {
            jm_say(m, "a request is allowed on the session of a user dropped after its login", x, u, A, tb, te, rc, dropb);
            v = 1;
        }
    } else if (!dropped_since && standing) {
        jm_say(m, "a request is denied while an acknowledged grant stands", x, u, A, tb, te, rc, standing);
        v = 1;
    }
    if (bad || dropb)
        m->ck_deny++;
    if (!dropped_since && standing)
        m->ck_allow++;
    pthread_mutex_unlock(&m->mu);
    return v;
}

/* ------------------------------------------------------------------ the runtime: the admin thread and the probers */

static struct jmodel jm;
static const char *const auth_user = "jroot", *const auth_pw = "jroot-pw";
static char auth_pw_path[PATH_MAX + 32];
static _Atomic int ju_run, ju_admin_run;
static _Atomic int ju_hot_user = -1;
static _Atomic int64_t ju_hot_until;
static pthread_t ju_admin_th, ju_probe_th[MAXN];
static _Atomic uint64_t ju_ops[5][4], ju_login[6], ju_req[3], ju_noop; /* ju_login: ok, AUTH, UNAVAILABLE, IO, other, skipped */

static uint64_t ju_rng(uint64_t *s)
{
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    return *s;
}
static void ju_pw(char *b, size_t n, int u, uint32_t id) { snprintf(b, n, "jpw_%d_%u", u, id); }

/* A client that logs in as `user` at member i: the tsdbc status, or JU_SKIP (the table has no such live node, the node
 * restarted meanwhile, or its port is another's). *s is the start of the process that answered. */
static int ju_connect(int i, const char *user, const char *pw, tsdbc **c, int64_t *s)
{
    pid_t pid;
    int cport, paused;
    int64_t s0;
    pthread_mutex_lock(&tbl_mu);
    pid = nodes[i].pid;
    cport = nodes[i].cport;
    s0 = nodes[i].proc_ns;
    paused = nodes[i].paused;
    pthread_mutex_unlock(&tbl_mu);
    *c = NULL;
    if (pid <= 0 || cport <= 0 || paused)
        return JU_SKIP;
    tsdbc_options o = {.user = user, .password = pw, .connect_ms = 1000, .require_auth = 1};
    int rc = tsdbc_connect_opts("127.0.0.1", cport, &o, c);
    pthread_mutex_lock(&tbl_mu);
    int same = nodes[i].pid == pid && nodes[i].proc_ns == s0 && !nodes[i].paused;
    pthread_mutex_unlock(&tbl_mu);
    if (!same || (rc == 0 && !th_owns_port(pid, cport))) {
        if (*c)
            tsdbc_close(*c);
        *c = NULL;
        return JU_SKIP;
    }
    *s = s0;
    if (rc != 0) {
        *c = NULL;
        return rc;
    }
    tsdbc_set_retry(*c, 0);
    tsdbc_set_timeout(*c, 3000);
    return 0;
}

/* the change whose password a probe tries: usually the newest that was not refused, else one of the last four */
static uint32_t ju_pick_pw(int u, uint64_t *rs)
{
    uint32_t cand[4], nc = 0, newest = 0;
    pthread_mutex_lock(&jm.mu);
    for (uint32_t k = jm.nux[u]; k > 0 && nc < 4; k--) {
        uint32_t id = jm.ux[u][k - 1];
        const struct jch *c = &jm.c[id];
        if (c->kind != JK_ADD && c->kind != JK_PASSWD)
            continue;
        if (!newest && c->st != JS_FAIL)
            newest = id + 1;
        cand[nc++] = id + 1;
    }
    pthread_mutex_unlock(&jm.mu);
    if (!nc)
        return 0;
    if (newest && ju_rng(rs) % 100 < 55)
        return newest;
    return cand[ju_rng(rs) % nc];
}

/* the first violation: what every member's metadata copy holds of the user, and who leads (the copy is the cluster
 * thread's; the login reads what the users watch took from it, a step behind) */
static void ju_diag(int x, int u, uint32_t a)
{
    static _Atomic int done;
    if (atomic_exchange(&done, 1))
        return;
    char name[16];
    snprintf(name, sizeof name, "ju%d", u);
    fprintf(stderr, "jepsen-cluster: users: at the first violation (member %d, user %s) at %lld ms:\n", x, name,
            (long long)(now_ns() / 1000000));
    for (int i = 0; i < nn; i++) {
        struct view v;
        if (view_get(i, &v, 1500) != 0) {
            fprintf(stderr, "  member %d: no STATUS\n", i);
            continue;
        }
        const struct meta_user *mu = meta_user(&v.m, name);
        fprintf(stderr, "  member %d: role %d, leader %u, applied %" PRIu64 ", %u users, %s %s (version %" PRIu64 "), users "
                "caught up %d\n", i, (int)v.st.role, (unsigned)v.st.leader, (uint64_t)v.m.applied, (unsigned)v.m.nusers,
                name, mu ? "present" : "absent", mu ? (uint64_t)mu->version : 0, (int)v.st.users_caught_up);
        view_free(&v);
        /* and what its login answers now: the same user and password, and the users the superuser sees there */
        char pw[40];
        ju_pw(pw, sizeof pw, u, a - 1);
        tsdbc *c = NULL;
        int64_t sp;
        int rc = ju_connect(i, name, pw, &c, &sp);
        if (c)
            tsdbc_close(c);
        int rr = ju_connect(i, auth_user, auth_pw, &c, &sp);
        size_t nu = 0;
        int found = 0;
        tsdbc_user *ul = NULL;
        if (rr == 0 && tsdbc_user_list(c, &ul, &nu) == 0) {
            for (size_t k = 0; k < nu; k++)
                found |= !strcmp(ul[k].name, name);
            tsdbc_users_free(ul, nu);
        }
        if (c)
            tsdbc_close(c);
        fprintf(stderr, "    login as %s now: %d; the superuser's login: %d, sees %zu users, %s %s\n", name, rc, rr, nu,
                name, found ? "among them" : "not among them");
    }
}

/* does the member's copy hold the read grant of user `name` on prefix `sel`? 1, 0, or -1 (the list failed) */
static int ju_holds(tsdbc *c, const char *name, const char *sel)
{
    tsdbc_user *ul = NULL;
    size_t nu = 0;
    if (tsdbc_user_list(c, &ul, &nu) != 0)
        return -1;
    int held = 0;
    for (size_t k = 0; k < nu; k++)
        if (!strcmp(ul[k].name, name))
            for (uint32_t g = 0; g < ul[k].ngrants; g++)
                if (ul[k].grants[g].kind == TSDB_SEL_PREFIX && !strcmp(ul[k].grants[g].selector, sel) &&
                    (ul[k].grants[g].rights & TSDB_RIGHT_READ))
                    held = 1;
    tsdbc_users_free(ul, nu);
    return held;
}

static void *ju_admin(void *arg)
{
    (void)arg;
    uint64_t rs = mix(seed * 31 + 5) | 1;
    int bel_exists[JU_NU] = {0}, bel_granted[JU_NU] = {0}; /* what the thread thinks; an answer that says no corrects it */
    while (atomic_load(&ju_admin_run) && !atomic_load(&stopping)) {
        usleep(10000 + (unsigned)(ju_rng(&rs) % 120000));
        int u = (int)(ju_rng(&rs) % JU_NU), exists = bel_exists[u], granted = bel_granted[u];
        uint64_t r = ju_rng(&rs) % 100;
        int kind = !exists ? JK_ADD : r < 18 ? JK_PASSWD : r < 30 ? JK_DROP : granted ? JK_REVOKE : JK_GRANT;
        if (exists && !granted && r >= 30 && r < 36)
            kind = JK_REVOKE; /* the user holds none: refused, as a definite failure */
        int i = (int)(ju_rng(&rs) % (uint64_t)nn);
        tsdbc *c;
        int64_t s;
        if (ju_connect(i, auth_user, auth_pw, &c, &s) != 0)
            continue;
        tsdbc_set_timeout(c, 40000);
        char name[16], pw[40], sel[16];
        snprintf(name, sizeof name, "ju%d", u);
        snprintf(sel, sizeof sel, "jp%d.", u);
        /* A grant the member's copy holds already changes nothing: the server answers OK from that copy at once, with no
         * proposal and so no wait for the members (auth.c admin_group: pr.op == 0), and a member that has not applied
         * the entry which made the grant (an indeterminate one, say) may lag. A server bug against A32 (docs/tasks/
         * users-stale-answers.md), not yet fixed: until it is, such an answer binds nobody and the model records it as
         * refused (counted as `noop`). The copy may move between the list and the grant, so the grant counts as a no-op
         * only if the copy still holds it after the answer; one that does not is indeterminate (it may have been real). */
        int held_before = kind == JK_GRANT ? ju_holds(c, name, sel) : 0;
        uint32_t id = jm_add(&jm, kind, u, now_ns());
        int rc;
        ju_pw(pw, sizeof pw, u, id - 1);
        switch (kind) {
        case JK_ADD: rc = tsdbc_user_create(c, name, pw, 0, 0); break;
        case JK_PASSWD: rc = tsdbc_user_password(c, name, pw, 0); break;
        case JK_DROP: rc = tsdbc_user_drop(c, name); break;
        case JK_GRANT: rc = tsdbc_grant(c, name, TSDB_SEL_PREFIX, sel, TSDB_RIGHT_READ); break;
        default: rc = tsdbc_revoke(c, name, TSDB_SEL_PREFIX, sel, TSDB_RIGHT_READ); break;
        }
        int64_t te = now_ns();
        int st = rc == 0 ? JS_OK
                 : rc == TSDB_ERR_INVALID || rc == TSDB_ERR_NOTFOUND || rc == TSDB_ERR_DENIED || rc == TSDB_ERR_UNAVAILABLE
                     ? JS_FAIL
                     : JS_INDET;
        int noop = rc == 0 && held_before == 1;
        if (noop) {
            int after = ju_holds(c, name, sel);
            if (after == 1)
                atomic_fetch_add(&ju_noop, 1);
            else {
                noop = 0;
                st = JS_INDET;
            }
        }
        jm_end(&jm, id, noop ? JS_FAIL : st, te);
        if (st != JS_FAIL) { /* applied, or maybe (assumed applied: the next answer corrects it) */
            if (kind == JK_ADD)
                bel_exists[u] = 1, bel_granted[u] = 0;
            else if (kind == JK_DROP)
                bel_exists[u] = bel_granted[u] = 0;
            else if (kind == JK_GRANT)
                bel_granted[u] = 1;
            else if (kind == JK_REVOKE)
                bel_granted[u] = 0;
        } else if (rc == TSDB_ERR_INVALID && kind == JK_ADD) {
            bel_exists[u] = 1; /* the name is taken */
        } else if (rc == TSDB_ERR_NOTFOUND) {
            if (kind == JK_REVOKE)
                bel_granted[u] = 0;
            else
                bel_exists[u] = bel_granted[u] = 0;
        }
        ev("users: %s ju%d (change %u) on node %d: %d", jk_name[kind], u, id - 1, i, rc);
        atomic_fetch_add(&ju_ops[kind][st], 1);
        if (st == JS_OK) {
            atomic_store(&ju_hot_user, u);
            atomic_store(&ju_hot_until, te + 400000000LL);
        }
        tsdbc_close(c);
    }
    return NULL;
}

struct jsess {
    tsdbc *c;
    uint32_t a;
    int64_t s, born;
};
static void js_close(struct jsess *s)
{
    if (s->c)
        tsdbc_close(s->c);
    s->c = NULL;
}

static void *ju_prober(void *arg)
{
    int x = (int)(intptr_t)arg;
    uint64_t rs = mix(seed * 977 + (uint64_t)x) | 1;
    struct jsess ss[JU_NU];
    memset(ss, 0, sizeof ss);
    while (atomic_load(&ju_run) && !atomic_load(&stopping)) {
        usleep(1000 + (unsigned)(ju_rng(&rs) % 5000));
        int64_t now = now_ns();
        int u = (int)(ju_rng(&rs) % JU_NU), hu = atomic_load(&ju_hot_user);
        if (hu >= 0 && now < atomic_load(&ju_hot_until) && ju_rng(&rs) % 100 < 70)
            u = hu;
        struct jsess *s = &ss[u];
        if (s->c && now - s->born > 2500000000LL)
            js_close(s);
        if (!s->c || ju_rng(&rs) % 100 < 25) {
            uint32_t a = ju_pick_pw(u, &rs);
            if (!a)
                continue;
            char name[16], pw[40];
            snprintf(name, sizeof name, "ju%d", u);
            ju_pw(pw, sizeof pw, u, a - 1);
            tsdbc *c;
            int64_t sp = 0, tb = now_ns();
            int rc = ju_connect(x, name, pw, &c, &sp);
            int64_t te = now_ns();
            if (rc == JU_SKIP) {
                atomic_fetch_add(&ju_login[5], 1);
                continue;
            }
            atomic_fetch_add(&ju_login[rc == 0 ? 0 : rc == TSDB_ERR_AUTH ? 1 : rc == TSDB_ERR_UNAVAILABLE ? 2
                                       : rc == TSDB_ERR_IO ? 3 : 4], 1);
            if (rc != TSDB_ERR_UNAVAILABLE && rc != TSDB_ERR_IO)
                jm_open(&jm, x, sp, te);
            if (rc == TSDB_ERR_IO || rc == TSDB_ERR_UNAVAILABLE)
                continue; /* not an answer about the user */
            if (jm_eval_login(&jm, x, sp, u, a, tb, te, rc))
                ju_diag(x, u, a);
            if (rc != 0)
                continue;
            js_close(s);
            *s = (struct jsess){c, a, sp, tb};
        }
        tsdb_sid sid = 0;
        char ser[16];
        snprintf(ser, sizeof ser, "jp%d.x", u);
        int64_t tb = now_ns();
        int rc = tsdbc_series(s->c, ser, 0, &sid);
        int64_t te = now_ns();
        int64_t cur;
        pthread_mutex_lock(&tbl_mu);
        cur = nodes[x].proc_ns;
        pthread_mutex_unlock(&tbl_mu);
        if (cur != s->s) { /* the member restarted: the session is dead */
            js_close(s);
            continue;
        }
        atomic_fetch_add(&ju_req[rc == 0 || rc == TSDB_ERR_NOTFOUND ? 0 : rc == TSDB_ERR_DENIED || rc == TSDB_ERR_AUTH ? 1 : 2], 1);
        if (jm_eval_req(&jm, x, s->s, u, s->a, tb, te, rc))
            ju_diag(x, u, s->a);
        if (rc != 0 && rc != TSDB_ERR_NOTFOUND && rc != TSDB_ERR_DENIED)
            js_close(s);
    }
    for (int u = 0; u < JU_NU; u++)
        js_close(&ss[u]);
    return NULL;
}

/* the superuser: made by the CLI (`tsdb cluster user add`) through member 0's real peer port, before the other members
 * join (the password in a file of the run, never argv) */
static void ju_make_superuser(int i)
{
    snprintf(auth_pw_path, sizeof auth_pw_path, "%s/jroot.pw", rundir);
    FILE *f = fopen(auth_pw_path, "w");
    if (!f || fprintf(f, "%s\n", auth_pw) < 0 || fclose(f) != 0)
        die(2, "cannot write %s", auth_pw_path);
    chmod(auth_pw_path, 0600);
    int pp;
    node_snap(i, NULL, NULL, &pp);
    const char *cli = getenv("TSDB_CLI") ? getenv("TSDB_CLI") : "./build/tsdb";
    char cmd[3 * PATH_MAX + 256], out[2048];
    snprintf(cmd, sizeof cmd, "%s cluster user add 127.0.0.1:%d %s --superuser --password-file %s --secret-file %s 2>&1",
             cli, pp, auth_user, auth_pw_path, secret_path);
    FILE *p = popen(cmd, "r");
    if (!p)
        die(2, "popen %s: %s", cmd, strerror(errno));
    size_t got = fread(out, 1, sizeof out - 1, p);
    out[got] = 0;
    int st = pclose(p);
    if (!WIFEXITED(st) || WEXITSTATUS(st) != 0 || !strstr(out, "put user "))
        die(2, "the superuser: %s", out);
}

/* A member that gets its Raft core (a learner promoted to voter, in the seconds after the node adds) rebuilds its copy
 * from nothing, and for about 100 ms its users watch is given an empty list: every login there is refused AUTH, the
 * superuser's too, and a session ends (a finding for the server, jepsen-cluster/NOTES: not a change that did not hold).
 * The workload starts when the promotions are over: the number of members with a core has been the same for 3 s. */
static void ju_await_voters(void)
{
    int64_t t0 = now_ms(), changed = t0;
    int last = -1;
    while (now_ms() - t0 < 60000) {
        int cores = 0;
        for (int i = 0; i < nn; i++) {
            struct view v;
            if (view_get(i, &v, 1500) == 0) {
                cores += v.st.mode == CLUSTER_MODE_CORE;
                view_free(&v);
            }
        }
        if (cores != last) {
            last = cores;
            changed = now_ms();
        }
        if (cores >= 3 && now_ms() - changed >= 3000)
            return;
        usleep(250000);
    }
    die(1, "the members did not settle into voters within 60 s (%d with a Raft core)", last);
}

static void ju_start(void)
{
    jm_init(&jm);
    atomic_store(&ju_run, 1);
    atomic_store(&ju_admin_run, 1);
    pthread_create(&ju_admin_th, NULL, ju_admin, NULL);
    for (int i = 0; i < nn; i++)
        pthread_create(&ju_probe_th[i], NULL, ju_prober, (void *)(intptr_t)i);
}
static void ju_stop_admin(void)
{
    if (!atomic_exchange(&ju_admin_run, 0))
        return;
    pthread_join(ju_admin_th, NULL);
}
static void ju_stop(void)
{
    if (!atomic_exchange(&ju_run, 0))
        return;
    for (int i = 0; i < nn; i++)
        pthread_join(ju_probe_th[i], NULL);
}

/* the hooks of the nemeses (no-ops without the workload) */
static void ju_fault(int i)
{
    if (want_users)
        jm_fault(&jm, i, now_ns());
}
/* the cuts of a partition are set: the members that stay in step with the group are no longer exempt */
static void ju_cut_done(const uint8_t in[MAXN][MAXN], int n)
{
    if (want_users)
        jm_relax(&jm, jm_clean_set(in, n), now_ns());
}
static void ju_settled(void)
{
    if (want_users)
        jm_settled(&jm, now_ns());
}

/* the end: the counts, and the violations (1 per failed run) */
static int ju_report(void)
{
    uint64_t ok = 0, fail = 0, indet = 0;
    for (int k = 0; k < 5; k++) {
        ok += atomic_load(&ju_ops[k][JS_OK]);
        fail += atomic_load(&ju_ops[k][JS_FAIL]);
        indet += atomic_load(&ju_ops[k][JS_INDET]);
    }
    printf("users: %" PRIu64 " changes acknowledged (add %" PRIu64 ", passwd %" PRIu64 ", drop %" PRIu64 ", grant %" PRIu64
           ", revoke %" PRIu64 "), %" PRIu64 " refused, %" PRIu64 " indeterminate; logins %" PRIu64 " ok, %" PRIu64
           " AUTH, %" PRIu64 " UNAVAILABLE, %" PRIu64 " IO, %" PRIu64 " other, %" PRIu64 " skipped; requests %" PRIu64
           " allowed, %" PRIu64 " denied, %" PRIu64 " other\n",
           ok, (uint64_t)atomic_load(&ju_ops[JK_ADD][JS_OK]), (uint64_t)atomic_load(&ju_ops[JK_PASSWD][JS_OK]),
           (uint64_t)atomic_load(&ju_ops[JK_DROP][JS_OK]), (uint64_t)atomic_load(&ju_ops[JK_GRANT][JS_OK]),
           (uint64_t)atomic_load(&ju_ops[JK_REVOKE][JS_OK]), fail, indet, (uint64_t)atomic_load(&ju_login[0]),
           (uint64_t)atomic_load(&ju_login[1]), (uint64_t)atomic_load(&ju_login[2]), (uint64_t)atomic_load(&ju_login[3]),
           (uint64_t)atomic_load(&ju_login[4]), (uint64_t)atomic_load(&ju_login[5]), (uint64_t)atomic_load(&ju_req[0]),
           (uint64_t)atomic_load(&ju_req[1]), (uint64_t)atomic_load(&ju_req[2]));
    printf("users: checked %" PRIu64 " logins that must fail and %" PRIu64 " that must succeed, %" PRIu64
           " requests that must be denied and %" PRIu64 " that must pass; %" PRIu64 " violation(s)\n", jm.ck_dead,
           jm.ck_live, jm.ck_deny, jm.ck_allow, jm.viol);
    printf("users: %" PRIu64 " grants answered OK from a copy that held them already (unbound: docs/tasks/users-stale-answers.md)\n",
           (uint64_t)atomic_load(&ju_noop));
    int rc = 0;
    if (jm.viol) {
        fprintf(stderr, "jepsen-cluster: FAIL: users: %" PRIu64 " violation(s): a change acknowledged did not hold\n",
                jm.viol);
        rc = 1;
    }
    if (seconds >= 20 && (!ok || !jm.ck_dead || !jm.ck_live || !jm.ck_deny || !jm.ck_allow)) { /* no evidence */
        fprintf(stderr, "jepsen-cluster: FAIL: users: the workload checked too little: %" PRIu64 " acknowledged changes, "
                "%" PRIu64 "/%" PRIu64 " logins that must fail/succeed, %" PRIu64 "/%" PRIu64 " requests that must "
                "be denied/pass\n", ok, jm.ck_dead, jm.ck_live, jm.ck_deny, jm.ck_allow);
        rc = 1;
    }
    return rc;
}

/* ------------------------------------------------------------------ --selftest-checker: the checks on histories made here */

#define SEC(x) ((int64_t)((x) * 1e9))
static uint32_t jt_ch(struct jmodel *m, int kind, int u, double b, double e, int st)
{
    uint32_t id = jm_add(m, kind, u, SEC(b));
    jm_end(m, id, st, SEC(e));
    return id;
}
static void jt_open(struct jmodel *m, int x, double s, double open)
{
    m->open_for[x] = SEC(s);
    m->open_ns[x] = SEC(open);
}

static int ju_selftest(void)
{
    struct jmodel m;
    jm_init(&m);
    m.quiet = 1;
    int bad = 0;
    for (int x = 0; x < 3; x++)
        jt_open(&m, x, -100, -50); /* members 0..2: old processes that were open long before */
    /* user 0: add (1), passwd (5), grant (6), revoke (8); a refused passwd (10); drop (12), add (13), grant (14) */
    uint32_t add = jt_ch(&m, JK_ADD, 0, 1, 1.1, JS_OK), pw2 = jt_ch(&m, JK_PASSWD, 0, 5, 5.1, JS_OK);
    jt_ch(&m, JK_GRANT, 0, 6, 6.1, JS_OK);
    jt_ch(&m, JK_REVOKE, 0, 8, 8.1, JS_OK);
    uint32_t pwf = jt_ch(&m, JK_PASSWD, 0, 10, 10.1, JS_FAIL);
    /* user 1: a passwd whose acknowledgement was slow */
    uint32_t add1 = jt_ch(&m, JK_ADD, 1, 1, 1.1, JS_OK);
    jt_ch(&m, JK_PASSWD, 1, 5, 9, JS_OK);
    struct {
        const char *what;
        int expect; /* a violation: 1, none: 0 */
        int got;
    } t[32];
    int n = 0;
#define CASE(w, e, call) do { uint64_t v0 = m.viol; call; t[n].what = w; t[n].expect = e; t[n].got = m.viol > v0; n++; } while (0)
    CASE("the password that a passwd replaced is refused", 0, jm_eval_login(&m, 0, SEC(-100), 0, add, SEC(9), SEC(9.01), TSDB_ERR_AUTH));
    CASE("the new password logs in", 0, jm_eval_login(&m, 0, SEC(-100), 0, pw2, SEC(9), SEC(9.01), 0));
    CASE("a request inside the grant is allowed", 0, jm_eval_req(&m, 0, SEC(-100), 0, pw2, SEC(7), SEC(7.01), TSDB_ERR_NOTFOUND));
    CASE("a request after the revoke is denied", 0, jm_eval_req(&m, 0, SEC(-100), 0, pw2, SEC(9), SEC(9.01), TSDB_ERR_DENIED));
    CASE("a login with the replaced password after the passwd was acknowledged", 1,
         jm_eval_login(&m, 0, SEC(-100), 0, add, SEC(9), SEC(9.01), 0));
    CASE("the acknowledged password is refused", 1, jm_eval_login(&m, 0, SEC(-100), 0, pw2, SEC(9), SEC(9.01), TSDB_ERR_AUTH));
    CASE("the password of a refused change logs in", 1, jm_eval_login(&m, 0, SEC(-100), 0, pwf, SEC(11), SEC(11.01), 0));
    CASE("a request is allowed after the revoke was acknowledged", 1,
         jm_eval_req(&m, 0, SEC(-100), 0, pw2, SEC(9), SEC(9.01), TSDB_ERR_NOTFOUND));
    CASE("a request is denied while the grant stands", 1, jm_eval_req(&m, 0, SEC(-100), 0, pw2, SEC(7), SEC(7.01), TSDB_ERR_DENIED));
    CASE("a request is allowed before any grant was made", 1,
         jm_eval_req(&m, 0, SEC(-100), 1, add1, SEC(7), SEC(7.01), 0));
    CASE("a slow acknowledgement binds nobody that was already running", 0,
         jm_eval_login(&m, 0, SEC(-100), 1, add1, SEC(10), SEC(10.01), 0));
    /* a member that restarted after the passwd (process started at 5.5 s) must have learned it */
    jt_open(&m, 3, 5.5, 6);
    CASE("a member restarted after the passwd takes the replaced password", 1,
         jm_eval_login(&m, 3, SEC(5.5), 0, add, SEC(9), SEC(9.01), 0));
    /* member 4 behind a partition (dirty from 4 to 30 s) keeps what it had */
    jm_fault(&m, 4, SEC(4));
    jm_settled(&m, SEC(30));
    jt_open(&m, 4, 5.5, 6);
    CASE("a restarted member behind a partition may take it", 0, jm_eval_login(&m, 4, SEC(5.5), 0, add, SEC(9), SEC(9.01), 0));
    jm_fault(&m, 2, SEC(4));
    jm_settled(&m, SEC(30));
    CASE("a paused member may take the replaced password", 0, jm_eval_login(&m, 2, SEC(-100), 0, add, SEC(9), SEC(9.01), 0));
    m.open_for[1] = SEC(-100);
    m.open_ns[1] = 0;
    CASE("a member never seen open binds nothing", 0, jm_eval_login(&m, 1, SEC(-100), 0, add, SEC(9), SEC(9.01), 0));
    /* member 1 (nothing binds it): the new password logs in, then the old one does again */
    CASE("the new password logs in on a member", 0, jm_eval_login(&m, 1, SEC(-100), 0, pw2, SEC(20), SEC(20.01), 0));
    CASE("the replaced password logs in after the new one did, on the same member", 1,
         jm_eval_login(&m, 1, SEC(-100), 0, add, SEC(21), SEC(21.01), 0));
    /* a user dropped, added again and granted: the old session must end, a new one is allowed */
    jt_ch(&m, JK_DROP, 0, 12, 12.1, JS_OK);
    uint32_t add2 = jt_ch(&m, JK_ADD, 0, 13, 13.1, JS_OK);
    jt_ch(&m, JK_GRANT, 0, 14, 14.1, JS_OK);
    CASE("the session of a dropped user is allowed after the user came back", 1,
         jm_eval_req(&m, 0, SEC(-100), 0, pw2, SEC(15), SEC(15.01), TSDB_ERR_NOTFOUND));
    CASE("a session of the new user is allowed", 0, jm_eval_req(&m, 0, SEC(-100), 0, add2, SEC(15), SEC(15.01), TSDB_ERR_NOTFOUND));
    CASE("the old session of a dropped user is denied (ended)", 0,
         jm_eval_req(&m, 0, SEC(-100), 0, pw2, SEC(15), SEC(15.01), TSDB_ERR_AUTH));
    CASE("a session of the new user is denied while its grant stands", 1,
         jm_eval_req(&m, 0, SEC(-100), 0, add2, SEC(15), SEC(15.01), TSDB_ERR_DENIED));
    /* check 5 and a fault: the member took the newer password at 20 s; a fault of it ended at 26 s, the older one logs in
     * at 40 s (a minority restart may hear a leader behind the newer change): no violation. Member 6 binds nothing. */
    jm_fault(&m, 6, SEC(22));
    jm_settled(&m, SEC(26));
    CASE("a newer password was seen on member 6", 0, jm_eval_login(&m, 6, SEC(-100), 0, pw2, SEC(20), SEC(20.01), 0));
    CASE("an older password after a fault of the member is not an older copy served again", 0,
         jm_eval_login(&m, 6, SEC(-100), 0, add, SEC(40), SEC(40.01), 0));
    /* which members stay in step under a set of cuts (jm_clean_set); n = 5 */
    struct {
        const char *what;
        int cuts[6][2];
        int ncuts, one_way;
        uint32_t want;
    } cs[] = {
        {"a leader cut from every other member: it alone lags", {{0, 1}, {0, 2}, {0, 3}, {0, 4}}, 4, 0, 0x1e},
        {"two members cut from the other three: the three stay", {{0, 2}, {0, 3}, {0, 4}, {1, 2}, {1, 3}, {1, 4}}, 6, 0, 0x1c},
        {"one member cut from the leader only: both lag", {{0, 1}}, 1, 0, 0x1c},
        {"a bridge (1 and 2 | 0 | 3 and 4): only the bridge", {{1, 3}, {1, 4}, {2, 3}, {2, 4}}, 4, 0, 0x01},
        {"one direction only: both ends lag", {{0, 1}}, 1, 1, 0x1c},
        {"no cut (delays): nobody is exempted from the mark", {{0, 0}}, 0, 0, 0},
    };
    for (size_t k = 0; k < sizeof cs / sizeof *cs; k++) {
        uint8_t in[MAXN][MAXN];
        memset(in, 0, sizeof in);
        for (int c = 0; c < cs[k].ncuts; c++) {
            in[cs[k].cuts[c][0]][cs[k].cuts[c][1]] = 1;
            if (!cs[k].one_way)
                in[cs[k].cuts[c][1]][cs[k].cuts[c][0]] = 1;
        }
        uint32_t got = jm_clean_set(in, 5);
        int ok = got == cs[k].want;
        printf("selftest-checker: users: %-70s %s\n", cs[k].what, ok ? "ok" : "WRONG");
        if (!ok) {
            fprintf(stderr, "selftest-checker: users: clean set %#x, wanted %#x\n", got, cs[k].want);
            bad++;
        }
    }
    for (int k = 0; k < n; k++) {
        int ok = t[k].got == t[k].expect;
        printf("selftest-checker: users: %-70s %s\n", t[k].what, ok ? "ok" : t[k].expect ? "NOT FOUND" : "FALSE ALARM");
        if (!ok)
            bad++;
    }
    jm_free(&m);
    return bad;
}
