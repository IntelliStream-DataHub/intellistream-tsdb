/*
 * Login and access control in tsdbd (docs/design-auth.md §3 and §5).
 *
 * The table. auth_ops says, for every op number, who may send it and where its series are (A14). It is indexed by
 * the op byte, so an op the table does not name (who == P_NONE) is refused, and a new op in TSDB_OPS without a row
 * is caught by tests/test_auth_ops.c, which walks the same list.
 *
 * Users. The `users` of USERS is not thread-safe (server/users.h): every call on it holds auth_mu, a leaf lock
 * (no engine lock is taken under it, and it is never held while a request waits for anything but USERS itself).
 * A logged-in user's connections share one auth_user: a private copy of the user (so a rights_of miss does not
 * touch the shared state), the rights on `all`, and the rights map by id. When USERS changes that user (an admin op
 * here, the CLI seen on the reload tick, a compaction), its auth_user is marked stale and leaves the registry; each
 * connection's next request sees the flag (one relaxed load), takes the user's new state or, if the user is gone,
 * is answered TSDB_ERR_AUTH and closed. A request already running finishes under the rights it started with.
 *
 * The rights map (A15): per volume (two levels of 256), 512 pointers -> 1024 pointers -> 4096 bytes, one byte per
 * local id: 0 not known yet, else 0x80 | the rights. Levels are calloc'd and published with a release CAS; bytes are
 * relaxed atomics (every writer of a byte stores the same value: names never change and ids are never reused, so
 * an answer is stale only when the grants change, and then the whole map goes with its auth_user). An id that
 * resolves to no name (unknown, or a follower has not seen it yet) is answered DENIED and not cached. Past MAP_CAP
 * bytes per user nothing more is cached: checks then compute every time, slower but never wrong.
 *
 * A failed write of USERS is fail-stop (A11): users.c calls on_fail, which ends tsdbd with status 1 before the
 * change is acknowledged.
 */
#define _GNU_SOURCE
#include "auth.h"
#include "tsdb_protocol.h"
#include "../client/tsdb_crypto.h"

#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define MAP_CAP ((size_t)64 << 20)

/* ------------------------------------------------------------ the table */

#define ID0(need, shape) {P_SERIES, W_ID0, shape, need, 0}
const auth_op auth_ops[256] = {
    [TSDB_OP_HELLO] = {P_ANY, W_NONE, S_STATUS, 0, 0},
    [TSDB_OP_AUTH_START] = {P_PRE, W_NONE, S_STATUS, 0, 0},
    [TSDB_OP_AUTH_CONTINUE] = {P_PRE, W_NONE, S_STATUS, 0, 0},
    [TSDB_OP_CANCEL] = {P_LOGIN, W_NONE, S_STATUS, 0, 0}, /* parse() handles it; listed so it is not "unknown" */
    [TSDB_OP_LAST_STATS] = {P_LOGIN, W_NONE, S_STATUS, 0, 0},
    [TSDB_OP_WHOAMI] = {P_LOGIN, W_NONE, S_STATUS, 0, 0},
    [TSDB_OP_SERIES] = {P_SERIES, W_NAME3, S_STATUS, NEED_ANY, 1},
    [TSDB_OP_SERIES_CREATE] = {P_SERIES, W_NAME3, S_STATUS, R_INSERT, 0},
    [TSDB_OP_SERIES_NAME] = ID0(NEED_ANY, S_STATUS),
    [TSDB_OP_SERIES_TYPE] = ID0(NEED_ANY, S_STATUS),
    [TSDB_OP_GET_RETENTION] = ID0(NEED_ANY, S_STATUS),
    [TSDB_OP_STATE] = {P_SERIES, W_ID0, S_STATUS, NEED_ANY, 9},
    [TSDB_OP_STATE_NAME] = ID0(NEED_ANY, S_STATUS),
    [TSDB_OP_INSERT] = {P_SERIES, W_ID8, S_STATUS, R_INSERT, 0},
    [TSDB_OP_INSERT_VALUES] = {P_SERIES, W_ID8, S_STATUS, R_INSERT, 0},
    [TSDB_OP_QUERY_RAW] = ID0(R_READ, S_RAW),
    [TSDB_OP_QUERY_VALUES] = ID0(R_READ, S_VALUES),
    [TSDB_OP_QUERY_LTTB] = ID0(R_READ, S_VALUES),
    [TSDB_OP_QUERY_BUCKETS] = ID0(R_READ, S_STATUS),
    [TSDB_OP_QUERY_BOUNDS] = ID0(R_READ, S_STATUS),
    [TSDB_OP_QUERY_TWA] = ID0(R_READ, S_STATUS),
    [TSDB_OP_QUERY_MULTI] = {P_SERIES, W_MULTI, S_MULTI, R_READ, 0},
    [TSDB_OP_DELETE_RANGE] = ID0(R_DELETE, S_STATUS),
    [TSDB_OP_SET_RETENTION] = ID0(R_DELETE, S_STATUS),
    [TSDB_OP_REPLACE_RANGE] = {P_SERIES, W_ID8, S_STATUS, R_INSERT | R_DELETE, 0},
    [TSDB_OP_COMMIT] = {P_COMMIT, W_NONE, S_STATUS, 0, 0},
    [TSDB_OP_FLUSH] = {P_SUPER, W_NONE, S_STATUS, 0, 0},
    [TSDB_OP_OPTIMIZE] = {P_SUPER, W_NONE, S_STATUS, 0, 0},
    [TSDB_OP_STATS] = {P_STATS, W_NONE, S_STATUS, 0, 0},
    [TSDB_OP_USER_CREATE] = {P_ADMIN, W_NONE, S_STATUS, 0, 0},
    [TSDB_OP_USER_DROP] = {P_ADMIN, W_NONE, S_STATUS, 0, 0},
    [TSDB_OP_USER_ALTER] = {P_ADMIN, W_NONE, S_STATUS, 0, 0},
    [TSDB_OP_USER_PASSWORD] = {P_ADMIN, W_NONE, S_STATUS, 0, 0},
    [TSDB_OP_GRANT] = {P_ADMIN, W_NONE, S_STATUS, 0, 0},
    [TSDB_OP_REVOKE] = {P_ADMIN, W_NONE, S_STATUS, 0, 0},
    [TSDB_OP_USER_LIST] = {P_ADMIN, W_NONE, S_STATUS, 0, 0},
};
#undef ID0

/* ------------------------------------------------------------ state */

typedef struct map_l2 {
    _Atomic(_Atomic uint8_t *) leaf[1024];
} map_l2;
typedef struct map_l1 {
    _Atomic(map_l2 *) l2[512];
} map_l1;
typedef struct map_vlo {
    _Atomic(map_l1 *) l1[256];
} map_vlo;

struct auth_user {
    uint32_t uid;
    uint64_t batch; /* of the user copy it was made from: another batch is another state */
    atomic_int refs; /* connections holding it: taken under auth_mu (au_get), dropped anywhere (au_put) */
    atomic_bool stale;
    uint8_t all;      /* rights on every series (R_EVERY for a superuser), admin expanded */
    bool superuser;
    bool writer;      /* insert or delete on some selector (COMMIT) */
    pthread_mutex_t mu; /* the copy's rights_of (its index is a cache) */
    user *u;
    _Atomic(map_vlo *) vhi[256];
    atomic_size_t map_bytes;
};

static pthread_mutex_t auth_mu = PTHREAD_MUTEX_INITIALIZER;
static users us;
static bool us_open, enabled, damaged_logged, no_super_logged;
static tsdb *sdb;
/* The registry: the current state of each user with connections (or had them until the last sync), and the states
 * replaced since, until their last connection lets go. Only reg_sync frees, under auth_mu, an entry no connection
 * holds; a reference is only ever taken under auth_mu, so none can appear behind its back. Connections drop theirs
 * without the lock: the event loop must not wait for auth_mu while the reload thread reads USERS over NFS. */
static auth_user **reg, **dead;
static size_t nreg, reg_cap, ndead, dead_cap;
static pthread_t reload_thread;
static bool reload_running;
static pthread_mutex_t stop_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t stop_cv = PTHREAD_COND_INITIALIZER;
static bool stop_flag;
const char *auth_test_nonce;

bool auth_enabled(void) { return enabled; }

static void on_users_fail(void *arg, const char *msg)
{
    (void)arg;
    fprintf(stderr, "tsdbd: USERS: %s: stopping (fail-stop: nothing more is acknowledged)\n", msg);
    _exit(1); /* before the change is acknowledged; a retried fsync proves nothing (A11) */
}

static void map_free(auth_user *au)
{
    for (int h = 0; h < 256; h++) {
        map_vlo *v = atomic_load_explicit(&au->vhi[h], memory_order_acquire);
        if (!v)
            continue;
        for (int l = 0; l < 256; l++) {
            map_l1 *a = atomic_load_explicit(&v->l1[l], memory_order_acquire);
            if (!a)
                continue;
            for (int i = 0; i < 512; i++) {
                map_l2 *b = atomic_load_explicit(&a->l2[i], memory_order_acquire);
                if (!b)
                    continue;
                for (int j = 0; j < 1024; j++)
                    free((void *)atomic_load_explicit(&b->leaf[j], memory_order_acquire));
                free(b);
            }
            free(a);
        }
        free(v);
    }
}

static void au_free(auth_user *au)
{
    map_free(au);
    user_release(au->u);
    pthread_mutex_destroy(&au->mu);
    free(au);
}

/* Any thread, no lock: reg_sync frees it once no connection holds it. */
static void au_put(auth_user *au)
{
    if (au)
        atomic_fetch_sub_explicit(&au->refs, 1, memory_order_release);
}

static bool unheld(auth_user *au) { return atomic_load_explicit(&au->refs, memory_order_acquire) == 0; }

static bool grow(auth_user ***a, size_t n, size_t *cap)
{
    if (n < *cap)
        return true;
    size_t c = *cap ? *cap * 2 : 16;
    auth_user **p = realloc(*a, c * sizeof *p);
    if (!p)
        return false;
    *a = p, *cap = c;
    return true;
}

/* The current state of user uid, made from USERS when no connection holds it; a new reference. auth_mu held. */
static auth_user *au_get(uint32_t uid)
{
    const user *u = users_by_uid(&us, uid);
    if (!u)
        return NULL;
    for (size_t i = 0; i < nreg; i++)
        if (reg[i]->uid == uid && reg[i]->batch == u->batch) {
            atomic_fetch_add_explicit(&reg[i]->refs, 1, memory_order_relaxed);
            return reg[i];
        }
    if (!grow(&reg, nreg, &reg_cap) || !grow(&dead, ndead + nreg, &dead_cap))
        return NULL; /* dead has room for every listed state, so reg_sync never fails to move one */
    auth_user *au = calloc(1, sizeof *au);
    if (!au || !(au->u = user_copy(u))) {
        free(au);
        return NULL;
    }
    pthread_mutex_init(&au->mu, NULL);
    au->uid = uid, au->batch = u->batch;
    atomic_init(&au->refs, 1);
    au->superuser = u->flags & USER_SUPERUSER;
    uint8_t all = 0;
    for (uint32_t i = 0; i < u->ng; i++) {
        if (u->g[i].kind == SEL_ALL)
            all |= u->g[i].rights;
        if (u->g[i].rights & (R_INSERT | R_DELETE | R_ADMIN))
            au->writer = true;
    }
    au->all = au->superuser || (all & R_ADMIN) ? R_EVERY : all;
    au->writer |= au->superuser;
    reg[nreg++] = au;
    return au;
}

/* After USERS changed (a reload, an admin op): every listed state whose user changed or went is marked stale and
 * leaves the registry; its connections move on at their next request. auth_mu held. */
static void reg_sync(void)
{
    for (size_t i = 0; i < ndead;) {
        if (unheld(dead[i])) {
            au_free(dead[i]);
            dead[i] = dead[--ndead];
        } else {
            i++;
        }
    }
    for (size_t i = 0; i < nreg;) {
        auth_user *au = reg[i];
        const user *u = users_by_uid(&us, au->uid);
        if (u && u->batch == au->batch && !unheld(au)) {
            i++;
            continue;
        }
        reg[i] = reg[--nreg];
        if (unheld(au)) { /* no connection: nothing to tell; a later login makes a new state */
            au_free(au);
            continue;
        }
        atomic_store_explicit(&au->stale, true, memory_order_relaxed);
        dead[ndead++] = au; /* room: au_get kept dead_cap >= ndead + nreg */
    }
    if (enabled && users_superusers(&us) == 0) {
        if (!no_super_logged)
            fprintf(stderr, "tsdbd: USERS has no superuser any more: logged-in users keep their grants; a restart "
                            "will refuse to start until 'tsdb <dir> user add NAME --superuser'\n");
        no_super_logged = true;
    } else {
        no_super_logged = false;
    }
}

int auth_init(const char *dir, bool on, bool writable, tsdb *db)
{
    enabled = on;
    sdb = db;
    if (!on)
        return 0; /* --no-auth: nothing reads USERS, and the admin ops are refused (auth_check) */
    int rc = users_open(&us, dir, writable);
    if (rc != 0) {
        fprintf(stderr, "tsdbd: %s/USERS: %s\n", dir, tsdb_strerror(rc));
        return rc;
    }
    us_open = true;
    us.on_fail = on_users_fail;
    if (users_superusers(&us) == 0) {
        fprintf(stderr, "tsdbd: no superuser: run 'tsdb %s user add NAME --superuser', or start with --no-auth\n",
                dir);
        return TSDB_ERR_AUTH;
    }
    return 0;
}

void auth_reload(void)
{
    pthread_mutex_lock(&auth_mu);
    if (us_open) {
        int rc = users_reload(&us);
        if (rc != 0 && !damaged_logged) {
            damaged_logged = true;
            fprintf(stderr, "tsdbd: USERS: %s: the users stay as last read; user admin ops are refused until a "
                            "restart\n", tsdb_strerror(rc));
        }
        reg_sync();
    }
    pthread_mutex_unlock(&auth_mu);
}

static void *reload_main(void *arg)
{
    (void)arg;
    pthread_mutex_lock(&stop_mu);
    while (!stop_flag) {
        struct timespec t;
        clock_gettime(CLOCK_REALTIME, &t);
        t.tv_sec += 1;
        pthread_cond_timedwait(&stop_cv, &stop_mu, &t);
        if (stop_flag)
            break;
        pthread_mutex_unlock(&stop_mu);
        auth_reload(); /* its own thread: a slow NFS read must not stall the event loop */
        pthread_mutex_lock(&stop_mu);
    }
    pthread_mutex_unlock(&stop_mu);
    return NULL;
}

int auth_start_reload(void)
{
    if (!us_open)
        return 0;
    int rc = pthread_create(&reload_thread, NULL, reload_main, NULL);
    reload_running = rc == 0;
    return rc;
}

void auth_shutdown(void)
{
    if (reload_running) {
        pthread_mutex_lock(&stop_mu);
        stop_flag = true;
        pthread_cond_broadcast(&stop_cv);
        pthread_mutex_unlock(&stop_mu);
        pthread_join(reload_thread, NULL);
        reload_running = false;
    }
    pthread_mutex_lock(&auth_mu); /* after every connection is gone */
    for (size_t i = 0; i < nreg; i++)
        au_free(reg[i]);
    for (size_t i = 0; i < ndead; i++)
        au_free(dead[i]);
    free(reg), free(dead);
    reg = dead = NULL;
    nreg = ndead = reg_cap = dead_cap = 0;
    if (us_open)
        users_close(&us);
    us_open = false;
    pthread_mutex_unlock(&auth_mu);
}

void auth_conn_init(auth_conn *a)
{
    memset(a, 0, sizeof *a);
    a->state = enabled ? AS_NONE : AS_IN;
    a->last_id = TSDB_SID_NONE;
}

static void scram_clear(auth_conn *a)
{
    free(a->msg);
    a->msg = NULL;
    a->msg_len = 0;
    tsdb_wipe(a->stored_key, sizeof a->stored_key);
    tsdb_wipe(a->server_key, sizeof a->server_key);
}

void auth_conn_free(auth_conn *a)
{
    scram_clear(a);
    au_put(a->au);
    a->au = NULL;
}

/* ------------------------------------------------------------ SCRAM */

size_t auth_hello(uint8_t *out)
{
    uint16_t flags = enabled ? TSDB_HELLO_AUTH : 0, n = 1;
    size_t ml = strlen(TSDB_SCRAM_SHA256);
    memcpy(out, &flags, 2);
    memcpy(out + 2, &n, 2);
    out[4] = (uint8_t)ml;
    memcpy(out + 5, TSDB_SCRAM_SHA256, ml);
    return 5 + ml;
}

/* A failed login: one log line a second at most, naming the user (if the name is one) and the peer; never a
 * proof, nonce or verifier (A21). */
static int login_failed(auth_conn *a, const char *peer, const char *why)
{
    static time_t last;
    time_t t = time(NULL);
    pthread_mutex_lock(&auth_mu);
    bool log = t != last;
    last = t;
    pthread_mutex_unlock(&auth_mu);
    if (log)
        fprintf(stderr, "tsdbd: login failed%s%s from %s: %s\n", a->name[0] ? " for user " : "", a->name,
                peer ? peer : "?", why);
    scram_clear(a);
    a->state = AS_FAILED;
    return TSDB_ERR_AUTH;
}

static bool printable_nonce(const char *s, size_t n)
{
    if (n == 0 || n > 256)
        return false;
    for (size_t i = 0; i < n; i++)
        if (s[i] < 0x21 || s[i] > 0x7e || s[i] == ',')
            return false;
    return true;
}

static size_t put_msg(uint8_t *out, const char *s, size_t n)
{
    uint16_t l = (uint16_t)n;
    memcpy(out, &l, 2);
    memcpy(out + 2, s, n);
    return 2 + n;
}

int auth_scram_start(auth_conn *a, const uint8_t *p, size_t len, uint8_t *out, size_t cap, size_t *out_len,
                     const char *peer)
{
    if (a->state == AS_IN)
        return TSDB_ERR_INVALID; /* a connection logs in once */
    if (a->state != AS_NONE) { /* a second AUTH_START is a client's bug; after a failure the answer stays AUTH */
        int rc = a->state == AS_STARTED ? TSDB_ERR_INVALID : TSDB_ERR_AUTH;
        scram_clear(a);
        a->state = AS_FAILED;
        return rc;
    }
    a->name[0] = 0;
    size_t ml = len ? p[0] : 0;
    uint16_t cl;
    if (len < 3 || len < 1 + ml + 2 || (memcpy(&cl, p + 1 + ml, 2), len != 3 + ml + cl))
        return login_failed(a, peer, "malformed AUTH_START");
    if (ml != strlen(TSDB_SCRAM_SHA256) || memcmp(p + 1, TSDB_SCRAM_SHA256, ml) != 0)
        return login_failed(a, peer, "unknown mechanism");
    const char *m = (const char *)p + 3 + ml;
    /* "n,," then client-first-message-bare "n=NAME,r=NONCE", nothing else (no a=, no extensions, no binding) */
    if (cl < 8 || memcmp(m, "n,,n=", 5) != 0)
        return login_failed(a, peer, "unsupported gs2 header");
    const char *name = m + 5, *end = m + cl;
    const char *comma = memchr(name, ',', (size_t)(end - name));
    if (!comma || !user_name_valid(name, (size_t)(comma - name)))
        return login_failed(a, peer, "malformed user name");
    size_t nl = (size_t)(comma - name);
    memcpy(a->name, name, nl);
    a->name[nl] = 0;
    if (end - comma < 4 || memcmp(comma, ",r=", 3) != 0 || !printable_nonce(comma + 3, (size_t)(end - comma - 3)))
        return login_failed(a, peer, "malformed client-first-message");
    const char *cnonce = comma + 3;
    size_t cnl = (size_t)(end - cnonce);

    uint8_t salt[SALT_MAX], salt_len;
    uint32_t iter;
    pthread_mutex_lock(&auth_mu);
    const user *u = us_open ? users_find(&us, a->name) : NULL;
    if (u && u->has_verifier) {
        a->uid = u->uid;
        salt_len = u->v.salt_len;
        memcpy(salt, u->v.salt, salt_len);
        iter = u->v.iterations;
        memcpy(a->stored_key, u->v.stored_key, 32);
        memcpy(a->server_key, u->v.server_key, 32);
    } else { /* A4: the same shape, a salt that is the same on every try and after a restart, a key no one holds */
        uint8_t h[32], msg[USER_NAME_MAX + 8];
        a->uid = 0;
        tsdb_hmac_sha256(us.mock_key, 32, a->name, nl, h);
        salt_len = 16;
        memcpy(salt, h, 16);
        iter = ITER_MIN;
        memcpy(msg, "stored:", 7);
        memcpy(msg + 7, a->name, nl);
        tsdb_hmac_sha256(us.mock_key, 32, msg, 7 + nl, a->stored_key);
        memcpy(msg, "server:", 7);
        tsdb_hmac_sha256(us.mock_key, 32, msg, 7 + nl, a->server_key);
    }
    pthread_mutex_unlock(&auth_mu);

    uint8_t sn[18];
    char b64salt[tsdb_base64_len(SALT_MAX) + 1], snb[64];
    if (auth_test_nonce) /* as is: the RFC 7677 test's server nonce is not base64 of 18 bytes */
        snprintf(snb, sizeof snb, "%s", auth_test_nonce);
    else if (tsdb_random(sn, sizeof sn) != 0)
        return login_failed(a, peer, "no random bytes");
    else
        tsdb_base64_encode(sn, sizeof sn, snb);
    tsdb_base64_encode(salt, salt_len, b64salt);
    a->nonce_len = cnl + strlen(snb);
    memcpy(a->nonce, cnonce, cnl);
    memcpy(a->nonce + cnl, snb, strlen(snb));
    char sf[512];
    int sfl = snprintf(sf, sizeof sf, "r=%.*s,s=%s,i=%u", (int)a->nonce_len, a->nonce, b64salt, iter);
    size_t bare = (size_t)cl - 3;
    a->msg_len = bare + 1 + (size_t)sfl;
    if (sfl < 0 || (size_t)sfl >= sizeof sf || 2 + (size_t)sfl > cap || !(a->msg = malloc(a->msg_len)))
        return login_failed(a, peer, "out of memory");
    memcpy(a->msg, m + 3, bare);
    a->msg[bare] = ',';
    memcpy(a->msg + bare + 1, sf, (size_t)sfl);
    *out_len = put_msg(out, sf, (size_t)sfl);
    a->state = AS_STARTED;
    return 0;
}

int auth_scram_continue(auth_conn *a, const uint8_t *p, size_t len, uint8_t *out, size_t cap, size_t *out_len,
                        const char *peer)
{
    if (a->state == AS_IN)
        return TSDB_ERR_INVALID;
    if (a->state != AS_STARTED)
        return login_failed(a, peer, "AUTH_CONTINUE before AUTH_START");
    uint16_t fl;
    if (len < 2 || (memcpy(&fl, p, 2), len != 2u + fl))
        return login_failed(a, peer, "malformed AUTH_CONTINUE");
    const char *f = (const char *)p + 2, *end = f + fl;
    /* c=biws (base64 of "n,,"), r=<both nonces>, p=<proof>: in that order, nothing else */
    size_t pre = 9 + a->nonce_len;
    if (fl < pre + 3 || memcmp(f, "c=biws,r=", 9) != 0 || memcmp(f + 9, a->nonce, a->nonce_len) != 0 ||
        memcmp(f + pre, ",p=", 3) != 0)
        return login_failed(a, peer, "malformed client-final-message");
    uint8_t proof[32];
    size_t got;
    if (tsdb_base64_decode(f + pre + 3, (size_t)(end - f - pre - 3), proof, sizeof proof, &got) != 0 || got != 32)
        return login_failed(a, peer, "malformed proof");
    size_t am_len = a->msg_len + 1 + pre;
    char *am = malloc(am_len);
    if (!am)
        return login_failed(a, peer, "out of memory");
    memcpy(am, a->msg, a->msg_len);
    am[a->msg_len] = ',';
    memcpy(am + a->msg_len + 1, f, pre);
    uint8_t sig[32], ck[32], h[32], ssig[32];
    tsdb_hmac_sha256(a->stored_key, 32, am, am_len, sig);
    for (int i = 0; i < 32; i++)
        ck[i] = proof[i] ^ sig[i];
    tsdb_sha256_buf(ck, 32, h);
    bool ok = tsdb_ct_equal(h, a->stored_key, 32) && a->uid != 0;
    tsdb_hmac_sha256(a->server_key, 32, am, am_len, ssig);
    free(am);
    tsdb_wipe(ck, sizeof ck);
    if (!ok)
        return login_failed(a, peer, "wrong password or unknown user");
    pthread_mutex_lock(&auth_mu);
    const user *u = us_open ? users_by_uid(&us, a->uid) : NULL;
    /* the password may have changed, or the user gone, since AUTH_START: the proof is for that verifier only */
    auth_user *au = u && u->has_verifier && tsdb_ct_equal(u->v.stored_key, a->stored_key, 32) ? au_get(a->uid) : NULL;
    pthread_mutex_unlock(&auth_mu);
    if (!au)
        return login_failed(a, peer, "the user changed during the login");
    char v[3 + tsdb_base64_len(32) + 1];
    memcpy(v, "v=", 2);
    tsdb_base64_encode(ssig, 32, v + 2);
    size_t vl = strlen(v);
    if (2 + vl > cap) {
        au_put(au);
        return login_failed(a, peer, "reply too large");
    }
    *out_len = put_msg(out, v, vl);
    scram_clear(a);
    a->au = au;
    a->last_id = TSDB_SID_NONE;
    a->state = AS_IN;
    return 0;
}

/* ------------------------------------------------------------ checks */

int auth_fresh(auth_conn *a)
{
    auth_user *old = a->au;
    if (!old || !atomic_load_explicit(&old->stale, memory_order_relaxed))
        return 0;
    pthread_mutex_lock(&auth_mu);
    a->au = au_get(old->uid);
    au_put(old);
    pthread_mutex_unlock(&auth_mu);
    a->last_id = TSDB_SID_NONE;
    if (!a->au) { /* dropped (or out of memory: refused all the same, never answered with stale rights) */
        a->state = AS_FAILED;
        return TSDB_ERR_AUTH;
    }
    return 0;
}

static bool enough(uint8_t have, uint8_t need) { return need == NEED_ANY ? (have & R_EVERY) != 0 : (have & need) == need; }

bool auth_all(const auth_conn *a, uint8_t need) { return !a->au || enough(a->au->all, need); }

static _Atomic uint8_t *map_slot(auth_user *au, tsdb_sid id, bool make)
{
    uint64_t vol = (id >> 32) - 1;
    uint32_t local = (uint32_t)id;
    if ((id >> 48) || vol >= 65536 || (local >> 31))
        return NULL;
    _Atomic(map_vlo *) *pv = &au->vhi[vol >> 8];
    map_vlo *v = atomic_load_explicit(pv, memory_order_acquire);
#define LEVEL(ptr, var, type, size)                                                                        \
    if (!var) {                                                                                            \
        if (!make || atomic_load_explicit(&au->map_bytes, memory_order_relaxed) + (size) > MAP_CAP)        \
            return NULL;                                                                                   \
        type *n_ = calloc(1, size);                                                                        \
        if (!n_)                                                                                           \
            return NULL;                                                                                   \
        if (atomic_compare_exchange_strong_explicit(ptr, &var, n_, memory_order_acq_rel, memory_order_acquire)) { \
            var = n_;                                                                                      \
            atomic_fetch_add_explicit(&au->map_bytes, size, memory_order_relaxed);                         \
        } else {                                                                                           \
            free(n_); /* another thread made it: var holds theirs */                                       \
        }                                                                                                  \
    }
    LEVEL(pv, v, map_vlo, sizeof(map_vlo))
    _Atomic(map_l1 *) *p1 = &v->l1[vol & 255];
    map_l1 *l1 = atomic_load_explicit(p1, memory_order_acquire);
    LEVEL(p1, l1, map_l1, sizeof(map_l1))
    _Atomic(map_l2 *) *p2 = &l1->l2[local >> 22];
    map_l2 *l2 = atomic_load_explicit(p2, memory_order_acquire);
    LEVEL(p2, l2, map_l2, sizeof(map_l2))
    _Atomic(_Atomic uint8_t *) *pl = &l2->leaf[(local >> 12) & 1023];
    _Atomic uint8_t *leaf = atomic_load_explicit(pl, memory_order_acquire);
    LEVEL(pl, leaf, _Atomic uint8_t, 4096)
#undef LEVEL
    return &leaf[local & 4095];
}

/* The map byte of id for this user: 0x80 | rights, or 0 for an id that names no series (not cached). */
static uint8_t rights_byte(auth_user *au, tsdb_sid id)
{
    _Atomic uint8_t *s = map_slot(au, id, false);
    uint8_t b = s ? atomic_load_explicit(s, memory_order_relaxed) : 0;
    if (b)
        return b;
    const char *name = tsdb_series_name(sdb, id);
    if (!name)
        return 0;
    pthread_mutex_lock(&au->mu);
    b = 0x80 | rights_of(au->u, name, strlen(name));
    pthread_mutex_unlock(&au->mu);
    if ((s = map_slot(au, id, true)))
        atomic_store_explicit(s, b, memory_order_relaxed);
    return b;
}

int auth_id(auth_conn *a, tsdb_sid id, uint8_t need)
{
    if (!a->au || enough(a->au->all, need))
        return 0;
    if (id != a->last_id || !a->last_byte) {
        a->last_byte = rights_byte(a->au, id);
        a->last_id = id;
    }
    return a->last_byte && enough(a->last_byte, need) ? 0 : TSDB_ERR_DENIED;
}

int auth_ids(auth_conn *a, const tsdb_sid *ids, size_t n, uint8_t need)
{
    if (!a->au || enough(a->au->all, need))
        return 0;
    for (size_t i = 0; i < n; i++)
        if (auth_id(a, ids[i], need) != 0)
            return TSDB_ERR_DENIED;
    return 0;
}

int auth_check(auth_conn *a, uint8_t op, const uint8_t *p, size_t len)
{
    const auth_op *e = &auth_ops[op];
    if (a->state != AS_IN)
        return e->who == P_ANY || e->who == P_PRE ? 0 : TSDB_ERR_AUTH;
    if (e->who == P_PRE)
        return TSDB_ERR_INVALID; /* AUTH_* after login */
    if (!a->au) {
        /* --no-auth: data ops run unchecked (an unknown op gets the handler's INVALID, as before login existed),
         * but USERS is not changed or listed over the network: a server run without login for a while must not
         * let any client plant a superuser that stays valid once login is on (A13, fail closed). The offline
         * 'tsdb <dir> user' verbs manage users then. */
        if (e->who != P_ADMIN)
            return 0;
        static _Atomic time_t last;
        time_t t = time(NULL);
        if (atomic_exchange_explicit(&last, t, memory_order_relaxed) != t)
            fprintf(stderr, "tsdbd: --no-auth: user admin op %u refused; manage users with 'tsdb <dir> user'\n",
                    op);
        return TSDB_ERR_DENIED;
    }
    if (e->who == P_NONE)
        return TSDB_ERR_DENIED;
    auth_user *au = a->au;
    switch (e->who) {
    case P_ANY:
    case P_LOGIN:
    case P_ADMIN: return 0; /* users.c decides who may change whom */
    case P_SUPER: return au->superuser ? 0 : TSDB_ERR_DENIED;
    case P_STATS: return au->superuser || (au->all & R_READ) ? 0 : TSDB_ERR_DENIED;
    case P_COMMIT: return au->writer ? 0 : TSDB_ERR_DENIED;
    default: break;
    }
    uint8_t need = e->need;
    if (e->create_at && len >= e->create_at && p[e->create_at - 1])
        need = R_INSERT;
    if (enough(au->all, need))
        return 0;
    tsdb_sid id;
    switch (e->where) {
    case W_ID0:
    case W_ID8: {
        size_t at = e->where == W_ID0 ? 0 : 8;
        if (len < at + 8)
            return TSDB_ERR_INVALID; /* no series to check: malformed, as the handler would say */
        memcpy(&id, p + at, 8);
        if (id == TSDB_MANY_SERIES && e->where == W_ID8 && op != TSDB_OP_REPLACE_RANGE)
            return 0; /* an id per row: the handler checks each (auth_ids) */
        return auth_id(a, id, need);
    }
    case W_NAME3: {
        uint16_t n;
        if (len < 3 || (memcpy(&n, p + 1, 2), n == 0 || len < 3u + n))
            return TSDB_ERR_INVALID;
        pthread_mutex_lock(&au->mu);
        uint8_t r = rights_of(au->u, (const char *)p + 3, n);
        pthread_mutex_unlock(&au->mu);
        return enough(r, need) ? 0 : TSDB_ERR_DENIED;
    }
    case W_MULTI: return 0; /* per series, in the handler (A17) */
    default: return TSDB_ERR_DENIED;
    }
}

/* ------------------------------------------------------------ admin ops */

typedef struct wbuf {
    uint8_t *p;
    size_t len, cap;
    bool oom;
} wbuf;

static void wput(wbuf *b, const void *d, size_t n)
{
    if (b->oom)
        return;
    if (b->len + n > b->cap) {
        size_t cap = b->cap ? b->cap * 2 : 256;
        while (cap < b->len + n)
            cap *= 2;
        uint8_t *q = realloc(b->p, cap);
        if (!q) {
            b->oom = true;
            return;
        }
        b->p = q, b->cap = cap;
    }
    memcpy(b->p + b->len, d, n);
    b->len += n;
}

static void wput_user(wbuf *b, const user *u, bool with_name16)
{
    uint16_t nl = (uint16_t)strlen(u->name);
    if (with_name16)
        wput(b, &nl, 2);
    wput(b, u->name, nl);
    uint8_t f = u->flags;
    wput(b, &f, 1);
    uint32_t ng = u->ng;
    wput(b, &ng, 4);
    for (uint32_t i = 0; i < u->ng; i++) {
        uint8_t h[2] = {u->g[i].kind, u->g[i].rights};
        wput(b, h, 2);
        wput(b, &u->g[i].len, 2);
        wput(b, u->g[i].sel, u->g[i].len);
    }
}

/* u16 len, name at p: copied NUL-terminated; the bytes after it. NULL when malformed. */
static const uint8_t *get_name(const uint8_t *p, const uint8_t *end, char *name)
{
    uint16_t n;
    if (end - p < 2 || (memcpy(&n, p, 2), n == 0 || n > USER_NAME_MAX || end - p - 2 < n) || memchr(p + 2, 0, n))
        return NULL;
    memcpy(name, p + 2, n);
    name[n] = 0;
    return p + 2 + n;
}

static const uint8_t *get_verifier(const uint8_t *p, const uint8_t *end, user_verifier *v)
{
    if (end - p < 5)
        return NULL;
    memset(v, 0, sizeof *v);
    memcpy(&v->iterations, p, 4);
    v->salt_len = p[4];
    if (v->salt_len > SALT_MAX || (size_t)(end - p) < 5u + v->salt_len + 64)
        return NULL;
    memcpy(v->salt, p + 5, v->salt_len);
    memcpy(v->stored_key, p + 5 + v->salt_len, 32);
    memcpy(v->server_key, p + 5 + v->salt_len + 32, 32);
    return p + 5 + v->salt_len + 64;
}

int auth_admin(auth_conn *a, uint8_t op, const uint8_t *p, size_t len, uint8_t **out, size_t *out_len)
{
    *out = NULL;
    *out_len = 0;
    const uint8_t *end = p + len;
    char name[USER_NAME_MAX + 1];
    user_verifier v;
    int rc;
    if (op == TSDB_OP_WHOAMI) {
        if (len != 0)
            return TSDB_ERR_INVALID;
        wbuf b = {0};
        if (a->au) {
            wput_user(&b, a->au->u, true);
        } else {
            uint8_t none[7] = {0, 0, TSDB_USER_SUPERUSER, 0, 0, 0, 0};
            wput(&b, none, sizeof none);
        }
        if (b.oom) {
            free(b.p);
            return TSDB_ERR_NOMEM;
        }
        *out = b.p, *out_len = b.len;
        return 0;
    }
    if (!a->au)
        return TSDB_ERR_DENIED; /* --no-auth: never actor 0 over the network (auth_check refuses first) */
    uint32_t actor = a->au->uid;
    const uint8_t *q = get_name(p, end, name);
    if (!q)
        return TSDB_ERR_INVALID;
    pthread_mutex_lock(&auth_mu);
    if (!us_open) {
        rc = TSDB_ERR_CORRUPT;
    } else {
        switch (op) {
        case TSDB_OP_USER_CREATE: {
            uint8_t flags = q < end ? *q++ : 0;
            const uint8_t *r = q < end ? get_verifier(q, end, &v) : NULL;
            rc = !r || r != end || (flags & ~USER_SUPERUSER) ? TSDB_ERR_INVALID : users_create(&us, actor, name, flags, &v, NULL);
            break;
        }
        case TSDB_OP_USER_DROP: rc = q != end ? TSDB_ERR_INVALID : users_drop(&us, actor, name); break;
        case TSDB_OP_USER_ALTER:
            rc = end - q != 1 || (*q & ~USER_SUPERUSER) ? TSDB_ERR_INVALID : users_set_flags(&us, actor, name, *q);
            break;
        case TSDB_OP_USER_PASSWORD: {
            const uint8_t *r = get_verifier(q, end, &v);
            rc = r != end ? TSDB_ERR_INVALID : users_set_verifier(&us, actor, name, &v);
            break;
        }
        case TSDB_OP_GRANT:
        case TSDB_OP_REVOKE: {
            uint16_t sl;
            if (end - q < 4 || (memcpy(&sl, q + 2, 2), (size_t)(end - q) != 4u + sl) || memchr(q + 4, 0, sl)) {
                rc = TSDB_ERR_INVALID;
                break;
            }
            rc = op == TSDB_OP_GRANT ? users_grant(&us, actor, name, q[0], (const char *)q + 4, sl, q[1])
                                     : users_revoke(&us, actor, name, q[0], (const char *)q + 4, sl, q[1]);
            break;
        }
        default: rc = TSDB_ERR_INVALID; break;
        }
        reg_sync();
    }
    pthread_mutex_unlock(&auth_mu);
    tsdb_wipe(&v, sizeof v);
    return rc;
}

#define LIST_CHUNK (1u << 20)

int auth_user_list(auth_conn *a, int (*emit)(void *ctx, bool last, uint32_t n, const uint8_t *e, size_t len),
                   void *ctx)
{
    wbuf b = {0};
    uint32_t *ends = NULL, n = 0;
    int rc = 0;
    pthread_mutex_lock(&auth_mu);
    if (!a->au) {
        rc = TSDB_ERR_DENIED; /* --no-auth (auth_check refuses first) */
    } else if (!us_open) {
        rc = TSDB_ERR_CORRUPT;
    } else {
        const user *x = users_by_uid(&us, a->au->uid);
        if (!x) {
            rc = TSDB_ERR_AUTH;
        } else if (!(ends = malloc(((size_t)us.n + 1) * sizeof *ends))) {
            rc = TSDB_ERR_NOMEM;
        } else {
            for (uint32_t i = 0; i < us.n; i++)
                if (may_list(x, us.u[i])) {
                    wput_user(&b, us.u[i], true);
                    ends[n++] = (uint32_t)b.len;
                }
        }
    }
    pthread_mutex_unlock(&auth_mu);
    if (rc == 0 && b.oom)
        rc = TSDB_ERR_NOMEM;
    if (rc == 0) { /* chunks of whole entries, each under LIST_CHUNK unless one entry alone is larger */
        uint32_t from = 0;
        size_t start = 0;
        do {
            uint32_t to = from;
            while (to < n && (to == from || ends[to] - start <= LIST_CHUNK))
                to++;
            size_t stop = to ? ends[to - 1] : 0;
            rc = emit(ctx, to == n, to - from, b.p ? b.p + start : NULL, stop - start);
            start = stop, from = to;
        } while (rc == 0 && from < n);
    }
    free(ends);
    free(b.p);
    return rc;
}
