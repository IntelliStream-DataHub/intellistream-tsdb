/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The swap of a settle and its recovery (src/ecswap.h; design-cluster-data.md §2.3 step 6, E17; R7's plan, D2).
 *
 * A settle has encoded the bytes of some parts P of one partition key into the chunk store and written, for each,
 * an index file tmp_<pid>_<n>.ec (fsynced). The swap puts each P.ec in P.part's place, with the same name and block
 * range, and renames P.part to settled_P.part (r7-keep keeps it for a while): same rows, same rank, same key, only
 * where the bytes live changes. It is all or nothing, like an insert (src/part.c, "atomic commits"):
 *
 *   1. under publish_lock: the intent "tsdb-intent 4 <n>", n lines "L <tmp_*.ec> <P.ec>", n lines
 *      "R <P.part> <settled_P.part>", "end <xxh64>", written, fdatasynced, and the directory fsynced. Its name is the
 *      commit point; the tmp .ec names are durable with it.
 *   2. latest_rw (lt_begin: nothing to fold, the rows are the same; lt_end bumps lgen) and db->lock, in one hold:
 *      every input still listed, not dropped and not in a merge, the engine not failed. Then the L renames, the
 *      R renames, each P replaced by its .ec at the same place in the list (the same key and block range sort the
 *      same), the statistics' events pushed.
 *   3. released; the directory fsynced (fail-stop: names became visible), the intent unlinked, MANIFEST published.
 *
 * Why the check is made in the hold of the renames: merges never take a settling part (r7-skip), but a drop can.
 * It takes no publish_lock: drop_retire takes latest_rw and db->lock and retires under db->lock (src/db.c). A check
 * before those locks could be stale by the time the swap renames, and the swap would list an .ec part for a part
 * the drop retired, bringing its rows back (CRITIQUE finding 1). An input that fails the check: no rename at all;
 * the temporaries are unlinked, the directory fsynced, the intent unlinked, and the objects handed to r7-gc.
 *
 * Why one key, all or nothing: a drop retires whole partitions, every part of a key in one hold of db->lock. So
 * either the drop came before the check (every source retired, the swap refused, nothing renamed) or after the
 * renames (every .ec retired), never between, and recovery can decide for the whole intent at once (below).
 *
 * The renames run with latest_rw and db->lock held (2n renameat calls, one directory): the check and the renames
 * must be one step against a drop, which takes no publish_lock. Like tsdb_drop_before's retires, they are the I/O an
 * exception to "latest_rw is held across no I/O" allows (AGENTS.md): on NFS, snapshots and latest lookups of the
 * volume wait for them.
 *
 * Recovery at open (ecswap_recover, before load_parts removes the tmp files) decides on one question: did the swap
 * rename anything? Every rename follows the input check under db->lock, and the L renames all come before the R
 * renames, so any L name (P.ec, or retired_P.ec: a drop or merge after the swap retired it) or settled_ name on disk
 * means the check passed and the swap stands: a drop after it can only retire the .ec parts (the sources are no
 * longer listed). What a missing source then means cannot be told apart (r7-keep released settled_P.part, or the
 * .ec retired and its file deleted at its last reference with obsolete_part_lifetime_s = 0), and nothing of either
 * may be undone. So:
 *   - a torn intent (hash wrong): no rename followed it (the renames start after the intent and the directory are
 *     synced): the parts stay; load_parts removes the tmp .ec files with the other temporaries.
 *   - complete, no L or settled_ name on disk, every tmp .ec and every P.part there: the swap was cut before its
 *     renames (or before they were synced): roll forward, as below.
 *   - complete, no L or settled_ name on disk, and a tmp .ec or a source missing: nothing was renamed (a refused
 *     swap's cleanup cut before its intent went, the key dropped before the check) or nothing of it is left to undo
 *     (the swap, a drop and the release of its sources all done): roll back, the tmp .ec files go.
 *   - complete otherwise: roll forward, every remaining tmp renamed to its L name, every P.part still there to
 *     settled_P.part. An L name under retired_, or under none of its names while its source is settled or gone,
 *     counts as done.
 *   Damage fails the open (the intent stays): a sealed intent whose lines break the rules of a swap's names, an .ec
 *   part under none of its names while its source is still P.part (no flow loses it: a drop after the swap finds the
 *   sources settled), a source under retired_ and not settled_ although the swap renamed (a drop before the check
 *   renames nothing; a retired_ name beside settled_ is an older file of that name: the open's cover rule retires a
 *   merge output a crash left unlisted, and the merge names it again),
 *   and a lookup that fails other than ENOENT (EIO, ESTALE on NFS: never read as "absent"). The directory is synced
 *   before ecswap_recover returns, so the intent is unlinked only after its renames are durable.
 *
 * Intent v5 (R8 / R15):
 *  - R8 replica apply of MK_COLD (src/ecapply.c): "tsdb-intent 5 <n>", lines "N <tmp_*.ec> <.ec>" (its objects' .ec
 *    files under fresh block numbers), "Y <tmp_0_*.part> <part>" (the late parts above them), "R <own part>
 *    <settled_own part>" (its own parts of the key at or below Z), "end <xxh64>".
 *  - R15 reverse unsettle (src/ecunsettle.c, ec_swap_unsettle): "tsdb-intent 5 <2m>", m lines
 *    "L <settled_P.part|tmp_*.part> <P.part>" and m lines "U <P.ec> retired_<P.ec>", "end <xxh64>". Serialized
 *    against concurrent drops via db_pub_begin/db_pub_end (held past the post-rename directory fsync); intent write
 *    and sync run before latest_rw > db->lock so only the 2m renameat calls run inside latest_rw and db->lock.
 *    Its rules are written above unsettle_recover / ecapply_recover.
 *
 * Crash points for r7-jepsen: TSDB_SETTLE_CRASH_AT=after-put|after-ec-tmp|after-intent|between-renames|before-unlink[:n]
 * ends the process
 * with exit(99) the n-th time (default 1) the point is passed; read once per process. exit, not _exit: under
 * jepsen's crashfs the point must be a power cut, which crashfs's destructor makes (it runs on exit only; tsdbd and the
 * engine register no atexit handler, so nothing else runs on the way out). after-intent: the intent is
 * durable, nothing renamed; between-renames: after each rename but the swap's last; before-unlink: the swap is
 * durable, its intent still there. after-put and after-ec-tmp are the settle's (src/settle.c, ecswap_crash_point):
 * its chunks are durable, and then its tmp_*.ec too, nothing renamed. An unknown name ends the process with status 2 at the first point passed.
 */
#include "ecswap.h"
#include "ecapply.h"
#include "eccache.h"
#include "ecdiverge.h"
#include "eckeep.h"
#include "ecgc.h"
#include "ecpart.h"
#include "ecunsettle.h"
#include "ppos.h"
#include "settle.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

void (*ec_swap_hook)(eng *db);
int (*ec_swap_publish)(eng *db) = manifest_publish;
int ecswap_mutant;
atomic_int ecswap_test_fail;
atomic_bool ecswap_test_logged;
void (*ecswap_discard_hook)(eng *db);
void (*ecswap_handed_hook)(eng *db);
void (*ecswap_intent_fail_hook)(eng *db);

/* ------------------------------------------------------------ crash points */

enum {
    SC_AFTER_INTENT,
    SC_BETWEEN_RENAMES,
    SC_BEFORE_UNLINK,
    SC_AFTER_PUT,
    SC_AFTER_EC_TMP,
    SC_RE_AFTER_INTENT,
    SC_RE_BETWEEN_RENAMES,
    SC_RE_BEFORE_UNLINK,
    SC_RE_AFTER_PUT,
    SC_RE_AFTER_EC_TMP,
    SC_N
};
#ifdef TSDB_TEST_HOOKS
static const char *const sc_names[SC_N] = {"after-intent",
                                           "between-renames",
                                           "before-unlink",
                                           "after-put",
                                           "after-ec-tmp",
                                           "reencode-after-intent",
                                           "reencode-between-renames",
                                           "reencode-before-unlink",
                                           "reencode-after-put",
                                           "reencode-after-ec-tmp"};
static pthread_once_t sc_once = PTHREAD_ONCE_INIT;
static int sc_point = -1; /* the armed point, -1: none */
static long sc_at = 1;
static atomic_long sc_hits;

static void sc_read(void)
{
    const char *v = getenv("TSDB_SETTLE_CRASH_AT");
    if (!v || !*v)
        return;
    const char *colon = strchr(v, ':');
    size_t len = colon ? (size_t)(colon - v) : strlen(v);
    for (int i = 0; i < SC_N; i++)
        if (strlen(sc_names[i]) == len && strncmp(v, sc_names[i], len) == 0)
            sc_point = i;
    char *end = NULL;
    if (colon)
        sc_at = strtol(colon + 1, &end, 10);
    if (sc_point < 0 || (colon && (*end || sc_at < 1))) {
        fprintf(stderr, "tsdb: TSDB_SETTLE_CRASH_AT=%s names no crash point: exiting with status 2\n", v);
        _exit(2); /* a typo in a harness must not pass as a point that never fired */
    }
}

static void settle_crash(int point)
{
    pthread_once(&sc_once, sc_read);
    if (point != sc_point)
        return;
    long hit = atomic_fetch_add(&sc_hits, 1) + 1;
    if (hit == sc_at) {
        fprintf(stderr, "tsdb: settle crash point %s (hit %ld): exiting with status 99\n", sc_names[point], hit);
        exit(99);
    }
}

/* TSDB_SWAP_MUTANT=swapearly arms SWAP_MUT_EARLY in a whole process (tsdbd under tests/jepsen/mutant-settle.sh, whose
 * settle-steps run must catch it); the other mutants are test_ecswap's, set through ecswap_mutant. Read once, at the
 * first swap; an unknown name ends the process with status 2, as TSDB_SETTLE_CRASH_AT does. */
static pthread_once_t sm_once = PTHREAD_ONCE_INIT;
static void sm_read(void)
{
    const char *v = getenv("TSDB_SWAP_MUTANT");
    if (!v || !*v || strcmp(v, "none") == 0)
        return;
    if (strcmp(v, "swapearly") != 0) {
        fprintf(stderr, "tsdb: TSDB_SWAP_MUTANT=%s names no mutant: exiting with status 2\n", v);
        _exit(2);
    }
    ecswap_mutant = SWAP_MUT_EARLY;
}

static inline void sm_init(void) { pthread_once(&sm_once, sm_read); }

void ecswap_crash_point(int which)
{
    int p = which == ECSWAP_CRASH_AFTER_PUT            ? SC_AFTER_PUT
            : which == ECSWAP_CRASH_AFTER_EC_TMP       ? SC_AFTER_EC_TMP
            : which == ECSWAP_CRASH_REENCODE_AFTER_PUT ? SC_RE_AFTER_PUT
                                                       : SC_RE_AFTER_EC_TMP;
    settle_crash(p);
}
#else
static inline void settle_crash(int point) { (void)point; }
static inline void sm_init(void) {}
void ecswap_crash_point(int which) { (void)which; }
#endif

/* ------------------------------------------------------------ names */

/* settled_<name> of a source */
static void settled_of(const char *src, char *out, size_t cap) { snprintf(out, cap, "settled_%s", src); }

/* the partition key of p<K>_..., as written in a name */
static bool name_key(const char *name, int64_t *key)
{
    return sscanf(name, "p%" SCNd64 "_", key) == 1;
}

/* partition key, block range and level of p<K>_<A>_<B>_<L>.{part,ec} */
static bool name_meta(const char *name, int64_t *key, uint64_t *a, uint64_t *b, uint32_t *lv)
{
    char dot;
    return sscanf(name, "p%" SCNd64 "_%" SCNu64 "_%" SCNu64 "_%" SCNu32 "%c", key, a, b, lv, &dot) == 5 &&
           dot == '.' && *a <= *b;
}

/* retired_<name> of a source */
static void retired_of(const char *src, char *out, size_t cap) { snprintf(out, cap, "retired_%s", src); }

/* P.ec for P.part: the same stem */
static bool same_stem(const char *part_name, const char *ec_name)
{
    size_t lp = strlen(part_name), le = strlen(ec_name);
    return lp > 5 && le > 3 && lp - 5 == le - 3 && strncmp(part_name, ec_name, lp - 5) == 0 &&
           strcmp(part_name + lp - 5, ".part") == 0 && strcmp(ec_name + le - 3, ".ec") == 0;
}

/* 1: the name exists, 0: it does not, TSDB_ERR_IO: the lookup failed (EIO, ESTALE on NFS), which is never "absent" */
static int name_at(eng *db, const char *intent, const char *name)
{
    if (faccessat(db->dirfd, name, F_OK, 0) == 0)
        return 1;
    if (errno == ENOENT)
        return 0;
    fprintf(stderr, "tsdb: settle intent %s: lookup of %s: %s: the open fails\n", intent, name, strerror(errno));
    return TSDB_ERR_IO;
}

/* ------------------------------------------------------------ the commit */

static inline void sn_add(size_t *len, size_t cap, int r)
{
    if (r > 0 && *len < cap)
        *len += (size_t)r < cap - *len ? (size_t)r : cap - *len - 1;
}

static int intent_write(eng *db, const char *intent, part **in, part **ec, const char *const *pend, size_t n)
{
    size_t cap = 64, len = 0; /* the lines' own lengths: no bound on a name's length is assumed */
    for (size_t i = 0; i < n; i++) {
        const char *pn = pend ? pend[i] : ec[i]->pending;
        cap += 16 + strlen(pn) + strlen(ec[i]->name) + 2 * strlen(in[i]->name);
    }
    int fail = atomic_load(&ecswap_test_fail); /* tests: one planted failure (ecswap.h) */
    if (fail == ECSWAP_FAIL_INTENT_NOMEM || fail == ECSWAP_FAIL_INTENT_IO)
        atomic_store(&ecswap_test_fail, ECSWAP_FAIL_NONE);
    char *buf = fail == ECSWAP_FAIL_INTENT_NOMEM ? NULL : malloc(cap);
    if (!buf)
        return TSDB_ERR_NOMEM;
    sn_add(&len, cap, snprintf(buf + len, cap - len, INTENT_V4_HEAD "%zu\n", n));
    for (size_t i = 0; i < n; i++) {
        const char *pn = pend ? pend[i] : ec[i]->pending;
        sn_add(&len, cap, snprintf(buf + len, cap - len, "L %s %s\n", pn, ec[i]->name));
    }
    for (size_t i = 0; i < n; i++)
        sn_add(&len, cap, snprintf(buf + len, cap - len, "R %s settled_%s\n", in[i]->name, in[i]->name));
    sn_add(&len, cap, snprintf(buf + len, cap - len, "end %016" PRIx64 "\n", xxh64(buf, len, 0)));
    int fd = openat(db->dirfd, intent, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    int rc = fd < 0 || write_full(fd, buf, len) != 0 || (db->opt.fsync && tsdb_fdatasync(fd) != 0) ? TSDB_ERR_IO : 0;
    if (fd >= 0 && close(fd) != 0 && rc == 0) /* on NFS, close sends the data to the server */
        rc = TSDB_ERR_IO;
    if (fail == ECSWAP_FAIL_INTENT_IO) /* as a failed fdatasync: the whole intent written, its name never synced */
        rc = TSDB_ERR_IO;
    free(buf);
    if (rc && ecswap_intent_fail_hook)
        ecswap_intent_fail_hook(db);
    if (rc) /* nothing committed: this thread did not sync the name (another one may have), and it goes now */
        unlinkat(db->dirfd, intent, 0);
    return rc;
}

/* the arguments' rules (src/ecswap.h) */
static bool args_valid(part **in, part **ec, size_t n)
{
    int64_t key = 0;
    for (size_t i = 0; i < n; i++) {
        part *p = in[i], *e = ec[i];
        if (!p || !e || p == e || !part_file_name(p->name) || !e->pending || !ec_tmp_name(e->pending) || !ec_file_name(e->name) ||
            !same_stem(p->name, e->name) || !atomic_load(&e->ec) || atomic_load(&p->ec) ||
            p->partition != e->partition || p->min_block != e->min_block || p->max_block != e->max_block ||
            p->level != e->level)
            return false;
        if (i == 0)
            key = p->partition;
        else if (p->partition != key) /* one key: see the top of the file */
            return false;
        for (size_t j = 0; j < i; j++)
            if (in[j] == p || ec[j] == e)
                return false;
    }
    return true;
}

/* A refused swap: its temporaries are unlinked, the directory is synced, the objects go to r7-gc, then the intent is
 * unlinked. Not on a failed engine, which names and removes nothing more: the next open sorts the files out.
 * The order of the unlinks themselves does not matter (an intent missing a temporary is rolled back at open, a
 * temporary with no intent is removed: ecswap_recover); what matters is that the objects go to r7-gc only after that
 * sync, also with no intent (a write of it failed: another thread's directory sync may have made its name durable
 * before intent_write removed it). Until the unlinks are durable a power cut can bring back an intent together with
 * every temporary and every source (a drop's renames of the sources are not synced either when the swap refuses), and
 * the open rolls such an intent forward: the .ec parts are listed and their chunks must still be there. A sync that
 * fails failed the engine: nothing is handed over, and the next open's sweep (after the intents are recovered) takes
 * the objects nothing names. test_settle gcpower (gcearly, nullnosync, syncfailgo). */
static void swap_discard(eng *db, const char *intent, part **ec, size_t n)
{
    if (eng_check(db))
        return;
    for (size_t i = 0; i < n; i++) {
        if (ec[i]->pending)
            unlinkat(db->dirfd, ec[i]->pending, 0);
        if (settle_mutant == SETTLE_MUT_GCEARLY) /* the bug: the chunks go while the intent can still roll forward */
            ecgc_unlisted_object(db, ec[i]);
    }
    if (ecswap_discard_hook)
        ecswap_discard_hook(db);
    if ((intent || settle_mutant != SETTLE_MUT_NULLNOSYNC) &&
        eng_sync_dir(db, "fsync of the directory after removing a refused settle's files") != 0 &&
        settle_mutant != SETTLE_MUT_SYNCFAILGO)
        return;
    for (size_t i = 0; i < n && settle_mutant != SETTLE_MUT_GCEARLY && settle_mutant != SETTLE_MUT_GCSKIPDISCARD; i++)
        ecgc_unlisted_object(db, ec[i]); /* its chunks are no one's now (r7-gc) */
    if (ecswap_handed_hook)
        ecswap_handed_hook(db);
    if (intent)
        unlinkat(db->dirfd, intent, 0);
}

/* A refusal of a swap whose objects a committed COLD marker names (ecapply_swap_pre's `logged`, R8): the replicas list
 * or will list .ec parts on them, so they are never r7-gc's, nor the caller's to clean up. Nothing was renamed; the
 * engine fails with a line, the temporaries (and an intent already written) stay, and the next open decides (r8h's
 * recovery of the COLD). TSDB_ERR_IO, as for every failed engine: a caller hands nothing over on it. */
static int swap_refuse_logged(eng *db, const char *why)
{
    char what[192];
    snprintf(what, sizeof what, "a settle swap whose objects a committed COLD marker names (%s; its objects kept)", why);
    eng_fail(db, what, 0);
    return TSDB_ERR_IO;
}

/* the L renames, then the R renames; false (the engine failed) when one fails */
static bool swap_renames(eng *db, part **in, part **ec, size_t n)
{
    char settled[sizeof in[0]->name + 16];
    for (size_t k = 0; k < 2 * n; k++) {
        if (k > 0)
            settle_crash(SC_BETWEEN_RENAMES);
        size_t i = k % n;
        if (k < n) {
            if (renameat(db->dirfd, ec[i]->pending, db->dirfd, ec[i]->name) != 0) {
                eng_fail(db, "rename of a settled part's .ec file into place", errno);
                return false;
            }
        } else {
            settled_of(in[i]->name, settled, sizeof settled);
            if (renameat(db->dirfd, in[i]->name, db->dirfd, settled) != 0) {
                eng_fail(db, "rename of a settled part's source to settled_", errno);
                return false;
            }
        }
    }
    return true;
}

/* R15 (eng_unsettle): true (>0) if any retired_<ps[i]->name> still exists in the graveyard or on disk, 0 if none,
 * -1 on I/O error (does not fail-stop the engine; nothing has been committed yet). Keeps a re-settle from colliding
 * with a retired_.ec from an earlier unsettle (which ecswap_recover would read as "the settle renamed") and keeps a
 * second unsettle from clobbering a retired_.ec whose chunks ecgc has not finished deleting. Caller holds
 * publish_lock, not db->lock. */
static int retired_ec_busy(eng *db, part *const *ps, size_t n, const char **hit)
{
    char retired[sizeof ps[0]->name + 16];
    pthread_mutex_lock(&db->lock);
    const char *found = NULL;
    for (size_t i = 0; i < n && !found; i++) {
        retired_of(ps[i]->name, retired, sizeof retired);
        for (size_t j = 0; j < db->ngraveyard; j++) {
            if (db->graveyard[j].kind == GY_EC && strcmp(db->graveyard[j].name, retired) == 0) {
                found = ps[i]->name;
                break;
            }
        }
    }
    pthread_mutex_unlock(&db->lock);
    if (found) {
        if (hit)
            *hit = found;
        return 1;
    }
    for (size_t i = 0; i < n; i++) {
        retired_of(ps[i]->name, retired, sizeof retired);
        if (faccessat(db->dirfd, retired, F_OK, 0) == 0) {
            if (hit)
                *hit = ps[i]->name;
            return 1;
        }
        if (errno != ENOENT)
            return -1;
    }
    return 0;
}

static void settled_of_ec(const char *ec_name, char *dst, size_t cap)
{
    size_t len = strlen(ec_name);
    if (len > 3)
        snprintf(dst, cap, "settled_%.*s.part", (int)(len - 3), ec_name);
    else
        dst[0] = '\0';
}

static void free_early_pend(char **arr, size_t n)
{
    if (!arr)
        return;
    for (size_t i = 0; i < n; i++)
        free(arr[i]);
    free(arr);
}

int ec_swap_commit(eng *db, part **in, part **ec, size_t n)
{
    sm_init();
    if (n == 0)
        return 0;
    if (db->opt.read_only || db->opt.follow)
        return TSDB_ERR_READONLY;
    if (!args_valid(in, ec, n))
        return TSDB_ERR_INVALID;
    size_t *at = NULL;
    char intent[64];
    snprintf(intent, sizeof intent, "txn_%d_%u.intent", (int)getpid(), atomic_fetch_add(&db->tmp_counter, 1));

    pthread_mutex_lock(&db->publish_lock);
    int rc = eng_check(db) ? TSDB_ERR_IO : 0;
    bool early = ecswap_mutant == SWAP_MUT_EARLY;
    /* R8 (src/ecapply.h): before the intent's commit point, a cluster ec volume's .ec PPOS records and the COLD's
     * RSTATE `doing` (a stub that does nothing elsewhere); failed, nothing of the swap is visible. `logged`: a
     * committed COLD marker names the objects, so no refusal from here on hands them to r7-gc (swap_refuse_logged) */
    bool logged = false;
    if (rc == 0) {
        rc = ecapply_swap_pre(db, in, ec, n, &logged);
        logged = (logged || atomic_load(&ecswap_test_logged)) && ecswap_mutant != SWAP_MUT_LOGGEDGC;
        if (rc && logged) {
            pthread_mutex_unlock(&db->publish_lock);
            return swap_refuse_logged(db, "its hook before the intent failed");
        }
        if (rc) {
            pthread_mutex_unlock(&db->publish_lock);
            if (rc != TSDB_ERR_NOMEM)
                swap_discard(db, NULL, ec, n);
            return rc;
        }
    }
    if (rc == 0) { /* after the hook, so that a logged swap is not handed back as out of memory (the caller's
                    * cleanup gives the objects to r7-gc) */
        if (atomic_load(&ecswap_test_fail) == ECSWAP_FAIL_AT_NOMEM)
            atomic_store(&ecswap_test_fail, ECSWAP_FAIL_NONE);
        else
            at = malloc(n * sizeof *at);
        if (!at) {
            pthread_mutex_unlock(&db->publish_lock);
            return logged ? swap_refuse_logged(db, "out of memory") : TSDB_ERR_NOMEM;
        }
    }
    if (rc == 0 && !logged) {
        const char *hit = NULL;
        int rbusy = retired_ec_busy(db, ec, n, &hit);
        if (rbusy != 0) {
            rc = rbusy < 0 ? TSDB_ERR_IO : TSDB_ERR_STOPPED;
            if (rbusy > 0)
                fprintf(stderr, "tsdb: %s: settle of %s deferred while retired_%s is still awaiting GC\n", db->dir, hit,
                        hit);
            pthread_mutex_unlock(&db->publish_lock);
            free(at);
            if (rbusy > 0)
                swap_discard(db, NULL, ec, n);
            return rc;
        }
    }
    if (rc == 0 && !early && (rc = intent_write(db, intent, in, ec, NULL, n)) != 0) {
        pthread_mutex_unlock(&db->publish_lock);
        free(at);
        if (logged)
            return swap_refuse_logged(db, "its intent could not be written");
        if (rc != TSDB_ERR_NOMEM || settle_mutant == SETTLE_MUT_GCDOUBLE) /* NOMEM: nothing written, the caller
                                                                           * cleans up (ecswap.h), never both */
            swap_discard(db, NULL, ec, n);
        return rc;
    }
    if (rc == 0 && !early && ecswap_mutant != SWAP_MUT_NODIRSYNC &&
        eng_sync_dir(db, "fsync of the directory after writing a settle's intent") != 0)
        rc = TSDB_ERR_IO; /* the engine failed; the intent's name is the commit: the next open decides */
    if (rc) {
        pthread_mutex_unlock(&db->publish_lock);
        free(at);
        return rc;
    }
    settle_crash(SC_AFTER_INTENT);
    if (ec_swap_hook)
        ec_swap_hook(db);

    char **early_pend = NULL;
    if (early) {
        early_pend = calloc(n, sizeof *early_pend);
        for (size_t i = 0; early_pend && i < n; i++)
            early_pend[i] = strdup(ec[i]->pending);
    }
    lt_sec lsec;
    lt_begin(db, &lsec);
    pthread_mutex_lock(&db->lock);
    bool ok = ecswap_mutant == SWAP_MUT_NOFAILCHECK || eng_check(db) == 0;
    for (size_t i = 0; i < n && ok; i++) {
        size_t j = 0;
        while (j < db->nparts && db->parts[j] != in[i])
            j++;
        at[i] = j;
        if (ecswap_mutant == SWAP_MUT_NODROPCHECK)
            continue;
        ok = (j < db->nparts || ecswap_mutant == SWAP_MUT_NOLISTED) && !in[i]->dropped && !in[i]->merging;
    }
    bool renamed = ok && swap_renames(db, in, ec, n);
    if (renamed) {
        for (size_t i = 0; i < n; i++) {
            part_ref(ec[i]);
            part_clear_pending(ec[i]);
            if (at[i] < db->nparts && db->parts[at[i]] == in[i]) {
                db->parts[at[i]] = ec[i]; /* the same key and block range: the same place in the order */
                if (ecswap_mutant != SWAP_MUT_NOSTATS) { /* the third change of the list beside insert and remove */
                    stats_ev_remove(db, in[i]);
                    stats_ev_add(db, ec[i]);
                }
                part_unref(in[i]); /* the list's reference; the caller holds its own */
            } else {
                db_parts_insert_locked(db, ec[i]); /* swapnodropcheck only: the check above keeps this away */
            }
        }
    }
    pthread_mutex_unlock(&db->lock);
    lt_end(db, &lsec);
    free(at);

    if (!ok && logged && eng_check(db) == 0) { /* a dropped, merging or unlisted source: the intent stays */
        free_early_pend(early_pend, n);
        pthread_mutex_unlock(&db->publish_lock);
        return swap_refuse_logged(db, "a source was dropped, merged or unlisted before its renames");
    }
    if (!ok) {
        free_early_pend(early_pend, n);
        rc = eng_check(db) ? TSDB_ERR_IO : TSDB_ERR_STOPPED;
        if (!early)
            swap_discard(db, intent, ec, n);
        pthread_mutex_unlock(&db->publish_lock);
        return rc;
    }
    if (!renamed) { /* fail-stop: names may be half visible; the next open finishes or undoes the swap */
        free_early_pend(early_pend, n);
        pthread_mutex_unlock(&db->publish_lock);
        return TSDB_ERR_IO;
    }
    if (early) { /* the planted bug: the intent after the fact */
        bool have_all = early_pend != NULL;
        for (size_t i = 0; have_all && i < n; i++)
            have_all = early_pend[i] != NULL;
        rc = have_all ? intent_write(db, intent, in, ec, (const char *const *)early_pend, n) : TSDB_ERR_NOMEM;
        free_early_pend(early_pend, n);
        if (rc != 0) {
            pthread_mutex_unlock(&db->publish_lock);
            return rc;
        }
    }
    if ((rc = eng_sync_dir(db, "fsync of the directory after swapping settled parts")) != 0) {
        pthread_mutex_unlock(&db->publish_lock);
        return rc; /* the engine failed; the intent is still there: the next open rolls the swap forward */
    }
    ecapply_swap_post(db, in, ec, n); /* R8: the swap is durable (a COLD's RSTATE `done`); under publish_lock */
    settle_crash(SC_BEFORE_UNLINK);
    /* The swap is durable and listed. An intent that stays (this unlink failed, or a power cut brings it back:
     * nothing syncs it) is rolled forward at the next open, which finds the L names (ecswap_recover). */
    if (unlinkat(db->dirfd, intent, 0) != 0)
        fprintf(stderr, "tsdb: unlink of settle intent %s after its swap: %s (the next open rolls it forward)\n",
                intent, strerror(errno));
    pthread_mutex_unlock(&db->publish_lock);
    eckeep_swapped(db, in, n); /* r7-keep: the settled_ files are held until their digests matched and keep_s passed */
    /* Its result is not the caller's, as in db_add_parts: the parts are committed and listed. A failed write of
     * MANIFEST failed the engine, which the next call sees; out of memory leaves the older MANIFEST until the next
     * publish. A caller that read an error here as "not swapped" would free the chunks of a listed .ec part. */
    int mrc = ec_swap_publish(db);
    return ecswap_mutant == SWAP_MUT_MANIFESTRC ? mrc : 0;
}

/* ------------------------------------------------------------ R10 re-encode swap */

static bool reencode_args_valid(part **in, size_t nin, part *out)
{
    if (!in || nin == 0 || nin > (1u << 16) || !out)
        return false;
    bool out_ec = atomic_load(&out->ec) != NULL;
    if (out_ec) {
        if (!out->pending || !ec_tmp_name(out->pending) || !ec_file_name(out->name))
            return false;
    } else {
        if (out->ng != 0 || !out->pending || !part_tmp_name(out->pending) || !part_file_name(out->name))
            return false;
    }
    uint64_t min_b = UINT64_MAX, max_b = 0;
    bool has_ec = false;
    for (size_t i = 0; i < nin; i++) {
        part *p = in[i];
        if (!p || p == out || p->partition != out->partition || strcmp(p->name, out->name) == 0)
            return false;
        bool pec = atomic_load(&p->ec) != NULL;
        if (pec ? !ec_file_name(p->name) : !part_file_name(p->name))
            return false;
        if (i > 0 && p->min_block <= in[i - 1]->max_block)
            return false;
        for (size_t j = 0; j < i; j++)
            if (in[j] == p)
                return false;
        has_ec |= pec;
        min_b = MIN(min_b, p->min_block);
        max_b = MAX(max_b, p->max_block);
    }
    return has_ec && out->min_block == min_b && out->max_block == max_b;
}

static int intent_write_reencode(eng *db, const char *intent, part **in, size_t nin, part *out, const char *pend)
{
    const char *pn = pend ? pend : out->pending;
    size_t cap = 96 + strlen(pn) + strlen(out->name), len = 0;
    for (size_t i = 0; i < nin; i++)
        cap += 20 + 2 * strlen(in[i]->name);
    int fail = atomic_load(&ecswap_test_fail);
    if (fail == ECSWAP_FAIL_INTENT_NOMEM || fail == ECSWAP_FAIL_INTENT_IO)
        atomic_store(&ecswap_test_fail, ECSWAP_FAIL_NONE);
    char *buf = fail == ECSWAP_FAIL_INTENT_NOMEM ? NULL : malloc(cap);
    if (!buf)
        return TSDB_ERR_NOMEM;
    len += (size_t)snprintf(buf + len, cap - len, INTENT_V4_HEAD "1 %zu\n", nin);
    len += (size_t)snprintf(buf + len, cap - len, "L %s %s\n", pn, out->name);
    for (size_t i = 0; i < nin; i++) {
        const char *pfx = atomic_load(&in[i]->ec) ? "retired_" : "settled_";
        len += (size_t)snprintf(buf + len, cap - len, "R %s %s%s\n", in[i]->name, pfx, in[i]->name);
    }
    len += (size_t)snprintf(buf + len, cap - len, "end %016" PRIx64 "\n", xxh64(buf, len, 0));
    int fd = openat(db->dirfd, intent, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    int rc = fd < 0 || write_full(fd, buf, len) != 0 || (db->opt.fsync && tsdb_fdatasync(fd) != 0) ? TSDB_ERR_IO : 0;
    if (fd >= 0 && close(fd) != 0 && rc == 0)
        rc = TSDB_ERR_IO;
    if (fail == ECSWAP_FAIL_INTENT_IO)
        rc = TSDB_ERR_IO;
    free(buf);
    if (rc && ecswap_intent_fail_hook)
        ecswap_intent_fail_hook(db);
    if (rc)
        unlinkat(db->dirfd, intent, 0);
    return rc;
}

static void swap_discard_reencode(eng *db, part *out)
{
    if (eng_check(db))
        return;
    if (out->pending)
        unlinkat(db->dirfd, out->pending, 0);
    if (ecswap_discard_hook)
        ecswap_discard_hook(db);
    struct ecpart *ep = atomic_load(&out->ec);
    if (!ep)
        return;
    eccache_forget(db->tier, db->tier_vol, ep->rec.oid);
    if (eng_sync_dir(db, "fsync of the directory after removing a refused re-encode's files") != 0 &&
        settle_mutant != SETTLE_MUT_SYNCFAILGO)
        return;
    if (settle_mutant != SETTLE_MUT_GCEARLY && settle_mutant != SETTLE_MUT_GCSKIPDISCARD)
        ecgc_unlisted_object(db, out);
    if (ecswap_handed_hook)
        ecswap_handed_hook(db);
}

/* L rename, then directory fsync (so L is durable before any .ec source is renamed to retired_, keeping crash recovery
 * unambiguous against a concurrent drop), then R renames. Caller holds publish_lock, latest_rw and db->lock. */
static bool swap_renames_reencode(eng *db, part **in, size_t nin, part *out)
{
    if (renameat(db->dirfd, out->pending, db->dirfd, out->name) != 0) {
        eng_fail(db, "rename of a re-encoded part into place", errno);
        return false;
    }
    if (ecswap_mutant != SWAP_MUT_NODIRSYNC &&
        eng_sync_dir(db, "fsync of the directory after naming a re-encoded part") != 0)
        return false;
    char dst[sizeof in[0]->name + 16];
    for (size_t i = 0; i < nin; i++) {
        settle_crash(SC_BETWEEN_RENAMES);
        settle_crash(SC_RE_BETWEEN_RENAMES);
        if (atomic_load(&in[i]->ec))
            retired_of(in[i]->name, dst, sizeof dst);
        else
            settled_of(in[i]->name, dst, sizeof dst);
        if (renameat(db->dirfd, in[i]->name, db->dirfd, dst) != 0) {
            eng_fail(db, "rename of a re-encode's source part", errno);
            return false;
        }
    }
    return true;
}

int ec_swap_reencode(eng *db, part **in, size_t nin, part *out)
{
    sm_init();
    if (db->opt.read_only || db->opt.follow)
        return TSDB_ERR_READONLY;
    if (!reencode_args_valid(in, nin, out))
        return TSDB_ERR_INVALID;
    char intent[64];
    snprintf(intent, sizeof intent, "txn_%d_%u.intent", (int)getpid(), atomic_fetch_add(&db->tmp_counter, 1));

    pthread_mutex_lock(&db->publish_lock);
    int rc = eng_check(db) ? TSDB_ERR_IO : 0;
    bool early = ecswap_mutant == SWAP_MUT_EARLY;
    bool logged = false;
    part *out1[1] = {out};
    if (rc == 0) {
        rc = ecapply_swap_pre(db, in, out1, 1, &logged);
        logged = (logged || atomic_load(&ecswap_test_logged)) && ecswap_mutant != SWAP_MUT_LOGGEDGC;
        if (rc && logged) {
            pthread_mutex_unlock(&db->publish_lock);
            return swap_refuse_logged(db, "its hook before the intent failed");
        }
        if (rc) {
            pthread_mutex_unlock(&db->publish_lock);
            if (rc != TSDB_ERR_NOMEM)
                swap_discard_reencode(db, out);
            return rc;
        }
    }
    part **pin_parts = NULL;
    if (rc == 0) {
        pin_parts = calloc(nin, sizeof *pin_parts);
        if (!pin_parts) {
            pthread_mutex_unlock(&db->publish_lock);
            return logged ? swap_refuse_logged(db, "out of memory") : TSDB_ERR_NOMEM;
        }
    }
    if (rc) {
        pthread_mutex_unlock(&db->publish_lock);
        free(pin_parts);
        return rc;
    }
    if (ec_swap_hook)
        ec_swap_hook(db);

    /* Hold latest_rw > db->lock across the input check, intent write, L rename+sync, R renames, list update and
     * lt_merge_marks: TTL drops run under db->lock, and drop_retire (whose intent preparation runs unlocked) waits in
     * drop_settle_locked for every p->reencoding in partitions <= ts while eng_reencode refuses partition_dropping, so
     * a drop intent and a re-encode intent never overlap on disk. */
    char *early_pend = early ? strdup(out->pending) : NULL;
    lt_sec lsec;
    lt_begin(db, &lsec);
    pthread_mutex_lock(&db->lock);
    bool ok = ecswap_mutant == SWAP_MUT_NOFAILCHECK || eng_check(db) == 0;
    size_t base = db->nparts;
    if (ok) {
        for (size_t j = 0; j < db->nparts; j++)
            if (db->parts[j] == in[0]) {
                base = j;
                break;
            }
        if (ecswap_mutant != SWAP_MUT_NODROPCHECK) {
            ok = base + nin <= db->nparts;
            for (size_t i = 0; i < nin && ok; i++)
                ok = (db->parts[base + i] == in[i] || ecswap_mutant == SWAP_MUT_NOLISTED) && !in[i]->dropped &&
                     !in[i]->merging;
        }
    }
    bool renamed = false;
    bool wrote_intent = false;
    if (ok && !early) {
        rc = intent_write_reencode(db, intent, in, nin, out, NULL);
        if (rc == 0) {
            wrote_intent = true;
            if (ecswap_mutant != SWAP_MUT_NODIRSYNC &&
                eng_sync_dir(db, "fsync of the directory after writing a re-encode's intent") != 0)
                rc = TSDB_ERR_IO;
        }
        if (rc == 0) {
            settle_crash(SC_AFTER_INTENT);
            settle_crash(SC_RE_AFTER_INTENT);
        }
    }
    if (ok && rc == 0)
        renamed = swap_renames_reencode(db, in, nin, out);
    size_t npart_src = 0;
    if (renamed) {
        lt_merge_marks(db, &lsec, in, nin);
        for (size_t i = 0; i < nin; i++) {
            if (atomic_load(&in[i]->ec)) {
                if (logged)
                    ecgc_retire_locked_digest(db, in[i]);
                else
                    ecgc_retire_locked(db, in[i]);
                if (db->opt.obsolete_part_lifetime_s <= 0) {
                    in[i]->retired = true;
                    atomic_store(&in[i]->obsolete, true);
                } else {
                    char retired[sizeof in[i]->name + 16];
                    retired_of(in[i]->name, retired, sizeof retired);
                    (void)graveyard_add(db, retired, GY_EC,
                                        eng_mono_ms() + (int64_t)db->opt.obsolete_part_lifetime_s * 1000);
                }
            } else {
                pin_parts[npart_src++] = in[i];
            }
            if (ecswap_mutant != SWAP_MUT_NOSTATS)
                stats_ev_remove(db, in[i]);
            part_unref(in[i]); /* the list's reference; caller holds its own */
        }
        part_ref(out);
        part_clear_pending(out);
        if (base + nin <= db->nparts) {
            db->parts[base] = out;
            if (nin > 1)
                memmove(db->parts + base + 1, db->parts + base + nin, (db->nparts - (base + nin)) * sizeof *db->parts);
            db->nparts -= nin - 1;
            if (ecswap_mutant != SWAP_MUT_NOSTATS)
                stats_ev_add(db, out);
        } else {
            db_parts_insert_locked(db, out);
        }
        db_stall_clear_locked(db, out->partition);
        pthread_cond_broadcast(&db->merged_cv);
    }
    pthread_mutex_unlock(&db->lock);
    lt_end(db, &lsec);

    if (!ok && logged && eng_check(db) == 0) {
        free(early_pend);
        pthread_mutex_unlock(&db->publish_lock);
        free(pin_parts);
        return swap_refuse_logged(db, "a source was dropped, merged or unlisted before its renames");
    }
    if (!ok) {
        free(early_pend);
        rc = eng_check(db) ? TSDB_ERR_IO : TSDB_ERR_STOPPED;
        swap_discard_reencode(db, out);
        pthread_mutex_unlock(&db->publish_lock);
        free(pin_parts);
        return rc;
    }
    if (rc != 0) {
        free(early_pend);
        if (logged) {
            pthread_mutex_unlock(&db->publish_lock);
            free(pin_parts);
            return swap_refuse_logged(db, "its intent could not be written");
        }
        if (!wrote_intent && rc != TSDB_ERR_NOMEM)
            swap_discard_reencode(db, out);
        pthread_mutex_unlock(&db->publish_lock);
        free(pin_parts);
        return rc;
    }
    if (!renamed) {
        free(early_pend);
        pthread_mutex_unlock(&db->publish_lock);
        free(pin_parts);
        return TSDB_ERR_IO;
    }
    if (early) {
        rc = early_pend ? intent_write_reencode(db, intent, in, nin, out, early_pend) : TSDB_ERR_NOMEM;
        free(early_pend);
        if (rc != 0) {
            pthread_mutex_unlock(&db->publish_lock);
            free(pin_parts);
            return rc;
        }
    }
    if ((rc = eng_sync_dir(db, "fsync of the directory after swapping re-encoded parts")) != 0) {
        pthread_mutex_unlock(&db->publish_lock);
        free(pin_parts);
        return rc;
    }
    ecapply_swap_post(db, in, out1, 1);
    settle_crash(SC_BEFORE_UNLINK);
    settle_crash(SC_RE_BEFORE_UNLINK);
    if (unlinkat(db->dirfd, intent, 0) != 0)
        fprintf(stderr, "tsdb: unlink of re-encode intent %s after its swap: %s (the next open rolls it forward)\n",
                intent, strerror(errno));
    pthread_mutex_unlock(&db->publish_lock);
    /* Re-encoded .part sources have no same-stem .ec, so eckeep's digest thread marks them ECKEEP_ORPHAN and releases
     * them after keep_s; for cluster re-encodes (logged), eccold_reencode hands them over after its leader digest. */
    if (npart_src && !logged)
        eckeep_swapped(db, pin_parts, npart_src);
    free(pin_parts);
    int mrc = ec_swap_publish(db);
    return ecswap_mutant == SWAP_MUT_MANIFESTRC ? mrc : 0;
}

/* ------------------------------------------------------------ R15 reverse (unsettle) swap */

static bool unsettle_args_valid(part **in, part **out, size_t n)
{
    if (!in || !out || n == 0 || n > (1u << 15))
        return false;
    bool rank_mut = ecunsettle_mut() == ECUNSETTLE_MUT_RANK;
    int64_t key = 0;
    for (size_t i = 0; i < n; i++) {
        part *e = in[i], *p = out[i];
        if (!e || !p || e == p || !atomic_load(&e->ec) || atomic_load(&p->ec) || !ec_file_name(e->name) ||
            !part_file_name(p->name) || !p->pending)
            return false;
        bool is_tmp = part_tmp_name(p->pending);
        bool is_settled = settled_part_name(p->pending);
        if (!is_tmp && !is_settled)
            return false;
        if (!rank_mut) {
            if (!same_stem(p->name, e->name) || p->partition != e->partition || p->min_block != e->min_block ||
                p->max_block != e->max_block || p->level != e->level)
                return false;
            if (is_settled && strcmp(p->pending + 8, p->name) != 0)
                return false;
        } else {
            if (p->partition != e->partition)
                return false;
            if (is_settled && !same_stem(p->pending + 8, e->name))
                return false;
        }
        if (i == 0)
            key = e->partition;
        else if (e->partition != key)
            return false;
        for (size_t j = 0; j < i; j++)
            if (in[j] == e || out[j] == p || strcmp(out[j]->pending, p->pending) == 0 ||
                strcmp(out[j]->name, p->name) == 0)
                return false;
    }
    return true;
}

static int intent_write_unsettle(eng *db, const char *intent, part **in, part **out, size_t n)
{
    size_t cap = 64, len = 0;
    for (size_t i = 0; i < n; i++)
        cap += 20 + strlen(out[i]->pending) + strlen(out[i]->name) + 2 * strlen(in[i]->name);
    char *buf = malloc(cap);
    if (!buf)
        return TSDB_ERR_NOMEM;
    sn_add(&len, cap, snprintf(buf + len, cap - len, INTENT_V5_HEAD "%zu\n", 2 * n));
    for (size_t i = 0; i < n; i++)
        sn_add(&len, cap, snprintf(buf + len, cap - len, "L %s %s\n", out[i]->pending, out[i]->name));
    for (size_t i = 0; i < n; i++)
        sn_add(&len, cap, snprintf(buf + len, cap - len, "U %s retired_%s\n", in[i]->name, in[i]->name));
    sn_add(&len, cap, snprintf(buf + len, cap - len, "end %016" PRIx64 "\n", xxh64(buf, len, 0)));
    int fd = openat(db->dirfd, intent, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    int rc = fd < 0 || write_full(fd, buf, len) != 0 || (db->opt.fsync && tsdb_fdatasync(fd) != 0) ? TSDB_ERR_IO : 0;
    if (fd >= 0 && close(fd) != 0 && rc == 0)
        rc = TSDB_ERR_IO;
    free(buf);
    if (rc)
        unlinkat(db->dirfd, intent, 0);
    return rc;
}

/* L renames (src -> P.part), then U renames (P.ec -> retired_P.ec). Caller holds publish_lock, db_pub_begin,
 * latest_rw and db->lock: only the 2n renameat calls run under latest_rw > db->lock (no fsync/fdatasync under them). */
static bool swap_renames_unsettle(eng *db, part **in, part **out, size_t n)
{
    for (size_t i = 0; i < n; i++) {
        if (i > 0)
            settle_crash(SC_BETWEEN_RENAMES);
        if (renameat(db->dirfd, out[i]->pending, db->dirfd, out[i]->name) != 0) {
            eng_fail(db, "rename of an unsettled part into place", errno);
            return false;
        }
    }
    char retired[sizeof in[0]->name + 16];
    for (size_t i = 0; i < n; i++) {
        settle_crash(SC_BETWEEN_RENAMES);
        retired_of(in[i]->name, retired, sizeof retired);
        if (renameat(db->dirfd, in[i]->name, db->dirfd, retired) != 0) {
            eng_fail(db, "rename of an unsettled .ec part to retired_", errno);
            return false;
        }
    }
    return true;
}

/* Cleans up refused unsettle temporaries (ONLY tmp_*.part, never settled_*.part), syncs the directory, then unlinks
 * the intent (if written) and syncs the directory again so a power cut cannot resurrect a rolled-back intent. */
static void unsettle_discard(eng *db, const char *intent, part **out, size_t n)
{
    if (eng_check(db))
        return;
    for (size_t i = 0; i < n; i++)
        if (out[i]->pending && part_tmp_name(out[i]->pending))
            unlinkat(db->dirfd, out[i]->pending, 0);
    if (intent) {
        if (eng_sync_dir(db, "fsync of the directory after removing a refused unsettle's files") != 0)
            return;
        if (unlinkat(db->dirfd, intent, 0) == 0 || errno == ENOENT)
            (void)eng_sync_dir(db, "fsync of the directory after removing a refused unsettle's intent");
    }
}

int ec_swap_unsettle_at(eng *db, part **in, part **out, size_t n, uint64_t unsettle_pos)
{
    sm_init();
    if (n == 0)
        return 0;
    if (db->opt.read_only || db->opt.follow)
        return TSDB_ERR_READONLY;
    if (!unsettle_args_valid(in, out, n))
        return TSDB_ERR_INVALID;
    size_t *at = malloc(n * sizeof *at);
    bool *was_settled = calloc(n, sizeof *was_settled);
    if (!at || !was_settled) {
        free(at);
        free(was_settled);
        return TSDB_ERR_NOMEM;
    }
    for (size_t i = 0; i < n; i++)
        was_settled[i] = settled_part_name(out[i]->pending);
    char intent[64];
    snprintf(intent, sizeof intent, "txn_%d_%u.intent", (int)getpid(), atomic_fetch_add(&db->tmp_counter, 1));

    if (ec_swap_hook)
        ec_swap_hook(db);
    pthread_mutex_lock(&db->publish_lock);
    int rc = eng_check(db) ? TSDB_ERR_IO : 0;
    if (rc) {
        pthread_mutex_unlock(&db->publish_lock);
        free(at);
        free(was_settled);
        return rc;
    }

    /* Serialize against concurrent drops (tsdb_drop_before and ttl_select_locked): waits for any settled drop touching
     * this key, then holds pub_active past the post-rename directory fsync so neither drop path can retire P.ec while
     * we are renaming P.ec -> retired_P.ec and listing P.part. */
    db_pub_begin(db, in, n);

    /* Pre-check under db->lock FIRST (no I/O, no latest_rw): if a drop already retired the key before db_pub_begin,
     * refuse with TSDB_ERR_STOPPED before checking retired_ or writing any intent on disk. Because pub_active stays
     * held, no drop can start after this check. */
    pthread_mutex_lock(&db->lock);
    bool pre_ok = eng_check(db) == 0;
    for (size_t i = 0; i < n && pre_ok; i++) {
        size_t j = 0;
        while (j < db->nparts && db->parts[j] != in[i])
            j++;
        pre_ok = j < db->nparts && !in[i]->dropped && !in[i]->merging && !in[i]->reencoding;
    }
    if (!pre_ok) {
        rc = eng_check(db) ? TSDB_ERR_IO : TSDB_ERR_STOPPED;
        db_pub_end_locked(db);
        pthread_mutex_unlock(&db->lock);
        unsettle_discard(db, NULL, out, n);
        pthread_mutex_unlock(&db->publish_lock);
        free(at);
        free(was_settled);
        return rc;
    }
    pthread_mutex_unlock(&db->lock);

    /* Refuse if any retired_<P>.ec from a previous unsettle is still awaiting GC (never clobber an active retired_). */
    const char *hit = NULL;
    int rbusy = retired_ec_busy(db, in, n, &hit);
    if (rbusy != 0) {
        rc = rbusy < 0 ? TSDB_ERR_IO : TSDB_ERR_LOCKED;
        if (rbusy > 0)
            fprintf(stderr, "tsdb: %s: unsettle of %s refused while retired_%s is still awaiting GC\n", db->dir, hit,
                    hit);
        unsettle_discard(db, NULL, out, n);
        db_pub_end(db);
        pthread_mutex_unlock(&db->publish_lock);
        free(at);
        free(was_settled);
        return rc;
    }

    /* Check source files exist BEFORE writing the intent (outside latest_rw and db->lock; eckeep_purge_entry never
     * unlinks settled_P.part while P.ec has settling set). */
    for (size_t i = 0; i < n; i++) {
        if (faccessat(db->dirfd, out[i]->pending, F_OK, 0) != 0) {
            if (errno == ENOENT)
                rc = TSDB_ERR_NOTFOUND;
            else {
                eng_fail(db, "stat of an unsettle source file", errno);
                rc = TSDB_ERR_IO;
            }
            unsettle_discard(db, NULL, out, n);
            db_pub_end(db);
            pthread_mutex_unlock(&db->publish_lock);
            free(at);
            free(was_settled);
            return rc;
        }
    }

    /* R10 (r10-unsettle; src/ppos.h): on a cluster EC volume (db->ppos != NULL), record PPOS for each restored .part
     * durably BEFORE writing the unsettle intent so the next open never refuses a listed .part lacking a PPOS record. */
    if (db->ppos) {
        for (size_t i = 0; i < n && rc == 0; i++) {
            uint64_t pp = 0;
            if (ppos_of(db, in[i]->name, &pp) != 0 || pp == 0) {
                struct ecpart *ep = atomic_load(&in[i]->ec);
                pp = (ep && ep->rec.ppos) ? ep->rec.ppos : PPOS_UNKNOWN;
            }
            const char *nm = out[i]->name;
            rc = ppos_record(db, &nm, 1, pp);
        }
        if (rc != 0) {
            unsettle_discard(db, NULL, out, n);
            db_pub_end(db);
            pthread_mutex_unlock(&db->publish_lock);
            free(at);
            free(was_settled);
            return rc;
        }
    }

    /* Write and sync the v5 intent OUTSIDE latest_rw and db->lock (matching ec_swap_commit's lock rules). */
    rc = intent_write_unsettle(db, intent, in, out, n);
    if (rc == 0 && eng_sync_dir(db, "fsync of the directory after writing an unsettle's intent") != 0)
        rc = TSDB_ERR_IO;
    if (rc != 0) {
        unsettle_discard(db, NULL, out, n);
        db_pub_end(db);
        pthread_mutex_unlock(&db->publish_lock);
        free(at);
        free(was_settled);
        return rc;
    }
    settle_crash(SC_AFTER_INTENT);

    bool rank_mut = ecunsettle_mut() == ECUNSETTLE_MUT_RANK;
    bool ok = true, renamed = false;

    /* Only the 2n renameat calls, graveyard/eckeep updates and part-list swap run under latest_rw > db->lock. */
    lt_sec lsec;
    lt_begin(db, &lsec);
    pthread_mutex_lock(&db->lock);
    ok = eng_check(db) == 0;
    for (size_t i = 0; i < n && ok; i++) {
        size_t j = 0;
        while (j < db->nparts && db->parts[j] != in[i])
            j++;
        at[i] = j;
        ok = j < db->nparts && !in[i]->dropped && !in[i]->merging && !in[i]->reencoding;
    }
    if (ok)
        renamed = swap_renames_unsettle(db, in, out, n);
    if (renamed) {
        for (size_t i = 0; i < n; i++) {
            /* Clear any GY_SETTLED / eckeep state for this stem on BOTH fast path and decode path so a corrupt
             * settled_P.part's ROT/ORPHAN entry never poisons a future re-settle of P. */
            char settled[sizeof in[0]->name + 16];
            settled_of_ec(in[i]->name, settled, sizeof settled);
            if (settled[0])
                eckeep_unsettle_locked(db, settled);
            ecgc_retire_locked_at(db, in[i], unsettle_pos);
            if (db->opt.obsolete_part_lifetime_s <= 0) {
                in[i]->retired = true;
                atomic_store(&in[i]->obsolete, true);
            } else {
                char retired[sizeof in[i]->name + 16];
                retired_of(in[i]->name, retired, sizeof retired);
                (void)graveyard_add(db, retired, GY_EC,
                                    eng_mono_ms() + (int64_t)db->opt.obsolete_part_lifetime_s * 1000);
            }
            part_ref(out[i]);
            part_clear_pending(out[i]);
            if (!rank_mut && at[i] < db->nparts && db->parts[at[i]] == in[i]) {
                db->parts[at[i]] = out[i]; /* exact same key and block range: same place in the order */
                stats_ev_remove(db, in[i]);
                stats_ev_add(db, out[i]);
                part_unref(in[i]); /* the list's reference; caller holds its own */
            } else {
                db_parts_remove_locked(db, in[i]); /* removes from list and drops the list's reference */
                db_parts_insert_locked(db, out[i]);
            }
        }
        pthread_cond_signal(&db->merge_cv);
    }
    pthread_mutex_unlock(&db->lock);
    lt_end(db, &lsec);
    free(at);

    if (!ok) {
        rc = eng_check(db) ? TSDB_ERR_IO : TSDB_ERR_STOPPED;
        unsettle_discard(db, intent, out, n);
        db_pub_end(db);
        pthread_mutex_unlock(&db->publish_lock);
        free(was_settled);
        return rc;
    }
    if (!renamed) {
        db_pub_end(db);
        pthread_mutex_unlock(&db->publish_lock);
        free(was_settled);
        return TSDB_ERR_IO;
    }
    if ((rc = eng_sync_dir(db, "fsync of the directory after retiring unsettled .ec parts")) != 0) {
        db_pub_end(db);
        pthread_mutex_unlock(&db->publish_lock);
        free(was_settled);
        return rc;
    }
    settle_crash(SC_BEFORE_UNLINK);
    if (unlinkat(db->dirfd, intent, 0) != 0)
        fprintf(stderr, "tsdb: unlink of unsettle intent %s after its swap: %s (the next open rolls it forward)\n",
                intent, strerror(errno));
    /* Decode path: if a corrupt settled_P.part was left beside P.ec, remove it and sync the directory now that P.part
     * is durable so a power loss cannot resurrect the corrupt settled_ copy into ECKEEP_ORPHAN. */
    bool unlinked_settled = false;
    for (size_t i = 0; i < n; i++) {
        if (!was_settled[i]) {
            char settled[sizeof in[0]->name + 16];
            settled_of_ec(in[i]->name, settled, sizeof settled);
            if (settled[0] && unlinkat(db->dirfd, settled, 0) == 0)
                unlinked_settled = true;
        }
    }
    if (unlinked_settled)
        (void)eng_sync_dir(db, "fsync of the directory after removing a corrupt settled_ copy");
    /* The swap is now durable on disk (while publish_lock is still held): release the leader unsettle hold so
     * snap_make and REPL.floor can advance past MK_UNSETTLE knowing P.part is listed and P.ec is retired. */
    if (unsettle_pos > 0)
        atomic_store_explicit(&db->unsettle_hold_pos, 0, memory_order_release);
    free(was_settled);
    db_pub_end(db);
    pthread_mutex_unlock(&db->publish_lock);
    eckeep_unsettle_cleanup_jobs(db);
    (void)ec_swap_publish(db);
    return 0;
}

int ec_swap_unsettle(eng *db, part **in, part **out, size_t n)
{
    return ec_swap_unsettle_at(db, in, out, n, 0);
}

/* ------------------------------------------------------------ R15 tier salvage */

static bool salvage_args_valid(part **in, size_t n)
{
    if (!in || n == 0 || n > (1u << 16))
        return false;
    int64_t key = 0;
    for (size_t i = 0; i < n; i++) {
        part *e = in[i];
        struct ecpart *ep = e ? atomic_load(&e->ec) : NULL;
        if (!e || !ep || ep->test_mark || !ec_file_name(e->name))
            return false;
        if (i == 0)
            key = e->partition;
        else if (e->partition != key)
            return false;
        for (size_t j = 0; j < i; j++)
            if (in[j] == e || strcmp(in[j]->name, e->name) == 0)
                return false;
    }
    return true;
}

static int intent_write_salvage(eng *db, const char *intent, part **in, size_t n)
{
    size_t cap = 64, len = 0;
    for (size_t i = 0; i < n; i++)
        cap += 20 + 2 * strlen(in[i]->name);
    char *buf = malloc(cap);
    if (!buf)
        return TSDB_ERR_NOMEM;
    sn_add(&len, cap, snprintf(buf + len, cap - len, INTENT_V5_HEAD "%zu\n", n));
    for (size_t i = 0; i < n; i++)
        sn_add(&len, cap, snprintf(buf + len, cap - len, "X %s retired_%s\n", in[i]->name, in[i]->name));
    sn_add(&len, cap, snprintf(buf + len, cap - len, "end %016" PRIx64 "\n", xxh64(buf, len, 0)));
    int fd = openat(db->dirfd, intent, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    int rc = fd < 0 || write_full(fd, buf, len) != 0 || (db->opt.fsync && tsdb_fdatasync(fd) != 0) ? TSDB_ERR_IO : 0;
    if (fd >= 0 && close(fd) != 0 && rc == 0)
        rc = TSDB_ERR_IO;
    free(buf);
    if (rc) {
        if (fd >= 0) {
            unlinkat(db->dirfd, intent, 0);
            if (!eng_check(db))
                (void)eng_sync_dir(db, "fsync of the directory after unlinking a failed salvage intent");
        }
    }
    return rc;
}

static bool salvage_shadows_lower_locked(const eng *db, part **in, size_t n)
{
    int64_t key = in[0]->partition;
    for (size_t qi = 0; qi < db->nparts; qi++) {
        const part *q = db->parts[qi];
        if (q->partition != key || q->dropped)
            continue;
        bool in_set = false;
        for (size_t i = 0; i < n; i++) {
            if (in[i] == q) {
                in_set = true;
                break;
            }
        }
        if (in_set)
            continue;
        for (size_t i = 0; i < n; i++) {
            const part *p = in[i];
            if (q->min_block >= p->max_block)
                continue; /* q is higher-block (newer) than p: retiring p cannot resurrect q */
            /* 1. Check whether any tombstone in p shadows rows in lower-block q. */
            for (uint32_t ti = 0; ti < p->nt; ti++) {
                uint32_t qa, qb;
                gix_run(&q->ix, p->t[ti].series, &qa, &qb);
                if (qa >= qb)
                    continue;
                if (qb > q->ng)
                    qb = q->ng;
                for (uint32_t g = qa; g < qb; g++) {
                    int64_t qlo, qhi;
                    gx_ts(q, g, &qlo, &qhi);
                    if (qlo < p->t[ti].end && qhi >= p->t[ti].start) {
                        fprintf(stderr,
                                "tsdb: %s: tier salvage of %s refused: its tombstone shadows surviving lower-block "
                                "part %s (retiring it would resurrect deleted rows)\n",
                                db->dir, p->name, q->name);
                        return true;
                    }
                }
            }
            /* 2. Check whether any granule in p overlaps a granule of the same series in lower-block q. */
            const gix *pix = &p->ix;
            for (uint32_t r = 0; r < pix->nruns; r++) {
                uint32_t s = pix->rser[r];
                uint32_t qa, qb;
                gix_run(&q->ix, s, &qa, &qb);
                if (qa >= qb)
                    continue;
                if (qb > q->ng)
                    qb = q->ng;
                uint32_t pa = gix_rstart(pix, r);
                uint32_t pb = r + 1 < pix->nruns ? gix_rstart(pix, r + 1) : p->ng;
                if (pb > p->ng)
                    pb = p->ng;
                uint32_t gpi = pa, gqi = qa;
                while (gpi < pb && gqi < qb) {
                    int64_t plo, phi, qlo, qhi;
                    gx_ts(p, gpi, &plo, &phi);
                    gx_ts(q, gqi, &qlo, &qhi);
                    if (phi < qlo)
                        gpi++;
                    else if (qhi < plo)
                        gqi++;
                    else {
                        fprintf(stderr,
                                "tsdb: %s: tier salvage of %s refused: its rows overlap surviving lower-block "
                                "part %s (retiring it would resurrect overwritten values)\n",
                                db->dir, p->name, q->name);
                        return true;
                    }
                }
            }
        }
    }
    return false;
}

static bool swap_renames_salvage(eng *db, part **in, size_t n)
{
    char retired[sizeof in[0]->name + 16];
    for (size_t i = 0; i < n; i++) {
        if (i > 0)
            settle_crash(SC_BETWEEN_RENAMES);
        retired_of(in[i]->name, retired, sizeof retired);
        if (renameat(db->dirfd, in[i]->name, db->dirfd, retired) != 0) {
            eng_fail(db, "rename of a salvaged .ec part to retired_", errno);
            return false;
        }
    }
    return true;
}

int ec_swap_salvage_at(eng *db, part **in, size_t n, bool no_lgen_mut, uint64_t salvage_pos, bool cluster_drop_obj,
                       bool clear_hold, size_t *shadowed_out)
{
    sm_init();
    if (n == 0)
        return 0;
    if (db->opt.read_only || db->opt.follow)
        return TSDB_ERR_READONLY;
    if (!salvage_args_valid(in, n))
        return TSDB_ERR_INVALID;

    char intent[64];
    snprintf(intent, sizeof intent, "txn_%d_%u.intent", (int)getpid(), atomic_fetch_add(&db->tmp_counter, 1));

    pthread_mutex_lock(&db->publish_lock);
    if (eng_check(db)) {
        pthread_mutex_unlock(&db->publish_lock);
        return TSDB_ERR_IO;
    }
    if (ec_swap_hook)
        ec_swap_hook(db);

    /* Serialize against concurrent drops (tsdb_drop_before and TTL drops) through post-rename dir fsync. */
    db_pub_begin(db, in, n);

    pthread_mutex_lock(&db->lock);
    bool pre_ok = eng_check(db) == 0;
    for (size_t i = 0; i < n && pre_ok; i++) {
        size_t j = 0;
        while (j < db->nparts && db->parts[j] != in[i])
            j++;
        pre_ok = j < db->nparts && !in[i]->dropped && !in[i]->merging && !in[i]->reencoding;
    }
    bool pre_shadow = pre_ok && !cluster_drop_obj && salvage_shadows_lower_locked(db, in, n);
    if (!pre_ok || pre_shadow) {
        int rc = pre_shadow ? TSDB_ERR_INVALID : eng_check(db) ? TSDB_ERR_IO : TSDB_ERR_STOPPED;
        db_pub_end_locked(db);
        pthread_mutex_unlock(&db->lock);
        pthread_mutex_unlock(&db->publish_lock);
        return rc;
    }
    pthread_mutex_unlock(&db->lock);

    /* Single-server WAL-replay guard: never retire a part whose WAL segment is not durably unlinked (retiring it
     * would drop the WAL replay barrier and let a crash replay its WAL segment). Cluster volumes retain WAL segments
     * for replication and rely on REPL.floor + unsettle_hold_pos instead. */
    if (!cluster_drop_obj) {
        uint64_t max_wal_seq = 0;
        for (size_t i = 0; i < n; i++)
            if (in[i]->wal_seq > max_wal_seq)
                max_wal_seq = in[i]->wal_seq;
        if (max_wal_seq > atomic_load(&db->wal_removed_synced)) {
            if (max_wal_seq <= atomic_load(&db->wal_removed)) {
                if (eng_sync_dir(db, "fsync of the directory to advance wal_removed_synced before salvage") != 0) {
                    db_pub_end(db);
                    pthread_mutex_unlock(&db->publish_lock);
                    return TSDB_ERR_IO;
                }
            }
            if (max_wal_seq > atomic_load(&db->wal_removed_synced)) {
                fprintf(stderr,
                        "tsdb: %s: tier salvage deferred: lost part wal_seq %" PRIu64 " > wal_removed_synced %" PRIu64
                        "\n",
                        db->dir, max_wal_seq, atomic_load(&db->wal_removed_synced));
                db_pub_end(db);
                pthread_mutex_unlock(&db->publish_lock);
                return TSDB_ERR_LOCKED;
            }
        }
    }

    const char *hit = NULL;
    int rbusy = retired_ec_busy(db, in, n, &hit);
    if (rbusy != 0) {
        if (rbusy > 0)
            fprintf(stderr, "tsdb: %s: salvage of %s refused while retired_%s is still awaiting GC\n", db->dir, hit,
                    hit);
        db_pub_end(db);
        pthread_mutex_unlock(&db->publish_lock);
        return rbusy < 0 ? TSDB_ERR_IO : TSDB_ERR_LOCKED;
    }

    int rc = intent_write_salvage(db, intent, in, n);
    if (rc == 0 && eng_sync_dir(db, "fsync of the directory after writing a salvage's intent") != 0)
        rc = TSDB_ERR_IO;
    if (rc != 0) {
        db_pub_end(db);
        pthread_mutex_unlock(&db->publish_lock);
        return rc;
    }
    settle_crash(SC_AFTER_INTENT);

    lt_sec lsec;
    lt_begin(db, &lsec);
#ifdef TSDB_TEST_HOOKS
    if (no_lgen_mut)
        lsec.no_lgen = true;
#else
    (void)no_lgen_mut;
#endif
    pthread_mutex_lock(&db->lock);
    bool ok = eng_check(db) == 0;
    for (size_t i = 0; i < n && ok; i++) {
        size_t j = 0;
        while (j < db->nparts && db->parts[j] != in[i])
            j++;
        ok = j < db->nparts && !in[i]->dropped && !in[i]->merging && !in[i]->reencoding;
    }
    bool shadows_lower = ok && salvage_shadows_lower_locked(db, in, n);
    bool shadow_err = !cluster_drop_obj && shadows_lower;
    if (shadow_err)
        ok = false;
    bool renamed = ok && swap_renames_salvage(db, in, n);
    if (renamed) {
        if (cluster_drop_obj && shadows_lower) {
            fprintf(stderr,
                    "tsdb: %s: cluster salvage of partition %" PRId64
                    " retired %zu lost object(s) that shadowed lower-block parts; overwritten or deleted rows in lower "
                    "blocks may become visible again\n",
                    db->dir, in[0]->partition, n);
            if (shadowed_out)
                *shadowed_out += n;
        }
        lt_salvage_marks(db, &lsec, in, n);
        for (size_t i = 0; i < n; i++) {
            char settled[sizeof in[0]->name + 16];
            settled_of_ec(in[i]->name, settled, sizeof settled);
            if (settled[0])
                eckeep_unsettle_locked(db, settled);
            in[i]->dropped = true;
            cls_dropped_note(db, in[i]);
            if (salvage_pos > 0)
                ecgc_retire_locked_at(db, in[i], salvage_pos);
            else
                ecgc_retire_locked(db, in[i]);
            if (db->opt.obsolete_part_lifetime_s <= 0) {
                in[i]->retired = true;
                atomic_store(&in[i]->obsolete, true);
            } else {
                char retired[sizeof in[0]->name + 16];
                retired_of(in[i]->name, retired, sizeof retired);
                (void)graveyard_add(db, retired, GY_EC,
                                    eng_mono_ms() + (int64_t)db->opt.obsolete_part_lifetime_s * 1000);
            }
            db_parts_remove_locked(db, in[i]); /* pushes stats_ev_remove and drops list's reference */
        }
        pthread_cond_signal(&db->merge_cv);
    }
    pthread_mutex_unlock(&db->lock);
    lt_end(db, &lsec);

    if (!ok) {
        rc = shadow_err ? TSDB_ERR_INVALID : eng_check(db) ? TSDB_ERR_IO : TSDB_ERR_STOPPED;
        if (eng_check(db) == 0 && (unlinkat(db->dirfd, intent, 0) == 0 || errno == ENOENT))
            (void)eng_sync_dir(db, "fsync of the directory after removing a refused salvage's intent");
        db_pub_end(db);
        pthread_mutex_unlock(&db->publish_lock);
        return rc;
    }
    if (!renamed) {
        db_pub_end(db);
        pthread_mutex_unlock(&db->publish_lock);
        return TSDB_ERR_IO;
    }
    if ((rc = eng_sync_dir(db, "fsync of the directory after retiring salvaged .ec parts")) != 0) {
        db_pub_end(db);
        pthread_mutex_unlock(&db->publish_lock);
        return rc;
    }
    settle_crash(SC_BEFORE_UNLINK);
    if (unlinkat(db->dirfd, intent, 0) != 0)
        fprintf(stderr, "tsdb: unlink of salvage intent %s after retire: %s (the next open rolls it forward)\n",
                intent, strerror(errno));
    /* Remove any corrupt settled_<P>.part left beside a retired .ec part so it does not become an orphan. */
    bool unlinked_settled = false;
    for (size_t i = 0; i < n; i++) {
        char settled[sizeof in[0]->name + 16];
        settled_of_ec(in[i]->name, settled, sizeof settled);
        if (settled[0] && unlinkat(db->dirfd, settled, 0) == 0)
            unlinked_settled = true;
    }
    if (unlinked_settled)
        (void)eng_sync_dir(db, "fsync of the directory after removing a corrupt settled_ copy");
    if (clear_hold)
        atomic_store_explicit(&db->unsettle_hold_pos, 0, memory_order_release);
    db_pub_end(db);
    pthread_mutex_unlock(&db->publish_lock);
    eckeep_unsettle_cleanup_jobs(db);

    for (size_t i = 0; i < n; i++) {
        struct ecpart *ep = atomic_load(&in[i]->ec);
        if (ep && !ep->test_mark && db->tier)
            eccache_forget(db->tier, db->tier_vol, ep->rec.oid);
    }
    if (db->tier)
        ecgc_kick_tier(db->tier);
    (void)ec_swap_publish(db);
    return 0;
}

int ec_swap_salvage(eng *db, part **in, size_t n, bool no_lgen_mut)
{
    return ec_swap_salvage_at(db, in, n, no_lgen_mut, 0, false, false, NULL);
}

/* ------------------------------------------------------------ recovery */

typedef struct {
    char tmp[96], fin[128]; /* L */
    bool is_ec;
} swap_l;

typedef struct {
    char src[128], dst[160]; /* R */
    bool is_ec;              /* src is .ec (dst is retired_<src>), else .part (dst is settled_<src>) */
} swap_r;

typedef struct {
    bool reencode;
    swap_l *l;
    size_t nl;
    swap_r *r;
    size_t nr;
} swap_plan;

static void swap_plan_free(swap_plan *p)
{
    free(p->l);
    free(p->r);
    memset(p, 0, sizeof *p);
}

/* Parses an unsealed v4 intent (either R7's 1-to-1 settle "tsdb-intent 4 <n>" or R10's re-encode
 * "tsdb-intent 4 1 <nr>"): 1; 0 when a line breaks the rules; TSDB_ERR_NOMEM. */
static int swap_parse(char *buf, swap_plan *p)
{
    memset(p, 0, sizeof *p);
    char *line = buf, *next = strchr(line, '\n'), extra;
    size_t nl = 0, nr = 0;
    if (!next)
        return 0;
    *next = '\0';
    int hdr = sscanf(line, INTENT_V4_HEAD "%zu %zu%c", &nl, &nr, &extra);
    if (hdr == 2) {
        p->reencode = true;
        if (nl != 1 || nr < 1 || nr > 1u << 16)
            return 0;
    } else if (sscanf(line, INTENT_V4_HEAD "%zu%c", &nl, &extra) == 1) {
        p->reencode = false;
        nr = nl;
        if (nl < 1 || nl > 1u << 16)
            return 0;
    } else {
        return 0;
    }
    p->l = calloc(nl, sizeof *p->l);
    p->r = calloc(nr, sizeof *p->r);
    if (!p->l || !p->r) {
        swap_plan_free(p);
        return TSDB_ERR_NOMEM;
    }
    p->nl = nl;
    p->nr = nr;
    size_t got = 0, total = nl + nr;
    int64_t key = 0;
    uint64_t lmin = 0, lmax = 0, rmin = UINT64_MAX, rmax = 0;
    bool has_ec = false;
    for (line = next + 1; got < total; line = next + 1) {
        next = strchr(line, '\n');
        if (next)
            *next = '\0';
        bool good = false;
        if (got < nl) {
            swap_l *x = &p->l[got];
            good = sscanf(line, "L %95s %127s%c", x->tmp, x->fin, &extra) == 2;
            if (good && ec_tmp_name(x->tmp) && ec_file_name(x->fin)) {
                x->is_ec = true;
            } else if (good && p->reencode && part_tmp_name(x->tmp) && part_file_name(x->fin)) {
                x->is_ec = false;
            } else {
                good = false;
            }
            if (good && p->reencode) {
                uint32_t lv = 0;
                good = name_meta(x->fin, &key, &lmin, &lmax, &lv);
            }
        } else {
            size_t i = got - nl;
            swap_r *x = &p->r[i];
            good = sscanf(line, "R %127s %159s%c", x->src, x->dst, &extra) == 2;
            if (good && !p->reencode) {
                char want[sizeof x->dst];
                settled_of(x->src, want, sizeof want);
                int64_t k = 0;
                x->is_ec = false;
                good = part_file_name(x->src) && strcmp(want, x->dst) == 0 && same_stem(x->src, p->l[i].fin) &&
                       name_key(x->src, &k) && (i == 0 ? (key = k, true) : k == key);
            } else if (good) {
                char want[sizeof x->dst];
                int64_t k = 0;
                uint64_t a = 0, b = 0;
                uint32_t lv = 0;
                if (ec_file_name(x->src)) {
                    x->is_ec = true;
                    retired_of(x->src, want, sizeof want);
                    has_ec = true;
                } else if (part_file_name(x->src)) {
                    x->is_ec = false;
                    settled_of(x->src, want, sizeof want);
                } else {
                    good = false;
                }
                if (good)
                    good = strcmp(want, x->dst) == 0 && strcmp(x->src, p->l[0].fin) != 0 &&
                           name_meta(x->src, &k, &a, &b, &lv) && k == key;
                for (size_t j = 0; good && j < i; j++)
                    if (strcmp(p->r[j].src, x->src) == 0)
                        good = false;
                if (good) {
                    rmin = MIN(rmin, a);
                    rmax = MAX(rmax, b);
                }
            }
        }
        if (!good) {
            swap_plan_free(p);
            return 0;
        }
        got++;
        if (!next)
            break;
    }
    if (got != total || next || (p->reencode && (!has_ec || rmin != lmin || rmax != lmax))) {
        swap_plan_free(p);
        return 0;
    }
    return 1;
}

int ecswap_recover(eng *db, const char *name, char *buf, size_t len)
{
    EC_CALLED(EC_CALL_SWAP_RECOVER);
    if (!intent_unseal(buf, len)) {
        /* no rename follows an intent before it and the directory are synced: the parts stay, and load_parts
         * removes the tmp files with the other temporaries */
        fprintf(stderr, "tsdb: settle intent %s is torn: the swap is rolled back\n", name);
        return 0;
    }
    swap_plan p;
    int rc = swap_parse(buf, &p);
    if (rc <= 0) {
        if (rc == 0)
            fprintf(stderr, "tsdb: settle intent %s: its checksum holds but a line breaks the rules of a swap's "
                            "names: damage, the open fails\n", name);
        return rc < 0 ? rc : TSDB_ERR_CORRUPT;
    }
    rc = 0;
    unsigned char (*on_l)[3] = calloc(p.nl, sizeof *on_l); /* 0: tmp, 1: fin, 2: retired_<fin> */
    unsigned char (*on_r)[3] = calloc(p.nr, sizeof *on_r); /* 0: src, 1: dst, 2: retired_<src> (for .part only) */
    if (!on_l || !on_r) {
        free(on_l);
        free(on_r);
        swap_plan_free(&p);
        return TSDB_ERR_NOMEM;
    }
    bool renamed = false, src_missing = false, tmp_missing = false;
    char nm[200];
    for (size_t i = 0; i < p.nl && rc == 0; i++) {
        int r0 = name_at(db, name, p.l[i].tmp);
        int r1 = r0 < 0 ? r0 : name_at(db, name, p.l[i].fin);
        snprintf(nm, sizeof nm, "retired_%s", p.l[i].fin);
        int r2 = r1 < 0 ? r1 : name_at(db, name, nm);
        if (r0 < 0 || r1 < 0 || r2 < 0) {
            rc = TSDB_ERR_IO;
            break;
        }
        on_l[i][0] = (unsigned char)r0, on_l[i][1] = (unsigned char)r1, on_l[i][2] = (unsigned char)r2;
        renamed |= r1 || r2;
        tmp_missing |= !r0;
    }
    for (size_t i = 0; i < p.nr && rc == 0; i++) {
        int r0 = name_at(db, name, p.r[i].src);
        int r1 = r0 < 0 ? r0 : name_at(db, name, p.r[i].dst);
        int r2 = 0;
        if (r1 >= 0 && !p.r[i].is_ec) {
            snprintf(nm, sizeof nm, "retired_%s", p.r[i].src);
            r2 = name_at(db, name, nm);
        }
        if (r0 < 0 || r1 < 0 || r2 < 0) {
            rc = TSDB_ERR_IO;
            break;
        }
        on_r[i][0] = (unsigned char)r0, on_r[i][1] = (unsigned char)r1, on_r[i][2] = (unsigned char)r2;
        if (!p.r[i].is_ec) {
            renamed |= r1;
            src_missing |= !r0 && !r1;
        } else {
            /* An .ec source's dst is retired_<src>, which a drop also creates; ec_swap_reencode syncs L before any R
             * rename runs, so retired_<src> alone without L or a settled_ .part does not mean the swap renamed. */
            src_missing |= !r0;
        }
    }
    if (rc == 0 && ecswap_mutant == SWAP_MUT_BACKONGONE && src_missing)
        renamed = false; /* the planted bug: a missing source read as a drop of the key, whatever was renamed */
    if (rc == 0 && renamed) {
        if (!p.reencode) {
            for (size_t i = 0; i < p.nl && rc == 0; i++) {
                const char *why = NULL;
                /* retired_P.part with settled_P.part beside it is an older file of the same name, not this source */
                if (on_r[i][2] && !on_r[i][0] && !on_r[i][1])
                    why = "a source is under retired_ although the swap renamed (a drop before its check renames nothing)";
                else if (!on_l[i][0] && !on_l[i][1] && !on_l[i][2] &&
                         (on_r[i][0] || ecswap_mutant == SWAP_MUT_LOSTSETTLED))
                    why = "an .ec part is under none of its names although the swap renamed";
                if (why) {
                    fprintf(stderr, "tsdb: settle intent %s: %s (%s): damage, the open fails\n", name, why, p.l[i].fin);
                    rc = TSDB_ERR_CORRUPT;
                }
            }
        } else {
            bool any_src = false;
            for (size_t i = 0; i < p.nr && rc == 0; i++) {
                any_src |= on_r[i][0] != 0;
                if (!p.r[i].is_ec && on_r[i][2] && !on_r[i][0] && !on_r[i][1]) {
                    fprintf(stderr, "tsdb: settle intent %s: a source is under retired_ although the swap renamed "
                                    "(%s): damage, the open fails\n", name, p.r[i].src);
                    rc = TSDB_ERR_CORRUPT;
                }
            }
            if (rc == 0 && !on_l[0][0] && !on_l[0][1] && !on_l[0][2] &&
                (any_src || ecswap_mutant == SWAP_MUT_LOSTSETTLED)) {
                fprintf(stderr, "tsdb: settle intent %s: %s is under none of its names although the swap "
                                "renamed (%s): damage, the open fails\n",
                        name, p.l[0].is_ec ? "an .ec part" : "a re-encoded part", p.l[0].fin);
                rc = TSDB_ERR_CORRUPT;
            }
        }
    }
    if (rc == 0 && !renamed && (src_missing || tmp_missing)) {
        fprintf(stderr, "tsdb: settle intent %s: nothing of the swap was renamed and %s: it is rolled back\n", name,
                src_missing ? "a source is gone" : "a tmp .ec is gone");
        for (size_t i = 0; i < p.nl && rc == 0; i++) {
            if (on_l[i][0] && unlinkat(db->dirfd, p.l[i].tmp, 0) != 0 && errno != ENOENT)
                rc = TSDB_ERR_IO;
            else if (ecswap_mutant == SWAP_MUT_BACKONGONE && on_l[i][1]) { /* the planted bug retires a live .ec */
                snprintf(nm, sizeof nm, "retired_%s", p.l[i].fin);
                rc = renameat(db->dirfd, p.l[i].fin, db->dirfd, nm) != 0 ? TSDB_ERR_IO : 0;
            }
            if (rc)
                fprintf(stderr, "tsdb: rolling settle intent %s back: %s: %s\n", name, p.l[i].tmp, strerror(errno));
        }
    } else if (rc == 0) {
        bool did_l = false;
        for (size_t i = 0; i < p.nl && rc == 0; i++)
            if (on_l[i][0]) {
                if (renameat(db->dirfd, p.l[i].tmp, db->dirfd, p.l[i].fin) != 0) {
                    fprintf(stderr, "tsdb: rolling settle intent %s forward: rename of %s: %s\n", name, p.l[i].tmp,
                            strerror(errno));
                    rc = TSDB_ERR_IO;
                }
                did_l = true;
            }
        if (rc == 0 && p.reencode && did_l && db->opt.fsync && tsdb_fsync(db->dirfd) != 0) {
            fprintf(stderr, "tsdb: rolling re-encode intent %s forward: fsync after L: %s\n", name, strerror(errno));
            rc = TSDB_ERR_IO;
        }
        for (size_t i = 0; i < p.nr && rc == 0 && ecswap_mutant != SWAP_MUT_PARTIAL; i++)
            if (on_r[i][0] && renameat(db->dirfd, p.r[i].src, db->dirfd, p.r[i].dst) != 0) {
                fprintf(stderr, "tsdb: rolling settle intent %s forward: rename of %s: %s\n", name, p.r[i].src,
                        strerror(errno));
                rc = TSDB_ERR_IO;
            }
    }
    free(on_l);
    free(on_r);
    swap_plan_free(&p);
    /* intents_recover unlinks the intent next and syncs the directory only after that: a power cut between may keep
     * the unlink and lose some of these renames (directory operations become durable in any order until a sync), and
     * with no intent left nothing would finish them. So they are made durable here, before the intent can go. */
    if (rc == 0 && db->opt.fsync && tsdb_fsync(db->dirfd) != 0) {
        fprintf(stderr, "tsdb: settle intent %s: fsync of the directory: %s\n", name, strerror(errno));
        rc = TSDB_ERR_IO;
    }
    return rc;
}

/* ------------------------------------------------------------ intent v5: cold apply, reverse unsettle, salvage */

enum { V5_COLD = 0, V5_UNSETTLE = 1, V5_SALVAGE = 2 };

typedef struct {
    char kind; /* N/Y/R (cold apply); L/U (unsettle); X (salvage) */
    char from[128], to[160];
} v5_line;

/* Parses an unsealed v5 intent: 1; 0 when a line breaks the rules (only the names a cold apply, reverse unsettle,
 * or tier salvage writes, one key, never mixed); TSDB_ERR_NOMEM. */
static int v5_parse(char *buf, v5_line **out, size_t *nout, int64_t *key, int *kind_out)
{
    *out = NULL;
    *nout = 0;
    *kind_out = V5_COLD;
    char *line = buf, *next = strchr(line, '\n'), extra;
    size_t n;
    if (!next)
        return 0;
    *next = '\0';
    if (sscanf(line, INTENT_V5_HEAD "%zu%c", &n, &extra) != 1 || n < 1 || n > 1u << 16)
        return 0;
    v5_line *s = calloc(n, sizeof *s);
    if (!s)
        return TSDB_ERR_NOMEM;
    bool rank_mut = ecunsettle_mut() == ECUNSETTLE_MUT_RANK;
    int v5k = V5_COLD;
    size_t got = 0;
    for (line = next + 1; got < n; line = next + 1) {
        next = strchr(line, '\n');
        if (next)
            *next = '\0';
        v5_line *x = &s[got];
        bool good = sscanf(line, "%c %127s %159s%c", &x->kind, x->from, x->to, &extra) == 3;
        int64_t k = 0;
        if (good && got == 0) {
            if (x->kind == 'L' || x->kind == 'U') {
                v5k = V5_UNSETTLE;
                if ((n & 1u) != 0)
                    good = false;
            } else if (x->kind == 'X') {
                v5k = V5_SALVAGE;
            }
        }
        if (good && v5k == V5_UNSETTLE) {
            size_t half = n / 2;
            if (got < half) {
                bool is_tmp = part_tmp_name(x->from);
                bool is_set = settled_part_name(x->from);
                good = x->kind == 'L' && (is_tmp || is_set) && part_file_name(x->to) && name_key(x->to, &k);
                if (good && is_set && !rank_mut)
                    good = strcmp(x->from + 8, x->to) == 0;
                for (size_t j = 0; good && j < got; j++)
                    if (strcmp(s[j].to, x->to) == 0 || strcmp(s[j].from, x->from) == 0)
                        good = false;
            } else {
                size_t idx = got - half;
                char want[sizeof x->to];
                retired_of(x->from, want, sizeof want);
                good = x->kind == 'U' && ec_file_name(x->from) && strcmp(want, x->to) == 0 && name_key(x->from, &k);
                if (good) {
                    if (!rank_mut)
                        good = same_stem(s[idx].to, x->from);
                    else if (settled_part_name(s[idx].from))
                        good = same_stem(s[idx].from + 8, x->from);
                }
                for (size_t j = half; good && j < got; j++)
                    if (strcmp(s[j].from, x->from) == 0)
                        good = false;
            }
        } else if (good && v5k == V5_SALVAGE) {
            char want[sizeof x->to];
            retired_of(x->from, want, sizeof want);
            good = x->kind == 'X' && ec_file_name(x->from) && strcmp(want, x->to) == 0 && name_key(x->from, &k);
            for (size_t j = 0; good && j < got; j++)
                if (strcmp(s[j].from, x->from) == 0)
                    good = false;
        } else if (good && x->kind == 'N')
            good = ec_tmp_name(x->from) && ec_file_name(x->to) && name_key(x->to, &k);
        else if (good && x->kind == 'Y')
            good = part_tmp_name(x->from) && part_file_name(x->to) && name_key(x->to, &k);
        else if (good && x->kind == 'R') {
            char want[sizeof x->to];
            if (part_file_name(x->from)) {
                settled_of(x->from, want, sizeof want);
                good = strcmp(want, x->to) == 0 && name_key(x->from, &k);
            } else if (ec_file_name(x->from)) {
                retired_of(x->from, want, sizeof want);
                good = strcmp(want, x->to) == 0 && name_key(x->from, &k);
            } else {
                good = false;
            }
        } else
            good = false;
        if (good && got == 0)
            *key = k;
        if (!good || k != *key) {
            free(s);
            return 0;
        }
        got++;
        if (!next)
            break;
    }
    if (got != n || next) {
        free(s);
        return 0;
    }
    *out = s;
    *nout = n;
    *kind_out = v5k;
    return 1;
}

/* Recovery of a v5 salvage intent (X lines) at open. A sealed intent always rolls forward: every listed pX.ec still
 * on disk is renamed to retired_pX.ec; parts already renamed (or already GC'd) are left alone and never resurrected.
 * Both pX.ec and retired_pX.ec present simultaneously is damage (TSDB_ERR_CORRUPT). Directory is synced before
 * returning so the intent is unlinked only after all renames are durable. */
static int salvage_recover(eng *db, const char *name, const v5_line *s, size_t n)
{
    unsigned char (*on)[2] = calloc(n, sizeof *on); /* 0: from (pX.ec), 1: to (retired_pX.ec) */
    if (!on)
        return TSDB_ERR_NOMEM;
    int rc = 0;
    for (size_t i = 0; i < n && rc == 0; i++) {
        int r0 = name_at(db, name, s[i].from);
        int r1 = r0 < 0 ? r0 : name_at(db, name, s[i].to);
        if (r0 < 0 || r1 < 0) {
            rc = TSDB_ERR_IO;
            break;
        }
        if (r0 == 1 && r1 == 1) {
            fprintf(stderr, "tsdb: salvage intent %s: both %s and %s exist on disk: damage, the open fails\n", name,
                    s[i].from, s[i].to);
            rc = TSDB_ERR_CORRUPT;
            break;
        }
        on[i][0] = (unsigned char)r0;
        on[i][1] = (unsigned char)r1;
    }
    size_t renamed_now = 0, already = 0, gone = 0;
    for (size_t i = 0; i < n && rc == 0; i++) {
        if (on[i][0]) {
            if (renameat(db->dirfd, s[i].from, db->dirfd, s[i].to) != 0) {
                fprintf(stderr, "tsdb: rolling salvage intent %s forward: rename of %s: %s\n", name, s[i].from,
                        strerror(errno));
                rc = TSDB_ERR_IO;
            } else {
                renamed_now++;
            }
        } else if (on[i][1]) {
            already++;
        } else {
            gone++;
        }
    }
    free(on);
    if (rc == 0) {
        fprintf(stderr,
                "tsdb: salvage intent %s rolled forward: %zu retired now, %zu already retired, %zu deleted\n", name,
                renamed_now, already, gone);
        if (db->opt.fsync && tsdb_fsync(db->dirfd) != 0) {
            fprintf(stderr, "tsdb: salvage intent %s: fsync of the directory: %s\n", name, strerror(errno));
            rc = TSDB_ERR_IO;
        }
    }
    return rc;
}

/* Recovery of a v5 reverse (unsettle) intent at open (before load_parts removes temporaries).
 * The swap checked every P.ec is listed under db_pub_begin (blocking drops until after the post-rename dir fsync),
 * wrote and synced the intent, then renamed all L (src -> P.part) and all U (P.ec -> retired_P.ec) under db->lock.
 * Because drops are blocked from the pre-check through the post-rename directory fsync, a sealed intent means no drop
 * retired P.ec before this swap. So:
 *   - swap started (`renamed`): any P.part on disk, OR any retired_P.part whose P.ec is gone (a stale leftover
 *     retired_P.part beside a live P.ec does NOT count), OR any retired_P.ec with P.ec gone and L.from still present
 *     (filesystem reordered U before L before the post-rename dir fsync). Roll forward (remaining L -> dir fsync -> U).
 *     An L part missing under all its names while its P.ec is still live is damage (TSDB_ERR_CORRUPT).
 *   - nothing renamed, every L.from and every U.from (P.ec) present: cut before renames -> roll forward.
 *   - nothing renamed, a source or P.ec missing (e.g. settled_ purged before renames, or already dropped+cleaned):
 *     roll back — unlink ONLY tmp_*.part (NEVER settled_*.part!).
 * The directory is synced before returning so the intent is unlinked only after its renames are durable. */
static int unsettle_recover(eng *db, const char *name, const v5_line *s, size_t n)
{
    size_t m = n / 2;
    const v5_line *ls = s, *us = s + m;
    unsigned char (*on_l)[3] = calloc(m, sizeof *on_l); /* 0: from, 1: to (P.part), 2: retired_<P.part> */
    unsigned char (*on_u)[2] = calloc(m, sizeof *on_u); /* 0: from (P.ec), 1: to (retired_P.ec) */
    if (!on_l || !on_u) {
        free(on_l);
        free(on_u);
        return TSDB_ERR_NOMEM;
    }
    int rc = 0;
    bool renamed = false, l_from_missing = false, u_src_missing = false;
    char nm[200];
    for (size_t i = 0; i < m && rc == 0; i++) {
        int r0 = name_at(db, name, ls[i].from);
        int r1 = r0 < 0 ? r0 : name_at(db, name, ls[i].to);
        snprintf(nm, sizeof nm, "retired_%s", ls[i].to);
        int r2 = r1 < 0 ? r1 : name_at(db, name, nm);
        if (r0 < 0 || r1 < 0 || r2 < 0) {
            rc = TSDB_ERR_IO;
            break;
        }
        on_l[i][0] = (unsigned char)r0, on_l[i][1] = (unsigned char)r1, on_l[i][2] = (unsigned char)r2;
        l_from_missing |= !r0;
    }
    for (size_t i = 0; i < m && rc == 0; i++) {
        int r0 = name_at(db, name, us[i].from);
        int r1 = r0 < 0 ? r0 : name_at(db, name, us[i].to);
        if (r0 < 0 || r1 < 0) {
            rc = TSDB_ERR_IO;
            break;
        }
        on_u[i][0] = (unsigned char)r0, on_u[i][1] = (unsigned char)r1;
        u_src_missing |= !r0;
    }
    for (size_t i = 0; i < m && rc == 0; i++) {
        /* P.part present; or retired_P.part present with P.ec gone (not a stale leftover retired_P.part beside a live
         * P.ec); or U persisted before L (retired_P.ec present, P.ec gone, L.from still on disk). */
        if (on_l[i][1] || (on_l[i][2] && !on_u[i][0]) || (on_u[i][1] && !on_u[i][0] && on_l[i][0]))
            renamed = true;
    }
    if (rc == 0 && renamed) {
        for (size_t i = 0; i < m && rc == 0; i++) {
            if (!on_l[i][0] && !on_l[i][1] && !on_l[i][2] && on_u[i][0]) {
                fprintf(stderr,
                        "tsdb: unsettle intent %s: %s is under none of its names while %s is still live: damage, "
                        "the open fails\n",
                        name, ls[i].to, us[i].from);
                rc = TSDB_ERR_CORRUPT;
            }
        }
    }
    if (rc == 0 && !renamed && (l_from_missing || u_src_missing)) {
        fprintf(stderr, "tsdb: unsettle intent %s: nothing was renamed and %s: it is rolled back\n", name,
                u_src_missing ? "an .ec source is gone" : "an unsettle source is gone");
        for (size_t i = 0; i < m && rc == 0; i++) {
            /* Never unlink settled_*.part on rollback: it is a kept source copy, not a temporary. */
            if (part_tmp_name(ls[i].from) && on_l[i][0] && unlinkat(db->dirfd, ls[i].from, 0) != 0 && errno != ENOENT) {
                fprintf(stderr, "tsdb: rolling unsettle intent %s back: %s: %s\n", name, ls[i].from, strerror(errno));
                rc = TSDB_ERR_IO;
            }
        }
    } else if (rc == 0) {
        bool did_l = false;
        for (size_t i = 0; i < m && rc == 0; i++) {
            if (on_l[i][0] && !on_l[i][1]) {
                if (renameat(db->dirfd, ls[i].from, db->dirfd, ls[i].to) != 0) {
                    fprintf(stderr, "tsdb: rolling unsettle intent %s forward: rename of %s: %s\n", name, ls[i].from,
                            strerror(errno));
                    rc = TSDB_ERR_IO;
                }
                did_l = true;
            }
        }
        if (rc == 0 && did_l && db->opt.fsync && tsdb_fsync(db->dirfd) != 0) {
            fprintf(stderr, "tsdb: rolling unsettle intent %s forward: fsync after L: %s\n", name, strerror(errno));
            rc = TSDB_ERR_IO;
        }
        for (size_t i = 0; i < m && rc == 0; i++) {
            if (on_u[i][0] && renameat(db->dirfd, us[i].from, db->dirfd, us[i].to) != 0) {
                fprintf(stderr, "tsdb: rolling unsettle intent %s forward: rename of %s: %s\n", name, us[i].from,
                        strerror(errno));
                rc = TSDB_ERR_IO;
            }
        }
    }
    free(on_l);
    free(on_u);
    if (rc == 0 && db->opt.fsync && tsdb_fsync(db->dirfd) != 0) {
        fprintf(stderr, "tsdb: unsettle intent %s: fsync of the directory: %s\n", name, strerror(errno));
        rc = TSDB_ERR_IO;
    }
    return rc;
}

/* Recovery of a v5 intent at open (before load_parts removes the temporaries). The apply wrote it with db->lock held
 * from its check (every R source listed, none above Z, the key not dropped) through its renames (N, then Y, then R),
 * so the rules are R7's, with Y beside N:
 *   - torn: no rename followed it; the temporaries go with load_parts.
 *   - complete, any N or Y name (or retired_ of it) or settled_ name on disk: the apply renamed, it stands: every
 *     remaining tmp to its name, every R source still there to settled_. A name under none of its names counts as
 *     done (a drop after the apply retired it, its file deleted at its last reference).
 *   - complete, nothing renamed, every tmp and every R source there: cut before its renames: roll forward.
 *   - complete, nothing renamed, a tmp or a source gone: no flow loses one while the apply holds db->lock (a drop or
 *     merge of the key waits for it), so this is damage: the tmps go (rolled back: the own parts stay listed) and,
 *     when a source is gone, the volume is DIVERGED (ec_diverge, durable): this member no longer holds the rows the
 *     COLD replaced. RSTATE's `doing` then makes the replay decide (nothing listed) and apply the COLD again.
 *   Damage that fails the open (the intent kept): a sealed intent whose lines break the rules, a lookup that fails
 *   other than ENOENT. The directory is synced before returning: the intent is unlinked only after its renames.
 *   Reverse (unsettle) v5 intents (L and U lines) are dispatched to unsettle_recover above. */
int ecapply_recover(eng *db, const char *name, char *buf, size_t len)
{
    if (!intent_unseal(buf, len)) {
        fprintf(stderr, "tsdb: v5 intent %s is torn: rolled back\n", name);
        return 0;
    }
    v5_line *s = NULL;
    size_t n = 0;
    int64_t key = 0;
    int v5k = V5_COLD;
    int rc = v5_parse(buf, &s, &n, &key, &v5k);
    if (rc <= 0) {
        if (rc == 0)
            fprintf(stderr, "tsdb: intent %s: its checksum holds but a line breaks the rules of a v5 intent's "
                            "names: damage, the open fails\n", name);
        return rc < 0 ? rc : TSDB_ERR_CORRUPT;
    }
    if (v5k == V5_UNSETTLE) {
        rc = unsettle_recover(db, name, s, n);
        free(s);
        return rc;
    }
    if (v5k == V5_SALVAGE) {
        rc = salvage_recover(db, name, s, n);
        free(s);
        return rc;
    }
    rc = 0;
    unsigned char (*on)[3] = calloc(n, sizeof *on); /* 0: from, 1: to, 2: retired_ of to (N, Y) */
    if (!on) {
        free(s);
        return TSDB_ERR_NOMEM;
    }
    bool renamed = false, from_missing = false, src_missing = false;
    char nm[200];
    for (size_t i = 0; i < n && rc == 0; i++) {
        int r0 = name_at(db, name, s[i].from), r1 = r0 < 0 ? r0 : name_at(db, name, s[i].to), r2 = 0;
        if (r1 >= 0 && s[i].kind != 'R') {
            snprintf(nm, sizeof nm, "retired_%s", s[i].to);
            r2 = name_at(db, name, nm);
        }
        if (r0 < 0 || r1 < 0 || r2 < 0) {
            rc = TSDB_ERR_IO;
            break;
        }
        on[i][0] = (unsigned char)r0, on[i][1] = (unsigned char)r1, on[i][2] = (unsigned char)r2;
        renamed |= r1 || r2;
        from_missing |= !r0;
        src_missing |= s[i].kind == 'R' && !r0 && !r1;
    }
    if (rc == 0 && !renamed && from_missing && ecapply_mutant != ECAPPLY_MUT_RECROLLMISSING) {
        fprintf(stderr, "tsdb: cold apply intent %s: nothing of the apply was renamed and %s: it is rolled back%s\n",
                name, src_missing ? "an own part is gone" : "a temporary is gone",
                src_missing ? ", the volume is DIVERGED" : "");
        for (size_t i = 0; i < n && rc == 0; i++)
            if (s[i].kind != 'R' && on[i][0] && unlinkat(db->dirfd, s[i].from, 0) != 0 && errno != ENOENT)
                rc = TSDB_ERR_IO;
        if (rc == 0 && src_missing && ecapply_mutant != ECAPPLY_MUT_RECNODIVERGE)
            rc = ec_diverge(db, key, NULL, EC_DIV_PPOS);
    } else if (rc == 0) {
        for (size_t i = 0; i < n && rc == 0; i++)
            if (on[i][0] && renameat(db->dirfd, s[i].from, db->dirfd, s[i].to) != 0) {
                fprintf(stderr, "tsdb: rolling cold apply intent %s forward: rename of %s: %s\n", name, s[i].from,
                        strerror(errno));
                rc = TSDB_ERR_IO;
            }
    }
    free(on);
    free(s);
    if (rc == 0 && db->opt.fsync && ecapply_mutant != ECAPPLY_MUT_RECNODIRSYNC && tsdb_fsync(db->dirfd) != 0) {
        fprintf(stderr, "tsdb: cold apply intent %s: fsync of the directory: %s\n", name, strerror(errno));
        rc = TSDB_ERR_IO;
    }
    return rc;
}

int ecswap_eng_open(eng *db)
{
    (void)db;
    EC_CALLED(EC_CALL_ENG_OPEN);
    return 0;
}

void ecswap_eng_close(eng *db)
{
    (void)db;
    EC_CALLED(EC_CALL_ENG_CLOSE);
}
