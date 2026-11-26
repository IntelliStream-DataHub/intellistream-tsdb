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
 * Intent v5 (R8): a replica's apply of MK_COLD (src/ecapply.c) writes "tsdb-intent 5 <n>", lines "N <tmp_*.ec> <.ec>"
 * (its objects' .ec files under fresh block numbers), "Y <tmp_0_*.part> <part>" (the late parts above them),
 * "R <own part> <settled_own part>" (its own parts of the key at or below Z), and later R10's "X" lines (refused
 * until then), "end <xxh64>". Its rules are written above ecapply_recover.
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

enum { SC_AFTER_INTENT, SC_BETWEEN_RENAMES, SC_BEFORE_UNLINK, SC_AFTER_PUT, SC_AFTER_EC_TMP, SC_N };
#ifdef TSDB_TEST_HOOKS
static const char *const sc_names[SC_N] = {"after-intent", "between-renames", "before-unlink", "after-put",
                                           "after-ec-tmp"};
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

void ecswap_crash_point(int which) { settle_crash(which == ECSWAP_CRASH_AFTER_PUT ? SC_AFTER_PUT : SC_AFTER_EC_TMP); }
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

static int intent_write(eng *db, const char *intent, part **in, part **ec, size_t n)
{
    size_t cap = 64, len = 0; /* the lines' own lengths: no bound on a name's length is assumed */
    for (size_t i = 0; i < n; i++)
        cap += 16 + strlen(ec[i]->pending) + strlen(ec[i]->name) + 2 * strlen(in[i]->name);
    int fail = atomic_load(&ecswap_test_fail); /* tests: one planted failure (ecswap.h) */
    if (fail == ECSWAP_FAIL_INTENT_NOMEM || fail == ECSWAP_FAIL_INTENT_IO)
        atomic_store(&ecswap_test_fail, ECSWAP_FAIL_NONE);
    char *buf = fail == ECSWAP_FAIL_INTENT_NOMEM ? NULL : malloc(cap);
    if (!buf)
        return TSDB_ERR_NOMEM;
    len += (size_t)snprintf(buf + len, cap - len, INTENT_V4_HEAD "%zu\n", n);
    for (size_t i = 0; i < n; i++)
        len += (size_t)snprintf(buf + len, cap - len, "L %s %s\n", ec[i]->pending, ec[i]->name);
    for (size_t i = 0; i < n; i++)
        len += (size_t)snprintf(buf + len, cap - len, "R %s settled_%s\n", in[i]->name, in[i]->name);
    len += (size_t)snprintf(buf + len, cap - len, "end %016" PRIx64 "\n", xxh64(buf, len, 0));
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
        if (!p || !e || p == e || !part_file_name(p->name) || !ec_tmp_name(e->pending) || !ec_file_name(e->name) ||
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
    if (rc == 0 && !early && (rc = intent_write(db, intent, in, ec, n)) != 0) {
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
            ec[i]->pending[0] = '\0';
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
        pthread_mutex_unlock(&db->publish_lock);
        return swap_refuse_logged(db, "a source was dropped, merged or unlisted before its renames");
    }
    if (!ok) {
        rc = eng_check(db) ? TSDB_ERR_IO : TSDB_ERR_STOPPED;
        if (!early)
            swap_discard(db, intent, ec, n);
        pthread_mutex_unlock(&db->publish_lock);
        return rc;
    }
    if (!renamed) { /* fail-stop: names may be half visible; the next open finishes or undoes the swap */
        pthread_mutex_unlock(&db->publish_lock);
        return TSDB_ERR_IO;
    }
    if (early && (rc = intent_write(db, intent, in, ec, n)) != 0) { /* the planted bug: the intent after the fact */
        pthread_mutex_unlock(&db->publish_lock);
        return rc;
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
        if (!ec_tmp_name(out->pending) || !ec_file_name(out->name))
            return false;
    } else {
        if (out->ng != 0 || !part_tmp_name(out->pending) || !part_file_name(out->name))
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

static int intent_write_reencode(eng *db, const char *intent, part **in, size_t nin, part *out)
{
    size_t cap = 96 + strlen(out->pending) + strlen(out->name), len = 0;
    for (size_t i = 0; i < nin; i++)
        cap += 20 + 2 * strlen(in[i]->name);
    int fail = atomic_load(&ecswap_test_fail);
    if (fail == ECSWAP_FAIL_INTENT_NOMEM || fail == ECSWAP_FAIL_INTENT_IO)
        atomic_store(&ecswap_test_fail, ECSWAP_FAIL_NONE);
    char *buf = fail == ECSWAP_FAIL_INTENT_NOMEM ? NULL : malloc(cap);
    if (!buf)
        return TSDB_ERR_NOMEM;
    len += (size_t)snprintf(buf + len, cap - len, INTENT_V4_HEAD "1 %zu\n", nin);
    len += (size_t)snprintf(buf + len, cap - len, "L %s %s\n", out->pending, out->name);
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
    part **pin_parts = calloc(nin, sizeof *pin_parts);
    if (!pin_parts)
        return TSDB_ERR_NOMEM;
    char intent[64];
    snprintf(intent, sizeof intent, "txn_%d_%u.intent", (int)getpid(), atomic_fetch_add(&db->tmp_counter, 1));

    pthread_mutex_lock(&db->publish_lock);
    int rc = eng_check(db) ? TSDB_ERR_IO : 0;
    bool early = ecswap_mutant == SWAP_MUT_EARLY;
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
        rc = intent_write_reencode(db, intent, in, nin, out);
        if (rc == 0) {
            wrote_intent = true;
            if (ecswap_mutant != SWAP_MUT_NODIRSYNC &&
                eng_sync_dir(db, "fsync of the directory after writing a re-encode's intent") != 0)
                rc = TSDB_ERR_IO;
        }
        if (rc == 0)
            settle_crash(SC_AFTER_INTENT);
    }
    if (ok && rc == 0)
        renamed = swap_renames_reencode(db, in, nin, out);
    size_t npart_src = 0;
    if (renamed) {
        lt_merge_marks(db, &lsec, in, nin);
        for (size_t i = 0; i < nin; i++) {
            if (atomic_load(&in[i]->ec)) {
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
        out->pending[0] = '\0';
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

    if (!ok) {
        rc = eng_check(db) ? TSDB_ERR_IO : TSDB_ERR_STOPPED;
        swap_discard_reencode(db, out);
        pthread_mutex_unlock(&db->publish_lock);
        free(pin_parts);
        return rc;
    }
    if (rc != 0) {
        if (!wrote_intent && rc != TSDB_ERR_NOMEM)
            swap_discard_reencode(db, out);
        pthread_mutex_unlock(&db->publish_lock);
        free(pin_parts);
        return rc;
    }
    if (!renamed) {
        pthread_mutex_unlock(&db->publish_lock);
        free(pin_parts);
        return TSDB_ERR_IO;
    }
    if (early && (rc = intent_write_reencode(db, intent, in, nin, out)) != 0) {
        pthread_mutex_unlock(&db->publish_lock);
        free(pin_parts);
        return rc;
    }
    if ((rc = eng_sync_dir(db, "fsync of the directory after swapping re-encoded parts")) != 0) {
        pthread_mutex_unlock(&db->publish_lock);
        free(pin_parts);
        return rc;
    }
    settle_crash(SC_BEFORE_UNLINK);
    if (unlinkat(db->dirfd, intent, 0) != 0)
        fprintf(stderr, "tsdb: unlink of re-encode intent %s after its swap: %s (the next open rolls it forward)\n",
                intent, strerror(errno));
    pthread_mutex_unlock(&db->publish_lock);
    /* Re-encoded .part sources have no same-stem .ec, so eckeep's digest thread marks them ECKEEP_ORPHAN and releases
     * them after keep_s; eng_reencode already verified ec_logical_digest_frozen before swapping. */
    if (npart_src)
        eckeep_swapped(db, pin_parts, npart_src);
    free(pin_parts);
    int mrc = ec_swap_publish(db);
    return ecswap_mutant == SWAP_MUT_MANIFESTRC ? mrc : 0;
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

/* ------------------------------------------------------------ intent v5: a replica's cold apply (src/ecapply.c) */

typedef struct {
    char kind;          /* N (an .ec part), Y (a late part), R (an own part retired) */
    char from[128], to[160];
} v5_line;

/* Parses an unsealed v5 intent: 1; 0 when a line breaks the rules (only the names an apply writes, one key);
 * TSDB_ERR_NOMEM */
static int v5_parse(char *buf, v5_line **out, size_t *nout, int64_t *key)
{
    *out = NULL;
    *nout = 0;
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
    size_t got = 0;
    for (line = next + 1; got < n; line = next + 1) {
        next = strchr(line, '\n');
        if (next)
            *next = '\0';
        v5_line *x = &s[got];
        bool good = sscanf(line, "%c %127s %159s%c", &x->kind, x->from, x->to, &extra) == 3;
        int64_t k = 0;
        if (good && x->kind == 'N')
            good = ec_tmp_name(x->from) && ec_file_name(x->to) && name_key(x->to, &k);
        else if (good && x->kind == 'Y')
            good = part_tmp_name(x->from) && part_file_name(x->to) && name_key(x->to, &k);
        else if (good && x->kind == 'R') {
            char want[sizeof x->to];
            settled_of(x->from, want, sizeof want);
            good = part_file_name(x->from) && strcmp(want, x->to) == 0 && name_key(x->from, &k);
        } else
            good = false; /* X (R10's retired .ec parts) is not written in R8 */
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
    return 1;
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
 *   other than ENOENT. The directory is synced before returning: the intent is unlinked only after its renames. */
int ecapply_recover(eng *db, const char *name, char *buf, size_t len)
{
    if (!intent_unseal(buf, len)) {
        fprintf(stderr, "tsdb: cold apply intent %s is torn: the apply is rolled back\n", name);
        return 0;
    }
    v5_line *s = NULL;
    size_t n = 0;
    int64_t key = 0;
    int rc = v5_parse(buf, &s, &n, &key);
    if (rc <= 0) {
        if (rc == 0)
            fprintf(stderr, "tsdb: cold apply intent %s: its checksum holds but a line breaks the rules of an apply's "
                            "names: damage, the open fails\n", name);
        return rc < 0 ? rc : TSDB_ERR_CORRUPT;
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
