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

void ecswap_crash_point(int which) { settle_crash(which == ECSWAP_CRASH_AFTER_PUT ? SC_AFTER_PUT : SC_AFTER_EC_TMP); }

/* ------------------------------------------------------------ names */

/* settled_<name> of a source */
static void settled_of(const char *src, char *out, size_t cap) { snprintf(out, cap, "settled_%s", src); }

/* the partition key of p<K>_..., as written in a name */
static bool name_key(const char *name, int64_t *key)
{
    return sscanf(name, "p%" SCNd64 "_", key) == 1;
}

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
    pthread_once(&sm_once, sm_read);
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

/* ------------------------------------------------------------ recovery */

typedef struct {
    char tmp[96], fin[128];    /* L */
    char src[128], dst[160];   /* R */
} swap_line;

/* Parses an unsealed v4 intent into its n pairs: 1; 0 when a line breaks the rules (a crafted intent must not
 * rename anything but the names a swap writes); TSDB_ERR_NOMEM. */
static int swap_parse(char *buf, swap_line **out, size_t *nout)
{
    *out = NULL;
    *nout = 0;
    char *line = buf, *next = strchr(line, '\n'), extra;
    size_t n;
    if (!next)
        return 0;
    *next = '\0';
    if (sscanf(line, INTENT_V4_HEAD "%zu%c", &n, &extra) != 1 || n < 1 || n > 1u << 16)
        return 0;
    swap_line *s = calloc(n, sizeof *s);
    if (!s)
        return TSDB_ERR_NOMEM;
    size_t got = 0;
    int64_t key = 0;
    for (line = next + 1; got < 2 * n; line = next + 1) {
        next = strchr(line, '\n');
        if (next)
            *next = '\0';
        size_t i = got % n;
        bool good;
        if (got < n)
            good = sscanf(line, "L %95s %127s%c", s[i].tmp, s[i].fin, &extra) == 2 && ec_tmp_name(s[i].tmp) &&
                   ec_file_name(s[i].fin);
        else
            good = sscanf(line, "R %127s %159s%c", s[i].src, s[i].dst, &extra) == 2 && part_file_name(s[i].src);
        if (good && got >= n) {
            char want[sizeof s[i].dst];
            settled_of(s[i].src, want, sizeof want);
            int64_t k;
            good = strcmp(want, s[i].dst) == 0 && same_stem(s[i].src, s[i].fin) && name_key(s[i].src, &k) &&
                   (i == 0 ? (key = k, true) : k == key);
        }
        if (!good) {
            free(s);
            return 0;
        }
        got++;
        if (!next)
            break;
    }
    if (got != 2 * n || next) {
        free(s);
        return 0;
    }
    *out = s;
    *nout = n;
    return 1;
}

enum { N_TMP, N_FIN, N_RFIN, N_SRC, N_DST, N_RSRC, N_K };

int ecswap_recover(eng *db, const char *name, char *buf, size_t len)
{
    EC_CALLED(EC_CALL_SWAP_RECOVER);
    if (!intent_unseal(buf, len)) {
        /* no rename follows an intent before it and the directory are synced: the parts stay, and load_parts
         * removes the tmp .ec files with the other temporaries */
        fprintf(stderr, "tsdb: settle intent %s is torn: the swap is rolled back\n", name);
        return 0;
    }
    swap_line *s = NULL;
    size_t n = 0;
    int rc = swap_parse(buf, &s, &n);
    if (rc <= 0) {
        if (rc == 0)
            fprintf(stderr, "tsdb: settle intent %s: its checksum holds but a line breaks the rules of a swap's "
                            "names: damage, the open fails\n", name);
        return rc < 0 ? rc : TSDB_ERR_CORRUPT;
    }
    rc = 0;
    unsigned char (*on)[N_K] = calloc(n, sizeof *on);
    if (!on) {
        free(s);
        return TSDB_ERR_NOMEM;
    }
    bool renamed = false, src_missing = false, tmp_missing = false;
    char nm[N_K][200];
    for (size_t i = 0; i < n && rc == 0; i++) {
        snprintf(nm[N_TMP], sizeof nm[0], "%s", s[i].tmp);
        snprintf(nm[N_FIN], sizeof nm[0], "%s", s[i].fin);
        snprintf(nm[N_RFIN], sizeof nm[0], "retired_%s", s[i].fin);
        snprintf(nm[N_SRC], sizeof nm[0], "%s", s[i].src);
        snprintf(nm[N_DST], sizeof nm[0], "%s", s[i].dst);
        snprintf(nm[N_RSRC], sizeof nm[0], "retired_%s", s[i].src);
        for (int k = 0; k < N_K && rc == 0; k++) {
            int r = name_at(db, name, nm[k]);
            if (r < 0)
                rc = r;
            else
                on[i][k] = (unsigned char)r;
        }
        renamed |= on[i][N_FIN] || on[i][N_RFIN] || on[i][N_DST];
        src_missing |= !on[i][N_SRC] && !on[i][N_DST];
        tmp_missing |= !on[i][N_TMP];
    }
    if (rc == 0 && ecswap_mutant == SWAP_MUT_BACKONGONE && src_missing)
        renamed = false; /* the planted bug: a missing source read as a drop of the key, whatever was renamed */
    for (size_t i = 0; i < n && rc == 0 && renamed; i++) {
        const char *why = NULL;
        /* retired_P.part with settled_P.part beside it is an older file of the same name, not this source: the open
         * retires a merge output a crash left unlisted when its sources are still there (load_parts' cover rule),
         * the merge runs again and names the same P, and the retired_ copy waits out obsolete_part_lifetime_s */
        if (on[i][N_RSRC] && !on[i][N_SRC] && !on[i][N_DST])
            why = "a source is under retired_ although the swap renamed (a drop before its check renames nothing)";
        else if (!on[i][N_TMP] && !on[i][N_FIN] && !on[i][N_RFIN] &&
                 (on[i][N_SRC] || ecswap_mutant == SWAP_MUT_LOSTSETTLED))
            why = "an .ec part is under none of its names although the swap renamed";
        if (why) {
            fprintf(stderr, "tsdb: settle intent %s: %s (%s): damage, the open fails\n", name, why, s[i].fin);
            rc = TSDB_ERR_CORRUPT;
        }
    }
    if (rc == 0 && !renamed && (src_missing || tmp_missing)) {
        fprintf(stderr, "tsdb: settle intent %s: nothing of the swap was renamed and %s: it is rolled back\n", name,
                src_missing ? "a source is gone" : "a tmp .ec is gone");
        for (size_t i = 0; i < n && rc == 0; i++) {
            if (on[i][N_TMP] && unlinkat(db->dirfd, s[i].tmp, 0) != 0 && errno != ENOENT)
                rc = TSDB_ERR_IO;
            else if (ecswap_mutant == SWAP_MUT_BACKONGONE && on[i][N_FIN]) { /* the planted bug retires a live .ec */
                snprintf(nm[0], sizeof nm[0], "retired_%s", s[i].fin);
                rc = renameat(db->dirfd, s[i].fin, db->dirfd, nm[0]) != 0 ? TSDB_ERR_IO : 0;
            }
            if (rc)
                fprintf(stderr, "tsdb: rolling settle intent %s back: %s: %s\n", name, s[i].tmp, strerror(errno));
        }
    } else if (rc == 0) {
        for (size_t i = 0; i < n && rc == 0; i++)
            if (on[i][N_TMP] && renameat(db->dirfd, s[i].tmp, db->dirfd, s[i].fin) != 0) {
                fprintf(stderr, "tsdb: rolling settle intent %s forward: rename of %s: %s\n", name, s[i].tmp,
                        strerror(errno));
                rc = TSDB_ERR_IO;
            }
        for (size_t i = 0; i < n && rc == 0 && ecswap_mutant != SWAP_MUT_PARTIAL; i++)
            if (on[i][N_SRC] && renameat(db->dirfd, s[i].src, db->dirfd, s[i].dst) != 0) {
                fprintf(stderr, "tsdb: rolling settle intent %s forward: rename of %s: %s\n", name, s[i].src,
                        strerror(errno));
                rc = TSDB_ERR_IO;
            }
    }
    free(on);
    free(s);
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
    if (rc == 0 && !renamed && from_missing) {
        fprintf(stderr, "tsdb: cold apply intent %s: nothing of the apply was renamed and %s: it is rolled back%s\n",
                name, src_missing ? "an own part is gone" : "a temporary is gone",
                src_missing ? ", the volume is DIVERGED" : "");
        for (size_t i = 0; i < n && rc == 0; i++)
            if (s[i].kind != 'R' && on[i][0] && unlinkat(db->dirfd, s[i].from, 0) != 0 && errno != ENOENT)
                rc = TSDB_ERR_IO;
        if (rc == 0 && src_missing)
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
    if (rc == 0 && db->opt.fsync && tsdb_fsync(db->dirfd) != 0) {
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
