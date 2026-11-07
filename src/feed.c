/*
 * The engine's side of the change feed's files (feed.h, feedfile.c): what an open does with them, and the live
 * enable and disable. docs/design-cdc.md 2.2, 2.13.
 *
 * Who decides whether a volume has the feed: a database is on exactly when its root has FEED (the root's FEED is the
 * commit point of an enable or a disable of the whole database). The root judges by its own files; every other
 * volume is brought to the root's state by its writer's open, before its WAL replay (the caller passes `want`, 1 or
 * 0; volumes.c). So a crash in the middle of an enable or disable across volumes is finished by the next open, in
 * either direction, and a volume added beside a running writer (its META copies the root's requires=) is repaired by
 * the open that follows. A reader or follower judges by its own files and writes nothing.
 *
 * Inside one volume the order is: the word in META, FEEDSLOTS, FEED (enable); FEED, then the rest, then the word
 * (disable), each step with its directory fsync. A volume with the word but no FEED is a half-made or half-removed
 * feed: the open finishes it as the root says. FEED without the word cannot happen and fails the open when nothing
 * says what to do.
 */
#include "internal.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>

static __thread int open_want = -1;
static __thread feed_opts open_opts;

void eng_set_open_feed_want(int want, const feed_opts *o)
{
    open_want = want;
    memset(&open_opts, 0, sizeof open_opts);
    if (o)
        open_opts = *o;
}

/* The first thing an open does, before any step that can fail: a want left set by an open that failed early would
 * be obeyed by the next open on this thread, whichever database that is. */
int eng_take_open_feed_want(feed_opts *o)
{
    int w = open_want;
    open_want = -1;
    *o = open_opts;
    memset(&open_opts, 0, sizeof open_opts);
    return w;
}

static void feed_fail_log(eng *db, const char *what, int rc)
{
    fprintf(stderr, "tsdb: %s: the change feed: %s failed (%d)\n", db->dir, what, rc);
}

int feed_open_reconcile(eng *db, int want, const feed_opts *wanted)
{
    feed_state st;
    int rc = feed_dir_state(db->dirfd, &st);
    if (rc)
        return rc;
    if (db->opt.read_only) { /* judged by its own files; a reader writes nothing */
        if (st.feed && st.word) {
            if ((rc = feed_cfg_read(db->dirfd, &db->feed)) != 0) {
                feed_fail_log(db, "reading FEED", rc);
                return TSDB_ERR_CORRUPT;
            }
            fslots fs;
            if ((rc = fslots_read(db->dirfd, &fs)) != 0) { /* the floor, for `feed info` and F2's reader */
                feed_fail_log(db, "reading FEEDSLOTS", rc);
                return rc == TSDB_ERR_NOTFOUND ? TSDB_ERR_CORRUPT : rc;
            }
            db->feed_floor_event = fs.floor_event;
            db->feed_floor_check = fs.floor_check;
            atomic_store(&db->feed_on, true);
        }
        return 0;
    }
    feed_dir fd = {db->dirfd, NULL, db->opt.fsync};
    if (st.feed && !st.word && want < 0) {
        fprintf(stderr, "tsdb: %s has a FEED file but META does not say requires=feed: that cannot come from this "
                        "release's enable or disable; the open fails (restore META, or remove FEED and FEEDSLOTS)\n",
                db->dir);
        return TSDB_ERR_CORRUPT;
    }
    bool target = want >= 0 ? want == 1 : st.feed && st.word;
    if (!target) {
        if (st.word || st.feed || st.feedslots || st.junk) {
            fprintf(stderr, "tsdb: %s: finishing a disable of the change feed that did not complete\n", db->dir);
            if ((rc = feed_dir_disable(&fd)) != 0) {
                feed_fail_log(db, "finishing the disable", rc);
                return rc;
            }
        }
        return 0;
    }
    if (!db->wal_enabled) {
        fprintf(stderr, "tsdb: %s has the change feed on, which needs the WAL: wal_sync_ms = -1 is refused\n", db->dir);
        return TSDB_ERR_INVALID;
    }
    if (st.feed && st.word) {
        if ((rc = feed_cfg_read(db->dirfd, &db->feed)) != 0) {
            fprintf(stderr, "tsdb: %s/FEED is damaged: the open fails\n", db->dir);
            return rc == TSDB_ERR_NOTFOUND ? TSDB_ERR_CORRUPT : rc;
        }
        if ((rc = fslots_open(db)) != 0) {
            if (rc == TSDB_ERR_NOTFOUND)
                fprintf(stderr, "tsdb: %s has FEED but no FEEDSLOTS: the open fails\n", db->dir);
            return rc == TSDB_ERR_NOTFOUND ? TSDB_ERR_CORRUPT : rc;
        }
        atomic_store(&db->feed_on, true);
        return 0;
    }
    /* the database has it on and this volume does not yet: made once the volume's numbers are known (feed_open_numbers),
     * before its WAL replay, with a fresh feed_id (nobody holds a cursor of it) */
    db->feed_enable_pending = true;
    db->feed_new_opts = *wanted;
    return 0;
}

int feed_open_numbers(eng *db)
{
    if (!db->feed_enable_pending && !(atomic_load(&db->feed_on) && db->fno_torn_open))
        return 0;
    feed_dir fd = {db->dirfd, NULL, db->opt.fsync};
    int rc;
    if (db->feed_enable_pending) {
        feed_cfg cfg;
        feed_cfg_defaults(&cfg, &db->feed_new_opts);
        /* nothing before the open's bound is retained: those segments are replayed (and with the feed on kept as
         * fwal_) below the floor, and the cleaner removes them at once */
        if ((rc = feed_dir_enable(&fd, &cfg, db->fno_next - 1)) != 0 ||
            (rc = feed_cfg_read(db->dirfd, &db->feed)) != 0 || (rc = fslots_open(db)) != 0) {
            feed_fail_log(db, "enabling the feed at the open", rc);
            return rc;
        }
        db->feed_enable_pending = false;
        atomic_store(&db->feed_on, true);
        return 0;
    }
    /* One FNO slot was bad: the numbers skip ahead by FNO_BELT, and the history before may be one a restore
     * rolled back. The new feed_id is durable before the first FNO store (the first fno_require comes after this):
     * were FNO raised first, a crash would leave the old feed_id with a huge last number, and a consumer from a
     * rolled-back history would resume instead of being told it is beyond the end. */
    feed_cfg cfg = db->feed;
    if ((rc = feed_dir_new_id(&fd, &cfg)) != 0) {
        feed_fail_log(db, "renewing feed_id after a torn FNO", rc);
        return rc;
    }
    db->feed = cfg;
    fprintf(stderr, "tsdb: %s: FNO was torn: the change feed has a new feed_id (consumers resync)\n", db->dir);
    return 0;
}

/* ------------------------------------------------------- live enable, disable */

int eng_feed_enable(eng *db, const feed_opts *o)
{
    if (db->opt.read_only || db->opt.follow || !db->wal_enabled)
        return TSDB_ERR_INVALID; /* a reader cannot, and without the WAL the feed would silently lack rows */
    pthread_mutex_lock(&db->flush_lock);
    int rc = eng_check(db);
    if (rc == 0 && !atomic_load(&db->feed_on)) {
        pthread_mutex_lock(&db->buf_lock);
        uint64_t floor = db->fno_next - 1; /* every later record is in the active segment or a later one */
        pthread_mutex_unlock(&db->buf_lock);
        feed_cfg cfg;
        feed_cfg_defaults(&cfg, o);
        feed_dir fd = {db->dirfd, db, db->opt.fsync};
        if ((rc = feed_dir_enable(&fd, &cfg, floor)) == 0 && (rc = feed_cfg_read(db->dirfd, &db->feed)) == 0 &&
            (rc = fslots_open(db)) == 0) {
            atomic_store(&db->feed_on, true); /* after FEED is durable: from here a flush renames, not unlinks */
            if ((rc = feed_cleaner_start(db)) != 0)
                atomic_store(&db->feed_on, false); /* on without a cleaner fills the disk (F18): all or nothing */
        }
        if (rc && !atomic_load(&db->feed_on)) { /* a partial enable must not be found as "on" at a later open */
            fslots_close(db);
            feed_index_free(db);
            if (eng_check(db) == 0 && feed_dir_disable(&fd) != 0)
                eng_fail(db, "rolling back a failed enable of the change feed", EIO);
        }
    } else if (rc == 0) {
        rc = feed_cleaner_start(db); /* already on: a cleaner that is missing is started (a no-op when it runs) */
    }
    pthread_mutex_unlock(&db->flush_lock);
    return rc;
}

int eng_feed_disable(eng *db)
{
    if (db->opt.read_only || db->opt.follow)
        return TSDB_ERR_INVALID;
    pthread_mutex_lock(&db->flush_lock);
    int rc = eng_check(db);
    if (rc == 0) {
        feed_cleaner_stop(db); /* before FEEDSLOTS and the fwal_ files go */
        atomic_store(&db->feed_on, false);
        fslots_close(db);
        feed_index_free(db);
        feed_dir fd = {db->dirfd, db, db->opt.fsync};
        rc = feed_dir_disable(&fd);
    }
    pthread_mutex_unlock(&db->flush_lock);
    return rc;
}

/* A restore needs a new identity (F19): FEED first, then FNO. Were FNO raised first, a crash would leave the old
 * feed_id with a last number far above anything a consumer from a rolled-back history holds, and it would resume
 * instead of being told it is beyond the end. FEED first: the worst crash leaves a new feed_id without the belt, and
 * consumers resync anyway; a rerun adds the belt. */
int eng_feed_new_id(eng *db)
{
    if (db->opt.read_only || db->opt.follow || !db->wal_enabled)
        return TSDB_ERR_INVALID;
    pthread_mutex_lock(&db->flush_lock);
    int rc = eng_check(db);
    if (rc == 0 && !atomic_load(&db->feed_on))
        rc = TSDB_ERR_INVALID;
    if (rc == 0) {
        feed_cfg cfg;
        pthread_mutex_lock(&db->feed_lock);
        cfg = db->feed;
        pthread_mutex_unlock(&db->feed_lock);
        feed_dir fd = {db->dirfd, db, db->opt.fsync};
        rc = feed_dir_new_id(&fd, &cfg);
        if (rc == 0) {
            pthread_mutex_lock(&db->feed_lock);
            db->feed = cfg;
            pthread_mutex_unlock(&db->feed_lock);
            pthread_mutex_lock(&db->buf_lock);
            if (db->fno_next >= FNO_BOUND_MAX - 2 * FNO_BELT) {
                rc = TSDB_ERR_INVALID; /* the numbers cannot go higher */
            } else {
                db->fno_next += FNO_BELT; /* a gap, never a repeat */
            }
            uint64_t next = db->fno_next;
            pthread_mutex_unlock(&db->buf_lock);
            if (rc == 0)
                rc = fno_require(db, next);
        }
    }
    pthread_mutex_unlock(&db->flush_lock);
    return rc;
}
