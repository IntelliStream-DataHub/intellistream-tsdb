/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The tenant list of tsdbd (server/tenants.c): a source that changes its mind while the tick thread hands the engine
 * the previous list. The list the engine took is applied (it cannot be taken back), but the newest offer, which was
 * the list in force, must come back after the settle time; before the fix an offer equal to the list in force cleared
 * the pending one and the newer list stayed until the next change of USERS.
 *
 *   ./build/test_tn_race [scratch]
 */
#define _GNU_SOURCE
#include "tsdb.h"
#include "tenants.h"

#include <limits.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>

static int failures;
#define CHECK(cond)                                                                                                    \
    do {                                                                                                               \
        if (!(cond)) {                                                                                                 \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond);                                   \
            failures++;                                                                                                \
        }                                                                                                              \
    } while (0)
#define OK(expr) CHECK((expr) == 0)

static pthread_mutex_t mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t cv = PTHREAD_COND_INITIALIZER;
static int in_hook, release;

/* The tick thread stops here after the engine took the list and before cur changes. */
static void hook(void)
{
    pthread_mutex_lock(&mu);
    in_hook = 1;
    pthread_cond_broadcast(&cv);
    while (!release)
        pthread_cond_wait(&cv, &mu);
    pthread_mutex_unlock(&mu);
}

static bool wait_for(bool (*pred)(void), int ms)
{
    for (int i = 0; i < ms / 20; i++) {
        if (pred())
            return true;
        struct timespec t = {0, 20 * 1000 * 1000};
        nanosleep(&t, NULL);
    }
    return pred();
}

static bool entered(void)
{
    pthread_mutex_lock(&mu);
    bool b = in_hook;
    pthread_mutex_unlock(&mu);
    return b;
}
static bool is_a(void) { return tn_slot_name("a/x", 3) != 0; }
static bool is_none(void) { return tn_slot_name("a/x", 3) == 0; }

int main(int argc, char **argv)
{
    char dir[PATH_MAX + 32];
    snprintf(dir, sizeof dir, "%s/tn-race-%d", argc > 1 ? argv[1] : "/tmp", (int)getpid());
    char cmd[PATH_MAX + 64];
    snprintf(cmd, sizeof cmd, "rm -rf '%s'", dir);
    if (system(cmd) != 0)
        return 1;

    printf("test_tn_race\n");
    char **init;
    size_t ninit;
    OK(mkdir(dir, 0755));
    OK(tn_init(dir, NULL, false, 1, &init, &ninit)); /* no list, settle time 1 s */
    tsdb_options o;
    tsdb_options_init(&o);
    tsdb *db;
    OK(tsdb_open(dir, &o, &db));
    tn_free_list(init, ninit);
    OK(tn_start(db));
    CHECK(is_none());

    tn_test_after_set = hook;
    char *a[1] = {"a/"};
    tn_offer(a, 1); /* the list {a/}, stable for the settle time: the tick hands it to the engine and stops in the hook */
    CHECK(wait_for(entered, 10000));
    tn_offer(NULL, 0); /* the source went back to the list in force (empty) while the engine took {a/} */
    pthread_mutex_lock(&mu);
    release = 1;
    pthread_cond_broadcast(&cv);
    pthread_mutex_unlock(&mu);
    tn_test_after_set = NULL;
    CHECK(wait_for(is_a, 10000)); /* {a/} was taken: it is in force until the empty list settles */
    CHECK(wait_for(is_none, 10000)); /* and the empty list, the newest offer, comes back */
    uint32_t g = tn_gen();
    CHECK(g >= 3);

    tn_stop();
    tsdb_close(db);
    if (failures) {
        fprintf(stderr, "test_tn_race: %d failures\n", failures);
        return 1;
    }
    if (system(cmd) != 0)
        fprintf(stderr, "test_tn_race: could not remove %s\n", dir);
    printf("test_tn_race: ok\n");
    return 0;
}
