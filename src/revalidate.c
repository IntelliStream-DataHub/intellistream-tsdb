/* Revalidation of a volume after a storage outage (ha.md 15.5 steps 3 and 4), the parts of it that compare the
 * halted engine's memory with what the storage holds now.
 *
 * The process kept running while its NFS server was away, so its memory is the best record of what was acknowledged.
 * A server that comes back with an older state (a rolled-back pool, an export restored from a snapshot) has lost
 * acknowledged data, and a plain reopen would replay what it has and say nothing. The volume is therefore opened
 * again only after these checks, which the engine's own threads cannot have changed (they are halted):
 *   - the last durable WAL record is read again, byte for byte (wal_verify_tail); if it is not there, the rows the
 *     memory holds go into a new segment first (wal_relog), so the open that follows replays them;
 *   - SERIES and STATES hold every id the engine handed out: entries the server lost are appended again with their
 *     names before anything else opens the files (a lost entry would give an id another name, and a client holding
 *     it would write into the wrong series); a prefix that differs is damage and fails;
 *   - after the open, every part that was live must be there, or lie inside the block range of a part that is (a
 *     merge that finished while the engine was stopped). Nothing else counts: a merge the storage took back brings
 *     its sources back, but an overwrite, a delete or an expiry that was merged into its output is not in them (the
 *     rows would silently revert), and counting rows cannot tell the two apart. What remains is lost: counted,
 *     logged, and the volume stays refused (DIVERGED) until an operator accepts it;
 *   - RETENTION and CLASSES are append-only: the open must apply as many bytes of each as the engine had (reval_logs).
 *
 * What this does not see: a payload damaged inside a part of the right name and size (only the header, index and
 * footer checksums at open, and verify_checksums at read time, see it), and a rollback of a volume whose engine
 * the process no longer holds (a restart during the outage: the local high-water cache covers SERIES and STATES). */
#include "internal.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>

/* The prefix of the dictionary on storage must be the one in memory. 0, TSDB_ERR_CORRUPT if a name differs. */
static int dict_same_prefix(series_dict *mem, series_dict *disk, uint32_t n, const char *file)
{
    for (uint32_t i = 0; i < n; i++)
        if (strcmp(mem->names[i], disk->names[i]) != 0 || mem->types[i] != disk->types[i]) {
            fprintf(stderr, "tsdb: %s entry %u is '%s' on the storage and '%s' in memory: the ids of this volume "
                            "mean different series now; refusing to go on\n",
                    file, i, disk->names[i], mem->names[i]);
            return TSDB_ERR_CORRUPT;
        }
    return 0;
}

static int dict_repair(series_dict *mem, int dirfd, const char *file, uint64_t *added)
{
    series_dict d;
    int rc = series_open(&d, dirfd, file, false, true);
    if (rc == 0) {
        uint32_t nm = atomic_load(&mem->count), nd = atomic_load(&d.count);
        rc = dict_same_prefix(mem, &d, nm < nd ? nm : nd, file);
        if (rc == 0 && nd < nm) {
            fprintf(stderr, "tsdb: %s lost %u entries on the storage: appending them again from memory\n", file,
                    nm - nd);
            rc = series_cut_tail(&d);
            for (uint32_t i = nd; rc == 0 && i < nm; i++) {
                uint32_t id;
                rc = series_get(&d, mem->names[i], true, mem->types[i], &id);
                if (rc == 0 && id != i)
                    rc = TSDB_ERR_CORRUPT;
                else if (rc == 0)
                    (*added)++;
            }
        }
    }
    series_close(&d);
    return rc;
}

/* Before the open: SERIES and STATES. */
int reval_dicts(eng *old, const char *dir, uint64_t *series_added, uint64_t *states_added)
{
    int dirfd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dirfd < 0)
        return TSDB_ERR_IO;
    int rc = dict_repair(&old->series, dirfd, "SERIES", series_added);
    if (rc == 0)
        rc = dict_repair(&old->states, dirfd, "STATES", states_added);
    if (rc == 0 && (*series_added || *states_added) && fsync(dirfd) != 0)
        rc = TSDB_ERR_IO;
    close(dirfd);
    return rc;
}

/* The parts the engine listed, taken once it is halted. */
int reval_capture(eng *old, reval_part **out, size_t *n)
{
    pthread_mutex_lock(&old->lock);
    reval_part *p = malloc((old->nparts ? old->nparts : 1) * sizeof *p);
    size_t k = 0;
    for (size_t i = 0; p && i < old->nparts; i++) {
        const part *a = old->parts[i];
        if (a->dropped || atomic_load(&a->obsolete))
            continue;
        snprintf(p[k].name, sizeof p[k].name, "%s", a->name);
        p[k].partition = a->partition;
        p[k].min_block = a->min_block, p[k].max_block = a->max_block;
        p[k].rows = a->rows;
        p[k].nt = a->nt;
        k++;
    }
    pthread_mutex_unlock(&old->lock);
    if (!p)
        return TSDB_ERR_NOMEM;
    *out = p;
    *n = k;
    return 0;
}

/* After the open: which of the captured parts are gone. */
void reval_parts(const eng *neu, const reval_part *x, size_t nx, uint64_t *lost, uint64_t *lost_rows)
{
    for (size_t i = 0; i < nx; i++) {
        const reval_part *o = &x[i];
        bool found = false, covered = false;
        for (size_t j = 0; j < neu->nparts; j++) {
            const part *p = neu->parts[j];
            if (strcmp(p->name, o->name) == 0) {
                found = true;
                break;
            }
            if (p->partition != o->partition)
                continue;
            if (p->min_block <= o->min_block && p->max_block >= o->max_block)
                covered = true; /* merged into this one while the engine was stopped (an interrupted merge) */
        }
        if (found || covered)
            continue;
        (*lost)++;
        *lost_rows += o->rows;
        fprintf(stderr, "tsdb: part %s (blocks %llu-%llu, %llu rows%s) is not on the storage any more: its rows are "
                        "lost\n",
                o->name, (unsigned long long)o->min_block, (unsigned long long)o->max_block,
                (unsigned long long)o->rows, o->nt ? ", with deletes" : "");
    }
}

/* After the open: RETENTION and CLASSES only grow, so fewer bytes applied than the halted engine had means records
 * the storage lost (a limit that was acknowledged, a class switch). The bytes are summed into *lost. */
void reval_logs(const eng *old, const eng *neu, uint64_t *lost)
{
    if (neu->ret_off < old->ret_off) {
        *lost += old->ret_off - neu->ret_off;
        fprintf(stderr, "tsdb: RETENTION holds %llu bytes of records on the storage and the engine had %llu: "
                        "acknowledged retention limits are gone\n",
                (unsigned long long)neu->ret_off, (unsigned long long)old->ret_off);
    }
    if (neu->cls_off < old->cls_off) {
        *lost += old->cls_off - neu->cls_off;
        fprintf(stderr, "tsdb: CLASSES holds %llu bytes of records on the storage and the engine had %llu: "
                        "acknowledged class switches are gone\n",
                (unsigned long long)neu->cls_off, (unsigned long long)old->cls_off);
    }
}
