/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Cluster reporting registries (R14, r14-report; design-cluster-data.md §14.4, PLAN.md D22/D23):
 *   1. Heartbeat section registry (`creport_section`): modules register custom heartbeat section types without editing
 *      cluster.c's section switch. `cluster.c`'s put_sections() calls registered encoders, heard_sections() dispatches
 *      registered decoders and stores decoded values per (heard, type) (freeing prior values on each beat and on
 *      cluster_stop), and STATUS carries each member's formatted section output. Unknown section types are skipped.
 *   2. Cluster metrics registry (`creport_metric_*`): gauges, counters, and dynamic collector callbacks with labels
 *      (volume, path, node, store, class, why, ...). `creport_snap_take()` produces an immutable heap snapshot without
 *      holding registry or cluster locks across collection or HTTP/Prometheus rendering in server/metrics.c.
 */
#ifndef TSDBD_CREPORT_H
#define TSDBD_CREPORT_H

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

/* ---- 1. Heartbeat section registry ---- */

#define CREPORT_SEC_BUILTIN_MAX 8 /* HBS_NODE(1) .. HBS_FILL(8) stay inline in cluster.c */
#define CREPORT_SEC_MAX 32
#define CREPORT_SEC_PAYLOAD_MAX 65536

/* Encode this server's section payload into buf[0..cap):
 * return bytes written (> 0 to emit), 0 to omit this beat, or < 0 on error (omitted). */
typedef ssize_t (*creport_sec_encode_fn)(uint8_t *buf, size_t cap);
/* Decode payload data[0..len) from member_id into *out_val: return 0 on success, -1 if malformed. */
typedef int (*creport_sec_decode_fn)(uint32_t member_id, const uint8_t *data, size_t len, void **out_val);
/* Free a value previously produced by decode_fn. */
typedef void (*creport_sec_free_fn)(void *val);
/* Format a decoded value for STATUS text output into buf[0..cap); returns bytes written (excluding NUL). */
typedef size_t (*creport_sec_format_fn)(uint32_t member_id, const void *val, char *buf, size_t cap);

/* Register a custom heartbeat section type (> CREPORT_SEC_BUILTIN_MAX).
 * Returns 0 on success, -1 on invalid/built-in/duplicate type or full table. Thread-safe. */
int creport_section(uint8_t type, creport_sec_encode_fn encode_fn, creport_sec_decode_fn decode_fn,
                    creport_sec_free_fn free_fn, creport_sec_format_fn format_fn);
/* Unregister a previously registered section type. Returns 0 if removed, -1 if not found. */
int creport_section_unregister(uint8_t type);

/* Per-member decoded custom sections (embedded inside cluster.c's struct heard). */
struct creport_sec_val {
    uint8_t type;
    void *val;
    creport_sec_free_fn free_fn;
};

struct creport_heard_secs {
    struct creport_sec_val items[CREPORT_SEC_MAX];
    uint32_t n;
};

void creport_heard_init(struct creport_heard_secs *hs);
void creport_heard_clear(struct creport_heard_secs *hs);
const void *creport_heard_get(const struct creport_heard_secs *hs, uint8_t type);
size_t creport_heard_format(const struct creport_heard_secs *hs, uint32_t member_id, char *buf, size_t cap);

/* cluster.c hooks: put_sections() emits each registered section; heard_sections() dispatches registered decoders. */
typedef void (*creport_sec_emit_fn)(void *ctx, uint16_t type, const uint8_t *data, size_t len);
void creport_sec_encode_all(creport_sec_emit_fn emit, void *ctx);
/* Returns 1 if type is not registered (skip unknown section cleanly), 0 if decoded and stored, -1 if malformed
 * (malformed custom sections are skipped and counted without discarding the rest of the heartbeat). */
int creport_sec_decode_into(struct creport_heard_secs *hs, uint16_t type, uint32_t member_id, const uint8_t *data,
                            size_t len);
uint64_t creport_sec_decode_errors(void);

/* ---- 2. Cluster metrics registry & immutable snapshot ---- */

enum creport_metric_type { CREPORT_GAUGE = 0, CREPORT_COUNTER = 1 };

#define CREPORT_LABELS_MAX 6
#define CREPORT_LABEL_KEY_MAX 24
#define CREPORT_LABEL_VAL_MAX 128

struct creport_label {
    const char *key;
    const char *val;
};

/* Direct registered gauge / counter handle (updated lock-free via relaxed atomics).
 * Lifetime: handles remain valid until passed to creport_metric_del() or cleared by creport_reset();
 * callers must not touch a handle after either call. */
struct creport_metric;

struct creport_metric *creport_metric_gauge(const char *name, const char *help, const struct creport_label *labels,
                                            size_t nlabels);
struct creport_metric *creport_metric_counter(const char *name, const char *help, const struct creport_label *labels,
                                              size_t nlabels);
void creport_metric_set(struct creport_metric *m, double val);
void creport_metric_add(struct creport_metric *m, uint64_t delta);
void creport_metric_inc(struct creport_metric *m);
void creport_metric_del(struct creport_metric *m);

/* Collector callback API: emits 0..N labelled samples into sink when creport_snap_take() runs. */
struct creport_sink;
typedef void (*creport_collect_fn)(void *ctx, struct creport_sink *sink);

int creport_metric_collector(const char *name, enum creport_metric_type type, const char *help, creport_collect_fn fn,
                             void *ctx);
int creport_metric_collector_del(const char *name);

void creport_sink_put(struct creport_sink *sink, const struct creport_label *labels, size_t nlabels, double val);
void creport_sink_put_i64(struct creport_sink *sink, const struct creport_label *labels, size_t nlabels, int64_t val);
void creport_sink_put_u64(struct creport_sink *sink, const struct creport_label *labels, size_t nlabels, uint64_t val);

/* Immutable snapshot rendered by server/metrics.c with zero locks held. */
struct creport_snap_label {
    char key[CREPORT_LABEL_KEY_MAX];
    char val[CREPORT_LABEL_VAL_MAX];
};

enum { CREPORT_VAL_DOUBLE = 0, CREPORT_VAL_I64 = 1, CREPORT_VAL_U64 = 2 };

struct creport_snap_sample {
    struct creport_snap_label labels[CREPORT_LABELS_MAX];
    uint8_t nlabels;
    uint8_t val_kind; /* CREPORT_VAL_* */
    double dval;
    int64_t ival;
    uint64_t uval;
};

struct creport_snap_family {
    char name[96];
    char help[320];
    enum creport_metric_type type;
    struct creport_snap_sample *samples;
    size_t nsamples, cap;
};

struct creport_snap {
    struct creport_snap_family *families;
    size_t nfamilies, cap;
};

struct creport_snap *creport_snap_take(void);
void creport_snap_free(struct creport_snap *snap);

/* Clears all registered sections, metrics, collectors and cached test state (shutdown / unit tests). */
void creport_reset(void);

/* Dumps registered sections and metric families into buf (for diagnostics and test_cluster metrics). */
size_t creport_dump_registry(char *buf, size_t cap);

/* Replication lag of held volume `vol` from server/repld.c (implemented in creport_drv.o for tsdbd). */
void creport_repld_lag(uint32_t vol, uint64_t *lag_positions, uint64_t *lag_ms);

#ifdef TSDB_TEST_HOOKS
#define CREPORT_SEC_TEST 250
/* Enables synthetic test section CREPORT_SEC_TEST and optional registry dump file under state_dir when
 * TSDB_CLUSTER_TEST contains report_test=1 (or TSDB_REPORT_TEST is set). Idempotent. */
void creport_test_init(const char *state_dir);
void creport_test_dump_file(const char *state_dir);
#endif

#endif /* TSDBD_CREPORT_H */
