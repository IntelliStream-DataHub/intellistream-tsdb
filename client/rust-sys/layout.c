/* Sizes and field offsets of the public structs as the C compiler lays them
 * out, for tests/layout.rs to compare with the Rust declarations. */
#include <stddef.h>

#include "tsdb.h"

#define S(t) sizeof(t)
#define O(t, f) offsetof(t, f)

static const size_t layout[] = {
    S(tsdb_options), O(tsdb_options, partition_ms), O(tsdb_options, granule_rows), O(tsdb_options, zstd_level),
    O(tsdb_options, value_codec), O(tsdb_options, fsync), O(tsdb_options, merge_threads),
    O(tsdb_options, query_threads), O(tsdb_options, buffer_rows), O(tsdb_options, flush_interval_ms),
    O(tsdb_options, wal_sync_ms), O(tsdb_options, wal_max_batch_rows), O(tsdb_options, max_part_bytes),
    O(tsdb_options, read_only), O(tsdb_options, follow), O(tsdb_options, refresh_ms), O(tsdb_options, merge_idle_s),
    O(tsdb_options, obsolete_part_lifetime_s), O(tsdb_options, verify_checksums),
    O(tsdb_options, retention_merge_s), O(tsdb_options, clock_ms), O(tsdb_options, clock_arg),
    O(tsdb_options, create_volumes),
    S(tsdb_values), O(tsdb_values, type), O(tsdb_values, values), O(tsdb_values, valid),
    S(tsdb_points), O(tsdb_points, ts), O(tsdb_points, values), O(tsdb_points, n), O(tsdb_points, cap),
    S(tsdb_bucket), O(tsdb_bucket, start), O(tsdb_bucket, count), O(tsdb_bucket, sum), O(tsdb_bucket, min),
    O(tsdb_bucket, max), O(tsdb_bucket, isum), O(tsdb_bucket, imin), O(tsdb_bucket, imax),
    S(tsdb_query_opts), O(tsdb_query_opts, order), O(tsdb_query_opts, limit),
    S(tsdb_group), O(tsdb_group, unit), O(tsdb_group, count), O(tsdb_group, timezone),
    S(tsdb_query_stats), O(tsdb_query_stats, elapsed_s), O(tsdb_query_stats, rows), O(tsdb_query_stats, rows_decoded),
    O(tsdb_query_stats, rows_from_stats), O(tsdb_query_stats, memtable_rows), O(tsdb_query_stats, bytes),
    O(tsdb_query_stats, bytes_read), O(tsdb_query_stats, parts), O(tsdb_query_stats, granules),
    O(tsdb_query_stats, granules_from_stats), O(tsdb_query_stats, result_rows),
    S(tsdb_stats), O(tsdb_stats, parts), O(tsdb_stats, partitions), O(tsdb_stats, rows), O(tsdb_stats, bytes),
    O(tsdb_stats, merges), O(tsdb_stats, merged_rows), O(tsdb_stats, passthrough_granules),
    O(tsdb_stats, memtable_rows), O(tsdb_stats, broken_parts), O(tsdb_stats, follow_errors),
    S(tsdb_part_info), O(tsdb_part_info, name), O(tsdb_part_info, partition), O(tsdb_part_info, min_ts),
    O(tsdb_part_info, max_ts), O(tsdb_part_info, rows), O(tsdb_part_info, bytes), O(tsdb_part_info, granules),
    O(tsdb_part_info, level), O(tsdb_part_info, created_s), O(tsdb_part_info, written_s),
    S(tsdb_type), S(tsdb_order), S(tsdb_unit),
};

const size_t *tsdb_sys_layout(size_t *n)
{
    *n = sizeof(layout) / sizeof(layout[0]);
    return layout;
}
