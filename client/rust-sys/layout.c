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
    O(tsdb_options, request_threads), O(tsdb_options, partition_classes), O(tsdb_options, pin_index),
    O(tsdb_options, insert_zstd_level),
    S(tsdb_values), O(tsdb_values, type), O(tsdb_values, values), O(tsdb_values, valid),
    S(tsdb_points), O(tsdb_points, ts), O(tsdb_points, values), O(tsdb_points, n), O(tsdb_points, cap),
    S(tsdb_bucket), O(tsdb_bucket, start), O(tsdb_bucket, count), O(tsdb_bucket, sum), O(tsdb_bucket, min),
    O(tsdb_bucket, max), O(tsdb_bucket, isum), O(tsdb_bucket, imin), O(tsdb_bucket, imax),
    S(tsdb_query_opts), O(tsdb_query_opts, order), O(tsdb_query_opts, limit),
    S(tsdb_group), O(tsdb_group, unit), O(tsdb_group, count), O(tsdb_group, timezone),
    S(tsdb_spread_bucket), O(tsdb_spread_bucket, b), O(tsdb_spread_bucket, range), O(tsdb_spread_bucket, var_pop),
    O(tsdb_spread_bucket, var_samp), O(tsdb_spread_bucket, stddev_pop), O(tsdb_spread_bucket, stddev_samp),
    S(tsdb_twa_opts), O(tsdb_twa_opts, method), O(tsdb_twa_opts, max_gap_ms), O(tsdb_twa_opts, order),
    O(tsdb_twa_opts, limit), O(tsdb_twa_opts, no_stats),
    S(tsdb_twa_bucket), O(tsdb_twa_bucket, start), O(tsdb_twa_bucket, twa), O(tsdb_twa_bucket, area),
    O(tsdb_twa_bucket, covered_ms), O(tsdb_twa_bucket, count),
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
    O(tsdb_part_info, cls), O(tsdb_part_info, start_ms), O(tsdb_part_info, end_ms),
    S(tsdb_multi_query), O(tsdb_multi_query, kind), O(tsdb_multi_query, start), O(tsdb_multi_query, end),
    O(tsdb_multi_query, starts), O(tsdb_multi_query, ends), O(tsdb_multi_query, opts),
    O(tsdb_multi_query, interval_ms), O(tsdb_multi_query, intervals), O(tsdb_multi_query, group),
    O(tsdb_multi_query, bounds), O(tsdb_multi_query, nbounds), O(tsdb_multi_query, trim),
    O(tsdb_multi_query, threshold), O(tsdb_multi_query, twa),
    S(tsdb_series_result), O(tsdb_series_result, index), O(tsdb_series_result, series),
    O(tsdb_series_result, status), O(tsdb_series_result, last), O(tsdb_series_result, n), O(tsdb_series_result, ts),
    O(tsdb_series_result, values), O(tsdb_series_result, buckets),
    S(tsdb_multi_opts), O(tsdb_multi_opts, threads), O(tsdb_multi_opts, max_rows), O(tsdb_multi_opts, max_held_bytes),
    O(tsdb_multi_opts, cancelled), O(tsdb_multi_opts, on_wait), O(tsdb_multi_opts, budget_take),
    O(tsdb_multi_opts, budget_give), O(tsdb_multi_opts, on_deferred), O(tsdb_multi_opts, hook_ctx),
    S(tsdb_type), S(tsdb_order), S(tsdb_unit), S(tsdb_twa_method),
};

const size_t *tsdb_sys_layout(size_t *n)
{
    *n = sizeof(layout) / sizeof(layout[0]);
    return layout;
}
