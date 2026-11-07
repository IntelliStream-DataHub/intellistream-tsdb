/*
 * Prints the size of every struct of tsdb.h that the Java binding lays out, and the offset of each field, as the
 * C compiler sees them: "tsdb_bucket 64" and "tsdb_bucket.count 8". LayoutTest compares them with Native's.
 */
#include "tsdb.h"

#include <stddef.h>
#include <stdio.h>

#define S(t) printf(#t " %zu\n", sizeof(t))
#define F(t, f) printf(#t "." #f " %zu\n", offsetof(t, f))

int main(void)
{
    S(tsdb_options);
    F(tsdb_options, partition_ms); F(tsdb_options, granule_rows); F(tsdb_options, zstd_level);
    F(tsdb_options, value_codec); F(tsdb_options, fsync); F(tsdb_options, merge_threads);
    F(tsdb_options, query_threads); F(tsdb_options, buffer_rows); F(tsdb_options, flush_interval_ms);
    F(tsdb_options, wal_sync_ms); F(tsdb_options, wal_max_batch_rows); F(tsdb_options, max_part_bytes);
    F(tsdb_options, read_only); F(tsdb_options, follow); F(tsdb_options, refresh_ms); F(tsdb_options, merge_idle_s);
    F(tsdb_options, obsolete_part_lifetime_s); F(tsdb_options, verify_checksums); F(tsdb_options, retention_merge_s);
    F(tsdb_options, clock_ms); F(tsdb_options, clock_arg); F(tsdb_options, create_volumes);
    F(tsdb_options, request_threads); F(tsdb_options, partition_classes); F(tsdb_options, pin_index);
    F(tsdb_options, insert_zstd_level); F(tsdb_options, stats); F(tsdb_options, stats_sweep_mb_s);
    S(tsdb_values);
    F(tsdb_values, type); F(tsdb_values, values); F(tsdb_values, valid);
    S(tsdb_bucket);
    F(tsdb_bucket, start); F(tsdb_bucket, count); F(tsdb_bucket, sum); F(tsdb_bucket, min); F(tsdb_bucket, max);
    F(tsdb_bucket, isum); F(tsdb_bucket, imin); F(tsdb_bucket, imax);
    S(tsdb_points);
    F(tsdb_points, ts); F(tsdb_points, values); F(tsdb_points, n); F(tsdb_points, cap);
    S(tsdb_group);
    F(tsdb_group, unit); F(tsdb_group, count); F(tsdb_group, timezone);
    S(tsdb_query_opts);
    F(tsdb_query_opts, order); F(tsdb_query_opts, limit);
    S(tsdb_query_stats);
    F(tsdb_query_stats, elapsed_s); F(tsdb_query_stats, rows); F(tsdb_query_stats, rows_decoded);
    F(tsdb_query_stats, rows_from_stats); F(tsdb_query_stats, memtable_rows); F(tsdb_query_stats, bytes);
    F(tsdb_query_stats, bytes_read); F(tsdb_query_stats, parts); F(tsdb_query_stats, granules);
    F(tsdb_query_stats, granules_from_stats); F(tsdb_query_stats, result_rows);
    S(tsdb_stats);
    F(tsdb_stats, parts); F(tsdb_stats, partitions); F(tsdb_stats, rows); F(tsdb_stats, bytes); F(tsdb_stats, merges);
    F(tsdb_stats, merged_rows); F(tsdb_stats, passthrough_granules); F(tsdb_stats, memtable_rows);
    F(tsdb_stats, broken_parts); F(tsdb_stats, follow_errors);
    S(tsdb_part_info);
    F(tsdb_part_info, name); F(tsdb_part_info, partition); F(tsdb_part_info, min_ts); F(tsdb_part_info, max_ts);
    F(tsdb_part_info, rows); F(tsdb_part_info, bytes); F(tsdb_part_info, granules); F(tsdb_part_info, level);
    F(tsdb_part_info, created_s); F(tsdb_part_info, written_s); F(tsdb_part_info, cls);
    F(tsdb_part_info, start_ms); F(tsdb_part_info, end_ms);
    S(tsdb_spread_bucket);
    F(tsdb_spread_bucket, b); F(tsdb_spread_bucket, range); F(tsdb_spread_bucket, var_pop);
    F(tsdb_spread_bucket, var_samp); F(tsdb_spread_bucket, stddev_pop); F(tsdb_spread_bucket, stddev_samp);
    S(tsdb_twa_opts);
    F(tsdb_twa_opts, method); F(tsdb_twa_opts, max_gap_ms); F(tsdb_twa_opts, order); F(tsdb_twa_opts, limit);
    F(tsdb_twa_opts, no_stats);
    S(tsdb_twa_bucket);
    F(tsdb_twa_bucket, start); F(tsdb_twa_bucket, twa); F(tsdb_twa_bucket, area); F(tsdb_twa_bucket, covered_ms);
    F(tsdb_twa_bucket, count);
    S(tsdb_multi_query);
    F(tsdb_multi_query, kind); F(tsdb_multi_query, start); F(tsdb_multi_query, end); F(tsdb_multi_query, starts);
    F(tsdb_multi_query, ends); F(tsdb_multi_query, opts); F(tsdb_multi_query, interval_ms);
    F(tsdb_multi_query, intervals); F(tsdb_multi_query, group); F(tsdb_multi_query, bounds);
    F(tsdb_multi_query, nbounds); F(tsdb_multi_query, trim); F(tsdb_multi_query, threshold); F(tsdb_multi_query, twa);
    S(tsdb_series_result);
    F(tsdb_series_result, index); F(tsdb_series_result, series); F(tsdb_series_result, status);
    F(tsdb_series_result, last); F(tsdb_series_result, n); F(tsdb_series_result, ts); F(tsdb_series_result, values);
    F(tsdb_series_result, buckets);
    S(tsdb_multi_opts);
    F(tsdb_multi_opts, threads); F(tsdb_multi_opts, max_rows); F(tsdb_multi_opts, max_held_bytes);
    F(tsdb_multi_opts, cancelled); F(tsdb_multi_opts, on_wait); F(tsdb_multi_opts, budget_take);
    F(tsdb_multi_opts, budget_give); F(tsdb_multi_opts, on_deferred); F(tsdb_multi_opts, hook_ctx);
    /* enum and value-type widths the binding assumes */
    printf("enum %zu\n", sizeof(tsdb_type));
    /* the codes it passes: "type.FLOAT64 1" is ValueType.FLOAT64.code(), "unit.DAY 4" Unit.DAY.code(), "order.DESC 1",
     * and "kind.tsdb_bucket 2" is tsdb_struct_kind, the index of the struct in Native.ALL_STRUCTS */
#define E(prefix, name, value) printf(prefix "." #name " %d\n", (int)(value))
    E("type", FLOAT32, TSDB_FLOAT32); E("type", FLOAT64, TSDB_FLOAT64); E("type", INT64, TSDB_INT64);
    E("type", BOOL, TSDB_BOOL); E("type", STATE, TSDB_STATE);
    E("unit", SECOND, TSDB_SECOND); E("unit", MINUTE, TSDB_MINUTE); E("unit", HOUR, TSDB_HOUR); E("unit", DAY, TSDB_DAY);
    E("unit", WEEK, TSDB_WEEK); E("unit", MONTH, TSDB_MONTH); E("unit", QUARTER, TSDB_QUARTER);
    E("unit", YEAR, TSDB_YEAR);
    E("order", ASC, TSDB_ASC); E("order", DESC, TSDB_DESC);
    E("twa", STEP, TSDB_TWA_STEP); E("twa", LINEAR, TSDB_TWA_LINEAR);
    E("kind", tsdb_options, TSDB_STRUCT_OPTIONS); E("kind", tsdb_values, TSDB_STRUCT_VALUES);
    E("kind", tsdb_bucket, TSDB_STRUCT_BUCKET); E("kind", tsdb_points, TSDB_STRUCT_POINTS);
    E("kind", tsdb_group, TSDB_STRUCT_GROUP); E("kind", tsdb_query_opts, TSDB_STRUCT_QUERY_OPTS);
    E("kind", tsdb_query_stats, TSDB_STRUCT_QUERY_STATS); E("kind", tsdb_stats, TSDB_STRUCT_STATS);
    E("kind", tsdb_part_info, TSDB_STRUCT_PART_INFO); E("kind", tsdb_spread_bucket, TSDB_STRUCT_SPREAD_BUCKET);
    E("kind", tsdb_twa_opts, TSDB_STRUCT_TWA_OPTS); E("kind", tsdb_twa_bucket, TSDB_STRUCT_TWA_BUCKET);
    E("kind", tsdb_multi_query, TSDB_STRUCT_MULTI_QUERY); E("kind", tsdb_series_result, TSDB_STRUCT_SERIES_RESULT);
    E("kind", tsdb_multi_opts, TSDB_STRUCT_MULTI_OPTS);
    E("multi", VALUES, TSDB_MULTI_VALUES); E("multi", BUCKETS, TSDB_MULTI_BUCKETS); E("multi", SPREAD, TSDB_MULTI_SPREAD);
    E("multi", TWA, TSDB_MULTI_TWA); E("multi", LTTB, TSDB_MULTI_LTTB);
    E("multi", TRIM_LO, TSDB_MULTI_TRIM_LO); E("multi", TRIM_HI, TSDB_MULTI_TRIM_HI);
    E("abi", VERSION, TSDB_ABI_VERSION);
    return 0;
}
