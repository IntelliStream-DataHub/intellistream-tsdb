// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

import java.util.List;

/**
 * The numbers of tenants ({@link TsdbClient#tenantStats}). A tenant is a prefix of series names; a series belongs to
 * the tenant with the longest prefix its name starts with, else to "(none)" (the empty prefix). The 64-bit counters
 * are unsigned on the wire and never reach 2^63 in practice.
 *
 * @param partial a sweep or a move of series between tenants is not finished: the numbers are stale
 * @param ingestApprox the ingest numbers are approximate
 * @param generation of the tenant list: one more at every change
 * @param listHash of the list's prefixes: two servers with the same list have the same hash
 * @param partsCounted parts the sweeper has counted (0 unless the user is a superuser or reads every series)
 * @param partsListed parts listed (0 likewise)
 * @param rows each tenant's summed row, then its volumes' rows when asked for
 */
public record TenantStats(boolean partial, boolean ingestApprox, int generation, long listHash, long partsCounted,
        long partsListed, List<Row> rows) {
    /** The volume of a tenant's summed row. */
    public static final int VOLUME_SUM = -1;

    /**
     * One row: a tenant, summed over the volumes or in one volume.
     *
     * @param status 0, {@link Status#ERR_NOTFOUND} (a prefix not in the tenant list, for a user who may see it) or
     *     {@link Status#ERR_DENIED}; the numbers are 0 then
     * @param volume {@link #VOLUME_SUM}, or the volume's number
     * @param prefix the tenant's prefix; "" is the tenant "(none)"
     * @param partial the numbers of this row are stale
     * @param numbers the numbers
     * @param caller with {@link TenantOptions#caller()}, on the summed row; else null
     * @param hot with {@link TenantOptions#hot()}, on the summed row, each kind ending in its threshold (id 0)
     */
    public record Row(int status, int volume, String prefix, boolean partial, Numbers numbers, Caller caller,
            List<Hot> hot) {
    }

    /**
     * The numbers of one tenant.
     *
     * @param series series that belong to it
     * @param seriesCreated series created since the server started
     * @param activeSeriesCur series active in the current hour (about 3 percent)
     * @param activeSeriesPrev series active in the hour before
     * @param rows rows (physical)
     * @param nulls nulls
     * @param logicalBytes part rows times (8 + the width of the type)
     * @param tsBytes bytes of timestamps
     * @param valueBytes bytes of values
     * @param indexBytes bytes of granule indexes
     * @param granules granules
     * @param ingestedRows rows flushed since the start
     * @param ingestedBytes bytes of them
     * @param mergedRows rows merges read
     * @param mergeRemovedRows rows merges removed
     * @param retentionDroppedRows rows retention dropped
     * @param residentBytes page cache, sampled
     * @param staleS the age in seconds of the oldest partition waiting for its recount
     */
    public record Numbers(long series, long seriesCreated, long activeSeriesCur, long activeSeriesPrev, long rows,
            long nulls, long logicalBytes, long tsBytes, long valueBytes, long indexBytes, long granules,
            long ingestedRows, long ingestedBytes, long mergedRows, long mergeRemovedRows, long retentionDroppedRows,
            long residentBytes, long staleS) {
    }

    /**
     * What a tenant's own requests cost: counted per request, never per row.
     *
     * @param requestsQuery query requests
     * @param requestsInsert insert requests
     * @param requestsDelete delete, replace and retention requests
     * @param requestsAdmin series and user admin requests
     * @param requestsOther other requests
     * @param rowsDecoded rows the queries decoded
     * @param rowsFromStats rows answered from granule statistics
     * @param rowsFromMemtable rows read from memtables
     * @param bytesRead bytes read
     * @param resultRows rows returned
     * @param queryUs microseconds the queries took
     * @param insertedRows rows inserted
     * @param insertedBytes bytes inserted
     * @param errorsInvalid requests that failed as invalid
     * @param errorsNotfound requests that failed as not found
     * @param errorsIo requests that failed with an I/O error
     * @param errorsOther other failures
     * @param deniedRequests requests refused as denied
     * @param deniedSeries series refused as denied
     * @param failedLogins failed logins
     */
    public record Caller(long requestsQuery, long requestsInsert, long requestsDelete, long requestsAdmin,
            long requestsOther, long rowsDecoded, long rowsFromStats, long rowsFromMemtable, long bytesRead,
            long resultRows, long queryUs, long insertedRows, long insertedBytes, long errorsInvalid,
            long errorsNotfound, long errorsIo, long errorsOther, long deniedRequests, long deniedSeries,
            long failedLogins) {
    }

    /** What a hot list counts. */
    public enum HotKind {
        /** Queries in the last 60 to 120 seconds (sampled). */
        READS,
        /** Rows written in the same window. */
        WRITES,
        /** The worst bytes per row times 1000, over full granules of merged parts. */
        BYTES_PER_ROW
    }

    /**
     * One hot-list entry; {@code series} 0 is the threshold (what a series not in the list weighs at most).
     *
     * @param kind the list
     * @param series the series id, or 0
     * @param estimate the estimate
     * @param error its error bound
     */
    public record Hot(HotKind kind, long series, long estimate, long error) {
    }
}
