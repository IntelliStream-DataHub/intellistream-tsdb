// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

/**
 * What {@link TsdbClient#tenantStats} is asked for.
 *
 * @param volumes a row per volume after each tenant's summed row
 * @param hot hot-list entries per kind on the summed row, 0 to 256; 0: none
 * @param caller the tenant's own request counters on the summed row
 */
public record TenantOptions(boolean volumes, int hot, boolean caller) {
    private static final TenantOptions DEFAULTS = new TenantOptions(false, 0, false);

    /**
     * Checks the values.
     *
     * @param volumes a row per volume
     * @param hot entries per kind, 0 to 256
     * @param caller the request counters
     */
    public TenantOptions {
        if (hot < 0 || hot > 256)
            throw new IllegalArgumentException("hot " + hot + ": 0 to 256");
    }

    /**
     * The summed row of each tenant only.
     *
     * @return the options
     */
    public static TenantOptions defaults() {
        return DEFAULTS;
    }

    /**
     * With a row per volume.
     *
     * @return the options
     */
    public TenantOptions withVolumes() {
        return new TenantOptions(true, hot, caller);
    }

    /**
     * With {@code k} hot-list entries per kind.
     *
     * @param k entries, 0 to 256
     * @return the options
     */
    public TenantOptions withHot(int k) {
        return new TenantOptions(volumes, k, caller);
    }

    /**
     * With the tenant's request counters.
     *
     * @return the options
     */
    public TenantOptions withCaller() {
        return new TenantOptions(volumes, hot, true);
    }
}
