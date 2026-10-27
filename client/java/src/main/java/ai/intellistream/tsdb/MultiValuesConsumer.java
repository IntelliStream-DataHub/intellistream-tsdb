package ai.intellistream.tsdb;

/**
 * Receives the rows of a query over many series as they arrive ({@link TsdbClient#queryValuesMulti(int[], long, long,
 * Order, long, MultiOptions, MultiValuesConsumer)}), series by series in the order of the list.
 */
@FunctionalInterface
public interface MultiValuesConsumer {
    /**
     * A chunk of rows of series {@code index}. The arrays are reused for the next chunk.
     *
     * @param index the series' index in the list
     * @param timestamps the timestamps, in {@code [0, n)}
     * @param values the values, in {@code [0, n)}
     * @param n the number of rows (0 for a series without rows: then only the value type)
     * @return false to stop the query (the rest is cancelled)
     */
    boolean accept(int index, long[] timestamps, Column values, int n);

    /**
     * The end of series {@code index}, after its last chunk.
     *
     * @param index the series' index in the list
     * @param status {@link Status#OK} or the series' error
     * @return false to stop the query
     */
    default boolean done(int index, int status) {
        return true;
    }
}
