package ai.intellistream.tsdb;

/**
 * Receives the points of a streamed typed query, chunk by chunk, in the requested order.
 *
 * @see TsdbClient#queryValues(long, long, long, Order, long, ValuesConsumer)
 */
@FunctionalInterface
public interface ValuesConsumer {
    /**
     * Takes one chunk of points. The timestamp array and the column belong to the client and are reused for the
     * next chunk: only the first {@code n} entries are valid ({@code values.size() == n}), and only during the
     * call.
     *
     * <p>The callback must not call the client that runs the query. An exception it throws stops delivery; the
     * client reads the rest of the stream and then rethrows it.
     *
     * @param timestamps ms since the epoch
     * @param values the values of the chunk, with their type and nulls
     * @param n the number of points in this chunk (at least 1)
     * @return {@code true} to continue, {@code false} to stop: no more chunks are delivered (the client still
     *     reads, and discards, the rest of the stream)
     */
    boolean accept(long[] timestamps, Column values, int n);
}
