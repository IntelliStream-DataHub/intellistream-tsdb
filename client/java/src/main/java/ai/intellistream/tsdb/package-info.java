/**
 * Client for {@code tsdbd}, the TCP server of the tsdb time series database.
 *
 * <p>Start with {@link ai.intellistream.tsdb.TsdbClient#connect(String, int)}. A series is a named sequence of
 * (timestamp, value) points; timestamps are milliseconds since the Unix epoch and values are 32-bit floats. Writing
 * an existing (series, timestamp) replaces its value: the last write wins. Time ranges are half open,
 * {@code [start, end)}.
 *
 * <p>The client has no runtime dependencies. It speaks the protocol of {@code include/tsdb_protocol.h} over one
 * blocking {@link java.nio.channels.SocketChannel} per client, with little-endian direct buffers, and moves rows as
 * primitive arrays.
 */
package ai.intellistream.tsdb;
