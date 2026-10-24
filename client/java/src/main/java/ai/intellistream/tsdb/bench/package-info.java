/**
 * The insert benchmark, {@link ai.intellistream.tsdb.bench.InsertBench}: loads a file of rows into tsdbd over one
 * or more connections and prints rows per second and bytes sent per row.
 *
 * <pre>{@code
 * java -cp target/tsdb-client-0.1.0-SNAPSHOT.jar ai.intellistream.tsdb.bench.InsertBench /tmp/x.rb --conns 4
 * }</pre>
 */
package ai.intellistream.tsdb.bench;
