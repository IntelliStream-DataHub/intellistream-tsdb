// SPDX-License-Identifier: AGPL-3.0-only
/**
 * The tsdb time series database in-process: {@link ai.intellistream.tsdb.embedded.EmbeddedDb} runs the engine
 * (libtsdb.so) in the JVM through the Java FFM API ({@code java.lang.foreign}, Java 22 and later), with no server
 * and no network in between.
 *
 * <p>Its methods mirror {@link ai.intellistream.tsdb.TsdbClient}, the client of tsdbd, and take and return the
 * same types ({@link ai.intellistream.tsdb.Bucket}, {@link ai.intellistream.tsdb.Column},
 * {@link ai.intellistream.tsdb.Group}, {@link ai.intellistream.tsdb.TsdbException}, ...), so code moves between a
 * database in the process and one behind a server by changing how it is opened.
 *
 * <p>The JVM must allow native access for this code: {@code --enable-native-access=ALL-UNNAMED} on the class path,
 * {@code --enable-native-access=ai.intellistream.tsdb.embedded} on the module path, or
 * {@code Enable-Native-Access: ALL-UNNAMED} in the manifest of an executable jar. Without it, the JVM warns on
 * the first call (and later JDKs refuse it).
 */
package ai.intellistream.tsdb.embedded;
