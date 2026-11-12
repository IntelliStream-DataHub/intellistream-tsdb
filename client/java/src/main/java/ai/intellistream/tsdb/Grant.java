// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

import java.util.Objects;

/**
 * One grant of a user: {@code rights} on the series {@code selector} matches.
 *
 * @param selector the series it covers
 * @param rights what it allows there
 */
public record Grant(Selector selector, Rights rights) {
    /**
     * Checks the parts.
     *
     * @param selector the series it covers
     * @param rights what it allows there
     */
    public Grant {
        Objects.requireNonNull(selector, "selector");
        Objects.requireNonNull(rights, "rights");
    }
}
