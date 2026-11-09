// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

/**
 * A frame that spans volumes was applied in part: ErrorInfo#rowNotApplied says which rows were not.
 *
 * <p>{@link #info()} has the body of the reply.
 */
public final class PartialException extends TsdbException {
    /** Version 1. */
    private static final long serialVersionUID = 1L;

    PartialException(ErrorInfo info) {
        super(Status.ERR_PARTIAL, info);
    }
}
