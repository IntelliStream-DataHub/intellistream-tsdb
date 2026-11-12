// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

/**
 * The rows are older than the volume's drop watermark and never accepted (ErrorInfo#droppedBefore).
 *
 * <p>{@link #info()} has the body of the reply.
 */
public final class DroppedException extends TsdbException {
    /** Version 1. */
    private static final long serialVersionUID = 1L;

    DroppedException(ErrorInfo info) {
        super(Status.ERR_DROPPED, info);
    }
}
