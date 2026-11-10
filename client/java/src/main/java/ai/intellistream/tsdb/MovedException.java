// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

/**
 * Another node owns the volume (nothing was applied). The client already followed it, within its time budget (TsdbClient#setRetry), or had nowhere to go.
 *
 * <p>{@link #info()} has the body of the reply.
 */
public final class MovedException extends TsdbException {
    /** Version 1. */
    private static final long serialVersionUID = 1L;

    MovedException(ErrorInfo info) {
        super(Status.ERR_MOVED, info);
    }
}
