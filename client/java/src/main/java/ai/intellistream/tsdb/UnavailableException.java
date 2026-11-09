// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

/**
 * The server refused the request because a volume it needs is not available (a storage outage, a node that stopped, ...); nothing was applied. The client already waited and retried within its time budget (TsdbClient#setRetry).
 *
 * <p>{@link #info()} has the body of the reply.
 */
public final class UnavailableException extends TsdbException {
    /** Version 1. */
    private static final long serialVersionUID = 1L;

    UnavailableException(ErrorInfo info) {
        super(Status.ERR_UNAVAILABLE, info);
    }
}
