// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb;

import java.io.IOException;

/**
 * A failed request: the server answered with a negative status, or the connection failed.
 *
 * <p>{@link #status()} is one of the {@link Status} codes. A status from the server leaves the connection usable.
 * A failure of the connection itself ({@link Status#ERR_IO}, or {@link Status#ERR_CORRUPT} for a malformed reply)
 * closes it: later calls on the same client fail with {@link Status#ERR_IO}.
 */
public class TsdbException extends IOException {
    /** Version 1. */
    private static final long serialVersionUID = 1L;

    /** The negative status code. */
    private final int status;

    /** The body of an UNAVAILABLE, MOVED, PARTIAL or DROPPED reply; {@code null} for any other error. */
    private final transient ErrorInfo info;

    /**
     * An error reported by the server.
     *
     * @param status the negative status code
     */
    public TsdbException(int status) {
        this(status, null, null);
    }

    /**
     * An error with a more specific message.
     *
     * @param status the negative status code
     * @param detail what failed, or {@code null}
     * @param cause the underlying exception, or {@code null}
     */
    public TsdbException(int status, String detail, Throwable cause) {
        super(Status.message(status) + " (" + status + ")" + (detail != null ? ": " + detail : ""), cause);
        this.status = status;
        this.info = null;
    }

    /**
     * An error with the body of the reply (protocol 13).
     *
     * @param status the negative status code
     * @param info the decoded body
     */
    TsdbException(int status, ErrorInfo info) {
        super(Status.message(status) + " (" + status + ")" + describe(info));
        this.status = status;
        this.info = info;
    }

    private static String describe(ErrorInfo i) {
        if (!i.hasBody())
            return "";
        if (!i.unavailable().isEmpty()) {
            ErrorInfo.Unavailable u = i.unavailable().get(0);
            return ": volume " + (u.vol() == ErrorInfo.VOL_ANY ? "any" : String.valueOf(u.vol())) + ", reason "
                    + u.reason() + ", retry after " + i.retryAfterMs() + " ms";
        }
        if (!i.moved().isEmpty())
            return ": volume " + i.moved().get(0).vol() + " is on " + i.moved().get(0).addr();
        return "";
    }

    /**
     * The body of an UNAVAILABLE, MOVED, PARTIAL or DROPPED reply (protocol 13).
     *
     * @return the body, or {@code null} for any other error
     */
    public ErrorInfo info() {
        return info;
    }

    /**
     * The status code.
     *
     * @return a negative {@link Status} code
     */
    public int status() {
        return status;
    }

    /**
     * Whether the connection failed because the server did not answer in time (see
     * {@link TsdbClient#setRequestTimeout}). The client is closed then, like after any {@link Status#ERR_IO}.
     *
     * @return {@code true} if a wait for the server timed out
     */
    public boolean isTimeout() {
        return getCause() instanceof java.net.SocketTimeoutException;
    }
}
