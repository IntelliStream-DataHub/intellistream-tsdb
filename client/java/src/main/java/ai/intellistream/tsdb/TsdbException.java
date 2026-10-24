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
    }

    /**
     * The status code.
     *
     * @return a negative {@link Status} code
     */
    public int status() {
        return status;
    }
}
