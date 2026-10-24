package ai.intellistream.tsdb.pulsar;

import ai.intellistream.tsdb.Status;
import ai.intellistream.tsdb.TsdbException;
import java.util.Set;

/**
 * Decides whether a failure is worth retrying.
 *
 * <p>Retryable failures leave the batch where it is: the sink backs off and sends the same batch again, in the same
 * order, until tsdb takes it. Nothing is acknowledged meanwhile, and the bounded in-flight window makes Pulsar stop
 * delivering. Permanent failures are a property of one record (bad data), so that record goes to the dead-letter
 * path and the rest of the batch proceeds.
 *
 * <p>The default is retryable for every code the sink does not know to be the record's fault. A wrong "permanent"
 * loses data (it ends in the dead-letter topic); a wrong "retryable" only stalls, visibly, until someone looks. That
 * is also how codes of later protocol versions (such as an "unavailable", "moved" or "overloaded" status) are
 * handled until this class names them: {@link #classify(int)} is the extension point, and the {@code permanentCodes}
 * and {@code retryableCodes} options override it without a new build.
 */
public class ErrorClassifier {
    /** What to do with a failure. */
    public enum Kind {
        /** Back off and send the same batch again. */
        RETRYABLE,
        /** The record is at fault: dead-letter it, keep going. */
        PERMANENT
    }

    private final Set<Integer> permanent;
    private final Set<Integer> retryable;

    /**
     * A classifier with overrides.
     *
     * @param permanent codes to treat as permanent whatever the default says
     * @param retryable codes to treat as retryable whatever the default says
     */
    public ErrorClassifier(Set<Integer> permanent, Set<Integer> retryable) {
        this.permanent = Set.copyOf(permanent);
        this.retryable = Set.copyOf(retryable);
    }

    /**
     * Classifies a tsdb status code.
     *
     * @param status a negative {@link Status} code
     * @return its kind, after the configured overrides
     */
    public Kind classify(int status) {
        if (retryable.contains(status))
            return Kind.RETRYABLE;
        if (permanent.contains(status))
            return Kind.PERMANENT;
        return defaultKind(status);
    }

    /**
     * Classifies an exception thrown while writing.
     *
     * @param e the failure
     * @return {@link Kind#PERMANENT} for arguments the protocol cannot carry and permanent status codes, otherwise
     *     {@link Kind#RETRYABLE}
     */
    public Kind classify(Throwable e) {
        if (e instanceof TsdbException)
            return classify(((TsdbException) e).status());
        if (e instanceof IllegalArgumentException)
            return Kind.PERMANENT; /* rejected by the client before sending, e.g. a name over 65535 bytes */
        return Kind.RETRYABLE;
    }

    /**
     * The built-in decision per code; see the REPORT and README for the reasoning.
     *
     * @param status a negative status code
     * @return its kind without overrides
     */
    protected Kind defaultKind(int status) {
        switch (status) {
        case Status.ERR_INVALID:          /* type mismatch, bad name, value of the wrong type */
        case Status.ERR_NOTFOUND:         /* unknown series and auto-create off */
        case Status.ERR_TOO_MANY_BUCKETS: /* never from a write; a query bug, not a server condition */
            return Kind.PERMANENT;
        case Status.ERR_IO:       /* connection lost, or the volume's disk (NFS) is unavailable */
        case Status.ERR_NOMEM:    /* the server is short of memory: busy */
        case Status.ERR_CORRUPT:  /* malformed reply (the connection is closed) or a damaged part on the server */
        case Status.ERR_LOCKED:   /* another process holds the directory: an operator is at work */
        case Status.ERR_STOPPED:  /* the server is shutting down */
        case Status.ERR_READONLY: /* a follower, or a writer that is failing over */
        default:                  /* codes of newer servers: stall rather than drop */
            return Kind.RETRYABLE;
        }
    }
}
