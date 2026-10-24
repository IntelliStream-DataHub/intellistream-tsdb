package ai.intellistream.tsdb;

/**
 * Status codes of the tsdb engine and server ({@code TSDB_ERR_*} in {@code include/tsdb.h}).
 *
 * <p>Every reply of the server starts with one of them; the client turns a non-zero status into a
 * {@link TsdbException}. {@link #ERR_IO} and {@link #ERR_CORRUPT} also stand for failures of the connection itself.
 */
public final class Status {
    /** Success. */
    public static final int OK = 0;
    /** I/O error on the server, or the connection failed or was lost. */
    public static final int ERR_IO = -1;
    /** The server ran out of memory. */
    public static final int ERR_NOMEM = -2;
    /** Corrupt data on the server, or a malformed reply (a protocol error). */
    public static final int ERR_CORRUPT = -3;
    /** An invalid argument, such as a negative interval or an unknown time zone. */
    public static final int ERR_INVALID = -4;
    /** The database directory is locked by another process. */
    public static final int ERR_LOCKED = -5;
    /** The series does not exist. */
    public static final int ERR_NOTFOUND = -6;
    /** An aggregate query would produce too many buckets. */
    public static final int ERR_TOO_MANY_BUCKETS = -7;
    /** A query was stopped, or the database is closing. */
    public static final int ERR_STOPPED = -8;
    /** The server serves a read-only database (or follows another writer). */
    public static final int ERR_READONLY = -9;

    private Status() {
    }

    /**
     * Describes a status code, as {@code tsdbc_strerror} does.
     *
     * @param status a status code
     * @return a short description; "unknown error" for codes this client does not know
     */
    public static String message(int status) {
        switch (status) {
        case OK: return "ok";
        case ERR_IO: return "I/O error (or connection lost)";
        case ERR_NOMEM: return "out of memory";
        case ERR_CORRUPT: return "corrupt data or protocol error";
        case ERR_INVALID: return "invalid argument";
        case ERR_LOCKED: return "database is locked by another process";
        case ERR_NOTFOUND: return "not found";
        case ERR_TOO_MANY_BUCKETS: return "too many buckets";
        case ERR_STOPPED: return "stopped";
        case ERR_READONLY: return "database is read only";
        default: return "unknown error";
        }
    }
}
