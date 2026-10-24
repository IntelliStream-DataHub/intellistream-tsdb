package ai.intellistream.tsdb;

/** The order of raw points and of buckets, by timestamp. */
public enum Order {
    /** Oldest first (the default). */
    ASC,
    /** Newest first. With a limit, the server reads backwards, so the newest N points cost about N rows. */
    DESC
}
