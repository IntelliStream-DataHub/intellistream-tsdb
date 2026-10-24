package ai.intellistream.tsdb;

/** Constants of the wire protocol; the authoritative description is {@code include/tsdb_protocol.h}. */
final class Protocol {
    static final int VERSION = 1;
    static final int DEFAULT_PORT = 7070;
    static final int MAX_FRAME = 256 << 20;
    static final int HEADER = 9;           /* u32 length, u32 id, u8 op */
    static final int REPLY_HEADER = 13;    /* header + i32 status */
    static final int MANY_SERIES = 0xffffffff;
    static final int AGG_ROW = 32;         /* i64 start, u64 count, f64 sum, f32 min, f32 max */

    static final byte OP_REPLY = 0;
    static final byte OP_HELLO = 1;
    static final byte OP_SERIES = 2;
    static final byte OP_SERIES_NAME = 3;
    static final byte OP_INSERT = 4;
    static final byte OP_QUERY_RAW = 5;
    static final byte OP_QUERY_AGG = 6;
    static final byte OP_COMMIT = 7;
    static final byte OP_FLUSH = 8;
    static final byte OP_OPTIMIZE = 9;
    static final byte OP_STATS = 10;
    static final byte OP_QUERY_GROUP = 11;

    private Protocol() {
    }
}
