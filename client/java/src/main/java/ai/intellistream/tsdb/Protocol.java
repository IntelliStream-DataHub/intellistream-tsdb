package ai.intellistream.tsdb;

/** Constants of the wire protocol; the authoritative description is {@code include/tsdb_protocol.h}. */
final class Protocol {
    static final int VERSION = 3;
    static final int DEFAULT_PORT = 7070;
    static final int MAX_FRAME = 256 << 20;
    static final int HEADER = 9;           /* u32 length, u32 id, u8 op */
    static final int REPLY_HEADER = 13;    /* header + i32 status */
    static final int MANY_SERIES = 0xffffffff;
    static final int AGG_ROW = 32;         /* version 1: i64 start, u64 count, f64 sum, f32 min, f32 max */
    static final int BUCKET_ROW = 64;      /* + f64 min, f64 max, i64 isum, i64 imin, i64 imax instead */
    static final int VALUES_HEADER = 16;   /* INSERT_VALUES: u32 n, u32 series, u8 type, u8 flags, u16 0, u32 0 */
    static final int STATS_REPLY = 88;     /* LAST_STATS: f64 elapsed_s, 10 x u64 */
    static final int VALID = 1;            /* flags: a validity bitmap follows the values */
    static final int PACKED = 2;           /* flags: u32 raw_len and a zstd frame of the packed arrays (v3) */

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
    static final byte OP_SERIES_CREATE = 12;
    static final byte OP_SERIES_TYPE = 13;
    static final byte OP_INSERT_VALUES = 14;
    static final byte OP_QUERY_VALUES = 15;
    static final byte OP_STATE = 16;
    static final byte OP_STATE_NAME = 17;
    static final byte OP_QUERY_BUCKETS = 18;
    static final byte OP_LAST_STATS = 19;

    private Protocol() {
    }
}
