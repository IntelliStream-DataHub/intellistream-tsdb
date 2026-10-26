package ai.intellistream.tsdb;

/** Constants of the wire protocol; the authoritative description is {@code include/tsdb_protocol.h}. */
final class Protocol {
    /* HELLO answers the lower of both versions; each newer op is sent only to a server that named its version */
    static final int VERSION = 9;
    static final int DEFAULT_PORT = 7070;
    static final int MAX_FRAME = 256 << 20;
    static final int HEADER = 9;           /* u32 length, u32 id, u8 op */
    static final int REPLY_HEADER = 13;    /* header + i32 status */
    static final int MANY_SERIES = 0xffffffff;
    static final int AGG_ROW = 32;         /* version 1: i64 start, u64 count, f64 sum, f32 min, f32 max */
    static final int BUCKET_ROW = 64;      /* + f64 min, f64 max, i64 isum, i64 imin, i64 imax instead */
    static final int VALUES_HEADER = 16;   /* INSERT_VALUES: u32 n, u32 series, u8 type, u8 flags, u16 0, u32 0 */
    static final int REPLACE_HEADER = 32;  /* REPLACE_RANGE: VALUES_HEADER, then i64 start, i64 end (v7) */
    static final int SPREAD = 1;           /* QUERY_BUCKETS/QUERY_BOUNDS fields: the spread follows each bucket (v8) */
    static final int SPREAD_ROW = 104;     /* BUCKET_ROW + f64 range, var_pop, var_samp, stddev_pop, stddev_samp */
    static final int TWA_HEADER = 56;      /* QUERY_TWA before the bounds (v6) */
    static final int TWA_ROW = 40;         /* i64 start, f64 twa, f64 area, i64 covered_ms, u64 count */
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
    static final byte OP_QUERY_BOUNDS = 20;
    static final byte OP_QUERY_LTTB = 21;     /* version 5; order and limit version 9 */
    static final byte OP_QUERY_TWA = 22;      /* version 6 */
    static final byte OP_DELETE_RANGE = 25;   /* version 7 */
    static final byte OP_REPLACE_RANGE = 26;
    static final byte OP_SET_RETENTION = 27;
    static final byte OP_GET_RETENTION = 28;

    private Protocol() {
    }
}
