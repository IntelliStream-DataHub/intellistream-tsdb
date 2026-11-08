package ai.intellistream.tsdb;

/** Constants of the wire protocol; the authoritative description is {@code include/tsdb_protocol.h}. */
final class Protocol {
    /* HELLO answers the lower of both versions; 12 (login) is the floor, 13 adds the HA bodies, 14 QUERY_LATEST */
    static final int VERSION = 14;
    static final int MIN_VERSION = 12;     /* the oldest server the client connects to */
    static final int DEFAULT_PORT = 7070;
    static final int MAX_FRAME = 256 << 20;
    static final int HEADER = 9;           /* u32 length, u32 id, u8 op */
    static final int REPLY_HEADER = 13;    /* header + i32 status */
    static final long MANY_SERIES = -1L;   /* u64 0xffffffffffffffff: the series field of a request with a series per row */
    static final int BUCKET_ROW = 64;      /* + f64 min, f64 max, i64 isum, i64 imin, i64 imax instead */
    static final int VALUES_HEADER = 16;   /* INSERT_VALUES: u32 n, u8 type, u8 flags, u16 0, u64 series */
    static final int REPLACE_HEADER = 32;  /* REPLACE_RANGE: VALUES_HEADER, then i64 start, i64 end (v7) */
    static final int SPREAD = 1;           /* QUERY_BUCKETS/QUERY_BOUNDS fields: the spread follows each bucket (v8) */
    static final int SPREAD_ROW = 104;     /* BUCKET_ROW + f64 range, var_pop, var_samp, stddev_pop, stddev_samp */
    static final int TWA_HEADER = 60;      /* QUERY_TWA before the bounds (v6) */
    static final int TWA_ROW = 40;         /* i64 start, f64 twa, f64 area, i64 covered_ms, u64 count */
    static final int STATS_REPLY = 88;     /* LAST_STATS: f64 elapsed_s, 10 x u64 */
    static final int VALID = 1;            /* flags: a validity bitmap follows the values */
    static final int PACKED = 2;           /* flags: u32 raw_len and a zstd frame of the packed arrays (v3) */
    /* QUERY_MULTI (v10): u8 op, u8 flags, u16 threads, u32 n, u64 max_rows, u32 tlen, u32 0, then the arrays */
    static final int MULTI_HEADER = 24;
    static final int MULTI_MAX_SERIES = 65536;
    static final int MULTI_END = 0xffffffff;  /* the index of the reply's last frame: u32 series answered */
    static final int MULTI_PER_START = 1;
    static final int MULTI_PER_END = 2;
    static final int MULTI_PER_INTERVAL = 4;
    static final int MULTI_TRIM_LO = 8;
    static final int MULTI_TRIM_HI = 16;

    /* QUERY_LATEST (v14): u8 flags, u8 0, u16 threads, u32 n, i64 end, [i64 ends[n]], u64 ids[n]; the reply is frames of
     * i32 status, u32 first, u32 m, u8 last, 3 zero bytes, then m records of 24 bytes */
    static final int LATEST_HEADER = 16;
    static final int LATEST_MAX_IDS = 1 << 20;
    static final int LATEST_FRAME_IDS = 65536;
    static final int LATEST_PREFIX = 12;
    static final int LATEST_REC = 24;
    static final int LATEST_WITH_NULLS = 1;
    static final int LATEST_PER_END = 2;

    static final byte OP_REPLY = 0;
    static final byte OP_HELLO = 1;
    static final byte OP_SERIES = 2;
    static final byte OP_SERIES_NAME = 3;
    static final byte OP_INSERT = 4;
    static final byte OP_QUERY_RAW = 5;
    static final byte OP_COMMIT = 7;
    static final byte OP_FLUSH = 8;
    static final byte OP_OPTIMIZE = 9;
    static final byte OP_STATS = 10;
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
    static final byte OP_QUERY_MULTI = 23;    /* version 10 */
    static final byte OP_CANCEL = 24;         /* version 10: no reply; its id is the id of the query it stops */
    static final byte OP_DELETE_RANGE = 25;   /* version 7 */
    static final byte OP_REPLACE_RANGE = 26;
    static final byte OP_SET_RETENTION = 27;
    static final byte OP_GET_RETENTION = 28;
    static final byte OP_AUTH_START = 29;     /* version 12: login and users */
    static final byte OP_AUTH_CONTINUE = 30;
    static final byte OP_WHOAMI = 31;
    static final byte OP_USER_CREATE = 32;
    static final byte OP_USER_DROP = 33;
    static final byte OP_USER_ALTER = 34;
    static final byte OP_USER_PASSWORD = 35;
    static final byte OP_GRANT = 36;
    static final byte OP_REVOKE = 37;
    static final byte OP_USER_LIST = 38;
    static final byte OP_QUERY_LATEST = 39;   /* version 14: the newest value of each of many series */
    static final byte OP_TOPOLOGY = 48;       /* version 13: the nodes and volumes of the server */
    static final byte OP_STATUS = 49;         /* version 13: key=value lines, a superuser */

    static final int HELLO_AUTH = 1;          /* HELLO reply flags: login is required */
    static final String SCRAM_SHA256 = "SCRAM-SHA-256";
    static final int USER_SUPERUSER = 1;      /* user flags */

    private Protocol() {
    }
}
