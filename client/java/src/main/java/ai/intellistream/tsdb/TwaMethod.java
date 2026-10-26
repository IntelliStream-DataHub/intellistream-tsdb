package ai.intellistream.tsdb;

/** How a time-weighted average joins one reading to the next. */
public enum TwaMethod {
    /** Each value holds until the next reading (last observation carried forward); a null ends the previous value. */
    STEP,
    /** The value runs on a straight line from one reading to the next; a segment with a null end covers nothing. */
    LINEAR
}
