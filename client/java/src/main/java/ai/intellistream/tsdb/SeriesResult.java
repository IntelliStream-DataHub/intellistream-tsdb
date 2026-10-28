package ai.intellistream.tsdb;

/**
 * The result of one series of a query over many series: its value, or the error the single call would have given
 * for it. One series failing does not fail the others.
 *
 * @param <T> the result of the single call
 */
public final class SeriesResult<T> {
    private final T value;
    private final int status;
    private final String detail;

    private SeriesResult(T value, int status, String detail) {
        this.value = value;
        this.status = status;
        this.detail = detail;
    }

    /**
     * A result.
     *
     * @param <T> the result type
     * @param value the series' result
     * @return the result
     */
    public static <T> SeriesResult<T> of(T value) {
        return new SeriesResult<>(value, Status.OK, null);
    }

    /**
     * An error.
     *
     * @param <T> the result type
     * @param status the series' status, one of {@link Status}, not {@link Status#OK}
     * @param detail what went wrong, or null for the status' message
     * @return the result
     */
    public static <T> SeriesResult<T> failed(int status, String detail) {
        if (status == Status.OK)
            throw new IllegalArgumentException("a failed result needs an error status");
        return new SeriesResult<>(null, status, detail);
    }

    /**
     * Whether the series has a result.
     *
     * @return true when {@link #status} is {@link Status#OK}
     */
    public boolean ok() {
        return status == Status.OK;
    }

    /**
     * The series' status.
     *
     * @return {@link Status#OK} or the error of the single call
     */
    public int status() {
        return status;
    }

    /**
     * The series' result.
     *
     * @return the result
     * @throws TsdbException with the series' status if it failed
     */
    public T get() throws TsdbException {
        if (status != Status.OK)
            throw new TsdbException(status, detail, null);
        return value;
    }

    @Override
    public String toString() {
        return status == Status.OK ? "SeriesResult[" + value + "]"
                : "SeriesResult[" + Status.message(status) + (detail != null ? ": " + detail : "") + "]";
    }
}
