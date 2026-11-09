// SPDX-License-Identifier: Apache-2.0
package ai.intellistream.tsdb.pulsar;

/** A record tsdb will never take as it is: it goes to the dead-letter path. */
public class PoisonException extends Exception {
    private static final long serialVersionUID = 1L;
    /** The tsdb status, or 0 when the sink itself refused the record. */
    private final int status;

    /**
     * A record refused by the sink before it reached tsdb.
     *
     * @param reason what is wrong with it
     */
    public PoisonException(String reason) {
        this(reason, 0);
    }

    /**
     * A record refused by tsdb or the sink.
     *
     * @param reason what is wrong with it
     * @param status the tsdb status code, or 0
     */
    public PoisonException(String reason, int status) {
        super(reason);
        this.status = status;
    }

    /**
     * The tsdb status code.
     *
     * @return a negative status, or 0 when the sink refused the record itself
     */
    public int status() {
        return status;
    }
}
