package ai.intellistream.tsdb;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;

/**
 * How {@link TsdbClient#connect(String, int, ConnectOptions)} connects: the login and the timeouts. Immutable: each
 * setter returns new options.
 *
 * <pre>{@code
 * ConnectOptions o = ConnectOptions.defaults().user("acme/ingest").password("s3cret").requireAuth(true);
 * try (TsdbClient c = TsdbClient.connect("tsdb.example", 7070, o)) { ... }
 * }</pre>
 *
 * <p>The password is its UTF-8 bytes, without normalization (ASCII passwords are safest). It never travels: the
 * login proves it with SCRAM-SHA-256, and the client checks that the server holds the user's verifier before the
 * connect returns. The options keep it for reconnects; {@link #toString()} does not show it.
 */
public final class ConnectOptions {
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final ConnectOptions DEFAULTS =
            new ConnectOptions(null, null, null, false, CONNECT_TIMEOUT, Duration.ZERO);

    private final String user;
    private final byte[] password;
    private final Path passwordFile;
    private final boolean requireAuth;
    private final Duration connectTimeout;
    private final Duration requestTimeout;

    private ConnectOptions(String user, byte[] password, Path passwordFile, boolean requireAuth,
            Duration connectTimeout, Duration requestTimeout) {
        this.user = user;
        this.password = password;
        this.passwordFile = passwordFile;
        this.requireAuth = requireAuth;
        this.connectTimeout = connectTimeout;
        this.requestTimeout = requestTimeout;
    }

    /**
     * No login, a 10 second connect timeout, requests that wait forever: what {@link TsdbClient#connect(String, int)}
     * does.
     *
     * @return the options
     */
    public static ConnectOptions defaults() {
        return DEFAULTS;
    }

    /**
     * The user to log in as, when the server asks for a login.
     *
     * @param name the user, 1 to 255 bytes without {@code ,}, {@code =} or NUL
     * @return the options
     */
    public ConnectOptions user(String name) {
        return new ConnectOptions(Objects.requireNonNull(name, "user"), password, passwordFile, requireAuth,
                connectTimeout, requestTimeout);
    }

    /**
     * The user's password.
     *
     * @param password 1 to {@link #PASSWORD_MAX} bytes of UTF-8 without NUL
     * @return the options
     * @throws IllegalArgumentException for a password outside that rule, or one that is not valid UTF-16 (an
     *     unpaired surrogate): it would not be the password the user typed
     */
    public ConnectOptions password(String password) {
        return password(Objects.requireNonNull(password, "password").toCharArray());
    }

    /**
     * The user's password, from an array the caller may clear afterwards (the options keep their own copy).
     *
     * @param password 1 to {@link #PASSWORD_MAX} bytes of UTF-8 without NUL
     * @return the options
     * @throws IllegalArgumentException as {@link #password(String)}
     */
    public ConnectOptions password(char[] password) {
        byte[] pw = Scram.utf8(Objects.requireNonNull(password, "password"));
        try {
            Scram.checkPassword(pw);
        } catch (IllegalArgumentException e) {
            Arrays.fill(pw, (byte) 0);
            throw e;
        }
        return new ConnectOptions(user, pw, passwordFile, requireAuth, connectTimeout, requestTimeout);
    }

    /**
     * The longest password in bytes, in every client and the CLI (design-auth.md §6); a password is at least 1 byte
     * and holds no NUL.
     */
    public static final int PASSWORD_MAX = 1023;

    /**
     * Reads the password from the first line of this file at every connect, without the LF or CR LF that ends it,
     * when no {@link #password(String) password} is given. A file that cannot be read, or whose first line is not a
     * password (empty, over {@link #PASSWORD_MAX} bytes), fails the connect with {@link Status#ERR_INVALID} naming the
     * file: a mistake in the setup, not a network error to retry.
     *
     * @param file the file, readable by this process only, ideally
     * @return the options
     */
    public ConnectOptions passwordFile(Path file) {
        return new ConnectOptions(user, password, Objects.requireNonNull(file, "file"), requireAuth, connectTimeout,
                requestTimeout);
    }

    /**
     * Refuses a server that does not ask for a login ({@code --no-auth}), which would otherwise be used without one:
     * the connect fails with {@link Status#ERR_AUTH}. Off by default.
     *
     * @param require whether a login is required
     * @return the options
     */
    public ConnectOptions requireAuth(boolean require) {
        return new ConnectOptions(user, password, passwordFile, require, connectTimeout, requestTimeout);
    }

    /**
     * How long to wait for each address of the host, and then for the HELLO and the login (10 seconds by default);
     * see {@link TsdbClient#connect(String, int, Duration, Duration)}.
     *
     * @param timeout not negative; zero waits as long as the operating system does for the connection, and the
     *     request timeout for the login
     * @return the options
     */
    public ConnectOptions connectTimeout(Duration timeout) {
        return new ConnectOptions(user, password, passwordFile, requireAuth, check(timeout), requestTimeout);
    }

    /**
     * How long any one wait for the server may take after the login; zero (the default) waits forever. See
     * {@link TsdbClient#connect(String, int, Duration, Duration)}.
     *
     * @param timeout not negative
     * @return the options
     */
    public ConnectOptions requestTimeout(Duration timeout) {
        return new ConnectOptions(user, password, passwordFile, requireAuth, connectTimeout, check(timeout));
    }

    private static Duration check(Duration d) {
        if (Objects.requireNonNull(d, "timeout").isNegative())
            throw new IllegalArgumentException("negative timeout " + d);
        return d;
    }

    String user() {
        return user;
    }

    boolean requireAuth() {
        return requireAuth;
    }

    Duration connectTimeout() {
        return connectTimeout;
    }

    Duration requestTimeout() {
        return requestTimeout;
    }

    /** Refuses a user without a password before anything is sent. */
    void validate() {
        if (user != null && password == null && passwordFile == null)
            throw new IllegalArgumentException("a user without a password or a password file");
    }

    /**
     * The password bytes, a copy the caller clears: given, or the first line of the password file.
     *
     * @throws TsdbException with {@link Status#ERR_INVALID}, naming the file, if it cannot be read or its first line
     *     is not a password (as the C client)
     */
    byte[] passwordBytes() throws TsdbException {
        if (password != null)
            return password.clone();
        byte[] all;
        try {
            all = Files.readAllBytes(passwordFile);
        } catch (IOException e) {
            throw new TsdbException(Status.ERR_INVALID, "password file " + passwordFile + ": " + e, e);
        }
        int n = 0;
        while (n < all.length && all[n] != '\n')
            n++;
        int end = n < all.length && n > 0 && all[n - 1] == '\r' ? n - 1 : n; // a '\r' ends a line only before '\n'
        byte[] pw = Arrays.copyOf(all, end);
        Arrays.fill(all, (byte) 0);
        try {
            Scram.checkPassword(pw);
        } catch (IllegalArgumentException e) {
            Arrays.fill(pw, (byte) 0);
            throw new TsdbException(Status.ERR_INVALID, "password file " + passwordFile
                    + ": no password in its first line (1 to " + PASSWORD_MAX + " bytes)", null);
        }
        return pw;
    }

    /** Everything but the password. */
    @Override
    public String toString() {
        return "ConnectOptions[user=" + user + ", password=" + (password != null ? "(hidden)" : "null")
                + ", passwordFile=" + passwordFile + ", requireAuth=" + requireAuth + ", connectTimeout="
                + connectTimeout + ", requestTimeout=" + requestTimeout + "]";
    }
}
