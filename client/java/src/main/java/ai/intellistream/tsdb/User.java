package ai.intellistream.tsdb;

import java.util.List;

/**
 * A user as {@link TsdbClient#whoami()} and {@link TsdbClient#userList()} show it.
 *
 * @param name the login name; "" from a server without login ({@code --no-auth})
 * @param superuser every right on every series, user management and the database-wide operations
 * @param grants the grants; a user's rights on a series are the union of those whose selector matches its name
 */
public record User(String name, boolean superuser, List<Grant> grants) {
    /**
     * Copies the grants.
     *
     * @param name the login name
     * @param superuser whether it is a superuser
     * @param grants the grants
     */
    public User {
        grants = List.copyOf(grants);
    }
}
