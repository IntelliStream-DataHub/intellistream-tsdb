/*
 * `tsdb <dir> user ...`: the users of tsdbd's login, edited in USERS without opening the engine, so it runs beside
 * a running tsdbd (which reads the change within a second). Whoever can write the directory holds every right
 * already, so these act as the operator (A12): the first superuser is made here. Passwords never come from argv
 * (visible in ps): from a prompt without echo, --password-file or TSDB_PASSWORD; the verifier is computed here and
 * only it is stored (A18).
 */
#define _GNU_SOURCE
#include "../server/users.h"
#include "../client/tsdb_client.h"
#include "../client/tsdb_crypto.h"
#include "../src/compat.h" /* tsdb_fsync (macOS: with the drive's cache flush) */
#include "tsdb.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <termios.h>
#include <unistd.h>

int user_main(const char *dir, int argc, char **argv);

static int user_usage(void)
{
    fprintf(stderr,
            "usage: tsdb <dir> user add NAME [--superuser] [--password-file FILE] [--iterations N]\n"
            "       tsdb <dir> user passwd NAME [--password-file FILE] [--iterations N]\n"
            "       tsdb <dir> user drop NAME\n"
            "       tsdb <dir> user alter NAME --superuser|--no-superuser\n"
            "       tsdb <dir> user grant NAME RIGHTS SELECTOR\n"
            "       tsdb <dir> user revoke NAME RIGHTS SELECTOR\n"
            "       tsdb <dir> user list\n"
            "       tsdb <dir> user compact\n"
            "       tsdb user add|passwd|drop|alter|grant|revoke|list|whoami ... --server HOST:PORT --login NAME\n"
            "            [--login-password-file FILE]   (through a running tsdbd, as user NAME)\n"
            "\n"
            "  RIGHTS    read, insert, delete, admin, write (insert and delete), comma-separated\n"
            "  SELECTOR  all, prefix:P, name:N or glob:G (* any bytes, ? one byte, \\ the next byte as is)\n"
            "  The password is read from FILE (its first line), else from TSDB_PASSWORD, else from the\n"
            "  terminal without echo; never from the command line. The login's password: FILE, else\n"
            "  TSDB_LOGIN_PASSWORD, else the terminal.\n"
            "  Run as the owner of <dir>, or as root: root on a <dir> another user owns becomes that\n"
            "  user first (and reads FILE as that user). A USERS that is a link is refused.\n");
    return 2;
}

static int fail(const char *what, int rc)
{
    fprintf(stderr, "tsdb: %s: %s\n", what, tsdb_strerror(rc));
    return 1;
}

typedef char password[TSDB_PASSWORD_MAX + 1];

/* A password from the first line of f, the rule of every client (tsdb_read_password): -1 with a line on stderr
 * naming `what` when there is none. */
static int read_line(FILE *f, const char *what, password buf)
{
    int rc = tsdb_read_password(f, buf);
    if (rc == -1)
        fprintf(stderr, "tsdb: %s: %s\n", what, strerror(errno));
    else if (rc)
        fprintf(stderr, "tsdb: %s: no password in its first line (1 to %d bytes)\n", what, TSDB_PASSWORD_MAX);
    return rc ? -1 : 0;
}

/* One line from the terminal without echo. */
static int prompt(FILE *tty, const char *what, password buf)
{
    struct termios old, quiet;
    int fd = fileno(tty);
    bool echo_off = tcgetattr(fd, &old) == 0;
    if (echo_off) {
        quiet = old;
        quiet.c_lflag &= ~(tcflag_t)ECHO;
        echo_off = tcsetattr(fd, TCSAFLUSH, &quiet) == 0;
    }
    fprintf(tty, "%s", what);
    fflush(tty);
    int rc = read_line(tty, "the terminal", buf);
    if (echo_off)
        tcsetattr(fd, TCSAFLUSH, &old);
    fprintf(tty, "\n");
    return rc;
}

/* A password from file, else the environment variable env, else the terminal (twice when it is a new one). */
static int get_password_from(const char *file, const char *env, const char *name, bool confirm, password pw)
{
    if (file) {
        FILE *f = strcmp(file, "-") == 0 ? stdin : fopen(file, "r");
        if (!f) {
            fprintf(stderr, "tsdb: %s: %s\n", file, strerror(errno));
            return -1;
        }
        int rc = read_line(f, file, pw);
        if (f != stdin)
            fclose(f);
        if (rc != 0)
            return -1;
    } else if (getenv(env)) {
        size_t n = strlen(getenv(env));
        if (n == 0 || n > TSDB_PASSWORD_MAX) {
            fprintf(stderr, "tsdb: %s: a password is 1 to %d bytes\n", env, TSDB_PASSWORD_MAX);
            return -1;
        }
        memcpy(pw, getenv(env), n + 1);
    } else {
        FILE *tty = fopen("/dev/tty", "r+");
        if (!tty) {
            fprintf(stderr, "tsdb: no terminal for the password of %s: use a password file or %s\n", name, env);
            return -1;
        }
        password again;
        char what[200];
        snprintf(what, sizeof what, "Password for %s: ", name);
        int rc = prompt(tty, what, pw);
        if (rc == 0 && confirm)
            rc = prompt(tty, "Again: ", again);
        fclose(tty);
        if (rc == 0 && confirm && strcmp(pw, again) != 0) {
            fprintf(stderr, "tsdb: the passwords differ\n");
            rc = -1;
        }
        tsdb_wipe(again, sizeof again);
        if (rc != 0)
            return -1;
    }
    return 0;
}

static int get_password(const char *file, const char *name, password pw)
{
    return get_password_from(file, "TSDB_PASSWORD", name, true, pw);
}

/* The verifier of a password with a fresh random salt (A18: the server never sees the password). */
static int make_verifier(const char *file, const char *name, uint32_t iterations, user_verifier *v)
{
    password pw;
    if (get_password(file, name, pw) != 0)
        return -1;
    memset(v, 0, sizeof *v);
    v->iterations = iterations;
    v->salt_len = SALT_MIN;
    int rc = tsdb_random(v->salt, v->salt_len);
    if (rc == 0)
        tsdb_scram_keys(pw, strlen(pw), v->salt, v->salt_len, iterations, NULL, v->stored_key, v->server_key);
    else
        fprintf(stderr, "tsdb: getrandom: %s\n", strerror(errno));
    tsdb_wipe(pw, sizeof pw);
    return rc;
}

static const char *kind_tag(uint8_t kind)
{
    static const char *t[] = {"all", "prefix:", "name:", "glob:"};
    return kind <= SEL_GLOB ? t[kind] : NULL;
}

/* As rights_parse reads it back (and every client prints it). */
static void print_rights(FILE *f, uint8_t r)
{
    char b[RIGHTS_TEXT_MAX];
    fputs(rights_text(r, b), f);
}

static void print_sel(FILE *f, uint8_t kind, const char *sel, size_t len)
{
    if (kind_tag(kind))
        fputs(kind_tag(kind), f);
    else
        fprintf(f, "kind#%u:", kind); /* a later server's kind, as the clients show it */
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)sel[i];
        if (c < 0x20 || c == 0x7f)
            fprintf(f, "\\x%02x", c); /* a glob shows its own escapes as stored */
        else
            fputc(c, f);
    }
}

/* The rights NAME holds on exactly that (normalized) selector, 0 for none. */
static uint8_t held_on(const users *us, const char *name, uint8_t kind, const char *sel, size_t len)
{
    const user *u = users_find(us, name);
    for (uint32_t i = 0; u && i < u->ng; i++)
        if (u->g[i].kind == kind && u->g[i].len == len && memcmp(u->g[i].sel, sel, len) == 0)
            return u->g[i].rights;
    return 0;
}

static int cmd_list(const users *us)
{
    for (uint32_t i = 0; i < us->n; i++) {
        const user *u = us->u[i];
        printf("%s\tuid %u%s%s\n", u->name, u->uid, (u->flags & USER_SUPERUSER) ? "\tsuperuser" : "",
               u->has_verifier ? "" : "\t(no password)");
        for (uint32_t j = 0; j < u->ng; j++) {
            printf("\t");
            print_rights(stdout, u->g[j].rights);
            printf("\t");
            print_sel(stdout, u->g[j].kind, u->g[j].sel, u->g[j].len);
            printf("\n");
        }
    }
    return 0;
}

static int ensure_dir(const char *dir)
{
    struct stat st;
    if (stat(dir, &st) == 0)
        return S_ISDIR(st.st_mode) ? 0 : -1;
    if (errno != ENOENT || mkdir(dir, 0755) != 0) {
        fprintf(stderr, "tsdb: %s: %s\n", dir, strerror(errno));
        return -1;
    }
    /* the new directory's entry is durable before USERS in it is acknowledged */
    char parent[4096];
    snprintf(parent, sizeof parent, "%s", dir);
    char *slash = strrchr(parent, '/');
    if (slash && slash != parent)
        *slash = '\0';
    else
        snprintf(parent, sizeof parent, "%s", slash ? "/" : ".");
    int fd = open(parent, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    int rc = fd < 0 || tsdb_fsync(fd) != 0 ? -1 : 0;
    if (fd >= 0)
        close(fd);
    if (rc)
        fprintf(stderr, "tsdb: syncing %s: %s\n", parent, strerror(errno));
    return rc;
}

/* The online verbs: the same changes through a running tsdbd, as the user who logs in (A9 decides what that user
 * may change; the server answers TSDB_ERR_DENIED otherwise). For tenant admins without shell access. */
static int user_online(const char *server, const char *login, const char *login_pwfile, const char *verb,
                       const char *const *args, int super, const char *pwfile, uint32_t iterations)
{
    char host[256];
    const char *colon = strrchr(server, ':');
    char *end;
    long port = colon ? strtol(colon + 1, &end, 10) : 0;
    if (!colon || colon == server || (size_t)(colon - server) >= sizeof host || *end || port < 1 || port > 65535) {
        fprintf(stderr, "tsdb: --server HOST:PORT\n");
        return 2;
    }
    memcpy(host, server, (size_t)(colon - server));
    host[colon - server] = '\0';
    if (host[0] == '[' && host[strlen(host) - 1] == ']') { /* [::1]:7070 */
        memmove(host, host + 1, strlen(host));
        host[strlen(host) - 1] = '\0';
    }
    password lpw;
    if (get_password_from(login_pwfile, "TSDB_LOGIN_PASSWORD", login, false, lpw) != 0)
        return 1;
    tsdbc_options o = {.user = login, .password = lpw, .require_auth = 1};
    tsdbc *c;
    int rc = tsdbc_connect_opts(host, (int)port, &o, &c);
    tsdb_wipe(lpw, sizeof lpw);
    if (rc) {
        fprintf(stderr, "tsdb: %s as %s: %s\n", server, login, tsdbc_strerror(rc));
        return 1;
    }
    const char *name = args[0];
    if (strcmp(verb, "list") == 0 || strcmp(verb, "whoami") == 0) {
        tsdbc_user *u;
        size_t n = 1;
        rc = strcmp(verb, "list") == 0 ? tsdbc_user_list(c, &u, &n) : tsdbc_whoami(c, &u);
        for (size_t i = 0; rc == 0 && i < n; i++) {
            printf("%s%s\n", u[i].name, (u[i].flags & USER_SUPERUSER) ? "\tsuperuser" : "");
            for (uint32_t j = 0; j < u[i].ngrants; j++) {
                printf("\t");
                print_rights(stdout, u[i].grants[j].rights);
                printf("\t");
                print_sel(stdout, u[i].grants[j].kind, u[i].grants[j].selector, strlen(u[i].grants[j].selector));
                printf("\n");
            }
        }
        if (rc == 0)
            tsdbc_users_free(u, n);
    } else if (strcmp(verb, "add") == 0 || strcmp(verb, "passwd") == 0) {
        password pw;
        if (get_password(pwfile, name, pw) != 0) {
            tsdbc_close(c);
            return 1;
        }
        rc = strcmp(verb, "add") == 0 ? tsdbc_user_create(c, name, pw, iterations, super == 1 ? USER_SUPERUSER : 0)
                                      : tsdbc_user_password(c, name, pw, iterations);
        tsdb_wipe(pw, sizeof pw);
    } else if (strcmp(verb, "drop") == 0) {
        rc = tsdbc_user_drop(c, name);
    } else if (strcmp(verb, "alter") == 0) {
        rc = tsdbc_user_alter(c, name, super == 1 ? USER_SUPERUSER : 0);
    } else { /* grant, revoke */
        uint8_t rights = 0, kind;
        const char *sel;
        size_t len;
        if (rights_parse(args[1], &rights) != 0 || !rights || sel_parse(args[2], &kind, &sel, &len) != 0 ||
            memchr(sel, 0, len)) {
            fprintf(stderr, "tsdb: %s %s: rights are read, insert, delete, admin and write, comma-separated; a "
                            "selector is all, prefix:P, name:N or glob:G\n", args[1], args[2]);
            tsdbc_close(c);
            return 1;
        }
        if (kind == SEL_PREFIX && len && sel[len - 1] != '/')
            fprintf(stderr, "tsdb: warning: prefix:%.*s also matches names such as %.*sX...; tenant prefixes end in "
                            "'/'\n", (int)len, sel, (int)len, sel);
        char *s = strndup(sel, len);
        rc = !s ? TSDB_ERR_NOMEM
             : strcmp(verb, "grant") == 0 ? tsdbc_grant(c, name, kind, s, rights)
                                          : tsdbc_revoke(c, name, kind, s, rights);
        free(s);
        if (rc == TSDB_ERR_NOTFOUND && strcmp(verb, "revoke") == 0) {
            fprintf(stderr, "tsdb: no user %s, or %s holds none of these rights on %s: nothing revoked\n", name, name,
                    args[2]);
            tsdbc_close(c);
            return 1;
        }
    }
    tsdbc_close(c);
    if (rc == TSDB_ERR_NOTFOUND && name)
        fprintf(stderr, "tsdb: no user %s\n", name);
    else if (rc == TSDB_ERR_DENIED)
        fprintf(stderr, "tsdb: %s: %s may not do that\n", verb, login);
    else if (rc)
        fprintf(stderr, "tsdb: %s: %s\n", verb, tsdbc_strerror(rc));
    return rc ? 1 : 0;
}

int user_main(const char *dir, int argc, char **argv)
{
    if (argc < 1)
        return user_usage();
    const char *verb = argv[0], *name = argc > 1 ? argv[1] : NULL;
    const char *pwfile = NULL, *server = NULL, *login = NULL, *login_pwfile = NULL;
    long iterations = ITER_MIN;
    int super = -1; /* --superuser 1, --no-superuser 0 */
    int pos = 0;
    const char *args[3] = {0};
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--superuser") == 0) {
            super = 1;
        } else if (strcmp(argv[i], "--no-superuser") == 0) {
            super = 0;
        } else if (strcmp(argv[i], "--password-file") == 0 && i + 1 < argc) {
            pwfile = argv[++i];
        } else if (strcmp(argv[i], "--server") == 0 && i + 1 < argc) {
            server = argv[++i];
        } else if (strcmp(argv[i], "--login") == 0 && i + 1 < argc) {
            login = argv[++i];
        } else if (strcmp(argv[i], "--login-password-file") == 0 && i + 1 < argc) {
            login_pwfile = argv[++i];
        } else if (strcmp(argv[i], "--iterations") == 0 && i + 1 < argc) {
            char *end;
            iterations = strtol(argv[++i], &end, 10);
            if (*end || iterations < ITER_MIN || iterations > 100000000) {
                fprintf(stderr, "tsdb: --iterations: %d to 100000000\n", ITER_MIN);
                return 2;
            }
        } else if (strncmp(argv[i], "--password", 10) == 0 && strcmp(argv[i], "--password-file") != 0) {
            fprintf(stderr, "tsdb: a password is never taken from the command line (ps shows it to every user):\n"
                            "      use --password-file FILE, TSDB_PASSWORD or the prompt\n");
            return 2;
        } else if (argv[i][0] == '-' && argv[i][1] == '-') {
            return user_usage();
        } else if (pos < 3) {
            args[pos++] = argv[i];
        } else {
            return user_usage();
        }
    }
    /* online (--server): no directory, and compact is the operator's; whoami only there */
    if (!server != !!dir || !server != !login || (!server && login_pwfile) ||
        strcmp(verb, server ? "compact" : "whoami") == 0)
        return user_usage();
    bool listing = strcmp(verb, "list") == 0 || strcmp(verb, "compact") == 0 || strcmp(verb, "whoami") == 0;
    int want = listing ? 0 : (strcmp(verb, "grant") == 0 || strcmp(verb, "revoke") == 0) ? 3 : 1;
    bool known = listing || strcmp(verb, "add") == 0 || strcmp(verb, "passwd") == 0 || strcmp(verb, "drop") == 0 ||
                 strcmp(verb, "alter") == 0 || strcmp(verb, "grant") == 0 || strcmp(verb, "revoke") == 0;
    if (!known || pos != want || (super >= 0 && strcmp(verb, "add") != 0 && strcmp(verb, "alter") != 0) ||
        (super == 0 && strcmp(verb, "add") == 0) || (super < 0 && strcmp(verb, "alter") == 0) ||
        ((pwfile || iterations != ITER_MIN) && strcmp(verb, "add") != 0 && strcmp(verb, "passwd") != 0))
        return user_usage();
    name = args[0];
    if (name && !user_name_valid(name, strlen(name))) {
        fprintf(stderr, "tsdb: %s: a user name is 1 to %d bytes of A-Z a-z 0-9 . _ - @ / : +, not starting with /\n",
                name, USER_NAME_MAX);
        return 1;
    }
    if (server)
        return user_online(server, login, login_pwfile, verb, args, super, pwfile, (uint32_t)iterations);
    if (strcmp(verb, "add") == 0 ? ensure_dir(dir) != 0 : access(dir, F_OK) != 0) {
        if (strcmp(verb, "add") != 0)
            fprintf(stderr, "tsdb: %s does not exist\n", dir);
        return 1;
    }
    users us;
    int rc = users_open(&us, dir, !(strcmp(verb, "list") == 0));
    if (rc) {
        users_close(&us);
        return fail("USERS", rc);
    }
    int ret = 0;
    user_verifier v;
    if (strcmp(verb, "list") == 0) {
        ret = cmd_list(&us);
    } else if (strcmp(verb, "compact") == 0) {
        rc = users_compact(&us);
    } else if (strcmp(verb, "add") == 0) {
        uint32_t uid;
        if (users_find(&us, name)) {
            fprintf(stderr, "tsdb: user %s exists\n", name);
            ret = 1;
        } else if (make_verifier(pwfile, name, (uint32_t)iterations, &v) != 0) {
            ret = 1;
        } else if ((rc = users_create(&us, 0, name, super == 1 ? USER_SUPERUSER : 0, &v, &uid)) == 0) {
            printf("%s\tuid %u%s\n", name, uid, super == 1 ? "\tsuperuser" : "");
        }
    } else if (strcmp(verb, "passwd") == 0) {
        if (!users_find(&us, name))
            rc = TSDB_ERR_NOTFOUND;
        else if (make_verifier(pwfile, name, (uint32_t)iterations, &v) != 0)
            ret = 1;
        else
            rc = users_set_verifier(&us, 0, name, &v);
    } else if (strcmp(verb, "drop") == 0 || strcmp(verb, "alter") == 0) {
        const user *u = users_find(&us, name);
        bool last = u && (u->flags & USER_SUPERUSER) && users_superusers(&us) == 1;
        rc = strcmp(verb, "drop") == 0 ? users_drop(&us, 0, name)
                                       : users_set_flags(&us, 0, name, super == 1 ? USER_SUPERUSER : 0);
        if (rc == 0 && last && users_superusers(&us) == 0)
            fprintf(stderr, "tsdb: warning: no superuser is left; tsdbd will not start until one is added "
                            "(tsdb %s user add NAME --superuser)\n", dir);
    } else {
        uint8_t rights = 0, kind;
        const char *sel;
        size_t len;
        if (rights_parse(args[1], &rights) != 0 || !rights) {
            fprintf(stderr, "tsdb: %s: rights are read, insert, delete, admin and write, comma-separated\n", args[1]);
            ret = 1;
        } else if (sel_parse(args[2], &kind, &sel, &len) != 0) {
            fprintf(stderr, "tsdb: %s: a selector is all, prefix:P, name:N or glob:G\n", args[2]);
            ret = 1;
        } else {
            char *norm = malloc(len + 1);
            uint8_t nk;
            size_t nl;
            if (!norm || sel_normalize(kind, sel, len, &nk, norm, &nl) != 0) {
                fprintf(stderr, "tsdb: %s: not a valid selector (globs: at most %d bytes, no lone \\ at the end)\n",
                        args[2], GLOB_MAX);
                ret = 1;
            } else {
                if (nk != kind) {
                    fprintf(stderr, "tsdb: %s is the same set as ", args[2]);
                    print_sel(stderr, nk, norm, nl);
                    fprintf(stderr, ", stored as that\n");
                }
                /* A7: a prefix is bytes, so "acme" also covers "acmecorp/..."; tenants end in '/' by convention */
                if (nk == SEL_PREFIX && norm[nl - 1] != '/')
                    fprintf(stderr, "tsdb: warning: prefix:%s also matches names such as %sX...; tenant prefixes "
                                    "end in '/'\n", norm, norm);
                bool grant = strcmp(verb, "grant") == 0;
                uint8_t had = held_on(&us, name, nk, norm, nl);
                rc = grant ? users_grant(&us, 0, name, kind, sel, len, rights)
                           : users_revoke(&us, 0, name, kind, sel, len, rights);
                if (rc == TSDB_ERR_INVALID && nk == SEL_GLOB)
                    fprintf(stderr, "tsdb: at most %d glob grants per user\n", USER_GLOBS);
                uint8_t now = held_on(&us, name, nk, norm, nl);
                if (!grant && rc == TSDB_ERR_NOTFOUND && users_find(&us, name)) {
                    /* the operator believes something was cut: say that nothing was (A23) */
                    fprintf(stderr, "tsdb: %s holds none of ", name);
                    print_rights(stderr, rights);
                    fprintf(stderr, " on ");
                    print_sel(stderr, nk, norm, nl);
                    fprintf(stderr, ": nothing revoked (grants on other selectors are revoked on their own)\n");
                    rc = 0, ret = 1;
                } else if (!grant && rc == 0 && (had & R_ADMIN) && !(rights & R_ADMIN)) {
                    fprintf(stderr, "tsdb: admin implies read, insert and delete: admin on ");
                    print_sel(stderr, nk, norm, nl);
                    fprintf(stderr, " is revoked too; %s keeps ", name);
                    print_rights(stderr, now);
                    fprintf(stderr, "%s there\n", now ? "" : "nothing");
                }
            }
            free(norm);
        }
    }
    tsdb_wipe(&v, sizeof v);
    users_close(&us);
    if (rc == TSDB_ERR_NOTFOUND && name)
        fprintf(stderr, "tsdb: no user %s\n", name);
    else if (rc)
        return fail(verb, rc);
    return rc || ret ? 1 : 0;
}
