/*
 * `tsdb <dir> user ...`: the users of tsdbd's login, edited in USERS without opening the engine, so it runs beside
 * a running tsdbd (which reads the change within a second). Whoever can write the directory holds every right
 * already, so these act as the operator (A12): the first superuser is made here. Passwords never come from argv
 * (visible in ps): from a prompt without echo, --password-file or TSDB_PASSWORD; the verifier is computed here and
 * only it is stored (A18).
 */
#define _GNU_SOURCE
#include "../server/users.h"
#include "../client/tsdb_crypto.h"
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
            "\n"
            "  RIGHTS    read, insert, delete, admin, write (insert and delete), comma-separated\n"
            "  SELECTOR  all, prefix:P, name:N or glob:G (* any bytes, ? one byte, \\ the next byte as is)\n"
            "  The password is read from FILE (its first line), else from TSDB_PASSWORD, else from the\n"
            "  terminal without echo; never from the command line.\n");
    return 2;
}

static int fail(const char *what, int rc)
{
    fprintf(stderr, "tsdb: %s: %s\n", what, tsdb_strerror(rc));
    return 1;
}

/* One line from fd without echo when it is a terminal. */
static int read_line(FILE *f, char *buf, size_t cap)
{
    if (!fgets(buf, (int)cap, f))
        return -1;
    size_t n = strcspn(buf, "\n");
    if (buf[n] != '\n' && !feof(f))
        return -1; /* longer than cap */
    buf[n] = '\0';
    if (n && buf[n - 1] == '\r')
        buf[n - 1] = '\0';
    return 0;
}

static int prompt(FILE *tty, const char *what, char *buf, size_t cap)
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
    int rc = read_line(tty, buf, cap);
    if (echo_off)
        tcsetattr(fd, TCSAFLUSH, &old);
    fprintf(tty, "\n");
    return rc;
}

static int get_password(const char *file, const char *name, char *pw, size_t cap)
{
    if (file) {
        FILE *f = strcmp(file, "-") == 0 ? stdin : fopen(file, "r");
        if (!f) {
            fprintf(stderr, "tsdb: %s: %s\n", file, strerror(errno));
            return -1;
        }
        int rc = read_line(f, pw, cap);
        if (f != stdin)
            fclose(f);
        if (rc != 0) {
            fprintf(stderr, "tsdb: %s: no password in it (or a line over %zu bytes)\n", file, cap - 1);
            return -1;
        }
    } else if (getenv("TSDB_PASSWORD")) {
        if (strlen(getenv("TSDB_PASSWORD")) >= cap)
            return -1;
        snprintf(pw, cap, "%s", getenv("TSDB_PASSWORD"));
    } else {
        FILE *tty = fopen("/dev/tty", "r+");
        if (!tty) {
            fprintf(stderr, "tsdb: no terminal for the password: use --password-file FILE or TSDB_PASSWORD\n");
            return -1;
        }
        char again[1024], what[200];
        snprintf(what, sizeof what, "Password for %s: ", name);
        int rc = prompt(tty, what, pw, cap);
        if (rc == 0)
            rc = prompt(tty, "Again: ", again, sizeof again);
        fclose(tty);
        if (rc == 0 && strcmp(pw, again) != 0) {
            fprintf(stderr, "tsdb: the passwords differ\n");
            rc = -1;
        }
        tsdb_wipe(again, sizeof again);
        if (rc != 0)
            return -1;
    }
    if (!pw[0]) {
        fprintf(stderr, "tsdb: the password is empty\n");
        return -1;
    }
    return 0;
}

/* The verifier of a password with a fresh random salt (A18: the server never sees the password). */
static int make_verifier(const char *file, const char *name, uint32_t iterations, user_verifier *v)
{
    char pw[1024];
    if (get_password(file, name, pw, sizeof pw) != 0)
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
    return kind <= SEL_GLOB ? t[kind] : "?";
}

static void print_rights(FILE *f, uint8_t r)
{
    static const char *w[] = {"read", "insert", "delete", "admin"};
    bool first = true;
    for (int i = 0; i < 4; i++)
        if (r & (1 << i)) {
            fprintf(f, "%s%s", first ? "" : ",", w[i]);
            first = false;
        }
}

static void print_sel(FILE *f, uint8_t kind, const char *sel, size_t len)
{
    fputs(kind_tag(kind), f);
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)sel[i];
        if (c < 0x20 || c == 0x7f)
            fprintf(f, "\\x%02x", c); /* a glob shows its own escapes as stored */
        else
            fputc(c, f);
    }
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
    int rc = fd < 0 || fsync(fd) != 0 ? -1 : 0;
    if (fd >= 0)
        close(fd);
    if (rc)
        fprintf(stderr, "tsdb: syncing %s: %s\n", parent, strerror(errno));
    return rc;
}

int user_main(const char *dir, int argc, char **argv)
{
    if (argc < 1)
        return user_usage();
    const char *verb = argv[0], *name = argc > 1 ? argv[1] : NULL;
    const char *pwfile = NULL;
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
    bool listing = strcmp(verb, "list") == 0 || strcmp(verb, "compact") == 0;
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
        uint8_t rights = rights_parse(args[1]), kind;
        const char *sel;
        size_t len;
        if (!rights) {
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
                rc = strcmp(verb, "grant") == 0 ? users_grant(&us, 0, name, kind, sel, len, rights)
                                                : users_revoke(&us, 0, name, kind, sel, len, rights);
                if (rc == TSDB_ERR_INVALID && nk == SEL_GLOB)
                    fprintf(stderr, "tsdb: at most %d glob grants per user\n", USER_GLOBS);
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
