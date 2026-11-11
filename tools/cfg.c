/* SPDX-License-Identifier: AGPL-3.0-only */
/* Configuration files for programs with command-line options (cfg.h). */
#include "cfg.h"
#include "props.h"

#include <errno.h>
#include <getopt.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define CFG_FILE_MAX (1 << 20)

static void seterr(char *err, size_t sz, const char *fmt, ...)
{
    if (!err || !sz)
        return;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(err, sz, fmt, ap);
    va_end(ap);
}

/* A whole number, optionally with a suffix: "90", "-1", "5m", "64M". Converted to the option's unit as a decimal
 * string. A plain number is already in the unit (and may be negative, as -1 is for some options); a suffix is exact
 * or an error ("1500ms" for an option in seconds would silently round). */
static int scaled(const char *s, enum cfg_kind kind, const char *unit, char *out, size_t outsz, char *why, size_t whysz)
{
    const char *p = s;
    int neg = *p == '-';
    if (neg)
        p++;
    if (*p < '0' || *p > '9') {
        if (kind == CFG_DUR)
            seterr(why, whysz, "expected a whole number of %s, or with ms, s, m, h or d (90s, 5m)", unit);
        else
            seterr(why, whysz, "expected a whole number of %s, or with B, K, M, G or T (64M)", unit);
        return -1;
    }
    long long v = 0;
    while (*p >= '0' && *p <= '9') {
        if (__builtin_mul_overflow(v, 10, &v) || __builtin_add_overflow(v, *p - '0', &v)) {
            seterr(why, whysz, "number too large");
            return -1;
        }
        p++;
    }
    if (neg)
        v = -v;
    if (*p) {
        long long f = 0, uf;
        if (neg) {
            seterr(why, whysz, "a negative number takes no unit suffix");
            return -1;
        }
        if (kind == CFG_DUR) {
            f = !strcmp(p, "ms") ? 1 : !strcmp(p, "s") ? 1000 : !strcmp(p, "m") ? 60000 : !strcmp(p, "h") ? 3600000
                : !strcmp(p, "d") ? 86400000 : 0;
            uf = !strcmp(unit, "ms") ? 1 : 1000;
        } else {
            static const char *const sfx[][4] = {{"B", 0, 0, 0}, {"K", "KB", "KiB", 0}, {"M", "MB", "MiB", 0},
                                                 {"G", "GB", "GiB", 0}, {"T", "TB", "TiB", 0}};
            for (int i = 0; i < 5 && !f; i++)
                for (int j = 0; j < 3 && sfx[i][j]; j++)
                    if (!strcmp(p, sfx[i][j]))
                        f = 1LL << (10 * i);
            uf = !strcmp(unit, "MB") ? 1LL << 20 : 1;
        }
        if (!f) {
            seterr(why, whysz, "unknown unit \"%s\" (%s)", p, kind == CFG_DUR ? "ms, s, m, h or d" : "B, K, M, G or T");
            return -1;
        }
        if (__builtin_mul_overflow(v, f, &v)) {
            seterr(why, whysz, "number too large");
            return -1;
        }
        if (v % uf) {
            seterr(why, whysz, "%s is not a whole number of %s", s, unit);
            return -1;
        }
        v /= uf;
    }
    snprintf(out, outsz, "%lld", v);
    return 0;
}

static int check_int(const char *s, char *why, size_t whysz)
{
    const char *p = s + (*s == '-');
    if (*p < '0' || *p > '9') {
        seterr(why, whysz, "expected a whole number");
        return -1;
    }
    for (; *p; p++)
        if (*p < '0' || *p > '9') {
            seterr(why, whysz, "expected a whole number");
            return -1;
        }
    return 0;
}

void cfg_free(struct cfg *c)
{
    for (size_t i = 0; c && i < c->n; i++)
        free(c->s[i].arg), free(c->s[i].optstr);
    if (c) {
        free(c->s);
        free(c->path);
        memset(c, 0, sizeof *c);
    }
}

static int add_set(struct cfg *c, size_t *cap, const struct cfg_key *k, char *arg, int bval, int line)
{
    char *os = malloc(strlen(k->opt) + 3);
    if (!os)
        return -1;
    sprintf(os, "--%s", k->opt);
    if (c->n == *cap) {
        *cap = *cap ? *cap * 2 : 16;
        struct cfg_set *ns = realloc(c->s, *cap * sizeof *ns);
        if (!ns) {
            free(os);
            return -1;
        }
        c->s = ns;
    }
    c->s[c->n++] = (struct cfg_set){k, arg, os, bval, line};
    return 0;
}

int cfg_load_text(const char *text, size_t len, const char *name, const struct cfg_key *tab, size_t ntab,
                  struct cfg *c, char *err, size_t errsz)
{
    memset(c, 0, sizeof *c);
    c->path = strdup(name);
    props p;
    char why[512];
    if (!c->path) {
        seterr(err, errsz, "out of memory");
        return -1;
    }
    if (props_parse(text, len, &p, why, sizeof why) != 0) {
        int ln;
        int off = 0;
        if (sscanf(why, "line %d: %n", &ln, &off) == 1 && off > 0)
            seterr(err, errsz, "%s:%d: %s", name, ln, why + off);
        else
            seterr(err, errsz, "%s: %s", name, why);
        cfg_free(c);
        return -1;
    }
    int *first = calloc(ntab ? ntab : 1, sizeof *first);
    size_t cap = 0;
    if (!first) {
        seterr(err, errsz, "out of memory");
        goto fail;
    }
    for (size_t i = 0; i < p.n; i++) {
        const prop *e = &p.v[i];
        size_t t = 0;
        for (; t < ntab; t++)
            if (tab[t].key && !strcmp(tab[t].key, e->key))
                break;
        if (t == ntab) {
            seterr(err, errsz, "%s:%d: unknown key \"%s\" (docs/config.md lists them)", name, e->line, e->key);
            goto fail;
        }
        const struct cfg_key *k = &tab[t];
        if (first[t]) {
            seterr(err, errsz, "%s:%d: %s is set twice (first on line %d)", name, e->line, k->key, first[t]);
            goto fail;
        }
        first[t] = e->line;
        char *val = NULL, **items = NULL;
        size_t nitems = 0;
        char num[32];
        why[0] = 0;
        int bad = 0, bval = 0;
        if (k->kind == CFG_LIST) {
            bad = props_split(e->raw, &items, &nitems, why, sizeof why) != 0;
        } else {
            bad = props_value(e->raw, &val, why, sizeof why) != 0;
            if (!bad && !*val) {
                seterr(why, sizeof why, "empty value (leave the key out to keep the default)");
                bad = 1;
            }
        }
        if (!bad && val) {
            switch (k->kind) {
            case CFG_STR: break;
            case CFG_INT: bad = check_int(val, why, sizeof why) != 0; break;
            case CFG_BOOL:
                if (!strcmp(val, "true") || !strcmp(val, "false"))
                    bval = val[0] == 't';
                else {
                    seterr(why, sizeof why, "expected true or false");
                    bad = 1;
                }
                break;
            case CFG_ENUM: {
                size_t vl = strlen(val);
                int ok = 0;
                const char *w = k->unit;
                while (w && *w && !ok) {
                    size_t wl = strcspn(w, "|");
                    ok = wl == vl && !memcmp(w, val, vl);
                    w += wl + (w[wl] == '|');
                }
                if (!ok) {
                    seterr(why, sizeof why, "expected one of %s", k->unit);
                    bad = 1;
                }
                break;
            }
            case CFG_DUR:
            case CFG_SIZE:
                if (scaled(val, k->kind, k->unit, num, sizeof num, why, sizeof why) != 0)
                    bad = 1;
                break;
            case CFG_LIST: break;
            }
        }
        if (bad) {
            seterr(err, errsz, "%s:%d: %s: %s", name, e->line, k->key, why);
            free(val);
            props_items_free(items, nitems);
            goto fail;
        }
        int rc = 0;
        if (k->kind == CFG_LIST) {
            for (size_t j = 0; j < nitems && !rc; j++) {
                rc = add_set(c, &cap, k, items[j], 0, e->line);
                if (!rc)
                    items[j] = NULL;
            }
            props_items_free(items, nitems);
        } else if (k->kind == CFG_BOOL) {
            rc = add_set(c, &cap, k, NULL, bval, e->line);
            free(val);
        } else if (k->kind == CFG_DUR || k->kind == CFG_SIZE) {
            free(val);
            char *a = strdup(num);
            rc = a ? add_set(c, &cap, k, a, 0, e->line) : -1;
            if (rc)
                free(a);
        } else {
            rc = add_set(c, &cap, k, val, 0, e->line);
            if (rc)
                free(val);
        }
        if (rc) {
            seterr(err, errsz, "out of memory");
            goto fail;
        }
    }
    free(first);
    props_free(&p);
    return 0;
fail:
    free(first);
    props_free(&p);
    cfg_free(c);
    return -1;
}

int cfg_load_file(const char *path, const struct cfg_key *tab, size_t ntab, struct cfg *c, char *err, size_t errsz)
{
    memset(c, 0, sizeof *c);
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        seterr(err, errsz, "%s: %s", path, strerror(errno));
        return -1;
    }
    struct stat st;
    if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode)) {
        seterr(err, errsz, "%s: not a regular file", path);
        close(fd);
        return -1;
    }
    if (st.st_mode & 022) {
        seterr(err, errsz, "%s: writable by its group or by others: whoever can change it decides how the server "
                           "stores and serves data (chmod go-w %s)", path, path);
        close(fd);
        return -1;
    }
    if (st.st_size > CFG_FILE_MAX) {
        seterr(err, errsz, "%s: larger than %d bytes", path, CFG_FILE_MAX);
        close(fd);
        return -1;
    }
    char *buf = malloc((size_t)st.st_size + 1);
    size_t n = 0;
    ssize_t r = 0;
    if (!buf) {
        seterr(err, errsz, "%s: out of memory", path);
        close(fd);
        return -1;
    }
    while (n < (size_t)st.st_size && (r = read(fd, buf + n, (size_t)st.st_size - n)) > 0)
        n += (size_t)r;
    close(fd);
    if (r < 0) {
        seterr(err, errsz, "%s: %s", path, strerror(errno));
        free(buf);
        return -1;
    }
    int rc = cfg_load_text(buf, n, path, tab, ntab, c, err, errsz);
    free(buf);
    return rc;
}

/* rest is what follows a flag's name on the command line: "=true" or "=false" (the option's own truth, whatever the key
 * means). 1 and *t, or 0 when it is neither. */
static int flag_form(const char *rest, int *t)
{
    if (!strcmp(rest, "=true"))
        *t = 1;
    else if (!strcmp(rest, "=false"))
        *t = 0;
    else
        return 0;
    return 1;
}

/* The values of the option of k on the command line, in order (at most max of them). */
static size_t cmdline_vals(int argc, char **argv, const struct cfg_key *k, const char **out, size_t max)
{
    size_t cnt = 0, ol = strlen(k->opt);
    int flag = k->kind == CFG_BOOL;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--"))
            break;
        if (strncmp(argv[i], "--", 2) || strncmp(argv[i] + 2, k->opt, ol))
            continue;
        const char *rest = argv[i] + 2 + ol, *v;
        int t;
        if (*rest == 0)
            v = flag ? (k->neg ? "false" : "true") : (i + 1 < argc ? argv[++i] : "");
        else if (flag && flag_form(rest, &t))
            v = (k->neg ? !t : t) ? "true" : "false"; /* the key's value: --no-auth=false is auth.enabled = true */
        else if (*rest == '=' && !flag)
            v = rest + 1;
        else
            continue;
        if (cnt < max)
            out[cnt] = v;
        cnt++;
    }
    return cnt;
}

int cfg_on_cmdline(int argc, char **argv, const struct cfg_key *k, const char **val)
{
    const char *v[1];
    size_t n = cmdline_vals(argc, argv, k, v, 1);
    if (n && val)
        *val = v[0];
    return n > 0;
}

char **cfg_args(const struct cfg *c, int argc, char **argv, const char *prefix, size_t *n)
{
    char **a = malloc((2 * c->n + 1) * sizeof *a);
    *n = 0;
    if (!a)
        return NULL;
    size_t w = 0, pl = prefix ? strlen(prefix) : 0;
    for (size_t i = 0; i < c->n; i++) {
        const struct cfg_set *s = &c->s[i];
        if (strncmp(s->k->key, prefix ? prefix : "", pl) || cfg_on_cmdline(argc, argv, s->k, NULL))
            continue;
        if (s->k->kind == CFG_BOOL && (s->k->neg ? s->bval : !s->bval))
            continue; /* the default's side: nothing to say */
        a[w++] = s->optstr;
        if (s->k->kind != CFG_BOOL)
            a[w++] = s->arg;
    }
    *n = w;
    return a;
}

/* getopt_long takes any unique prefix of a long option; the rule of the file needs the whole name. */
static int exact_options(int argc, char **argv, const struct option *opts, char *err, size_t errsz)
{
    for (int i = 1; i < argc && strcmp(argv[i], "--"); i++) {
        if (strncmp(argv[i], "--", 2))
            continue;
        const char *name = argv[i] + 2;
        size_t nl = strcspn(name, "=");
        if (!nl)
            continue;
        const struct option *exact = NULL, *pre = NULL;
        for (const struct option *o = opts; o->name; o++) {
            if (!strncmp(o->name, name, nl)) {
                if (!o->name[nl])
                    exact = o;
                else if (!pre)
                    pre = o;
            }
        }
        if (exact) {
            if (exact->has_arg == required_argument && !name[nl])
                i++; /* its value, whatever it looks like */
        } else if (pre) {
            seterr(err, errsz, "--%.*s is an abbreviation of --%s: write the whole option name (docs/config.md)", (int)nl,
                   name, pre->name);
            return -1;
        }
    }
    return 0;
}

char **cfg_argv(const struct cfg *c, int argc, char **argv, const struct option *opts, const struct cfg_key *tab,
                size_t ntab, int *nargc, char *err, size_t errsz)
{
    if (exact_options(argc, argv, opts, err, errsz) != 0)
        return NULL;
    size_t nca = 0, chars = 0;
    char **ca = c ? cfg_args(c, argc, argv, NULL, &nca) : NULL;
    if (c && !ca) {
        seterr(err, errsz, "out of memory");
        return NULL;
    }
    for (int i = 1; i < argc; i++)
        chars += strlen(argv[i]) + 1;
    char **nv = malloc(((size_t)argc + 1 + nca) * sizeof *nv + chars);
    if (!nv) {
        free(ca);
        seterr(err, errsz, "out of memory");
        return NULL;
    }
    char *sp = (char *)(nv + argc + 1 + nca);
    int w = 0;
    nv[w++] = argv[0];
    for (size_t i = 0; i < nca; i++)
        nv[w++] = ca[i];
    free(ca);
    for (int i = 1; i < argc; i++) {
        const char *a = argv[i];
        if (!strcmp(a, "--")) { /* the rest is operands */
            for (; i < argc; i++)
                nv[w++] = argv[i];
            break;
        }
        const struct cfg_key *fk = NULL;
        int t = 0;
        size_t nl = 0;
        if (!strncmp(a, "--", 2)) {
            nl = strcspn(a + 2, "=");
            for (size_t j = 0; j < ntab && !fk; j++)
                if (tab[j].kind == CFG_BOOL && strlen(tab[j].opt) == nl && !strncmp(a + 2, tab[j].opt, nl) &&
                    flag_form(a + 2 + nl, &t))
                    fk = &tab[j];
        }
        if (fk) {
            if (t) { /* --FLAG=true is --FLAG */
                memcpy(sp, a, nl + 2);
                sp[nl + 2] = 0;
                nv[w++] = sp;
                sp += nl + 3;
            } /* --FLAG=false: the option is not given */
            continue;
        }
        nv[w++] = argv[i];
        if (!strncmp(a, "--", 2) && !a[2 + strcspn(a + 2, "=")]) { /* an option that takes a value takes the next one */
            for (const struct option *o = opts; o->name; o++)
                if (!strcmp(o->name, a + 2)) {
                    if (o->has_arg == required_argument && i + 1 < argc)
                        nv[w++] = argv[++i];
                    break;
                }
        }
    }
    nv[w] = NULL;
    *nargc = w;
    return nv;
}

const char *cfg_where_opt(const struct cfg *c, int argc, char **argv, const char *opt, char *buf, size_t sz)
{
    for (size_t i = 0; c && c->s && i < c->n; i++)
        if (!strcmp(c->s[i].k->opt, opt) && !cfg_on_cmdline(argc, argv, c->s[i].k, NULL)) {
            snprintf(buf, sz, "%s (%s:%d)", c->s[i].k->key, c->path, c->s[i].line);
            return buf;
        }
    return NULL;
}

const char *cfg_where(const struct cfg *c, const char *arg, char *buf, size_t sz)
{
    for (size_t i = 0; c && c->s && i < c->n; i++)
        if (c->s[i].arg == arg && arg) {
            snprintf(buf, sz, "%s (%s:%d)", c->s[i].k->key, c->path, c->s[i].line);
            return buf;
        }
    return NULL;
}

static void put_escaped(FILE *f, const char *s)
{
    size_t n = strlen(s);
    for (size_t i = 0; i < n; i++) {
        unsigned char ch = (unsigned char)s[i];
        if (ch == '\\' || ch == ',')
            fprintf(f, "\\%c", ch);
        else if (ch == ' ' && (i == 0 || i == n - 1))
            fputs("\\ ", f);
        else if (ch == '\t')
            fputs("\\t", f);
        else if (ch == '\n')
            fputs("\\n", f);
        else if (ch == '\r')
            fputs("\\r", f);
        else if (ch == '\f')
            fputs("\\f", f);
        else if (ch < 0x20 || ch == 0x7F)
            fprintf(f, "\\u%04X", ch);
        else
            fputc(ch, f);
    }
}

static const char *unit_suffix(const struct cfg_key *k, const char *v)
{
    if (v[0] == '-')
        return "";
    return k->kind == CFG_DUR ? k->unit : k->kind == CFG_SIZE ? (!strcmp(k->unit, "MB") ? "M" : "B") : "";
}

void cfg_print_effective(FILE *f, const struct cfg *c, const struct cfg_key *tab, size_t ntab, int argc, char **argv)
{
    char last_group[32] = "";
    for (size_t t = 0; t < ntab; t++) {
        const struct cfg_key *k = &tab[t];
        if (!k->key)
            continue;
        size_t gl = strcspn(k->key, ".");
        if (strlen(last_group) != gl || strncmp(last_group, k->key, gl)) {
            if (last_group[0])
                fputc('\n', f);
            snprintf(last_group, sizeof last_group, "%.*s", (int)(gl < 31 ? gl : 31), k->key);
        }
        const char *cv[64];
        size_t nc = cmdline_vals(argc, argv, k, cv, 64);
        if (nc > 64)
            nc = 64;
        if (nc) {
            fprintf(f, "# command line\n%s = ", k->key);
            if (k->kind == CFG_BOOL)
                fputs(cv[0], f);
            for (size_t i = 0; k->kind != CFG_BOOL && i < nc; i++) {
                if (i)
                    fputs(", ", f);
                put_escaped(f, cv[i]);
                if (k->kind == CFG_DUR || k->kind == CFG_SIZE)
                    fputs(unit_suffix(k, cv[i]), f);
            }
            fputc('\n', f);
            continue;
        }
        int shown = 0;
        for (size_t i = 0; c && i < c->n; i++) {
            const struct cfg_set *s = &c->s[i];
            if (s->k != k)
                continue;
            if (!shown)
                fprintf(f, "# %s:%d\n%s = ", c->path, s->line, k->key);
            else
                fputs(", ", f);
            if (k->kind == CFG_BOOL)
                fputs(s->bval ? "true" : "false", f);
            else {
                put_escaped(f, s->arg);
                if (k->kind == CFG_DUR || k->kind == CFG_SIZE)
                    fputs(unit_suffix(k, s->arg), f);
            }
            shown = 1;
        }
        if (shown) {
            fputc('\n', f);
            continue;
        }
        if (k->dflt && *k->dflt && k->dflt[0] != '(')
            fprintf(f, "# default\n%s = %s\n", k->key, k->dflt);
        else
            fprintf(f, "# not set%s%s\n#%s =\n", k->dflt && *k->dflt ? " " : "", k->dflt ? k->dflt : "", k->key);
    }
}

static const char *kind_name(const struct cfg_key *k, char *buf, size_t sz)
{
    switch (k->kind) {
    case CFG_STR: return "string";
    case CFG_INT: return "integer";
    case CFG_BOOL: return "boolean";
    case CFG_ENUM: {
        size_t w = 0;
        for (const char *p = k->unit; *p && w + 4 < sz; p++)
            buf[w++] = *p == '|' ? ',' : *p;
        buf[w] = 0;
        return buf;
    }
    case CFG_DUR: snprintf(buf, sz, "duration (plain: %s)", k->unit); return buf;
    case CFG_SIZE: snprintf(buf, sz, "size (plain: %s)", !strcmp(k->unit, "MB") ? "MiB" : "bytes"); return buf;
    case CFG_LIST: return "list";
    }
    return "";
}

void cfg_print_markdown(FILE *f, const struct cfg_key *tab, size_t ntab)
{
    char kb[96];
    fputs("| Key | Option | Value | Default | Meaning |\n|---|---|---|---|---|\n", f);
    for (size_t t = 0; t < ntab; t++) {
        const struct cfg_key *k = &tab[t];
        if (!k->key)
            continue;
        fprintf(f, "| `%s` | `--%s`%s | %s | %s | %s |\n", k->key, k->opt, k->kind == CFG_BOOL && k->neg ? " (when false)" : "",
                kind_name(k, kb, sizeof kb), k->dflt && *k->dflt ? k->dflt : "unset", k->doc);
    }
    fputs("\nOptions with no key:\n\n| Option | Why |\n|---|---|\n", f);
    for (size_t t = 0; t < ntab; t++)
        if (!tab[t].key)
            fprintf(f, "| `--%s` | %s |\n", tab[t].opt, tab[t].doc);
}
