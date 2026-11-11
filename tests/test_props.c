/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The .properties reader (tools/props.c) and the key table layer over it (tools/cfg.c): every rule of the syntax, the
 * escapes, continuation, lists with escaped commas, the errors and the line they name, the value kinds, precedence
 * (an option on the command line is not taken from the file), the refusal of a writable file, and that the effective
 * settings print as a file that loads to the same settings. Usage: test_props [scratch dir]
 */
#define _GNU_SOURCE
#include "../tools/cfg.h"
#include "../tools/props.h"
#include "../server/tsdbd_keys.h"

#include <getopt.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int fails;
#define CHECK(c)                                                                                                       \
    do {                                                                                                               \
        if (!(c)) {                                                                                                    \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);                                               \
            fails++;                                                                                                   \
        }                                                                                                              \
    } while (0)
#define CHECK_STR(a, b)                                                                                                \
    do {                                                                                                               \
        const char *a_ = (a), *b_ = (b);                                                                               \
        if (!a_ || strcmp(a_, b_)) {                                                                                   \
            fprintf(stderr, "FAIL %s:%d: \"%s\" != \"%s\"\n", __FILE__, __LINE__, a_ ? a_ : "(null)", b_);            \
            fails++;                                                                                                   \
        }                                                                                                              \
    } while (0)

static int parse(const char *text, props *p, char *err)
{
    return props_parse(text, strlen(text), p, err, 512);
}

/* the one entry that text must hold: its key and its value with escapes processed */
static void one(const char *text, const char *key, const char *val)
{
    props p;
    char err[512], *v = NULL;
    if (parse(text, &p, err) != 0) {
        fprintf(stderr, "FAIL parse of \"%s\": %s\n", text, err);
        fails++;
        return;
    }
    CHECK(p.n == 1);
    if (p.n == 1) {
        CHECK_STR(p.v[0].key, key);
        CHECK(props_value(p.v[0].raw, &v, err, sizeof err) == 0);
        if (v)
            CHECK_STR(v, val);
        free(v);
    }
    props_free(&p);
}

static void bad(const char *text, const char *msg_part)
{
    props p;
    char err[512];
    int rc = parse(text, &p, err);
    int parsed = rc == 0; /* the entries are ours to free then, whichever way the values go */
    for (size_t i = 0; rc == 0 && i < p.n; i++) { /* escapes are processed with the value, not by the parser */
        char *v;
        if (props_value(p.v[i].raw, &v, err, sizeof err) == 0)
            free(v);
        else
            rc = -1;
    }
    if (rc == 0) {
        fprintf(stderr, "FAIL \"%s\" was accepted\n", text);
        fails++;
    }
    if (parsed)
        props_free(&p);
    if (rc == 0)
        return;
    if (!strstr(err, msg_part)) {
        fprintf(stderr, "FAIL \"%s\": error \"%s\" lacks \"%s\"\n", text, err, msg_part);
        fails++;
    }
}

static void split_is(const char *raw, size_t n, ...)
{
    char **items, err[512];
    size_t cnt;
    CHECK(props_split(raw, &items, &cnt, err, sizeof err) == 0);
    CHECK(cnt == n);
    va_list ap;
    va_start(ap, n);
    for (size_t i = 0; i < n; i++) {
        const char *want = va_arg(ap, const char *);
        if (i < cnt)
            CHECK_STR(items[i], want);
    }
    va_end(ap);
    props_items_free(items, cnt);
}

static void test_syntax(void)
{
    one("a=b", "a", "b");
    one("a = b", "a", "b");
    one("a: b", "a", "b");
    one("a b", "a", "b");
    one("  a \t= \t b c  ", "a", "b c");      /* trailing white space dropped, inner kept */
    one("a=b=c", "a", "b=c");                 /* only the first separator counts */
    one("a:b:c", "a", "b:c");
    one("a", "a", "");                        /* a key alone: an empty value (the table refuses it) */
    one("a=", "a", "");
    one("a.b-c_d=1", "a.b-c_d", "1");
    one("\xEF\xBB\xBF" "a=b", "a", "b");         /* a byte order mark */
    one("a=b\r\n", "a", "b");
    one("a=b\r", "a", "b");
    one("k\\ ey = v", "k ey", "v");           /* an escaped space in a key */
    one("k\\=ey = v", "k=ey", "v");
    one("a = \\ x\\ ", "a", " x ");           /* escaped leading and trailing space are kept */
    one("a = x   ", "a", "x");
    one("a = x\\t", "a", "x\t");

    props p;
    char err[512];
    CHECK(parse("# one\n! two\n\n   \n  # three\na=1\n\nb=2\r\nc=3\rd=4\n", &p, err) == 0);
    CHECK(p.n == 4);
    CHECK(p.v[0].line == 6 && p.v[1].line == 8 && p.v[2].line == 9 && p.v[3].line == 10);
    props_free(&p);
    CHECK(parse("", &p, err) == 0 && p.n == 0);
    props_free(&p);
    CHECK(parse("# only a comment", &p, err) == 0 && p.n == 0);
    props_free(&p);
    CHECK(parse("a=1 # not a comment\n", &p, err) == 0 && p.n == 1); /* comments are whole lines only */
    char *v;
    CHECK(props_value(p.v[0].raw, &v, err, sizeof err) == 0);
    CHECK_STR(v, "1 # not a comment");
    free(v);
    props_free(&p);

    /* continuation: the next line's leading white space is dropped, the backslash and newline go */
    one("a = x\\\n    y", "a", "xy");
    one("a = x \\\n    y", "a", "x y");
    one("a = x\\\r\n\ty", "a", "xy");
    one("a = x\\\\", "a", "x\\");
    CHECK(parse("a = 1\\\n# not a comment, a continuation\nb=2\n", &p, err) == 0 && p.n == 2);
    CHECK_STR(p.v[0].raw, "1# not a comment, a continuation"); /* the next line is part of the value, # and all */
    props_free(&p);
    CHECK(parse("a = x\\\\\nb = 1", &p, err) == 0 && p.n == 2); /* an even run of backslashes is no continuation */
    props_free(&p);
    CHECK(parse("# comment \\\nb=2\n", &p, err) == 0 && p.n == 1); /* a comment line is never continued */
    CHECK(p.v[0].line == 2);
    props_free(&p);
    CHECK(parse("a = 1,\\\n    2,\\\n    3\nb=1\n", &p, err) == 0 && p.n == 2);
    CHECK(p.v[0].line == 1 && p.v[1].line == 4); /* a multi-line entry is at its first line */
    props_free(&p);

    /* escapes */
    one("a = \\t\\n\\r\\f\\\\\\=\\:\\#\\!\\ \\,", "a", "\t\n\r\f\\=:#! ,");
    one("a = \\u00e9\\u00C9", "a", "\xC3\xA9\xC3\x89");
    one("a = \\u20AC", "a", "\xE2\x82\xAC");
    one("a = \\uD83D\\uDE00", "a", "\xF0\x9F\x98\x80");    /* a surrogate pair */
    one("a = caf\xC3\xA9", "a", "caf\xC3\xA9");             /* UTF-8 as is */
    one("a = \xF0\x9F\x98\x80", "a", "\xF0\x9F\x98\x80");

    /* errors name their line */
    bad("a=1\nb=\\q\n", "unknown escape \\q");
    bad("a=1\nb=\\u12\n", "four hex digits");
    bad("a=1\nb=\\u12G4\n", "four hex digits");
    bad("a=\\uD800\n", "surrogate");
    bad("a=\\uDC00x\n", "surrogate");
    bad("a=\\uD800\\u0041\n", "surrogate");
    bad("a=\\u0000\n", "NUL");
    bad("a=1\n=x\n", "no key");
    bad("a=1\n: x\n", "no key");
    bad("a=1\nb=2\\", "ends in a backslash");
    bad("a=1\nb=2\\\n", "ends in a backslash");
    bad("a=1\n\nb\\q=2\n", "key: unknown escape");
    bad("a=caf\xE9\n", "not valid UTF-8");              /* ISO-8859-1 is not read */
    bad("a=\xC0\x80\n", "not valid UTF-8");              /* overlong */
    bad("a=\xED\xA0\x80\n", "not valid UTF-8");          /* a surrogate in UTF-8 */
    bad("a=\xF4\x90\x80\x80\n", "not valid UTF-8");      /* above U+10FFFF */
    bad("a=\xE2\x82", "not valid UTF-8");                /* cut short */
    {
        props q;
        char e2[512];
        CHECK(props_parse("a=1\nb=2\n\xFF\n", 12, &q, e2, sizeof e2) == -1);
        CHECK(strstr(e2, "line 3:") != NULL);
        CHECK(props_parse("a=1\0b=2\n", 8, &q, e2, sizeof e2) == -1); /* a NUL byte */
        CHECK(strstr(e2, "line 1:") != NULL);
    }
    {
        char *v2;
        CHECK(props_value("a\\qb", &v2, err, sizeof err) == -1);
        CHECK(strstr(err, "unknown escape \\q") != NULL);
        CHECK(props_value("a, b", &v2, err, sizeof err) == -1);
        CHECK(strstr(err, "list where one value") != NULL);
        CHECK(props_value("a\\, b", &v2, err, sizeof err) == 0);
        CHECK_STR(v2, "a, b");
        free(v2);
    }
}

static void test_lists(void)
{
    char **items, err[512];
    size_t n;
    split_is("", 0);
    split_is("   ", 0);
    split_is("a", 1, "a");
    split_is("a,b,c", 3, "a", "b", "c");
    split_is(" a , b ,c ", 3, "a", "b", "c");
    split_is("/mnt/nfs1, /mnt/nfs2", 2, "/mnt/nfs1", "/mnt/nfs2");
    split_is("a\\,b, c", 2, "a,b", "c");                    /* an escaped comma belongs to the element */
    split_is("a\\\\,b", 2, "a\\", "b");                     /* an escaped backslash does not escape the comma */
    split_is("\\ a,b\\ ", 2, " a", "b ");                  /* escaped white space is kept at the edges */
    split_is("a\\tb,c\\u0041", 2, "a\tb", "cA");
    split_is("x\\ , y", 2, "x ", "y");
    CHECK(props_split("a,,b", &items, &n, err, sizeof err) == -1 && strstr(err, "empty element"));
    CHECK(props_split("a,", &items, &n, err, sizeof err) == -1);
    CHECK(props_split(",a", &items, &n, err, sizeof err) == -1);
    CHECK(props_split("a, ,b", &items, &n, err, sizeof err) == -1);
    CHECK(props_split("a,\\q", &items, &n, err, sizeof err) == -1 && strstr(err, "unknown escape"));
}

/* a table with one key of every kind */
static const struct cfg_key T[] = {
    {"g.str", "str", CFG_STR, 0, NULL, "x", "a string"},
    {"g.int", "int", CFG_INT, 0, NULL, "7", "a number"},
    {"g.on", "on", CFG_BOOL, 0, NULL, "false", "a flag"},
    {"g.fsync", "no-fsync", CFG_BOOL, 1, NULL, "true", "a negative flag"},
    {"g.mode", "mode", CFG_ENUM, 0, "auto|a|b", "auto", "a word"},
    {"g.dur.s", "dur-s", CFG_DUR, 0, "s", "60s", "seconds"},
    {"g.dur.ms", "dur-ms", CFG_DUR, 0, "ms", "5s", "milliseconds"},
    {"g.size.mb", "size-mb", CFG_SIZE, 0, "MB", "1G", "megabytes"},
    {"g.size.b", "size-b", CFG_SIZE, 0, "B", "", "bytes"},
    {"g.list", "item", CFG_LIST, 0, NULL, "", "a list"},
    {NULL, "version", CFG_STR, 0, NULL, "", "no key"},
};
#define NT (sizeof T / sizeof T[0])

static int load(const char *text, struct cfg *c, char *err)
{
    return cfg_load_text(text, strlen(text), "f.properties", T, NT, c, err, 512);
}

/* loads text; the arguments it makes, joined by spaces, with prefix filter NULL and an empty command line */
static const char *args_of(const char *text, int argc, char **argv)
{
    static char out[2048];
    struct cfg c;
    char err[512];
    if (load(text, &c, err) != 0) {
        snprintf(out, sizeof out, "ERROR %s", err);
        return out;
    }
    size_t n;
    char **a = cfg_args(&c, argc, argv, NULL, &n);
    out[0] = 0;
    for (size_t i = 0; i < n; i++)
        snprintf(out + strlen(out), sizeof out - strlen(out), "%s%s", i ? " " : "", a[i]);
    free(a);
    cfg_free(&c);
    return out;
}

static const char *err_of(const char *text)
{
    static char out[512];
    struct cfg c;
    if (load(text, &c, out) == 0) {
        cfg_free(&c);
        return "accepted";
    }
    return out;
}

static void test_kinds(void)
{
    char *none[] = {"prog"};
    CHECK_STR(args_of("", 1, none), "");
    CHECK_STR(args_of("g.str = hello\ng.int = 12\n", 1, none), "--str hello --int 12");
    CHECK_STR(args_of("g.int = -1", 1, none), "--int -1");
    CHECK_STR(args_of("g.on = true", 1, none), "--on");
    CHECK_STR(args_of("g.on = false", 1, none), "");           /* a flag's default side is not passed */
    CHECK_STR(args_of("g.fsync = false", 1, none), "--no-fsync");
    CHECK_STR(args_of("g.fsync = true", 1, none), "");
    CHECK_STR(args_of("g.mode = epoll", 1, none), "ERROR f.properties:1: g.mode: expected one of auto|a|b");
    CHECK_STR(args_of("g.mode = b", 1, none), "--mode b");
    CHECK_STR(args_of("g.dur.s = 90", 1, none), "--dur-s 90");
    CHECK_STR(args_of("g.dur.s = 90s", 1, none), "--dur-s 90");
    CHECK_STR(args_of("g.dur.s = 5m", 1, none), "--dur-s 300");
    CHECK_STR(args_of("g.dur.s = 2h", 1, none), "--dur-s 7200");
    CHECK_STR(args_of("g.dur.s = 1d", 1, none), "--dur-s 86400");
    CHECK_STR(args_of("g.dur.s = 3000ms", 1, none), "--dur-s 3");
    CHECK_STR(args_of("g.dur.s = -1", 1, none), "--dur-s -1");
    CHECK_STR(args_of("g.dur.ms = 250", 1, none), "--dur-ms 250");
    CHECK_STR(args_of("g.dur.ms = 250ms", 1, none), "--dur-ms 250");
    CHECK_STR(args_of("g.dur.ms = 2s", 1, none), "--dur-ms 2000");
    CHECK_STR(args_of("g.dur.ms = 1d", 1, none), "--dur-ms 86400000");
    CHECK_STR(args_of("g.size.mb = 64", 1, none), "--size-mb 64");
    CHECK_STR(args_of("g.size.mb = 64M", 1, none), "--size-mb 64");
    CHECK_STR(args_of("g.size.mb = 2G", 1, none), "--size-mb 2048");
    CHECK_STR(args_of("g.size.mb = 1GiB", 1, none), "--size-mb 1024");
    CHECK_STR(args_of("g.size.mb = 1T", 1, none), "--size-mb 1048576");
    CHECK_STR(args_of("g.size.mb = 2048K", 1, none), "--size-mb 2");
    CHECK_STR(args_of("g.size.b = 1K", 1, none), "--size-b 1024");
    CHECK_STR(args_of("g.size.b = 1MB", 1, none), "--size-b 1048576");
    CHECK_STR(args_of("g.size.b = 77B", 1, none), "--size-b 77");
    CHECK_STR(args_of("g.size.b = 5", 1, none), "--size-b 5");
    CHECK_STR(args_of("g.list = a, b\\,c ,d", 1, none), "--item a --item b,c --item d");
    CHECK_STR(args_of("g.list =", 1, none), "");               /* an empty list: nothing */
    CHECK_STR(args_of("g.str = a\\, b", 1, none), "--str a, b");

    /* errors: the file, the line, the key, the reason */
    CHECK_STR(err_of("\n\ng.nope = 1"), "f.properties:3: unknown key \"g.nope\" (docs/config.md lists them)");
    CHECK_STR(err_of("g.str = a\ng.int = 1\ng.str = b"), "f.properties:3: g.str is set twice (first on line 1)");
    CHECK_STR(err_of("g.on = yes"), "f.properties:1: g.on: expected true or false");
    CHECK_STR(err_of("g.on = True"), "f.properties:1: g.on: expected true or false");
    CHECK_STR(err_of("g.int = 1.5"), "f.properties:1: g.int: expected a whole number");
    CHECK_STR(err_of("g.int = 12x"), "f.properties:1: g.int: expected a whole number");
    CHECK_STR(err_of("g.int = +1"), "f.properties:1: g.int: expected a whole number");
    CHECK_STR(err_of("g.int = 0x10"), "f.properties:1: g.int: expected a whole number");
    CHECK_STR(err_of("g.int = 1,2"), "f.properties:1: g.int: a list where one value is expected "
                                    "(write \\, for a comma that belongs to the value)");
    CHECK_STR(err_of("g.str = a,b"), "f.properties:1: g.str: a list where one value is expected "
                                    "(write \\, for a comma that belongs to the value)");
    CHECK_STR(err_of("g.str ="), "f.properties:1: g.str: empty value (leave the key out to keep the default)");
    CHECK_STR(err_of("g.int"), "f.properties:1: g.int: empty value (leave the key out to keep the default)");
    CHECK_STR(err_of("g.dur.s = 1500ms"), "f.properties:1: g.dur.s: 1500ms is not a whole number of s");
    CHECK_STR(err_of("g.dur.s = 5x"), "f.properties:1: g.dur.s: unknown unit \"x\" (ms, s, m, h or d)");
    CHECK_STR(err_of("g.dur.s = s"), "f.properties:1: g.dur.s: expected a whole number of s, or with ms, s, m, h or d "
                                    "(90s, 5m)");
    CHECK_STR(err_of("g.dur.s = -5m"), "f.properties:1: g.dur.s: a negative number takes no unit suffix");
    CHECK_STR(err_of("g.dur.s = 99999999999999999999"), "f.properties:1: g.dur.s: number too large");
    CHECK_STR(err_of("g.dur.s = 9999999999999999d"), "f.properties:1: g.dur.s: number too large");
    CHECK_STR(err_of("g.size.mb = 1500K"), "f.properties:1: g.size.mb: 1500K is not a whole number of MB");
    CHECK_STR(err_of("g.size.mb = 2X"), "f.properties:1: g.size.mb: unknown unit \"X\" (B, K, M, G or T)");
    CHECK_STR(err_of("g.size.b = 9000000T"), "f.properties:1: g.size.b: number too large");
    CHECK_STR(err_of("g.list = a,,b"), "f.properties:1: g.list: an empty element in the list (a doubled or trailing "
                                      "comma)");
    CHECK_STR(err_of("version = 1"), "f.properties:1: unknown key \"version\" (docs/config.md lists them)");
    CHECK_STR(err_of("g.str = \\q"), "f.properties:1: g.str: unknown escape \\q (a backslash is written \\\\)");
    CHECK_STR(err_of("a=1\n\xFF"), "f.properties:2: not valid UTF-8 (the file is read as UTF-8; write other characters "
                                  "as \\uXXXX) or holds a NUL byte");
    CHECK_STR(err_of("# c\n\ng.str\\q = 1"), "f.properties:3: key: unknown escape \\q (a backslash is written \\\\)");
}

static void test_precedence(void)
{
    char *cl1[] = {"prog", "--int", "5"};
    CHECK_STR(args_of("g.int = 12\ng.str = s", 3, cl1), "--str s");           /* on the command line: not from the file */
    char *cl2[] = {"prog", "--int=5", "--str", "x"};
    CHECK_STR(args_of("g.int = 12\ng.str = s", 4, cl2), "");
    char *cl3[] = {"prog", "--item", "z"};
    CHECK_STR(args_of("g.list = a,b\ng.int = 1", 3, cl3), "--int 1");          /* a list on the command line replaces */
    char *cl4[] = {"prog", "--no-fsync"};
    CHECK_STR(args_of("g.fsync = false\ng.int = 1", 2, cl4), "--int 1");
    char *cl5[] = {"prog", "--on"};
    CHECK_STR(args_of("g.on = true", 2, cl5), "");
    char *cl6[] = {"prog", "--", "--int", "5"}; /* after "--" nothing is an option */
    CHECK_STR(args_of("g.int = 12", 4, cl6), "--int 12");
    char *cl7[] = {"prog", "--integer", "5"};  /* another option that starts the same */
    CHECK_STR(args_of("g.int = 12", 3, cl7), "--int 12");

    const char *val = NULL;
    CHECK(cfg_on_cmdline(3, cl1, &T[1], &val) && !strcmp(val, "5"));
    CHECK(cfg_on_cmdline(2, cl4, &T[3], &val) && !strcmp(val, "false"));
    CHECK(cfg_on_cmdline(2, cl5, &T[2], &val) && !strcmp(val, "true"));
    CHECK(!cfg_on_cmdline(2, cl5, &T[1], &val));

    /* a prefix filter: only the keys of one group */
    struct cfg c;
    char err[512];
    char *none[] = {"prog"};
    CHECK(load("g.str = a\ng.int = 1", &c, err) == 0);
    size_t n;
    char **a = cfg_args(&c, 1, none, "g.s", &n);
    CHECK(n == 2 && !strcmp(a[0], "--str") && !strcmp(a[1], "a"));
    free(a);
    a = cfg_args(&c, 1, none, "nothing.", &n);
    CHECK(n == 0);
    free(a);

    /* where an argument came from */
    char wb[256];
    char **all = cfg_args(&c, 1, none, NULL, &n);
    CHECK(n == 4);
    CHECK_STR(cfg_where(&c, all[1], wb, sizeof wb), "g.str (f.properties:1)");
    CHECK_STR(cfg_where(&c, all[3], wb, sizeof wb), "g.int (f.properties:2)");
    CHECK(cfg_where(&c, "1", wb, sizeof wb) == NULL); /* a command-line string is nobody's */
    free(all);
    cfg_free(&c);
}

/* the argv that the option parser reads: the last form of a flag counts, whatever the forms */
static const char *argv_of(const char *text, int argc, char **argv)
{
    static const struct option opts[] = {{"str", required_argument, 0, 's'}, {"int", required_argument, 0, 'i'},
                                         {"on", no_argument, 0, 'o'},        {"no-fsync", no_argument, 0, 'f'},
                                         {"item", required_argument, 0, 'l'}, {0, 0, 0, 0}};
    static char out[2048];
    struct cfg c;
    char err[512];
    if (text && load(text, &c, err) != 0) {
        snprintf(out, sizeof out, "ERROR %s", err);
        return out;
    }
    int nargc = 0;
    char **nv = cfg_argv(text ? &c : NULL, argc, argv, opts, T, NT, &nargc, err, sizeof err);
    out[0] = 0;
    if (!nv)
        snprintf(out, sizeof out, "ERROR %s", err);
    for (int i = 1; nv && i < nargc; i++)
        snprintf(out + strlen(out), sizeof out - strlen(out), "%s%s", i > 1 ? " " : "", nv[i]);
    free(nv);
    if (text)
        cfg_free(&c);
    return out;
}

static void test_last_wins(void)
{
    char *a1[] = {"prog", "--on", "--on=false"};
    CHECK_STR(argv_of(NULL, 3, a1), "");                         /* the last form is =false: not given */
    CHECK_STR(argv_of("g.on = true", 3, a1), "");                /* and the file's true stays out: the command line spoke */
    char *a2[] = {"prog", "--on=false", "--on"};
    CHECK_STR(argv_of(NULL, 3, a2), "--on");
    char *a3[] = {"prog", "--on=true", "--on=false", "--on=true"};
    CHECK_STR(argv_of(NULL, 4, a3), "--on");
    char *a4[] = {"prog", "--no-fsync", "--no-fsync=false"};     /* --no-X=false is the default's side again */
    CHECK_STR(argv_of("g.fsync = false", 3, a4), "");
    char *a5[] = {"prog", "--no-fsync=false", "--no-fsync"};
    CHECK_STR(argv_of(NULL, 3, a5), "--no-fsync");
    char *a6[] = {"prog", "--str", "--on", "--on=false"};        /* a value that looks like a flag is a value */
    CHECK_STR(argv_of(NULL, 4, a6), "--str --on");
    char *a7[] = {"prog", "--on=false", "--", "--on"};           /* after -- nothing is an option */
    CHECK_STR(argv_of(NULL, 4, a7), "-- --on");
    char *a8[] = {"prog", "--on=maybe"};                          /* not a flag form: left for the parser to refuse */
    CHECK_STR(argv_of(NULL, 2, a8), "--on=maybe");
    char *a9[] = {"prog", "--int", "1", "--int", "2"};            /* scalars are the parser's: last wins there */
    CHECK_STR(argv_of("g.int = 5", 5, a9), "--int 1 --int 2");

    const char *val = NULL;
    CHECK(cfg_on_cmdline(3, a1, &T[2], &val) && !strcmp(val, "false"));
    CHECK(cfg_on_cmdline(3, a2, &T[2], &val) && !strcmp(val, "true"));
    CHECK(cfg_on_cmdline(3, a4, &T[3], &val) && !strcmp(val, "true"));   /* --no-fsync=false: fsync on */
    CHECK(cfg_on_cmdline(3, a5, &T[3], &val) && !strcmp(val, "false"));
    CHECK(cfg_on_cmdline(5, a9, &T[1], &val) && !strcmp(val, "2"));

    /* --check-config's printout names the last one */
    char *buf = NULL;
    size_t blen = 0;
    FILE *f = open_memstream(&buf, &blen);
    cfg_print_effective(f, NULL, T, NT, 5, a9);
    fclose(f);
    CHECK(strstr(buf, "# command line\ng.int = 2\n") != NULL && !strstr(buf, "g.int = 1"));
    free(buf);
    f = open_memstream(&buf, &blen);
    cfg_print_effective(f, NULL, T, NT, 3, a1);
    fclose(f);
    CHECK(strstr(buf, "# command line\ng.on = false\n") != NULL);
    free(buf);
    char *a10[] = {"prog", "--item", "a", "--item", "b"};
    f = open_memstream(&buf, &blen);
    cfg_print_effective(f, NULL, T, NT, 5, a10);
    fclose(f);
    CHECK(strstr(buf, "g.list = a, b\n") != NULL); /* a list keeps every element */
    free(buf);
}

static void test_drop(void)
{
    struct cfg c;
    char err[512], names[256];
    char *none[] = {"prog"};
    CHECK(load("g.str = a\ng.int = 1\ng.list = x, y\ng.on = true\ng.dur.s = 5", &c, err) == 0);
    CHECK(cfg_drop(&c, "g.", "g.int", names, sizeof names) == 5); /* a, x, y, true, 5: the list counts per element */
    CHECK_STR(names, "g.str, g.list, g.on, g.dur.s");              /* each key once */
    size_t n;
    char **a = cfg_args(&c, 1, none, NULL, &n);
    CHECK(n == 2 && !strcmp(a[0], "--int") && !strcmp(a[1], "1"));
    free(a);
    CHECK(cfg_drop(&c, "nothing.", NULL, names, sizeof names) == 0 && names[0] == 0);
    char small[8];
    CHECK(cfg_drop(&c, "g.", NULL, small, sizeof small) == 1 && !strcmp(small, "g.int")); /* truncation is safe */
    CHECK(c.n == 0);
    cfg_free(&c);

    /* a reserved key is refused with its line, and is no row of the printout */
    static const struct cfg_key R[] = {
        {"g.int", "int", CFG_INT, 0, NULL, "7", "a number"},
        {"g.disks", NULL, CFG_RESERVED, 0, NULL, "", "not built"},
    };
    CHECK(cfg_load_text("g.int = 1\ng.disks = /a", 22, "f.properties", R, 2, &c, err, sizeof err) == -1);
    CHECK_STR(err, "f.properties:2: g.disks is reserved and not implemented yet: not built (remove the key)");
    char *buf = NULL;
    size_t blen = 0;
    FILE *f = open_memstream(&buf, &blen);
    cfg_print_effective(f, NULL, R, 2, 1, none);
    cfg_print_markdown(f, R, 2);
    fclose(f);
    CHECK(strstr(buf, "g.disks = ") == NULL && strstr(buf, "| `g.disks` | not built |") != NULL);
    free(buf);
}

static void write_file(const char *path, const char *text, mode_t mode)
{
    unlink(path);
    FILE *f = fopen(path, "w");
    if (!f) {
        perror(path);
        exit(1);
    }
    fputs(text, f);
    fclose(f);
    chmod(path, mode);
}

static void test_files(const char *dir)
{
    char path[1024], err[1024];
    struct cfg c;
    snprintf(path, sizeof path, "%s/c.properties", dir);
    write_file(path, "g.int = 3\n", 0600);
    CHECK(cfg_load_file(path, T, NT, &c, err, sizeof err) == 0 && c.n == 1);
    cfg_free(&c);
    write_file(path, "g.int = 3\n", 0644); /* readable by others is fine: the file holds no secret */
    CHECK(cfg_load_file(path, T, NT, &c, err, sizeof err) == 0);
    cfg_free(&c);
    write_file(path, "g.int = 3\n", 0664);
    CHECK(cfg_load_file(path, T, NT, &c, err, sizeof err) == -1 && strstr(err, "writable by its group or by others"));
    write_file(path, "g.int = 3\n", 0602);
    CHECK(cfg_load_file(path, T, NT, &c, err, sizeof err) == -1 && strstr(err, "writable by its group or by others"));
    snprintf(err, sizeof err, "%s/missing.properties", dir);
    char e2[1024];
    CHECK(cfg_load_file(err, T, NT, &c, e2, sizeof e2) == -1 && strstr(e2, "No such file"));
    CHECK(cfg_load_file(dir, T, NT, &c, e2, sizeof e2) == -1 && strstr(e2, "not a regular file"));
    write_file(path, "g.int = 3\ng.nope = 1\n", 0600);
    CHECK(cfg_load_file(path, T, NT, &c, e2, sizeof e2) == -1);
    char want[1200];
    snprintf(want, sizeof want, "%s:2: unknown key \"g.nope\" (docs/config.md lists them)", path);
    CHECK_STR(e2, want);
    unlink(path);
}

/* what the effective-settings printout holds must load to the same options (the printout is a valid file) */
static void test_print(void)
{
    struct cfg c, c2;
    char err[512];
    const char *text = "g.str = a\\, b\\ \ng.int = -3\ng.on = true\ng.fsync = false\ng.mode = a\ng.dur.s = 5m\n"
                       "g.dur.ms = 1500ms\ng.size.mb = 3G\ng.size.b = 4K\ng.list = x\\ y, z\\,w, \\ q\n";
    CHECK(load(text, &c, err) == 0);
    char *buf = NULL;
    size_t blen = 0;
    FILE *f = open_memstream(&buf, &blen);
    char *none[] = {"prog"};
    cfg_print_effective(f, &c, T, NT, 1, none);
    fclose(f);
    CHECK(load(buf, &c2, err) == 0);
    {
        size_t n1, n2;
        char **a1 = cfg_args(&c, 1, none, NULL, &n1), **a2 = cfg_args(&c2, 1, none, NULL, &n2);
        /* the printout adds the defaults of the keys the text left out: every option of the text is in it, as given */
        for (size_t i = 0; i < n1; i++) {
            int found = 0;
            for (size_t j = 0; j < n2; j++)
                found |= !strcmp(a1[i], a2[j]);
            if (!found) {
                fprintf(stderr, "FAIL printout lacks %s\n%s\n", a1[i], buf);
                fails++;
            }
        }
        free(a1);
        free(a2);
    }
    CHECK(strstr(buf, "# f.properties:1\ng.str = a\\, b\\ \n") != NULL);
    CHECK(strstr(buf, "# f.properties:6\ng.dur.s = 300s\n") != NULL);
    CHECK(strstr(buf, "# default\ng.str") == NULL);
    CHECK(strstr(buf, "# not set\n#g.size.b") == NULL); /* it is set */
    cfg_free(&c2);
    free(buf);

    /* with nothing set: defaults where there is one, a commented key where there is none */
    cfg_free(&c);
    f = open_memstream(&buf, &blen);
    cfg_print_effective(f, NULL, T, NT, 1, none);
    fclose(f);
    CHECK(strstr(buf, "# default\ng.str = x\n") != NULL);
    CHECK(strstr(buf, "# not set\n#g.size.b =\n") != NULL);
    CHECK(strstr(buf, "# not set\n#g.list =\n") != NULL);
    free(buf);

    /* the command line is named as the source, with its values */
    CHECK(load("g.int = 1\ng.str = s", &c, err) == 0);
    char *cl[] = {"prog", "--int", "9", "--item", "p", "--item", "q,r", "--no-fsync"};
    f = open_memstream(&buf, &blen);
    cfg_print_effective(f, &c, T, NT, 8, cl);
    fclose(f);
    CHECK(strstr(buf, "# command line\ng.int = 9\n") != NULL);
    CHECK(strstr(buf, "# command line\ng.list = p, q\\,r\n") != NULL);
    CHECK(strstr(buf, "# command line\ng.fsync = false\n") != NULL);
    CHECK(strstr(buf, "# f.properties:2\ng.str = s\n") != NULL);
    free(buf);
    cfg_free(&c);
}

/* the real table of tsdbd: every default that is written as a value must be a valid value of its kind */
static void test_real_table(void)
{
    for (size_t i = 0; i < TSDBD_NKEYS; i++) {
        const struct cfg_key *k = &tsdbd_keys[i];
        CHECK(k->doc && *k->doc);
        CHECK((k->kind == CFG_RESERVED) == !k->opt); /* a reserved row has a key and no option, nothing else is */
        CHECK(k->kind != CFG_RESERVED || k->key);
        if (!k->key)
            continue;
        for (size_t j = i + 1; j < TSDBD_NKEYS; j++) {
            CHECK(!tsdbd_keys[j].key || strcmp(k->key, tsdbd_keys[j].key));
            CHECK(!k->opt || !tsdbd_keys[j].opt || strcmp(k->opt, tsdbd_keys[j].opt));
        }
        if (k->kind == CFG_RESERVED)
            continue;
        CHECK(*k->opt);
        if (!k->dflt || !*k->dflt || k->kind == CFG_LIST)
            continue;
        char text[512], err[512];
        struct cfg c;
        snprintf(text, sizeof text, "%s = %s\n", k->key, k->dflt);
        int rc = cfg_load_text(text, strlen(text), "default", tsdbd_keys, TSDBD_NKEYS, &c, err, sizeof err);
        if (rc != 0) {
            fprintf(stderr, "FAIL the default of %s does not load: %s\n", k->key, err);
            fails++;
        } else
            cfg_free(&c);
    }
}

int main(int argc, char **argv)
{
    const char *dir = argc > 1 ? argv[1] : "/tmp";
    test_syntax();
    test_lists();
    test_kinds();
    test_precedence();
    test_last_wins();
    test_drop();
    test_files(dir);
    test_print();
    test_real_table();
    if (fails) {
        fprintf(stderr, "test_props: %d failures\n", fails);
        return 1;
    }
    printf("test_props: ok\n");
    return 0;
}
