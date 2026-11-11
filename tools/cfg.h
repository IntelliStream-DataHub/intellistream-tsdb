/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * Configuration files for the programs that take command-line options (docs/config.md): a table maps each dotted key
 * of a .properties file (props.h) to one long option; loading checks every key and value against the table and turns
 * the settings into option arguments, which the program's own option parser then reads in front of the real command
 * line. The command line therefore wins, by one rule: an option that appears on it is not taken from the file at all
 * (so a list on the command line replaces the file's list, it does not extend it). When an option or flag appears more
 * than once on the command line, its last occurrence counts, in every form (--no-auth then --no-auth=false is login on). Nothing here knows what an option
 * means; ranges and combinations stay in the program, which says where a bad value came from through cfg_where.
 */
#ifndef TSDB_CFG_H
#define TSDB_CFG_H

#include <stddef.h>
#include <stdio.h>

enum cfg_kind {
    CFG_STR,  /* one string */
    CFG_INT,  /* a whole number (the program checks the range) */
    CFG_BOOL, /* true or false: a flag; neg: the option is the --no-X form and is given when the value is false */
    CFG_ENUM, /* one of the words in `values`, separated by | */
    CFG_DUR,  /* a whole number of `unit` (ms or s), or a number with ms, s, m, h or d: 90s, 5m, 1h */
    CFG_SIZE, /* a whole number of `unit` (B or MB), or a number with B, K, M, G or T (binary: 1K = 1024 B) */
    CFG_LIST, /* comma-separated: the option is given once per element */
    CFG_RESERVED, /* a key of a design that is not built: opt is NULL, doc says why; a file that sets it is refused */
};

struct cfg_key {
    const char *key;    /* the dotted key; NULL: an option that has none, and `doc` says why */
    const char *opt;    /* the long option, without the dashes (NULL: CFG_RESERVED) */
    enum cfg_kind kind;
    int neg;            /* CFG_BOOL */
    const char *unit;   /* CFG_DUR, CFG_SIZE: the unit the option takes; CFG_ENUM: the words */
    const char *dflt;   /* what the program does without it, for the key table and --check-config */
    const char *doc;
};

struct cfg_set {
    const struct cfg_key *k;
    char *arg; /* the option's argument (NULL for a flag); one entry per list element */
    char *optstr; /* "--option", owned by the set: what cfg_args hands out lives as long as the cfg */
    int bval;  /* CFG_BOOL */
    int line;
};

struct cfg {
    char *path;
    struct cfg_set *s;
    size_t n;
};

/* Reads and checks the file at path: a regular file that is not writable by its group or others (whoever can change
 * it decides how the server stores and serves data). 0, or -1 with "path:line: key: message" in err. */
int cfg_load_file(const char *path, const struct cfg_key *tab, size_t ntab, struct cfg *c, char *err, size_t errsz);
/* The same on text already in memory; name stands for the path in messages. */
int cfg_load_text(const char *text, size_t len, const char *name, const struct cfg_key *tab, size_t ntab,
                  struct cfg *c, char *err, size_t errsz);
void cfg_free(struct cfg *c);
/* Forgets the settings of the keys that start with prefix, except the key `keep` (NULL: none): for a program that lets a
 * command-line switch turn a whole group off (--cluster=false). The names of the keys dropped go in names, comma
 * separated (truncated to size); the count is returned. */
size_t cfg_drop(struct cfg *c, const char *prefix, const char *keep, char *names, size_t size);

/* The option arguments the file contributes, for the options that do not appear in argv[1..argc) (an argument "--"
 * ends the options). Only keys that start with prefix (NULL: all). A malloc'd array of *n pointers into c: free the
 * array whenever you like; the strings it points to belong to c and live until cfg_free(c). */
char **cfg_args(const struct cfg *c, int argc, char **argv, const char *prefix, size_t *n);

struct option;
/* The argv for getopt_long of a program whose options are opts: argv[0], then the file's settings (c may be NULL), then
 * argv[1..argc) with two changes. (1) A flag of the table may be written --FLAG=true or --FLAG=false on the command
 * line: the file can say true where the default is false and the command line must be able to say it back. =true
 * becomes the bare flag and =false drops out (the option is the default's side then); a flag on the command line
 * in either form keeps the file's value out, like every option on it. (2) An abbreviated long option (getopt_long
 * accepts --work for --workers) is refused: the table's rule "an option on the command line is not taken from the
 * file" matches names exactly, and an abbreviation would pass as absent and be applied twice. The result is a malloc'd
 * block (the strings of (1) are inside it) that must stay alive as long as the options are read from it (optarg points
 * into it): a program that parses once and ends keeps it. NULL with a message in err on an abbreviation or when out of
 * memory. *nargc is the new count. */
char **cfg_argv(const struct cfg *c, int argc, char **argv, const struct option *opts, const struct cfg_key *tab,
                size_t ntab, int *nargc, char *err, size_t errsz);

/* Does argv carry the option of k? If so and val is not NULL, *val is its LAST argument, the one that counts (or
 * "true"/"false" for a flag: the value of the key, so for a --no-X option --no-X is "false" and --no-X=false is "true"). */
int cfg_on_cmdline(int argc, char **argv, const struct cfg_key *k, const char **val);

/* "key (path:line)" for an argument that cfg_args handed out, else NULL (it came from the command line). */
const char *cfg_where(const struct cfg *c, const char *arg, char *buf, size_t sz);

/* "key (path:line)" when the option opt (without dashes) is in effect through the file: a key of the file set it, and
 * the command line does not carry it. Else NULL. */
const char *cfg_where_opt(const struct cfg *c, int argc, char **argv, const char *opt, char *buf, size_t sz);

/* The effective settings as a .properties file: each key with its value, and a comment above it that says where the
 * value came from (the command line, path:line, or the default). c may be NULL. */
void cfg_print_effective(FILE *f, const struct cfg *c, const struct cfg_key *tab, size_t ntab, int argc, char **argv);

/* The key table as Markdown (docs/config.md holds this text, and a test compares them), the options without a key
 * in a second table. */
void cfg_print_markdown(FILE *f, const struct cfg_key *tab, size_t ntab);

#endif
