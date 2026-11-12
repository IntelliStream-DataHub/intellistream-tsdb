/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * A reader of Java-style .properties files, for tsdbd's and the CLI's configuration (docs/config.md).
 *
 * The syntax is Java's (java.util.Properties) where that is safe, and stricter where Java would hide a mistake:
 *   - "key = value", "key: value" or "key value"; leading white space of a line and around the separator is skipped;
 *   - lines starting with # or ! are comments (a comment line is never continued); blank lines are skipped;
 *   - a line ending in an odd number of backslashes continues on the next line, whose leading white space is skipped;
 *   - the escapes \t \n \r \f \\ \= \: \# \! \<space> \, and \uXXXX (UTF-16, surrogate pairs joined); any other
 *     backslash sequence is an error, where Java drops the backslash (a Windows path or a typo would pass);
 *   - the file is UTF-8 (Java reads ISO-8859-1 and wants \uXXXX for anything else); invalid UTF-8 and NUL are errors;
 *   - white space at the end of a value is dropped unless escaped (Java keeps it, which makes "dir = /data " a
 *     directory with a space in its name nobody sees);
 *   - an empty key is an error. Repeated keys are the caller's to judge (props keeps them all, in file order).
 *
 * A value is kept raw (escapes not yet processed) so that a list can be cut at its unescaped commas first:
 * props_split splits at commas, trims each element and then processes its escapes, so "a\, b" is one element
 * "a, b" and "\ x" starts with a space. Errors are "line N: what", in the caller's buffer.
 */
#ifndef TSDB_PROPS_H
#define TSDB_PROPS_H

#include <stddef.h>

typedef struct prop {
    char *key; /* escapes processed */
    char *raw; /* the value as written, continuations joined, escapes untouched */
    int line;  /* the line the entry starts on (1-based) */
} prop;

typedef struct props {
    prop *v;
    size_t n;
} props;

/* Parses len bytes. 0 on success; -1 with a message in err (never more than errsz bytes, always terminated). */
int props_parse(const char *buf, size_t len, props *out, char *err, size_t errsz);
void props_free(props *p);

/* The value of raw with escapes processed (white space around it is already gone); an unescaped comma is an error
 * (a list where one value is expected). *out is malloc'd. 0, or -1 with err. */
int props_value(const char *raw, char **out, char *err, size_t errsz);

/* Splits raw at its unescaped commas, trims each element, processes its escapes. An empty (or all white space) raw is
 * an empty list; an empty element ("a,,b", "a,") is an error. *items is malloc'd with n malloc'd strings. */
int props_split(const char *raw, char ***items, size_t *n, char *err, size_t errsz);
void props_items_free(char **items, size_t n);

#endif
