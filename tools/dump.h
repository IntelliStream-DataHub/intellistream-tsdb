/* SPDX-License-Identifier: AGPL-3.0-only */
/* tsdb export / import: the stream format of docs/export-format.md (tools/dump.c). */
#ifndef TSDB_TOOLS_DUMP_H
#define TSDB_TOOLS_DUMP_H

#include <stdio.h>

/* `tsdb <dir> export [args]`: returns the exit status. */
int dump_export_main(const char *dir, int argc, char **argv);

/* `tsdb <dir> import [args]`: returns the exit status, or -1 when the input is not an export stream and has no
 * export options: then *csv is the whole input (the bytes already read included) for the CSV import. */
int dump_import_main(const char *dir, int argc, char **argv, FILE **csv);

#endif
