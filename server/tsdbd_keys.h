/* SPDX-License-Identifier: AGPL-3.0-only */
/*
 * The configuration keys of tsdbd (docs/config.md): one row per command-line option of tsdbd, in the order of the
 * groups of the file. This is the only place a key is named; tsdbd's option parser, `tsdbd --config-keys` (the
 * Markdown table that docs/config.md holds) and tests/test_config.sh (every option of tsdbd.c has a row, and the
 * document says what this table says) all read it. A row without a key is an option the file cannot take, and says
 * why.
 *
 * Reserved for the device classes (docs/design-cluster-data.md, not built): storage.disks (the --disk of that design,
 * PATH[:CLASS] per element) and storage.tiers (the stores of the single-server tier, as `tier add`). They are rows
 * with no option (CK_X): a file that sets one is refused with a message, so a file written for a later version is
 * never read as if the key did nothing. When the design is built the row becomes a list with its option.
 */
#ifndef TSDBD_KEYS_H
#define TSDBD_KEYS_H

#include "../tools/cfg.h"

#define CK_NONE (const char *)0
#define CK_S(key, opt, dflt, doc) {key, opt, CFG_STR, 0, NULL, dflt, doc}
#define CK_I(key, opt, dflt, doc) {key, opt, CFG_INT, 0, NULL, dflt, doc}
#define CK_B(key, opt, dflt, doc) {key, opt, CFG_BOOL, 0, NULL, dflt, doc}
#define CK_N(key, opt, dflt, doc) {key, opt, CFG_BOOL, 1, NULL, dflt, doc} /* the option is --no-X, given when false */
#define CK_E(key, opt, words, dflt, doc) {key, opt, CFG_ENUM, 0, words, dflt, doc}
#define CK_D(key, opt, unit, dflt, doc) {key, opt, CFG_DUR, 0, unit, dflt, doc}
#define CK_Z(key, opt, unit, dflt, doc) {key, opt, CFG_SIZE, 0, unit, dflt, doc}
#define CK_L(key, opt, dflt, doc) {key, opt, CFG_LIST, 0, NULL, dflt, doc}
#define CK_X(key, doc) {key, NULL, CFG_RESERVED, 0, NULL, "", doc} /* a key of a design that is not built */

static const struct cfg_key tsdbd_keys[] = {
    /* storage */
    CK_S("storage.dir", "dir", "", "The database directory (required, unless this is a witness)."),
    CK_N("storage.fsync", "no-fsync", "true",
       "false turns fsync off: a power loss then looks like damage and fails the next open. For tests and "
       "benchmarks only."),
    CK_B("storage.read.only", "read-only", "false", "Serve queries only, from a database another server writes."),
    CK_B("storage.follow", "follow", "false",
       "Follow a database that another writer on shared storage updates (read-only; see docs/ha.md)."),
    CK_D("storage.refresh", "refresh-ms", "ms", "100ms", "How often a follower looks for new parts."),
    CK_B("storage.verify.checksums", "verify-checksums", "false",
       "Check the hash of every data block read. Turn it on unless the file system checks its data itself (ZFS)."),
    CK_B("storage.pin.index", "pin-index", "false", "Keep the granule index of every part in memory."),
    CK_D("storage.partition", "partition-ms", "ms", "1d", "Length of a base partition. Used only when the database is created."),
    CK_I("storage.partition.classes", "partition-classes", "1",
       "1: series of different rates go to partitions of 1, 7, 28 or 364 days; 0: all to base partitions. Used only "
       "when the database is created."),
    CK_I("storage.granule.rows", "granule-rows", "8192", "Rows per granule. Used only when the database is created."),
    CK_X("storage.disks", "the disks of a device-class server, PATH[:CLASS] each (docs/design-cluster-data.md, section 15)"),
    CK_X("storage.tiers", "the stores of the single-server tier, PATH[:CLASS] each, as `tier add` makes them "
                          "(docs/design-cluster-data.md)"),
    /* server */
    CK_S("server.listen", "listen", "127.0.0.1", "The address clients connect to (there is no TLS: a trusted interface)."),
    CK_I("server.port", "port", "7070", "The client port; 0 lets the kernel choose (see server.port.file)."),
    CK_S("server.port.file", "port-file", "",
       "A file that tsdbd writes, once it serves, with \"PORT PID\" (and the metrics port), for a launcher to read."),
    CK_I("server.workers", "workers", "", "Worker threads. Default: the number of CPUs."),
    CK_I("server.max.connections", "max-connections", "10000", "Most client connections (also bounded by the descriptor limit)."),
    CK_E("server.io", "io", "auto|uring|epoll", "auto", "The event loop: io_uring, or epoll where io_uring is disabled."),
    CK_D("server.send.timeout", "send-timeout", "s", "60s", "Close a connection whose reader takes no reply for this long."),
    CK_D("server.frame.timeout", "frame-timeout", "s", "60s", "Close a connection that stops in the middle of a request."),
    CK_Z("server.max.request.memory", "max-request-memory", "MB", "",
       "Memory all requests in flight may hold. Default: an eighth of the RAM, at least 1G."),
    /* auth */
    CK_N("auth.enabled", "no-auth", "true",
       "false: no login, every client may read and write every series, the user admin operations are refused. A "
       "trusted network only."),
    CK_D("auth.login.timeout", "login-timeout", "s", "10s", "Close a connection that has not logged in within this time."),
    /* wal */
    CK_D("wal.sync", "wal-sync-ms", "ms", "0",
       "0: every write returns after its own fsync (shared by concurrent writers); a time: sync that often and bound "
       "the loss of a power cut to it; -1: no WAL."),
    CK_I("wal.max.batch.rows", "wal-max-batch-rows", "65536", "Most rows of one insert that go through the WAL."),
    /* merge and flush */
    CK_D("merge.idle", "merge-idle-s", "s", "300s",
       "Merge a partition completely once it has had no new rows for this long; -1 never."),
    CK_D("flush.interval", "flush-interval-ms", "ms", "5s", "How long rows stay in memory before they are flushed to a part."),
    CK_I("insert.zstd.level", "insert-zstd-level", "0",
       "The zstd level of parts that inserts and flushes write; 0 none, -1 the level merges use."),
    /* query */
    CK_I("query.threads", "query-threads", "", "Threads one tsdbd uses for queries. Default: the number of CPUs."),
    CK_I("query.request.threads", "request-threads", "", "Most threads one request may use. Default: all of them."),
    CK_I("query.multi.max.rows", "multi-max-rows", "268435456", "Most rows one QUERY_MULTI answer may carry."),
    CK_I("query.lttb.max.threshold", "lttb-max-threshold", "16777216", "Most points an LTTB query may ask for."),
    /* metrics */
    CK_S("metrics.port", "metrics-port", "", "The Prometheus port (a number, or auto for one the kernel chooses). Off by default."),
    CK_S("metrics.bind", "metrics-bind", "", "The address of the metrics port. Default: server.listen."),
    /* stats and the latest table */
    CK_N("stats.enabled", "no-stats", "true", "false turns the statistics (docs/design-stats.md) off."),
    CK_Z("latest.cache", "latest-cache", "B", "", "Memory of the latest-value table. Default: a sixteenth of the RAM."),
    /* health */
    CK_L("health.domains", "health-domain", "",
       "Path prefixes of storage that can fail alone (an NFS mount each): a volume under one answers UNAVAILABLE "
       "while its storage is down and the others go on. A comma-separated list."),
    /* cluster */
    CK_B("cluster.enabled", "cluster", "false", "Join a cluster (needs cluster.secret.file and cluster.state.dir)."),
    CK_I("cluster.peer.port", "peer-port", "", "The port of the other members. Default: server.port + 1."),
    CK_S("cluster.peer.listen", "peer-listen", "", "The address of the peer port. Default: server.listen."),
    CK_S("cluster.advertise", "advertise", "", "The host name other members and clients use for this one."),
    CK_S("cluster.rack", "rack", "", "The failure domain of this node (a rack name)."),
    CK_S("cluster.secret.file", "cluster-secret-file", "", "The file with the cluster's shared secret (mode 600)."),
    CK_S("cluster.state.dir", "state-dir", "", "Where the metadata group keeps its state (a local disk, never storage.dir)."),
    CK_S("cluster.power.feed", "power-feed", "", "The power feed of this node, a failure domain."),
    CK_S("cluster.bmc", "bmc", "", "The address of this node's BMC, for fencing."),
    CK_B("cluster.witness", "witness", "false", "A member that only votes: no volumes, no client port."),
    CK_E("cluster.read.barrier", "read-barrier", "on|off", "on", "Whether reads wait for the leader's confirmation."),
    /* options without a key */
    CK_S(CK_NONE, "version", "", "Prints the version and exits."),
    CK_S(CK_NONE, "config", "", "Names the file itself."),
    CK_S(CK_NONE, "no-config", "", "Reads no file, not even /etc/tsdb/tsdbd.properties."),
    CK_S(CK_NONE, "check-config", "", "Checks the file and the command line, prints the effective settings and exits."),
    CK_S(CK_NONE, "config-keys", "", "Prints the table of keys (Markdown) and exits."),
    CK_S(CK_NONE, "exit-with-parent", "",
       "Test harnesses only: stop when the parent process goes away. A file must not make a server depend on it."),
};
#define TSDBD_NKEYS (sizeof tsdbd_keys / sizeof tsdbd_keys[0])

#endif
