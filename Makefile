# `make` alone builds `all`: a rule for single objects above its line (the chunk objects' headers) would otherwise
# become the default goal (`make` built only build/chunk.o from R6 on; `make check-default-goal` tests it).
.DEFAULT_GOAL := all
CC      ?= cc
UNAME_S := $(shell uname -s)
# macOS is a developer platform (see docs/macos.md): no io_uring, no CPU levels, the portable scalar kernels on
# arm64, zstd from Homebrew. The jepsen, crashfs, dist and distro targets are Linux only.
ifeq ($(UNAME_S),Darwin)
NO_URING ?= 1
SOEXT   := dylib
BREW_ZSTD := $(shell brew --prefix zstd 2>/dev/null)
PLAT_CFLAGS :=
ifneq ($(BREW_ZSTD),)
PLAT_CFLAGS += -I$(BREW_ZSTD)/include
ZSTD_LIB ?= -L$(BREW_ZSTD)/lib -lzstd
endif
# clang, unlike GCC, warns on every designated initializer that leaves a field out
PLAT_CFLAGS += -Wno-missing-field-initializers
# the fuzz driver's weak reference to the sanitizer runtime, absent in the plain replay build
PLAT_FUZZ_LDFLAGS := -Wl,-U,___sanitizer_set_death_callback
ifeq ($(shell uname -m),arm64)
ARCH    ?= -mcpu=native
endif
CRASHFS_SO :=
OUTAGE_SERVER :=
else
SOEXT   := so
CRASHFS_SO = $(B)/libcrashfs.so $(B)/liboutagefs.so
# tests/outage_server.c: tsdbd on a storage domain that can be taken away, for the Rust, Python and Java client tests
OUTAGE_SERVER = $(B)/outage_server $(B)/liboutagefs.so
endif
# -march=native gives the codecs BMI/LZCNT; the aggregation kernels pick
# AVX-512/AVX2/scalar at runtime either way. `make PORTABLE=1` for a generic build.
ifeq ($(PORTABLE),1)
ARCH    ?=
else
ARCH    ?= -march=native
endif
CFLAGS  ?= -O3 -g
# -ffp-contract=off: GNU C modes let GCC fuse a*b+c into FMA where the target has it, so a
# -march=native build and a portable one would round differently; codec choices (and the
# bytes they write) must not depend on the CPU the code was built for, nor on the CPU
# level of `make dist` in use (src/levels.h).
override CFLAGS += -std=gnu11 -Wall -Wextra -Wno-unused-parameter -fno-math-errno -ffp-contract=off $(ARCH) -Iinclude -fPIC
override CFLAGS += $(PLAT_CFLAGS)
# `make dist` links these statically (-l:libzstd.a); glibc stays dynamic.
ZSTD_LIB  ?= -lzstd
URING_LIB ?= -luring
LDLIBS  += $(ZSTD_LIB) -lpthread -lm $(LINK_EXTRA)
# Output directory; `make tsan` builds into its own, next to a normal build.
B       ?= build
# the power-loss sweeps need libcrashfs.so: where it is built (every platform but macOS) a missing one is a failure
ifneq ($(UNAME_S),Darwin)
export TSDB_REQUIRE_CRASHFS := 1
endif
# where the tests make their scratch directories (each run makes its own below it)
SCRATCH ?= $(or $(TMPDIR),/tmp)
# the fuzz harnesses (tests/fuzz/fuzz_<name>.c; see `make fuzz`)
FUZZ_HARNESSES := codec codecx part manifest wal dict volumes tz import users classes marker wire props chunk ec repl replframe ecpart diverge
# objects a harness links besides the library (in $(B) or build-fuzz/)
FUZZ_OBJS_users := users.o tsdb_crypto.o
FUZZ_OBJS_props := props.o cfg.o
FUZZ_OBJS_replframe := peerlink.o tsdb_crypto.o replwire.o fwd.o chunksrv.o
# the data links' frame check: inline XXH3 (server/plcheck.h, server/xxhash); whoever includes it depends on both
PLCHECK_HDR := server/plcheck.h server/xxhash/xxhash.h
# test_peerlink's cases that carry frames on every link kind, run again at the check levels header and full
PL_KINDS_TESTS := log,bulk,resume,flip,kinds,chunk,fwd,fwdstream,fake,fakeend
TSDBD   ?= $(B)/tsdbd
# Event loop backends the server tests and jepsen run against; `make test IO=epoll`
# where io_uring is disabled (RHEL and AlmaLinux 10). NO_URING=1 builds tsdbd on
# epoll alone, without liburing, for hosts where io_uring is disabled anyway.
ifeq ($(NO_URING),1)
IO      ?= epoll
URING_FLAGS := -DTSDBD_NO_URING
URING_LIBS  :=
else
IO      ?= uring epoll
URING_FLAGS :=
URING_LIBS   = $(URING_LIB)  # -l:liburing.a in make dist
endif

# The verbs transport of the peer links (docs/design-rdma.md §5.4): built where rdma-core's headers are
# (libibverbs-devel / libibverbs-dev; RDMA_CFLAGS=-I... for headers elsewhere), else as NO_RDMA=1 with one line (on
# stderr: `make -s print-ec` and the like are read by scripts). No
# RDMA library is linked either way: server/plrdma_dl.c loads libibverbs with dlopen (tools/check-rdma-syms.sh).
# macOS never has it.
ifeq ($(UNAME_S),Darwin)
NO_RDMA := 1
endif
RDMA_CFLAGS ?=
ifeq ($(NO_RDMA),)
NO_RDMA := $(shell echo 'int x;' | $(CC) $(RDMA_CFLAGS) -include infiniband/verbs.h -E -x c - >/dev/null 2>&1 && echo 0 || echo 1)
ifeq ($(NO_RDMA),1)
$(warning RDMA: rdma-core's headers (infiniband/verbs.h) not found: tsdbd is built without the verbs transport (NO_RDMA=1))
endif
endif
RDMA_FLAGS := $(if $(filter 1,$(NO_RDMA)),,-DTSDB_RDMA $(RDMA_CFLAGS))
# plrdma_dl.c's dlopen and dlsym live in libdl before glibc 2.34 (EL8, Debian 11); --as-needed keeps it out of the
# links where libc has them (check-glibc.sh allows libdl either way)
LDLIBS += $(if $(filter 1,$(NO_RDMA)),,-Wl,--push-state,--as-needed -ldl -Wl,--pop-state)

comma := ,

SRC  := $(wildcard src/*.c)
OBJ  := $(SRC:src/%.c=$(B)/%.o)

# CPU levels (src/levels.h): with CPU_LEVELS="v3 v4" these files are compiled
# once for $(ARCH) and once per level, and src/cpu.c picks one at startup.
# Off by default: a -march=native build has nothing to pick.
LV_SRC := codec codec_alp codec_typed codec_xor values
LV_FLAGS_v3 := -march=x86-64-v3
# 256-bit vectors: the gain of v4 is AVX-512's instructions (int64 <-> double,
# masks, 64-bit min/max), and Skylake-SP slows the core after 512-bit ones.
LV_FLAGS_v4 := -march=x86-64-v4 -mprefer-vector-width=256
ifneq ($(strip $(CPU_LEVELS)),)
override CFLAGS += -DTSDB_LEVELS $(foreach l,$(CPU_LEVELS),-DTSDB_HAVE_$(l))
OBJ := $(filter-out $(LV_SRC:%=$(B)/%.o),$(OBJ)) $(foreach l,base $(CPU_LEVELS),$(LV_SRC:%=$(B)/%.$(l).o))
endif
# The erasure codec (src/ec, docs/design-cluster-data.md E32): intellistream-objects' src/codec copied unchanged
# (tools/sync-ec.sh), in libtsdb. It is x86-64 code (SSSE3 and wider kernels behind runtime dispatch; no scalar port of
# the copy is needed, since the scalar kernel is in it): other machines build without it. -DISO_GF256_NO_BOOT leaves
# out gf256.c's constructor; src/cpu.c calls tsdb_ec_select() instead (AGENTS.md "CPU levels"). Compiled once, at the
# baseline of $(ARCH): the AVX2, AVX-512 and GFNI kernels carry target attributes and run only where src/ec/compat.c's
# iso_cpu_has() says so (tools/check-isa.sh checks that nothing else in them goes beyond x86-64-v2).
# NO_EC=1 builds an x86-64 engine without it, as the embedded bindings and other machines do (after make clean, or with
# its own B=): every R7 stub and test must build and pass both ways. Only NO_EC=1 turns it off (NO_EC=0 or empty: on).
ifeq ($(shell uname -m)$(filter 1,$(NO_EC)),x86_64)
EC_OBJ := $(B)/ec_gf256.o $(B)/ec_gf256_avx2.o $(B)/ec_gf256_avx512.o $(B)/ec_rs.o $(B)/ec_compat.o
OBJ  += $(EC_OBJ)
EC_BENCH := $(B)/ec-bench
override CFLAGS += -DTSDB_EC
endif
EC_HDR := $(wildcard src/ec/*.h) $(wildcard src/ec/compat/*/*.h)
# R7's headers (settled .ec parts, the tier; src/ecpart.h): every engine object may include them
R7_HDR := src/ecpart.h src/tier.h src/eccache.h src/ecgc.h src/eckeep.h src/ecswap.h src/settle.h src/chunk.h src/ecskip.h src/ecdiverge.h
# -mssse3: gf256.c's SSSE3 kernel has no target attribute (the copy is never edited), so a generic ARCH (PORTABLE=1,
# make tsan, make asan) needs it; the dist build's x86-64-v2 has it already. -fno-tree-vectorize keeps the compiler from
# using SSSE3 (pshufb) outside that kernel, in iso_gf256_init, which runs in every process that loads libtsdb, before
# the CPU is asked: a generic build stays x86-64 baseline there (tools/check-isa.sh checks the objects).
EC_CFLAGS := -mssse3 -fno-tree-vectorize
$(B)/ec_%.o: src/ec/%.c $(EC_HDR) src/internal.h | $(B)
	$(CC) $(CFLAGS) $(EC_CFLAGS) -Isrc/ec/compat -DISO_GF256_NO_BOOT -c -o $@ $<
# Objects, chunk files and stores (src/chunk*.c, docs/design-cluster-data.md R6): not part of the codec copy, but chunk.c and
# chunkread.c include its rs.h, which wants the one-line headers of src/ec/compat.
$(B)/chunk.o $(B)/chunkread.o: override CFLAGS += -Isrc/ec/compat
$(B)/chunk.o $(B)/chunkstore.o $(B)/chunkread.o $(B)/chunkheal.o: src/chunk.h src/chunk_priv.h $(EC_HDR)
$(B)/fuzz-ec: override CFLAGS += -Isrc/ec/compat
LIB  := $(B)/libtsdb.a
# defined here, before any rule names it: a rule's prerequisites are expanded when it is read, so a later definition left
# them empty (test_readonly linked a client library that nothing had built yet after make clean)
CLIB := $(B)/libtsdbclient.a
BINS := $(B)/tsdb $(B)/tsdb-bench $(B)/test_tsdb

all: $(LIB) $(B)/libtsdb.$(SOEXT) $(B)/tsdb $(B)/tsdb-bench $(B)/codec-bench $(B)/insert-bench $(B)/tsdbd $(CLIB) $(B)/tsdbc-bench $(B)/outage-load $(B)/scale-bench $(B)/type-bench $(B)/twa-bench $(B)/fold-bench $(B)/threads-bench $(B)/wtail-bench $(B)/decode-bench $(B)/link-bench $(EC_BENCH)

$(B)/%.o: src/%.c src/internal.h src/stats_priv.h src/compat.h src/ext80.h src/levels.h src/bitpack.h src/fpmode.h include/tsdb.h $(R7_HDR) | $(B)
	$(CC) $(CFLAGS) -c -o $@ $<
# simd.c includes its kernels twice each (SSE and AVX, AVX and AVX2)
$(B)/simd.o: src/simd_avx.inc src/simd_sse.inc
$(B)/health.o: src/health_rules.h
# Its lanes start at zero (arrays of 16 and 32 doubles): GCC's generic tuning (the dist build) clears 33 to 8192
# known bytes with `rep stosq`, whose startup (some 30 cycles) was a third of a short run's fold (fold-bench --rows
# 32, sse kernels); -march=native picks vector stores itself. Vector stores up to 512 bytes, memset beyond.
MEMSET_FLAG := $(shell $(CC) -mmemset-strategy=vector_loop:512:noalign,libcall:-1:noalign -E -x c /dev/null \
                 >/dev/null 2>&1 && echo -mmemset-strategy=vector_loop:512:noalign,libcall:-1:noalign)
$(B)/simd.o: override CFLAGS += $(MEMSET_FLAG)

$(B)/%.base.o: src/%.c src/internal.h src/ext80.h src/levels.h src/bitpack.h src/fpmode.h include/tsdb.h | $(B)
	$(CC) $(CFLAGS) -DTSDB_LV=base -c -o $@ $<

$(B)/%.v3.o: src/%.c src/internal.h src/levels.h src/bitpack.h src/fpmode.h include/tsdb.h | $(B)
	$(CC) $(CFLAGS) $(LV_FLAGS_v3) -DTSDB_LV=v3 -c -o $@ $<

$(B)/%.v4.o: src/%.c src/internal.h src/levels.h src/bitpack.h src/fpmode.h include/tsdb.h | $(B)
	$(CC) $(CFLAGS) $(LV_FLAGS_v4) -DTSDB_LV=v4 -c -o $@ $<

# the calendar helper is client-side code, in both libraries: the engine itself knows UTC only
$(LIB): $(OBJ) $(B)/tsdb_calendar.o
	$(AR) rcs $@ $^

# The engine as a shared library for the embedded bindings (Rust, Python, Java FFM).
# Only the public tsdb_* API is exported (tools/libtsdb.map); zstd is linked in
# statically when ZSTD_LIB says so (make dist), else dynamically.
ifeq ($(UNAME_S),Darwin)
# no version script. ld64 takes either -exported_symbols_list or -unexported_symbols_list, never both ("can't use
# -unexported_symbols_list and -exported_symbols_list"), so one list: every tsdb_* the objects define except the
# tsdb_test_* hooks (tools/libtsdb.map's twin; nobody has run this on a Mac yet: docs/tasks/macos.md)
$(B)/libtsdb.$(SOEXT): $(OBJ) $(B)/tsdb_calendar.o tools/libtsdb.map
	nm -gUj $(OBJ) $(B)/tsdb_calendar.o | grep '^_tsdb_' | grep -v '^_tsdb_test_' | sort -u > $(B)/libtsdb.exports
	$(CC) $(CFLAGS) -dynamiclib -Wl,-exported_symbols_list,$(B)/libtsdb.exports \
	    -install_name @rpath/libtsdb.dylib -o $@ $(OBJ) $(B)/tsdb_calendar.o $(LDLIBS)
else
$(B)/libtsdb.so: $(OBJ) $(B)/tsdb_calendar.o tools/libtsdb.map
	$(CC) $(CFLAGS) -shared -Wl,--version-script=tools/libtsdb.map -Wl,-soname,libtsdb.so -o $@ \
	    $(OBJ) $(B)/tsdb_calendar.o $(LDLIBS)
endif

$(B)/tsdb_calendar.o: client/tsdb_calendar.c include/tsdb_calendar.h include/tsdb.h | $(B)
	$(CC) $(CFLAGS) -c -o $@ $<

# export writes the tsdb version and the on-disk format version (not in the public API) into its stream header
TSDB_FORMAT := $(shell sed -n 's/^\#define FORMAT_VERSION \([0-9]*\).*/\1/p' src/internal.h)

# configuration files (tools/props.c, tools/cfg.c; docs/config.md): in tsdbd, in every binary that includes tsdbd.c, and
# in the CLI
CFG_OBJ := $(B)/props.o $(B)/cfg.o
$(B)/props.o: tools/props.c tools/props.h | $(B)
	$(CC) $(CFLAGS) -c -o $@ $<
$(B)/cfg.o: tools/cfg.c tools/cfg.h tools/props.h | $(B)
	$(CC) $(CFLAGS) -c -o $@ $<

# CLUSTER_OBJ (defined further down, with the cluster) is a prerequisite there too: `tsdb cluster` (cluster-cli.c)
$(B)/tsdb: tools/tsdb-cli.c tools/dump.c tools/dump.h tools/common.h tools/users-cli.c tools/cluster-cli.c tools/cfg.h \
          server/cluster.h client/tsdb_client.h $(B)/users.o $(B)/tsdb_crypto.o $(B)/libtsdbclient.a $(CFG_OBJ) $(LIB)
	$(CC) $(CFLAGS) -DTSDB_BUILD_VERSION='"$(TSDB_VERSION)"' -DTSDB_FORMAT_VERSION=$(TSDB_FORMAT) -o $@ $< tools/dump.c \
	    tools/users-cli.c $(B)/cluster-cli.o $(B)/cluster-user-cli.o $(CLUSTER_OBJ) $(CFG_OBJ) $(B)/users.o $(B)/tsdb_crypto.o $(CLIB) $(LIB) $(LDLIBS)

# login (docs/design-auth.md): the crypto is client code, shared with tsdbd and the CLI; USERS and the grant model
# are tsdbd's and the CLI's
$(B)/tsdb_crypto.o: client/tsdb_crypto.c client/tsdb_crypto.h | $(B)
	$(CC) $(CFLAGS) -c -o $@ $<

$(B)/users.o: server/users.c server/users.h client/tsdb_crypto.h include/tsdb.h | $(B)
	$(CC) $(CFLAGS) -c -o $@ $<

$(B)/test_crypto: tests/test_crypto.c tests/rfc7677.h $(B)/tsdb_crypto.o
	$(CC) $(CFLAGS) -Iclient -o $@ $< $(B)/tsdb_crypto.o

# The C client with its test hooks (tests/client_hooks.h), which libtsdbclient.a does not have; linked before
# $(CLIB), so the archive's own tsdb_client.o is not pulled in.
$(B)/tsdb_client_hooks.o: client/tsdb_client.c client/tsdb_client.h client/tsdb_crypto.h include/tsdb_protocol.h \
                          include/tsdb.h include/tsdb_wire.h | $(B)
	$(CC) $(CFLAGS) -DTSDBC_TEST_HOOKS -c -o $@ $<

# starts tsdbd (login on) and links server/auth.c for the RFC 7677 exchange on the server side in this process
$(B)/test_auth: tests/test_auth.c tests/harness.h tests/auth_harness.h tests/rfc7677.h tests/client_hooks.h \
                server/auth.c server/auth.h server/tenants.c server/tenants.h server/users.h $(B)/users.o $(B)/tsdb_client_hooks.o $(CLIB) $(LIB)
	$(CC) $(CFLAGS) -Iclient -Iserver -o $@ $< server/auth.c server/tenants.c $(B)/users.o $(B)/tsdb_client_hooks.o $(CLIB) $(LIB) \
	    $(LDLIBS)

# every op of TSDB_OPS across tenants (links server/auth.c for its table, makes USERS with server/users.c)
$(B)/test_auth_ops: tests/test_auth_ops.c tests/harness.h tests/auth_harness.h server/auth.c server/auth.h \
                    server/users.h include/tsdb_protocol.h $(B)/users.o $(CLIB) $(LIB)
	$(CC) $(CFLAGS) -Iclient -Iserver -o $@ $< server/auth.c server/tenants.c $(B)/users.o $(CLIB) $(LIB) $(LDLIBS)

# STATS_SERIES and STATS_TENANTS over the network: tsdbd with login on and off, the C client, a fake server
$(B)/test_stats_net: tests/test_stats_net.c tests/harness.h tests/auth_harness.h server/users.h $(B)/users.o $(B)/libtsdbclient.a $(LIB)
	$(CC) $(CFLAGS) -Iclient -Iserver -o $@ $< $(B)/users.o $(CLIB) $(LIB) $(LDLIBS)

# tsdbd --read-only, op by op (the frames of test_auth_ops; links server/users.c for the harness's login helpers)
$(B)/test_readonly: tests/test_readonly.c tests/harness.h tests/auth_harness.h server/users.h include/tsdb_protocol.h \
                    $(B)/users.o $(CLIB) $(LIB)
	$(CC) $(CFLAGS) -Iclient -Iserver -o $@ $< $(B)/users.o $(CLIB) $(LIB) $(LDLIBS)

# includes server/users.c (its record builders) and defines write/fdatasync/fsync/rename to inject errors into it
$(B)/test_users: tests/test_users.c server/users.c server/users.h $(B)/tsdb_crypto.o $(LIB)
	$(CC) $(CFLAGS) -Iclient -Iserver -o $@ $< $(B)/tsdb_crypto.o $(LIB) $(LDLIBS)

$(B)/tsdb-bench: tools/tsdb-bench.c tools/common.h tools/datasets.h tools/cbench.h $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_tsdb: tests/test_tsdb.c tests/samekeys.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# its own write/fsync/fdatasync/pwrite/renameat inject storage errors into the library
# the .properties reader and the key tables (tools/props.c, tools/cfg.c, server/tsdbd_keys.h)
$(B)/test_props: tests/test_props.c server/tsdbd_keys.h tools/cfg.h tools/props.h $(CFG_OBJ)
	$(CC) $(CFLAGS) -o $@ $< $(CFG_OBJ)

$(B)/test_sched: tests/test_sched.c src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_multi: tests/test_multi.c src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS) -lm

$(B)/test_latest: tests/test_latest.c src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_threads: tests/test_threads.c src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_buckets: tests/test_buckets.c src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS) -lm

# The replication core (src/repl.c, sans-IO) under its simulator (docs/design-cluster.md §13.1). sim_repl_mut has
# the planted bugs (MUT() in repl.c, SMUT() in its own transfer model) that tests/sim_repl.sh checks the checker
# catches.
$(B)/sim_repl: tests/sim_repl.c src/repl.c src/repl.h | $(B)
	$(CC) $(CFLAGS) -Isrc -o $@ tests/sim_repl.c src/repl.c

$(B)/sim_repl_mut: tests/sim_repl.c src/repl.c src/repl.h | $(B)
	$(CC) $(CFLAGS) -DREPL_MUTANTS -Isrc -o $@ tests/sim_repl.c src/repl.c

# Scenarios the simulator reaches rarely or never, driven by hand (tests/test_repl.c); tests/sim_repl.sh checks
# with test_repl_mut that each fails with the bug it guards.
$(B)/test_repl: tests/test_repl.c src/repl.c src/repl.h | $(B)
	$(CC) $(CFLAGS) -Isrc -o $@ tests/test_repl.c src/repl.c

$(B)/test_repl_mut: tests/test_repl.c src/repl.c src/repl.h | $(B)
	$(CC) $(CFLAGS) -DREPL_MUTANTS -Isrc -o $@ tests/test_repl.c src/repl.c

$(B)/repl.o: src/repl.h

# `make sim-repl-soak [SOAK_SEEDS=1000000] [SOAK_JOBS=n]`: the long run, then every planted bug with 20,000 seeds.
SOAK_SEEDS ?= 1000000
SOAK_JOBS  ?= $(shell nproc)
sim-repl-soak: $(B)/sim_repl $(B)/sim_repl_mut $(B)/test_repl $(B)/test_repl_mut
	B=$(B) tests/sim_repl.sh soak $(SOAK_SEEDS) $(SOAK_JOBS)

sim-repl-mutants: $(B)/sim_repl $(B)/sim_repl_mut $(B)/test_repl $(B)/test_repl_mut
	B=$(B) tests/sim_repl.sh mutants

# `tsdb DIR upgrade` on the fixture release 0.9.0 wrote (tests/data), power loss at every step (crashfs); the mutant
# run plants "a format-8 header hashed as 48 bytes" and must fail
$(B)/test_upgrade: tests/test_upgrade.c src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# the numbers of the log (fno): never back across opens, FNO is the bound, markers are not rows; power loss at every
# step (crashfs). The mutant runs plant the bugs it guards against and must fail.
$(B)/test_fno: tests/test_fno.c src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_ppos: tests/test_ppos.c src/internal.h src/ppos.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# the DIVERGED file (src/ecdiverge.c, R8 D8): damage, power loss and EIO at every step (crashfs), --clear, and tsdbd's
# one answer for it (STATUS, UNAVAILABLE, /metrics, the root included). The mutant runs plant the bugs it guards against.
$(B)/test_diverge: tests/test_diverge.c tests/harness.h src/internal.h src/ecdiverge.h $(B)/libtsdbclient.a $(LIB) $(CRASHFS_SO)
	$(CC) $(CFLAGS) -Isrc -Iclient -o $@ $< $(CLIB) $(LIB) $(LDLIBS)

# an insert's intent recovered at the open (src/part.c intents_recover), power lost at every step of it (crashfs);
# the mutant unlinks the intent before its renames are synced and must fail
$(B)/test_intents: tests/test_intents.c src/internal.h $(LIB) $(CRASHFS_SO)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)
$(B)/test_drops: tests/test_drops.c src/internal.h $(LIB) $(CRASHFS_SO)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# the REPL file of a replica directory (src/replfile.c, design-cluster.md 4.3): two checksummed slots, power loss at
# every step of create and save (crashfs); the mutant runs plant the bugs it guards against and must fail
$(B)/test_replfile: tests/test_replfile.c src/replfile.h src/repl.h src/internal.h $(LIB) $(CRASHFS_SO)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)
# the power-loss shim itself: writev and pwritev are tracked (the Raft log appends with pwritev)
$(B)/test_crashfs: tests/test_crashfs.c $(CRASHFS_SO) | $(B)
	$(CC) $(CFLAGS) -o $@ $<

# the engine as a replica (src/replica.c, design-cluster.md step 3): the mutant runs plant the bugs it guards against
$(B)/test_replica: tests/test_replica.c src/replica.h src/internal.h $(LIB) $(CRASHFS_SO)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)
$(B)/test_snapshot: tests/test_snapshot.c src/snapshot.h src/marker.h $(R7_HDR) src/internal.h src/replica.h src/replfile.h src/feed.h $(LIB) $(CRASHFS_SO)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# cluster volumes in a tsdb handle (src/cvol.h): holes, routing by the cluster name, the engine swap (promote)
$(B)/test_cvol: tests/test_cvol.c src/cvol.h src/replica.h src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# the disk side of tsdbd's volume manager (server/cvolfs.c): make v<N>, REPL's open generation and nonce; power loss
# at every step (crashfs); the mutant runs plant the bugs it guards against and must fail
$(B)/test_cvolfs: tests/test_cvolfs.c server/cvolfs.c server/cvolfs.h server/cvolset.c server/cvolset.h server/cluster.h server/snapinst.c server/snapinst.h src/replfile.h src/cvol.h src/internal.h $(B)/tsdb_crypto.o $(LIB) $(CRASHFS_SO)
	$(CC) $(CFLAGS) -Isrc -Iserver -Iclient -o $@ tests/test_cvolfs.c server/cvolfs.c server/cvolset.c server/snapinst.c $(B)/tsdb_crypto.o $(LIB) $(LDLIBS)

# the change feed's disk side (design-cdc.md step F1): FEED, requires=feed, retained segments, the cleaner
$(B)/test_feedstore: tests/test_feedstore.c tests/jepsen/feedcheck.c src/internal.h src/feed.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ tests/test_feedstore.c tests/jepsen/feedcheck.c $(LIB) $(LDLIBS)

$(B)/test_failstop: tests/test_failstop.c $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

# the retired-part lifetime against a stepped wall clock (a fake time(): ELF interposition)
$(B)/test_graveyard: tests/test_graveyard.c $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_abandon: tests/test_abandon.c $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_recover: tests/test_recover.c $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

# CI's plain-build job runs TEST_GROUPS as four runners (test-groups-1 .. 4: BT_GROUPS_N, about equal by their ASan times);
# tests/check_ci_groups.sh requires the four to be exactly TEST_GROUPS, each once
BT_TARGETS := test-groups-1 test-groups-2 test-groups-3 test-groups-4
UNITS_GROUPS := test-units-c test-units-d test-units-e test-units-f test-units-g test-units-h1 test-units-h2 test-units-h3 test-units-i
UNITS_B_GROUPS := test-units-b2 test-units-b3 test-units-b4 test-units-b5 test-units-b6 test-units-b7
# the binaries of `make test`, once: the groups below build the same set, so that a binary added here is built for every group
TEST_BINS := $(B)/test_props $(B)/test_tsdb $(B)/test_sched $(B)/test_multi $(B)/test_latest $(B)/test_threads $(B)/test_buckets $(B)/test_shared $(B)/test_compat $(B)/test_failstop $(B)/test_graveyard $(B)/test_abandon $(B)/test_recover $(B)/test_damage $(B)/test_upgrade $(B)/test_fno $(B)/test_diverge $(B)/test_intents $(B)/test_drops $(B)/test_fwd $(B)/test_replfile $(B)/test_crashfs $(B)/test_replica $(B)/test_snapshot $(B)/test_cvol $(B)/test_cvolfs $(B)/test_feedstore $(if $(filter Darwin,$(UNAME_S)),,$(B)/test_crashfs_old) $(B)/test_types $(B)/test_delete $(B)/test_levels $(B)/test_server $(B)/test_server_multi $(B)/test_stability $(B)/test_ports $(B)/test_client $(B)/test_wire $(B)/test_follow $(B)/test_volumes $(B)/test_group $(B)/test_lttb $(B)/test_twa $(B)/test_spread $(B)/test_limits $(B)/test_small_parts $(B)/test_classes $(B)/test_crafted $(B)/test_gindex $(B)/test_flush_tail $(B)/test_stats $(B)/test_tenants $(B)/test_tn_race $(B)/test_tzfiles $(B)/test_export $(B)/test_crypto $(B)/test_users $(B)/test_auth $(B)/test_auth_ops $(B)/test_readonly $(B)/test_stats_net $(B)/tsdb-bench $(FUZZ_HARNESSES:%=$(B)/fuzz-%) $(B)/tsdbd $(CRASHFS_SO) $(B)/tsdb $(B)/fuzz_frames $(B)/test_out_send $(B)/sim_repl $(B)/sim_repl_mut $(B)/test_repl $(B)/test_repl_mut $(B)/test_meta $(B)/test_meta_mut $(B)/test_cluster $(B)/test_peerlink $(B)/test_margins $(B)/test_plrdma
$(UNITS_GROUPS) test-groups $(BT_TARGETS): $(TEST_BINS)
test: $(TEST_BINS)
	./$(B)/test_tsdb $(SCRATCH)
	B=$(B) tests/sim_repl.sh quick
	./$(B)/test_sched $(SCRATCH)
	./$(B)/test_multi $(SCRATCH)
	./$(B)/test_latest $(SCRATCH)
	./$(B)/test_threads $(SCRATCH)
	./$(B)/test_buckets $(SCRATCH)
	LD_LIBRARY_PATH=$(B) DYLD_LIBRARY_PATH=$(B) ./$(B)/test_shared $(SCRATCH)
	./$(B)/test_compat $(SCRATCH)
	TSDB_FAST_FSYNC=1 ./$(B)/test_compat $(SCRATCH)
	./$(B)/test_failstop $(SCRATCH)
	./$(B)/test_graveyard $(SCRATCH)
	./$(B)/test_abandon $(SCRATCH)
	./$(B)/test_recover $(SCRATCH)
	./$(B)/test_damage $(SCRATCH)
	./$(B)/test_upgrade $(SCRATCH)
	$(MUT) --name test_upgrade --rc 1 --grep 'the upgraded database opens' --list "v8hash48" -- ./$(B)/test_upgrade --mutant @M@ @D@ # the planted bug must fail it, with this check
	./$(B)/test_fno $(SCRATCH)
	$(MUT) --name test_fno --rc 1 --grep 'next == 1001' --list "reuse manifest" -- ./$(B)/test_fno --mutant @M@ @D@ # each planted bug must fail it, with its own check
	$(MUT) --name test_fno --rc 1 --grep 'a segment of markers replays' --list "markers" -- ./$(B)/test_fno --mutant @M@ @D@
	$(MUT) --name test_fno --rc 1 --grep 'f > mx' --list "nosync" -- ./$(B)/test_fno --mutant @M@ @D@
	$(MUT) --name test_fno --list "flushclose" -- ./$(B)/test_fno --mutant @M@ @D@ # a flush that leaves a record open crashes in the seal (status 139): the one planted bug whose symptom is a crash
	$(MUT) --name test_fno --rc 1 --grep 'due0 >= t0 \+ N' --list "walclock" -- ./$(B)/test_fno --mutant @M@ @D@ # the periodic sync's clock from 0 must fail the cadence check
	TSDBD=$(TSDBD) ./$(B)/test_diverge $(SCRATCH)
	$(MUT) --name test_diverge --list "nocorrupt seed0 nocut notailconfirm readerrend" -- ./$(B)/test_diverge --case damage --mutant @M@ @D@ # each planted bug must fail it
	$(MUT) --name test_diverge --list "nofail nodirsync nofdatasync metaopen" -- ./$(B)/test_diverge --case eio,power,preexist,cli --mutant @M@ @D@
	$(if $(TEST_GROUPS),$(MAKE) -j1 $(TEST_GROUPS))

# The rest of `make test` as groups (the recipe lines are those that were here, in the same order). `make test` runs
# them last, one after the other; CI runs each on a runner of its own (`make tsan TEST_TARGET=test-meta`,
# .github/workflows/ci.yml): ThreadSanitizer and ASan take about 80 minutes for all of them on 4 CPUs. A group has the
# prerequisites it needs, so that it builds and runs alone. `make test TEST_GROUPS=` runs only the lines above.
# the planted bugs of a test binary, in parallel (tools/mutants.sh: MUTANT_JOBS, MUTANT_URING_JOBS, MUTANT_TIMEOUT)
# Under ThreadSanitizer a planted bug that the binary catches late runs longer than the 1800 s default (test_replica
# takes about 2000 s whole under TSan, more with MUTANT_JOBS of them side by side): a timeout is a survivor, so those
# loops get TSAN_MUT_TIMEOUT unless MUTANT_TIMEOUT is set.
TSAN_MUT_TIMEOUT ?= 5400
MUT = tools/mutants.sh --tmp $(SCRATCH)$(if $(findstring -fsanitize=thread,$(CFLAGS)), --timeout $${MUTANT_TIMEOUT:-$(TSAN_MUT_TIMEOUT)})
META_GROUPS := test-meta-1 test-meta-2 test-cluster-1 test-cluster-2 test-cluster-3 test-links
TEST_GROUPS ?= $(UNITS_GROUPS) test-units-b $(UNITS_B_GROUPS) $(META_GROUPS) test-meta-mutants-1 test-meta-mutants-2 test-net
TEST_TARGET ?= test
.PHONY: test-groups $(BT_TARGETS) test-units-b $(UNITS_GROUPS) $(UNITS_B_GROUPS) test-meta $(META_GROUPS) test-meta-mutants-1 test-meta-mutants-2 test-net
TEST_B_BINS_1 := $(B)/test_ppos $(B)/test_chunk $(B)/test_chunksrv $(B)/test_chunkpeer $(B)/test_chunk_heal $(B)/test_ecnames $(B)/test_settle_oracle $(B)/test_ecswap $(B)/test_seam $(B)/test_ecpart $(B)/test_ecskip $(B)/test_tier $(B)/test_ecgc $(B)/test_keep $(B)/test_ecdigest $(B)/test_settle $(B)/test_eccold $(B)/test_ecapply $(B)/test_marker
TEST_B_BINS_2 := $(B)/test_props $(B)/test_watchdog $(B)/test_group $(B)/test_lttb $(B)/test_twa $(B)/test_spread $(B)/test_limits $(B)/test_small_parts $(B)/test_classes $(B)/test_follow $(B)/test_volumes $(B)/test_rs $(B)/test_crypto $(B)/test_users $(B)/test_export $(B)/fuzz_frames $(B)/test_out_send $(B)/tsdb $(B)/tsdbd $(B)/tsdb-bench $(CLIB) $(CRASHFS_SO)
$(UNITS_B_GROUPS) test-groups $(BT_TARGETS): $(if $(filter Linux,$(UNAME_S)),$(B)/libtsdb.so)
$(UNITS_B_GROUPS) test-groups $(BT_TARGETS): $(TEST_B_BINS_1) $(TEST_B_BINS_2)
test-units-b: $(if $(filter Linux,$(UNAME_S)),$(B)/libtsdb.so)
test-units-b: $(TEST_B_BINS_1) $(TEST_B_BINS_2)
	./$(B)/test_props $(SCRATCH)
	B=$(B) tests/test_config.sh $(TMPDIR)
	./$(B)/test_watchdog
	./$(B)/test_group $(SCRATCH)
	./$(B)/test_lttb $(SCRATCH)
	B=$(B) tests/test_cli_lttb.sh
	B=$(B) tests/test_cli_latest.sh
	./$(B)/test_twa $(SCRATCH)
	./$(B)/test_spread $(SCRATCH)
	B=$(B) tests/test_cli_spread.sh
	B=$(B) tests/test_cli_input.sh
	B=$(B) tests/test_bench_export.sh
	B=$(B) tests/test_bench_qps.sh
	B=$(B) tests/test_fs_warning.sh
	./$(B)/test_limits $(SCRATCH)
	./$(B)/test_small_parts $(SCRATCH)
	./$(B)/test_classes $(SCRATCH)
	./$(B)/test_follow $(SCRATCH)
	./$(B)/test_volumes $(SCRATCH)
	./$(B)/test_rs $(SCRATCH)
	$(if $(EC_OBJ),$(MUT) --name test_rs --list "coef kernel guard" -- ./$(B)/test_rs --quick --mutant @M@) # each planted bug must fail it (--quick: seconds, not 20 s a run)
	./$(B)/test_chunk $(SCRATCH)
	$(if $(EC_OBJ),$(MUT) --name test_chunk --list "nosums nofence eof settings nosync epochnosync" -- ./$(B)/test_chunk --quick --mutant @M@ @D@) # each planted bug must fail it
	$(if $(EC_OBJ),$(MUT) --name test_chunk --fail --grep 'stream put .* differ from chunk_encode' --list "streamreread" -- ./$(B)/test_chunk --quick --mutant @M@ @D@) # pinned to its oracle's line
	./$(B)/test_chunksrv $(SCRATCH)
	$(if $(EC_OBJ),for c in header full; do ./$(B)/test_chunksrv $(SCRATCH) --net --check $$c || exit 1; done) # its peer links at the other check levels
	$(if $(EC_OBJ),$(MUT) --name test_chunksrv --list "chunknofence chunkselfraise putokearly nocellcheck storeforeign nometacheck sumsonly noreqcheck noopmatch norespcheck" -- ./$(B)/test_chunksrv --quick --mutant @M@ @D@) # each planted bug must fail it
	./$(B)/test_chunkpeer $(SCRATCH)
	$(if $(EC_OBJ),$(MUT) --name test_chunkpeer --list "ownepoch nosumcmp staleraise delbeforegc sticky unsuredown" -- ./$(B)/test_chunkpeer --mutant @M@ @D@) # each planted bug must fail it
	./$(B)/test_chunk_heal $(SCRATCH)
	./$(B)/test_ecnames $(SCRATCH)
	$(MUT) --name test_ecnames --list "nameloose tierloose" -- ./$(B)/test_ecnames --mutant @M@ @D@ # each planted bug must fail it

# What `make test` runs after its own lines, as one target that builds everything first in parallel (make -j4 test-groups) and
# then runs the groups one after the other: the second plain job of CI, beside `make test TEST_GROUPS=`.
test-groups:
	$(MAKE) -j1 $(TEST_GROUPS)
BT_GROUPS_1 := test-units-h3 test-units-b test-units-c test-units-d
BT_GROUPS_2 := test-units-e test-units-f test-units-g test-units-h1 test-units-h2 test-units-b6 test-units-b7
BT_GROUPS_3 := test-cluster-3 test-cluster-2 test-cluster-1 test-links test-net test-units-i
BT_GROUPS_4 := test-units-b2 test-units-b3 test-units-b4 test-units-b5 test-meta-1 test-meta-2 test-meta-mutants-1 test-meta-mutants-2
$(BT_TARGETS): test-groups-%:
	$(MAKE) -j1 $(BT_GROUPS_$*)

# The recipe lines of `test` and of test-units-b that follow the first lines of each, as groups of their own (the lines are those
# that were there, in the same order): under ThreadSanitizer test_intents alone takes 50 minutes and each planted-bug line of
# test_drops about 17 (it runs the whole test again), so CI runs each group on a runner of its own. `make test` runs them
# after its own lines, in this order (TEST_GROUPS).
test-units-c:
	./$(B)/test_intents $(SCRATCH)
test-units-d:
	$(MUT) --name test_intents --stdout-only --grep 'ROWS WRONG' --list "unlinkfirst" -- ./$(B)/test_intents --mutant @M@ @D@ # the old order must lose rows
	./$(B)/test_replfile $(SCRATCH)
	$(MUT) --name test_replfile --list "nosync sameslot nochecksum" -- ./$(B)/test_replfile --mutant @M@ @D@ # each planted bug must fail it
	./$(B)/test_crashfs $(SCRATCH)
	./$(B)/test_snapshot $(SCRATCH)
	$(MUT) --name test_snapshot --list "snapafterp snapdictshort snapnokeep snapnostage snapnoretired snapnodirsync snapnoneedseq snapnosegretry snaplowerfence snapraiseled snapkeepunsure snapswapfirst snapinstnodirsync snapnoreserved snapdroppart" -- ./$(B)/test_snapshot --mutant @M@ @D@ # each planted bug must fail it
	$(if $(EC_OBJ),$(MUT) --name test_snapshot_ec --list "snapinstnoppos snappposafter snapcarrysettled snapskipec snapecnoverify snapecnozeq snapecnozp snapecnoppos" -- ./$(B)/test_snapshot --only ec --mutant @M@ @D@) # snap-ec: each planted bug of an ec volume's snapshot must fail it
test-units-e:
	./$(B)/test_drops $(SCRATCH)
	$(MUT) --name test_drops --stdout-only --grep 'HALF DROP' --list "nointent nosync unlinkfirst" -- ./$(B)/test_drops --mutant @M@ @D@ # each planted bug must leave a half drop
	$(MUT) --name test_drops --stdout-only --grep 'TTL ORDER WRONG' --list "ttlreverse" -- ./$(B)/test_drops --mutant @M@ @D@
	$(MUT) --name test_drops --stdout-only --grep 'EC LEFT' --list "skipec" -- ./$(B)/test_drops --mutant @M@ @D@
test-units-f:
	$(MUT) --name test_drops --stdout-only --grep 'HALF DROP' --list "dropfirst nosettled two ttlunlinkfirst" -- ./$(B)/test_drops --mutant @M@ @D@
	$(MUT) --name test_drops --stdout-only --grep 'NAMES ACCEPTED' --list "novalidate" -- ./$(B)/test_drops --mutant @M@ @D@
	$(MUT) --name test_drops --stdout-only --grep 'EIO AS GONE' --list "eioasgone" -- ./$(B)/test_drops --mutant @M@ @D@
	$(MUT) --name test_drops --stdout-only --grep 'TTL FAIL-STOP' --list "ttlfailstop" -- ./$(B)/test_drops --mutant @M@ @D@
test-units-g:
	./$(B)/test_replica $(SCRATCH)
	$(if $(findstring -fsanitize=thread,$(CFLAGS)),TEST_REPLICA_RO_ONLY=1 $(MUT) --name test_replica --rc 66 --grep manifest_publish --list "walsequnlocked" -- ./$(B)/test_replica --mutant @M@ @D@) # under TSan the planted race (a rotation's wal_seq written unlocked) must fail it
	$(if $(findstring -fsanitize=thread,$(CFLAGS)),TEST_REPLICA_CUT_PUBLISH_ONLY=1 $(MUT) --name test_replica --rc 66 --grep manifest_publish --grep replica_cut --list "cutwalsequnlocked" -- ./$(B)/test_replica --mutant @M@ @D@) # and a cut's
	./$(B)/test_cvol $(SCRATCH)
	./$(B)/test_cvolfs $(SCRATCH)
	$(MUT) --name test_cvolfs --list "nosync nodirsync nononce trustunsure tornstale samerid removenodirsync" -- ./$(B)/test_cvolfs --mutant @M@ @D@ # each planted bug must fail it
test-units-h1:
	$(MUT) --name test_replica --list "applyall ackunsynced keepcut nofloor walclock nopos classmax noclasspurge opensync gapcheck noleftover gapbyseq gapnokept torncut floorcheck durablegate applycontig rotsync cutsync cutdirsync replayskip nonow commitclock earlyseal sealedrule cutgen nostagehold cutnohold nocommitflush nodoneskip keepstaged nodropskip nodropflush nostagesync promoteunapplied promotekeepsegs" -- ./$(B)/test_replica --mutant @M@ @D@
test-units-h2:
	$(MUT) --name test_replica --list "epochseg nodropserial stagetrunc coldnostage coldkeyany coldnameany ppostale noorphans leadervisible leaderflush deposefails admitafter bytesaftersync markerrc gatenohook nosynced dictearly wtaillocal classmarkerlate nowtailfrom markersplit droppedglobal wtailnocap wtailfoldall wtailfoldmin wtailfoldnewer wtailfolddropold wtailsortnomax wtailremoved nocutoff zerorate noraisefrom pruneoff1 freshunchecked leaderclose leadernorstate" -- ./$(B)/test_replica --mutant @M@ @D@
test-units-h3:
	$(MUT) --name test_replica --list "closewait rscommittedtrue rscommittedfalse keepignored nohardlink promotenocarry keptreplayed leaderkeptnosave readgap cursoranyway tailoksealed notfoundsealed nosweep b0off applyholdsmu nocutgen dropunheld linkbyname leadernoprune leaderpruneall notelate rowsholdmu promotenoapplymu switchunattached freshunattached replicadropsfloor replicadropsfiles leaderlinknosync replaykeptearly replaystartlog replaystarthigh replaystartunsaved replayleadnofloor replayopennone replayopennofloor" -- ./$(B)/test_replica --mutant @M@ @D@
test-units-i:
	$(if $(filter Darwin,$(UNAME_S)),,./$(B)/test_crashfs_old $(CURDIR)/$(B)/libcrashfs.so $(SCRATCH))
	./$(B)/test_feedstore $(SCRATCH)
	$(MUT) --name test_feedstore --rc 1 --grep 'renamed before its data was synced' --list "noseal" -- ./$(B)/test_feedstore --mutant @M@ @D@ # a replayed segment renamed without its fdatasync must fail it
	$(MUT) --name test_feedstore --rc 1 --grep 'removed before the floor was durable' --list "latefloor" -- ./$(B)/test_feedstore --mutant @M@ @D@ # the cleaner's floor set after its unlinks must fail it
	./$(B)/test_crafted $(SCRATCH)
	./$(B)/test_gindex $(SCRATCH)
	./$(B)/test_flush_tail
	./$(B)/test_stats $(SCRATCH)
	./$(B)/test_tenants $(SCRATCH)
	./$(B)/test_tn_race $(SCRATCH)
	./$(B)/test_tzfiles $(SCRATCH)
	for h in $(FUZZ_HARNESSES); do FUZZ_IMPORT_EXPECT_END=1 ./$(B)/fuzz-$$h -replay tests/fuzz/corpus/$$h || exit 1; done
	tests/test_fuzz_replay.sh $(B) $(SCRATCH) # a replay of nothing, or of a missing corpus, fails
	./$(B)/test_types $(SCRATCH)
	./$(B)/test_delete $(SCRATCH)
	./$(B)/test_levels
test-units-b2:
	./$(B)/test_settle_oracle $(SCRATCH)
	$(if $(EC_OBJ),tests/oracle_mutants.sh ./$(B)/test_settle_oracle $(SCRATCH) $(ORACLE_MUTANTS)) # each planted bug must fail it by its own line (EC builds: without TSDB_EC the oracle skips)
	./$(B)/test_ppos $(SCRATCH)
	$(if $(EC_OBJ),$(MUT) --name test_ppos --list "pposafter pposnosync pposflushhi pposreplay pposunknown0 pposfallback0 pposnofail pposnobadgood pposflags" -- ./$(B)/test_ppos --mutant @M@ @D@) # each planted bug must fail it
test-units-b3:
	./$(B)/test_ecswap $(SCRATCH)
	$(if $(EC_OBJ),$(MUT) --name test_ecswap --list "swapearly swappartial swapnodirsync swapnodropcheck swapnolisted swapnofailcheck swapnostats swapmanifestrc swapbackongone swaplostsettled swaploggedgc" -- ./$(B)/test_ecswap --quick --mutant @M@ @D@) # each planted bug must fail it
	./$(B)/test_ecgc $(SCRATCH)
	$(if $(EC_OBJ),$(MUT) --name test_ecgc --list "gcunlinkfirst gcunrefleak gcnohold sweeplive sweepinflight sweepforeign unlistednoop noretrytimer" -- ./$(B)/test_ecgc --quick --mutant @M@ @D@) # each planted bug must fail it
test-units-b4:
	./$(B)/test_settle $(SCRATCH)
	$(if $(EC_OBJ),$(MUT) --name test_settle --list "ecrankdown settlenocheck settletwopass settlebusy coarsewhole" -- ./$(B)/test_settle --quick --mutant @M@ @D@) # each planted bug must fail it
	$(if $(EC_OBJ),$(MUT) --name test_settle --list "gcskipput gcskipindex gcskipopen gcskipelse gcskipswap gcskipdiscard gccommitted gcdouble" -- ./$(B)/test_settle --quick --case gcpaths --mutant @M@ @D@) # each failure path's handoff (int-h)
	$(if $(EC_OBJ),$(MUT) --name test_settle --list "gcearly nullnosync syncfailgo" -- ./$(B)/test_settle --quick --case gcpower --mutant @M@ @D@) # the handoff after the directory sync, never on a failed engine
	$(if $(EC_OBJ),$(MUT) --name test_settle --list "nohaltstop" -- ./$(B)/test_settle --quick --case halt --mutant @M@ @D@) # a halt stops the settle
test-units-b5:
	./$(B)/test_eccold $(SCRATCH)
	$(if $(EC_OBJ),$(MUT) --name test_eccold --list "cutnogate cutnowait ttlsettling cutnorotate fencekeyonly stripewaitfenced pposswapnone pposswapafter jrbarge" -- ./$(B)/test_eccold --mutant @M@ @D@) # each planted bug of the leader's gate and cut must fail it
	./$(B)/test_ecapply $(SCRATCH)
	$(if $(EC_OBJ),$(MUT) --name test_ecapply --list "ycold ecabovey pposignored v5nodirsync coldnoflushthrough leaderswapnors pposafterswap mergekept notfoundzero noorder checkz seenskip noyhcheck decidesome recrollmissing recnodiverge recnodirsync" -- ./$(B)/test_ecapply --mutant @M@ @D@) # each planted bug of a replica's COLD apply must fail it
	$(if $(EC_OBJ),$(MUT) --name test_ecapply_digest --list "digestnocompare digestanyz digestnosupersede digestnocrcwalk digestrotmatches digestnojobremove" -- ./$(B)/test_ecapply --case digest --mutant @M@ @D@) # r8j: each planted bug of a replica's digest job must fail it
	$(if $(EC_OBJ),$(MUT) --name test_ecapply_digestpower --list "digestjobnosync" -- ./$(B)/test_ecapply --case digestpower --mutant @M@ @D@) # r8j: the job file written without its fdatasync must fail it
test-units-b6:
	./$(B)/test_seam $(SCRATCH)
	$(if $(EC_OBJ),$(MUT) --name test_seam --list "seamnoverify seamnoretry cachekeyoff seamnorecheck seamretryerr seamkeepstale forgetnoop prefetchnocheck prefetchevictpinned prefetchnodouble prefetchdupkey prefetchforgetlive prefetchnofallback" -- ./$(B)/test_seam --mutant @M@ @D@) # each planted bug must fail it
	./$(B)/test_marker $(SCRATCH)
	$(MUT) --name test_marker --list "mkcoldbounds mkadmitall ecpposloose mkwriteany mkretired" -- ./$(B)/test_marker --mutant @M@ @D@ # each planted bug must fail it
	./$(B)/test_ecpart $(SCRATCH)
	$(if $(EC_OBJ),$(MUT) --name test_ecpart --list "ecnohash ecnowalseq ecskipname ecnofno ecnolayout" -- ./$(B)/test_ecpart --mutant @M@ @D@) # each planted bug must fail it
	./$(B)/test_keep $(SCRATCH)
	$(if $(EC_OBJ),$(MUT) --name test_keep --list "digestskip digestraw digestnohold keepmtime noobjcmp openwait nolencheck orphannow nohaltstop divergemem clearcluster releasenosync rotdiverges" -- ./$(B)/test_keep --mutant @M@ @D@) # each planted bug must fail it
	./$(B)/test_ecdigest $(SCRATCH)
	$(if $(EC_OBJ),$(MUT) --name test_ecdigest --list "digestbytes digestnotomb digestnoretention digestbelowtrue digestnots digestfreezerows digestlocalbelow" -- ./$(B)/test_ecdigest --mutant @M@ @D@) # each planted bug must fail it
	./$(B)/test_ecskip $(SCRATCH)
	$(if $(EC_OBJ),$(MUT) --name test_ecskip --list "mergeec soloec dropwaitsettle waitcountec waitnobreak waitwall waitsettle mergecountec" -- ./$(B)/test_ecskip --mutant @M@ @D@) # each planted bug must fail it
	./$(B)/test_tier $(SCRATCH)
	$(if $(EC_OBJ),$(MUT) --name test_tier --list "tierperengine tierunsure tierstrict tierfresh tierfence selfepoch" -- ./$(B)/test_tier --mutant @M@ @D@,$(MUT) --name test_tier --list "tiernoec" -- ./$(B)/test_tier --mutant @M@ @D@) # each planted bug must fail it (tiernoec in a NO_EC=1 build)
	$(if $(filter x86_64,$(shell uname -m)),test "$$($(MAKE) -s --no-print-directory NO_EC=0 print-ec)" = ec && test "$$($(MAKE) -s --no-print-directory NO_EC=1 print-ec)" = noec) # only NO_EC=1 turns the codec off
	$(if $(EC_OBJ),$(MUT) --name test_chunk_heal --list "heal_noverify heal_epoch0 replace_late open_busy_down open_id_up heal_kminus1 heal_resurrect heal_count_target settle_up_always" -- ./$(B)/test_chunk_heal --quick --mutant @M@ @D@) # each planted bug must fail it
test-units-b7:
	tests/test_sync_ec.sh
	tools/sync-ec.sh --check
	tests/test_check_ec_symbols.sh
	$(if $(EC_OBJ),EC_CFLAGS='$(EC_CFLAGS)' CC='$(CC)' tools/check-ec-baseline.sh)
	B=$(B) tools/check-ec-symbols.sh
	$(if $(EC_OBJ),! B=$(B) tools/check-ec-symbols.sh --lib $(LIB) >/dev/null 2>&1) # the engine's library holds the codec: the check must fail on it
	$(if $(filter Linux,$(UNAME_S)),B=$(B) tools/check-exports.sh && tests/test_check_exports.sh)
	./$(B)/test_crypto
	./$(B)/test_users $(SCRATCH)
	B=$(B) tests/test_cli_users.sh
	B=$(B) tests/test_cli_diverged.sh
	B=$(B) tests/test_cli_feed.sh
	B=$(B) tests/test_cli_chunkstore.sh
	! nm $(CLIB) | grep -wE '_?tsdbc_test_[a-z0-9_]*' # the client's test hooks are in the tests' build only (_: Mach-O)
	./$(B)/test_export $(SCRATCH)
	tests/test_jepsen_rerun.sh $(SCRATCH) # the driver of the jepsen gates: INCONCLUSIVE is rerun, a violation never
	./$(B)/fuzz_frames replay tests/fuzz/frames
	./$(B)/test_out_send


# test-meta is the metadata simulator, test_cluster and the link tests, in groups that CI runs on runners of their own
# (test_meta under ThreadSanitizer takes about 15 minutes, test_cluster about as long again: TEST_GROUPS below).
# test_meta's seeds 1 to 12 in two halves; the tests that precede its simulator run in both.
test-meta-1 test-meta-2: test-meta-%: $(B)/test_meta
	./$(B)/test_meta $(SCRATCH) --seeds 6 --first $(if $(filter 1,$*),1,7) --seconds 15

# test_cluster's scenarios, in the order of the table all[] in tests/test_cluster.c, one group each (a scenario named in
# none, or in two, fails tests/check_ci_groups.sh; test_cluster with no names runs them all, in one process, in the
# table's order: `make test-meta` does not, it runs these)
CLUSTER_SCEN_1 := secret relay initcrash dirsync install reseed torn rebase slowinstall ids watch stall witness users \
    login userimport userstale userpromote userreplay userlease flags hbsections retired feed syncrace power \
    powersnap life copyback voterstale dirsyncshared installshared powervote pendshared copyfault volumes \
    volrefuse snapversion links fwdlinks replicate replindet replfail replflip replepoch repldesc replmeta replbulk failover failoverkeep fencecrash \
    promotecrash deposecrash pause promoteslow openread openreadlead
CLUSTER_SCEN_2 := jointpause leasestale resign refused cut cuthold deposetail readdepose oneway oneway2 cutstuck manystuck \
    noowner isr topology copyhold movedself route forward fwdseries fwdseriesstrict fwdlease fwddepose \
    fwdload viabench ccstress minisr closegone writersync shipstall keepcap snapxfer snaplarge snapnospace \
    snapbusy snapsrckill snaplost snapstall rejoin rejoinsteady rejoinslow tickwall rejoinlate keepfloor \
    reopenhold backpressure stallstop inboxshared idleseal chunksrv ecadd margins copycaught
CLUSTER_SCEN_3 := ecbasic r4stubs addreplica setnext addswitch removereplica removeindex removedown setstale setchurn resetlost \
    volcmd transfer transferslow transferlag transferhold transfergrant transferadmin transferstale \
    transfermarkers transferkill transferraftkill transferraftdrain balance balancetwo balancecut balancehb \
    snapcatch snapkill snappower snapfence snapdiverged addsnap classfailover removesnap move movelag \
    moveleader moverep0 moveraft movekill movecancel moveold ackdurable writerfail writerfailq diverged \
    writerdepose lease hungloop leasedown leasequorum hardslot repcheck
test-cluster-1 test-cluster-2 test-cluster-3: test-cluster-%: $(B)/test_cluster $(B)/tsdbd $(B)/tsdb $(B)/tsdbc-bench $(CRASHFS_SO)
	CRASHFS_SO=$(B)/libcrashfs.so TSDBD=$(TSDBD) TSDB_CLI=$(B)/tsdb TSDBC_BENCH=$(B)/tsdbc-bench ./$(B)/test_cluster $(SCRATCH) $(CLUSTER_SCEN_$*)

test-links: $(B)/test_peerlink $(B)/test_fwd $(B)/test_margins $(B)/test_plrdma $(B)/tsdbd $(B)/tsdb
	./$(B)/test_peerlink $(SCRATCH)
	for c in header full; do ./$(B)/test_peerlink $(SCRATCH) --quick --check $$c --only $(PL_KINDS_TESTS) || exit 1; done # every link kind at the other check levels
	$(MUT) --name test_peerlink --list "notag noseq nosalt checktype samesalt notranscript dialanytype notype halfcmp seq8 sharedsocket nobackpressure accauth uuid endhash nothrottle zstdsize chunkoff resumebound nokick queuedasunsent kindshared lateclose nocredit inboxwait inboxdrop chunkfirst wbwritten pausedead kindbucket noready closefirst wnoshort nhandall donestatus statusneg updownleak noidcheck" -- ./$(B)/test_peerlink @D@ --quick --mutant @M@ # each planted bug must fail it
	./$(B)/test_margins
	$(MUT) --name test_margins --list "marginoff youngrep lostasdown sharedcopies corruptfirst jepsenlost" -- ./$(B)/test_margins --mutant @M@ # each planted bug must fail it
	./$(B)/test_plrdma $(SCRATCH)
	$(MUT) --name test_plrdma --list "writtencqe noctl ctlstorm pausectl nodrain unsigearly nocredit splitoff room crown norecvmark norearm anchorbyte" -- ./$(B)/test_plrdma @D@ --quick --mutant @M@ # each planted bug must fail it
	./$(B)/test_fwd
	$(MUT) --name test_fwd --list "noage retry unavonbreak teardown" -- ./$(B)/test_fwd --mutant @M@ # each planted bug must fail it
	$(MAKE) cluster-mutants

# all of them, one after the other (what `make test` runs as its test-meta groups, and what a hand run wants)
test-meta:
	$(MAKE) -j1 $(META_GROUPS)

# the planted bugs, in two halves (tests/meta_mutants.sh --part K/N): 23 runs of 2 minutes each under a sanitizer
test-meta-mutants-1 test-meta-mutants-2: test-meta-mutants-%: $(B)/test_meta_mut
	tests/meta_mutants.sh --part $*/2 $(B)/test_meta_mut

test-net: $(B)/test_readonly $(B)/test_ports $(B)/test_client $(B)/test_wire $(B)/test_server $(B)/test_server_multi $(B)/test_auth $(B)/test_auth_ops $(B)/test_stats_net $(B)/test_stability $(B)/tsdbd $(B)/tsdb $(CRASHFS_SO)
	TSDBD=$(TSDBD) ./$(B)/test_ports
	./$(B)/test_client
	./$(B)/test_wire tests/data/wire13-vectors.txt
	for io in $(IO); do echo "== tsdbd --io $$io"; \
	    TSDBD=$(TSDBD) TSDBD_IO=$$io ./$(B)/test_server && \
	    TSDBD=$(TSDBD) TSDBD_IO=$$io ./$(B)/test_server_multi && \
	    TSDBD=$(TSDBD) TSDBD_IO=$$io ./$(B)/test_auth && \
	    TSDBD=$(TSDBD) TSDBD_IO=$$io ./$(B)/test_auth_ops && \
	    TSDBD=$(TSDBD) TSDBD_IO=$$io ./$(B)/test_readonly $(SCRATCH) && \
	    TSDBD=$(TSDBD) TSDBD_IO=$$io ./$(B)/test_stats_net && \
	    TSDBD=$(TSDBD) TSDBD_IO=$$io ./$(B)/test_stability || exit 1; done

# the portability headers (macOS shims) are part of these tests and of the server
$(B)/test_classes $(B)/test_damage $(B)/test_export $(B)/test_failstop $(B)/test_follow $(B)/test_group $(B)/test_sched \
$(B)/test_server_multi $(B)/test_server $(B)/test_spread $(B)/test_stability $(B)/test_tsdb $(B)/test_volumes \
$(B)/test_users $(B)/test_auth $(B)/test_auth_ops $(B)/test_readonly $(B)/test_stats_net $(B)/test_ports $(B)/test_out_send $(B)/fuzz_frames $(B)/tsdbd $(B)/users.o $(B)/tsdb: \
    tests/portable.h src/compat.h server/epoll_compat.h

# ------------------------------------------------------------------ fuzzing
# tests/fuzz/: one harness per decoder or loader of what lies on disk (driver.c has the details).
# `make fuzz FUZZ_SECONDS=N [FUZZ="codec part"]` runs each for N seconds, built in build-fuzz/ with
# ASan + UBSan and GCC's trace-pc coverage (or libFuzzer when FUZZ_CC=clang); new inputs and
# crash-*/timeout-* files go to build-fuzz/out/<harness>/. `make test` replays the committed corpus
# (tests/fuzz/corpus/<harness>/) through the normal build; `make asan` does so under the sanitizers.
# Every input a run found a bug with is copied into the corpus with its fix.
FUZZ           ?= $(FUZZ_HARNESSES)
FUZZ_SECONDS   ?= 60
FUZZ_CC        ?= $(CC)
# The CPU level of `make fuzz`: generic x86-64 by default (scalar unpack). FUZZ_ARCH=-march=x86-64-v3 reaches the
# AVX2 kernels, -march=x86-64-v4 the AVX-512 code generation (codecx, part and the others: all of it matters, the
# decoders compile differently); the harnesses must work at every level, ASan's fake stack included.
FUZZ_ARCH      ?=
# Its own directory per level (build-fuzz, build-fuzz-v4, build-fuzz-native), and a level change in a directory you
# name yourself empties its objects and binaries (the libtsdb.a rule below records the flags in .flags).
FUZZ_B         ?= build-fuzz$(subst -march=,-,$(subst -march=x86-64-,-,$(FUZZ_ARCH)))
FUZZ_SAN       := -O1 -g -fsanitize=address,undefined -fno-sanitize-recover=undefined -fno-omit-frame-pointer
FUZZ_COMMON    := -std=gnu11 -Wall -Wextra -Wno-unused-parameter -fno-math-errno -ffp-contract=off -Iinclude -Isrc -Iclient -Iserver -Isrc/ec/compat
ifeq ($(findstring clang,$(FUZZ_CC)),clang)
FUZZ_COV  := -fsanitize=fuzzer-no-link
FUZZ_LINK := -fsanitize=fuzzer
FUZZ_DRV  :=
else
FUZZ_COV  := -fsanitize-coverage=trace-pc
FUZZ_LINK :=
FUZZ_DRV  := tests/fuzz/driver.c
endif

$(B)/fuzz-%: tests/fuzz/fuzz_%.c tests/fuzz/driver.c tests/fuzz/fuzz.h tests/fuzz/template.h src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -Iclient -Iserver -o $@ $< tests/fuzz/driver.c $(FUZZ_OBJS_$*:%=$(B)/%) $(LIB) $(LDLIBS) \
	    $(PLAT_FUZZ_LDFLAGS)
$(B)/fuzz-codecx: tests/fuzz/fuzz_codec.c
$(B)/fuzz-users: $(B)/users.o $(B)/tsdb_crypto.o
$(B)/fuzz-replframe: $(B)/peerlink.o $(B)/tsdb_crypto.o $(B)/replwire.o $(B)/fwd.o $(B)/chunksrv.o server/peerlink.h $(PLCHECK_HDR) server/replwire.h server/fwd.h server/chunksrv.h
$(B)/fuzz-props: $(CFG_OBJ) server/tsdbd_keys.h tools/cfg.h tools/props.h
$(B)/fuzz-import: tools/dump.c tools/dump.h tools/common.h

fuzz-replay: $(FUZZ_HARNESSES:%=$(B)/fuzz-%)
	for h in $(FUZZ_HARNESSES); do FUZZ_IMPORT_EXPECT_END=1 FUZZ_TMP=$(SCRATCH) ./$(B)/fuzz-$$h -replay tests/fuzz/corpus/$$h || exit 1; done

# abspath: FUZZ_B=./build from the environment is build/ too, and must not redirect the normal build's rules
ifneq ($(abspath $(B)),$(abspath $(FUZZ_B)))
FUZZ_SIG = $(FUZZ_CC) | $(FUZZ_ARCH) | $(FUZZ_SAN) | $(FUZZ_COV)
$(FUZZ_B)/libtsdb.a: FORCE
	@mkdir -p $(FUZZ_B); \
	if [ "$$(cat $(FUZZ_B)/.flags 2>/dev/null)" != "$(FUZZ_SIG)" ]; then \
	    [ -f $(FUZZ_B)/libtsdb.a ] && echo "fuzz: compiler or flags changed since the last build: rebuilding $(FUZZ_B)"; \
	    rm -f $(FUZZ_B)/*.o $(FUZZ_B)/*.a $(FUZZ_B)/fuzz-*; printf '%s\n' "$(FUZZ_SIG)" >$(FUZZ_B)/.flags; \
	fi
	$(MAKE) B=$(FUZZ_B) CC=$(FUZZ_CC) ARCH="$(FUZZ_ARCH)" CFLAGS="$(FUZZ_SAN) $(FUZZ_COV)" $@ \
	    $(FUZZ_B)/users.o $(FUZZ_B)/tsdb_crypto.o $(FUZZ_B)/props.o $(FUZZ_B)/cfg.o $(FUZZ_B)/peerlink.o $(FUZZ_B)/replwire.o $(FUZZ_B)/fwd.o \
	    $(FUZZ_B)/chunksrv.o

# the driver is built without coverage: it holds the hook
$(FUZZ_B)/driver.o: tests/fuzz/driver.c $(FUZZ_B)/libtsdb.a
	$(FUZZ_CC) $(FUZZ_COMMON) $(FUZZ_SAN) -c -o $@ $<

$(FUZZ_B)/fuzz-%: tests/fuzz/fuzz_%.c tests/fuzz/fuzz.h tests/fuzz/template.h src/internal.h $(FUZZ_B)/libtsdb.a \
                  $(if $(FUZZ_DRV),$(FUZZ_B)/driver.o)
	$(FUZZ_CC) $(FUZZ_COMMON) $(if $(EC_OBJ),-DTSDB_EC) $(FUZZ_ARCH) $(FUZZ_SAN) $(FUZZ_COV) -c -o $@.o $<
	$(FUZZ_CC) $(FUZZ_SAN) $(FUZZ_LINK) -o $@ $@.o $(if $(FUZZ_DRV),$(FUZZ_B)/driver.o) \
	    $(FUZZ_OBJS_$*:%=$(FUZZ_B)/%) $(FUZZ_B)/libtsdb.a $(LDLIBS)
$(FUZZ_B)/fuzz-codecx: tests/fuzz/fuzz_codec.c

fuzz: $(FUZZ:%=$(FUZZ_B)/fuzz-%)
	for h in $(FUZZ); do echo "== fuzz $$h, $(FUZZ_SECONDS) s"; mkdir -p $(FUZZ_B)/out/$$h; \
	    ASAN_OPTIONS=abort_on_error=1:detect_leaks=0 UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
	    $(FUZZ_B)/fuzz-$$h $(if $(FUZZ_DRV),-t $(FUZZ_SECONDS) -o $(FUZZ_B)/out/$$h,-max_total_time=$(FUZZ_SECONDS) \
	        -artifact_prefix=$(FUZZ_B)/out/$$h/) $(FUZZ_B)/out/$$h tests/fuzz/corpus/$$h || exit 1; done

# The guard of the _Alignas fix (an input alone does not trip it, a replay of the whole directory does): the harnesses
# that read bytes without opening a database, built at x86-64-v4 under ASan with use-after-return detection on, replay
# their whole corpus. Not part of `make test`: it needs AVX-512 (skipped without) and a build of the engine under ASan.
FUZZ_V4_CHECK  ?= part codecx
fuzz-replay-asan: $(FUZZ_V4_CHECK:%=$(FUZZ_B)/fuzz-%)
	for h in $(FUZZ_V4_CHECK); do FUZZ_TMP=$(SCRATCH) ASAN_OPTIONS=detect_stack_use_after_return=1:detect_leaks=0 \
	    UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 ./$(FUZZ_B)/fuzz-$$h -replay tests/fuzz/corpus/$$h || exit 1; done

# writes each harness's generated seeds into tests/fuzz/corpus/<harness>/ (after a format change)
fuzz-seeds: $(FUZZ_HARNESSES:%=$(B)/fuzz-%)
	for h in $(FUZZ_HARNESSES); do FUZZ_TMP=$(SCRATCH) ./$(B)/fuzz-$$h -seeds tests/fuzz/corpus/$$h || exit 1; done

endif
fuzz-v4-check:
	@if grep -q avx512f /proc/cpuinfo; then $(MAKE) FUZZ_ARCH=-march=x86-64-v4 fuzz-replay-asan; \
	else echo "fuzz-v4-check: no AVX-512 on this CPU, skipped"; fi
FORCE:
.PHONY: fuzz fuzz-replay fuzz-replay-asan fuzz-v4-check fuzz-seeds FORCE

# Without -march=native: with it, GCC 15.2's AddressSanitizer crashes in memcpy into
# fake-stack frames (use-after-return detection), in code that did not change.
# The runtime-dispatched AVX-512 kernels are still tested.
# UBSan only prints by default and the run still exits 0: halt_on_error makes the first report fail
# it. `ubsan-check` proves that with a program that overflows on purpose, then runs the CLI's parsers on
# out-of-range input with float-cast-overflow added (GCC's `undefined` leaves it out).
# ASAN_ARCH=-march=x86-64-v4 (or -v3) runs `make asan` at that CPU level (default: generic, see above). At v4 GCC
# copies 64-byte structs (the part footer) with aligned zmm moves, which fault in ASan's fake stack (frames aligned
# to 32 bytes: part.c part_index_intact), so use-after-return detection is off for those runs.
ASAN_ARCH  ?=
ASAN_FLAGS := -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer
UBSAN_RUN  := halt_on_error=1:print_stacktrace=1
ASAN_RUN   := $(if $(ASAN_ARCH),detect_stack_use_after_return=0:)
asan: ubsan-check
	$(MAKE) clean
	ASAN_OPTIONS="$(ASAN_RUN)$$ASAN_OPTIONS" UBSAN_OPTIONS="$(UBSAN_RUN) $$UBSAN_OPTIONS" \
	$(MAKE) ARCH="$(ASAN_ARCH)" CFLAGS="$(ASAN_FLAGS)" LDFLAGS="-fsanitize=address,undefined" $(TEST_TARGET)

ubsan-check:
	@mkdir -p $(B)
	$(CC) $(ASAN_FLAGS) -o $(B)/ubsan_overflow tests/ubsan_overflow.c
	@if UBSAN_OPTIONS="$(UBSAN_RUN)" ./$(B)/ubsan_overflow >/dev/null 2>&1; then \
	    echo "ubsan-check: an undefined behaviour report did not fail the run"; exit 1; fi
	@echo "ubsan-check: ok"
	$(CC) $(ASAN_FLAGS) -fsanitize=float-cast-overflow -o $(B)/ubsan_cli_parse tests/ubsan_cli_parse.c
	UBSAN_OPTIONS="$(UBSAN_RUN)" ./$(B)/ubsan_cli_parse

# ThreadSanitizer, in build-tsan/ (no `make clean`: a normal build in build/
# stays), the server under test included. -O1 keeps reports readable;
# halt_on_error makes the first race fail the run. Not with ASan: the two do
# not combine.
TSAN_FLAGS := -O1 -g -fsanitize=thread -fno-omit-frame-pointer
tsan:
	TSAN_OPTIONS="halt_on_error=1 second_deadlock_stack=1 $$TSAN_OPTIONS" \
	$(MAKE) B=build-tsan TSDBD=build-tsan/tsdbd ARCH= CFLAGS="$(TSAN_FLAGS)" $(TEST_TARGET)
# `make tsan` with the groups of TEST_GROUPS (and the rest of `make test`) side by side: one build in build-tsan/, then
# every group as a make of its own, their planted-bug runs sharing MUTANT_SLOTS (tools/tsan-par.sh; logs in $TSAN_PAR_LOGS)
tsan-par:
	TSAN_FLAGS="$(TSAN_FLAGS)" tools/tsan-par.sh "$(MAKE)" "$(TEST_GROUPS)"

$(B):
	mkdir -p $(B)

clean:
	rm -rf build build-tsan build-fuzz build-fuzz-v3 build-fuzz-v4 build-fuzz-native build-fuzz-frames $(DIST_B)

# ---- portable binaries (dist/README.dist): built here, run on AlmaLinux 10 ----
# x86-64-v2 for CPUs as old as Nehalem (no AVX needed), with the codecs also compiled for
# v3 and v4 and picked at runtime (src/levels.h); zstd and liburing static,
# glibc dynamic and no newer than GLIBC_MAX (2.34: AlmaLinux 9 as well as 10).
# CC=gcc-16 works like any CC. The object rules know only the sources, so a
# change of compiler or flags empties $(DIST_B) (its .flags records them):
# otherwise `make dist CC=gcc-16` after a GCC 15 build would package the old
# objects as GCC 16's.
DIST_B     ?= build-dist
DIST_ARCH  ?= -march=x86-64-v2 -mtune=generic
GLIBC_MAX  ?= 2.34
DIST_VARS   = B=$(DIST_B) TSDBD=$(DIST_B)/tsdbd ARCH="$(DIST_ARCH) -include tools/glibc-compat.h" \
              CPU_LEVELS="v3 v4" ZSTD_LIB=-l:libzstd.a URING_LIB=-l:liburing.a LINK_EXTRA=-static-libgcc
DIST_BINS   = tsdbd tsdb codec-bench type-bench twa-bench fold-bench tsdb-bench insert-bench tsdbc-bench scale-bench
DIST_TESTS  = test_tsdb test_types test_levels test_rs test_group test_lttb test_twa test_spread test_limits test_follow test_volumes test_server test_stability test_ports
DIST_FILES  = $(addprefix $(DIST_B)/,$(DIST_BINS) $(DIST_TESTS) libcrashfs.so)

DIST_SIG    = $$($(CC) --version | head -n 1) | $(CC) | $(CFLAGS) | $(subst ",,$(DIST_VARS)) | $(RDMA_FLAGS) | $(LV_FLAGS_v3) | $(LV_FLAGS_v4) \
              | $$(cksum <tools/glibc-compat.h)

# The release carries the verbs transport (docs/design-rdma.md §5.4): without rdma-core's headers `make dist` stops,
# unless NO_RDMA=1 was asked for (a tarball for TCP only).
dist:
	@if [ "$(NO_RDMA)" = 1 ] && [ "$(origin NO_RDMA)" = file ]; then \
	    echo "dist: rdma-core's headers (infiniband/verbs.h) not found: install libibverbs-dev or rdma-core-devel," \
	         "or give RDMA_CFLAGS=-I<dir>, or NO_RDMA=1 for a tarball without the verbs transport" >&2; exit 1; \
	fi
	@sig="$(DIST_SIG)"; \
	if [ "$$(cat $(DIST_B)/.flags 2>/dev/null)" != "$$sig" ]; then \
	    [ -d $(DIST_B) ] && echo "dist: compiler or flags changed since the last build: rebuilding $(DIST_B)"; \
	    rm -rf $(DIST_B) && mkdir -p $(DIST_B) && printf '%s\n' "$$sig" >$(DIST_B)/.flags; \
	fi
	$(MAKE) $(DIST_VARS) all $(DIST_TESTS:%=$(DIST_B)/%) $(DIST_B)/libcrashfs.so
	tools/check-glibc.sh -c $(GLIBC_MAX) $(DIST_FILES)
	tools/check-isa.sh $(DIST_B) $(DIST_FILES)
	tools/check-rdma-syms.sh $(if $(filter 1,$(NO_RDMA)),,--rdma) $(DIST_B)/tsdbd
	CC="$(CC)" BUILD_ID="$(TSDB_VERSION)" tools/mkdist.sh $(DIST_B) $(RELEASE_VERSION)

# The tarball's own test suite on this machine, every CPU level included.
dist-test: dist
	cd $(DIST_B)/tsdb-*-x86_64-linux && tests/run-tests.sh

# The same inside AlmaLinux 10 (Podman), and under an emulated Westmere and Sandy Bridge.
dist-check-almalinux10: dist
	tests/distro/dist-check.sh $(DIST_B)/tsdb-*-x86_64-linux.tar.gz

dist-check-sandybridge: dist
	tests/distro/sandybridge.sh $(DIST_B)

# Clients in other languages; not part of `make test`, so the C build needs no
# cargo, Python or JDK. Each runs its tests against build/tsdbd.
PYVENV ?= build/pyvenv

# client/rust is the network client (Apache-2.0) and links no engine; client/rust-embedded (AGPL-3.0-only) is the
# embedded Db over client/rust-sys.
test-rust: $(B)/tsdbd $(B)/tsdb $(OUTAGE_SERVER)
	cd client/rust && cargo test
	cd client/rust && cargo build --all-targets
	cd client/rust-embedded && cargo test
	cd client/rust-sys && cargo test

# numpy is an optional extra of the package, but the tests use it (numpy results, batch calls); a venv made before
# it was needed gets it too. The test build has the test hooks (feature test-hooks: a fixed client nonce, the
# PBKDF2 count), which wheels never have.
# tsdb_client (client/python, Apache-2.0) is tested alone, then tsdb_embedded (client/python-embedded, AGPL-3.0-only)
# next to it: its tests also run tsdb_client's against a Db, and check that one `except tsdb_client.TsdbError` catches both.
test-python: $(B)/tsdbd $(B)/tsdb $(OUTAGE_SERVER)
	test -x $(PYVENV)/bin/maturin || (python3 -m venv $(PYVENV) && $(PYVENV)/bin/pip install -q maturin pytest)
	$(PYVENV)/bin/python -c 'import numpy' 2>/dev/null || $(PYVENV)/bin/pip install -q numpy
	cd client/python && VIRTUAL_ENV=$(abspath $(PYVENV)) $(abspath $(PYVENV))/bin/maturin develop --release \
	    --features test-hooks && $(abspath $(PYVENV))/bin/python -m pytest tests
	cd client/python-embedded && VIRTUAL_ENV=$(abspath $(PYVENV)) $(abspath $(PYVENV))/bin/maturin develop --release \
	    && $(abspath $(PYVENV))/bin/python -m pytest tests

# The Python sdists: intellistream-tsdb-client (module tsdb_client, Apache-2.0, no engine) and
# intellistream-tsdb-embedded (module tsdb_embedded, AGPL-3.0-only). The second carries intellistream-tsdb-sys as a
# path dependency, so the engine's C sources travel in it as the copy client/rust-sys/vendor.sh makes (csrc/).
# test-python-sdist builds a wheel from each sdist and imports them together; the file names follow the distribution names.
python-sdist:
	test -x $(PYVENV)/bin/maturin || (python3 -m venv $(PYVENV) && $(PYVENV)/bin/pip install -q maturin pytest)
	client/rust-sys/vendor.sh
	rm -rf $(B)/sdist
	cd client/python && VIRTUAL_ENV=$(abspath $(PYVENV)) $(abspath $(PYVENV))/bin/maturin sdist -o $(abspath $(B))/sdist
	cd client/python-embedded && VIRTUAL_ENV=$(abspath $(PYVENV)) $(abspath $(PYVENV))/bin/maturin sdist -o $(abspath $(B))/sdist

test-python-sdist: python-sdist
	rm -rf $(B)/sdist-check && mkdir -p $(B)/sdist-check/whl
	PATH=$(abspath $(PYVENV))/bin:$$PATH $(PYVENV)/bin/pip wheel -q --no-build-isolation --no-deps -w $(B)/sdist-check/whl $(B)/sdist/intellistream_tsdb_client-*.tar.gz
	PATH=$(abspath $(PYVENV))/bin:$$PATH $(PYVENV)/bin/pip wheel -q --no-build-isolation --no-deps -w $(B)/sdist-check/whl $(B)/sdist/intellistream_tsdb_embedded-*.tar.gz
	cd $(B)/sdist-check && unzip -q whl/intellistream_tsdb_client-*.whl -d site && unzip -q whl/intellistream_tsdb_embedded-*.whl -d site
	$(abspath $(PYVENV))/bin/python -I tools/check-wheels-import.py $(B)/sdist-check/site
	tools/check-package-licenses.sh sdist $(B)/sdist/intellistream_tsdb_client-*.tar.gz apache rust=apache python=apache python-common=apache
	tools/check-package-licenses.sh wheel $(B)/sdist-check/whl/intellistream_tsdb_client-*.whl apache
	tools/check-package-licenses.sh sdist $(B)/sdist/intellistream_tsdb_embedded-*.tar.gz agpl rust=apache rust-sys=agpl rust-embedded=agpl python-embedded=agpl python-common=apache
	tools/check-package-licenses.sh wheel $(B)/sdist-check/whl/intellistream_tsdb_embedded-*.whl agpl

test-java: $(B)/tsdbd $(B)/tsdb $(OUTAGE_SERVER)
	cd client/java && mvn -q test

# The embedded Java binding (Java 22+, FFM) needs the TCP client's jar for the shared types. It is installed into a
# local repository of this tree's build (read through to ~/.m2), never into the ~/.m2 that other trees share.
JAVA_M2 := -Dmaven.repo.local=$(abspath $(B))/m2 -Dmaven.repo.local.tail=$(HOME)/.m2/repository
test-java-embedded: $(B)/tsdbd $(B)/libtsdb.$(SOEXT)
	cd client/java && mvn -q $(JAVA_M2) install -DskipTests
	cd client/java-embedded && mvn -q $(JAVA_M2) test

# Every package carries the LICENSE of its own licence (Apache-2.0 for the client libraries, AGPL-3.0-only for the
# engine and the embedded bindings, LICENSING.md) and NOTICE, and those that pack other software its notices
# (docs/licenses.md): the Rust crates (cargo package --list), the four jars and the Pulsar NAR, built here without
# their tests, and tools/check-spdx.sh (the SPDX line of every source file and the licence fields of the manifests).
# The Python sdists and wheels are checked by test-python-sdist. The Pulsar NAR needs build/m2 of
# test-java-embedded's way (the tsdb-client jar) and the Maven plugins of connectors/pulsar.
test-licenses:
	tools/check-spdx.sh
	tools/check-package-licenses.sh sources
	tools/check-package-licenses.sh crate client/rust apache
	tools/check-package-licenses.sh crate client/rust-sys agpl
	tools/check-package-licenses.sh crate client/rust-embedded agpl
	cd client/java && mvn -q $(JAVA_M2) install -DskipTests
	tools/check-package-licenses.sh jar client/java/target/tsdb-client-$(RELEASE_VERSION).jar apache
	cd client/java-embedded && mvn -q $(JAVA_M2) -DskipTests package
	tools/check-package-licenses.sh jar client/java-embedded/target/tsdb-embedded-$(RELEASE_VERSION).jar agpl
	cd connectors/pulsar && mvn -q $(JAVA_M2) -DskipTests package
	tools/check-package-licenses.sh nar connectors/pulsar/target/tsdb-pulsar-sink-$(RELEASE_VERSION).nar apache

# The three Java targets (test-java, test-java-embedded, test-licenses) all build client/java (and its target/), so
# `make -j4 test-clients` runs them one after the other: two Maven runs in one target/ overwrote each other's classes
# ("Unable to create test class", 1 run in 5 on 4 CPUs). The Rust and Python targets run beside them; what the
# sub-makes need is built here first, once, by this make.
.PHONY: test-clients-java
test-clients-java: $(B)/tsdbd $(B)/tsdb $(OUTAGE_SERVER) $(B)/libtsdb.$(SOEXT)
	$(MAKE) test-java
	$(MAKE) test-java-embedded
	$(MAKE) test-licenses
	$(MAKE) test-pulsar

test-clients: test-rust test-python test-python-sdist test-clients-java

# The same suites against tsdbd on epoll (production on AlmaLinux 10 runs it; make test-clients takes io_uring where
# there is one). TSDBD_TEST_IO is read by tsdbd when a start names no --io, and by nothing else.
test-clients-epoll:
	TSDBD_TEST_IO=epoll $(MAKE) test-rust test-python test-java

# The Pulsar sink's own tests (they start tsdbd), and the Pulsar-in-Podman run is connectors/pulsar/test/integration.sh
test-pulsar: $(B)/tsdbd $(B)/tsdb
	cd client/java && mvn -q $(JAVA_M2) install -DskipTests
	cd connectors/pulsar && mvn -q $(JAVA_M2) test

# Commit rules as a git hook (no session links, DCO sign-off last); once per clone.
hooks:
	git config core.hooksPath .githooks

# the client benchmark harness on a tiny data set (embedded and the C client); numbers are meaningless
bench-clients-smoke: $(B)/tsdbd $(B)/tsdb-bench $(B)/tsdbc-bench
	bench/clients/smoke.sh

# End-to-end tests (tests/integration/run.py): real tsdbd processes, the C, Rust, Python and Java clients and the
# tsdb CLI, on both server backends. Needs what test-clients needs, and builds its programs into $(B)/integ.
# `make integration-tests INTEG_ARGS="--only matrix --io epoll"` runs part of it; see run.py --help.
INTEG      := $(B)/integ
INTEG_ARGS ?=

$(INTEG)/itc: tests/integration/c/itc.c client/tsdb_client.h $(B)/libtsdbclient.a
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) -Iclient -o $@ $< $(B)/libtsdbclient.a $(ZSTD_LIB) -lpthread -lm $(LINK_EXTRA)

integ-rust:
	cd tests/integration/rust && cargo build --release --locked --target-dir $(abspath $(INTEG))/rust

integ-python:
	test -x $(PYVENV)/bin/maturin || (python3 -m venv $(PYVENV) && $(PYVENV)/bin/pip install -q maturin pytest)
	cd client/python && VIRTUAL_ENV=$(abspath $(PYVENV)) $(abspath $(PYVENV))/bin/maturin develop --release

integ-java:
	cd client/java && mvn -q -DskipTests package
	@mkdir -p $(INTEG)/java
	javac -d $(INTEG)/java -cp client/java/target/tsdb-client-$(RELEASE_VERSION).jar tests/integration/java/Itj.java

integration-tests: $(B)/tsdbd $(B)/tsdb $(INTEG)/itc integ-rust integ-python integ-java
	python3 tests/integration/run.py --bin-dir $(B) $(INTEG_ARGS)

# The harness's own tests (seconds): a broken io_uring backend fails the run, signals, mutate.py's verdicts.
# run.py refuses to start without every client program (rc 2), so the selftest builds them as integration-tests does.
integration-selftest: $(B)/tsdbd $(B)/tsdb $(INTEG)/itc integ-rust integ-python integ-java
	python3 tests/integration/selftest.py

# Does the integration suite have teeth? Plants each known bug in a copy of the tree and requires the scenario that
# should notice to fail (tests/integration/mutate.py; a mutant that survives, or a run that errors, fails this).
# MUTATE_ARGS="--only M3,M5" runs some.
MUTATE_ARGS ?=
integration-mutants: $(B)/tsdbd $(B)/tsdb $(INTEG)/itc integ-rust integ-python integ-java
	python3 tests/integration/mutate.py $(MUTATE_ARGS)

# The same against the binaries of `make dist`: its tsdbd and tsdb, and the C client linked with its libtsdbclient.a.
integration-tests-dist: dist integ-rust integ-python integ-java
	$(CC) -O2 -g -std=gnu11 -Iinclude -Iclient -o $(DIST_B)/itc tests/integration/c/itc.c $(DIST_B)/libtsdbclient.a \
	    $(ZSTD_LIB) -lpthread -lm
	python3 tests/integration/run.py --bin-dir $(DIST_B) --itc $(DIST_B)/itc $(INTEG_ARGS)

.PHONY: check-rdma-syms margin-mutants lease-mutants chunkpeer-mutants transfer-mutants move-mutants rset-mutants fwd-mutants copy-mutants auth-mutants check-default-goal test-cli-stats test-cli-stats-net test-cli-cluster test-cluster-tutorial sim-repl-soak sim-repl-mutants check-version fuzz-frames jepsen-selftest bench-clients-smoke hooks all test asan ubsan-check tsan clean jepsen test-almalinux10 test-rust test-python test-java test-java-embedded test-clients python-sdist test-python-sdist test-licenses dist dist-test dist-check-almalinux10 dist-check-sandybridge integ-rust integ-python integ-java integration-tests integration-tests-dist integration-selftest cluster-mutants rewind-mutants test-clients-epoll test-pulsar integration-mutants

$(B)/codec-bench: tools/codec-bench.c tools/common.h tools/datasets.h $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

# the latest table's flush and lookup benchmark (embedded; uses the test knobs of internal.h): not part of `all`
$(B)/latest-bench: tools/latest-bench.c src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

$(B)/insert-bench: tools/insert-bench.c tools/common.h tools/datasets.h $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

# The release version is the file VERSION (also in include/tsdb.h and the client manifests: `make check-version`).
# TSDB_VERSION is the build id shown next to it by `tsdbd --version`, `tsdb --version` and tsdb_build_info.
RELEASE_VERSION := $(shell cat VERSION)
TSDB_VERSION ?= $(shell git describe --always --dirty 2>/dev/null || echo dev)

# The cluster (docs/design-cluster.md): the Raft core copied from intellistream-objects (src/raft/UPSTREAM, built as
# upstream wrote it, with -Isrc -Isrc/raft/compat for what it includes from isod) and the metadata state machine
# (server/meta.c). Not part of libtsdb: the cluster is a tsdbd mode (C24). raft_sim is the tests' only.
RAFT_HDR := $(wildcard src/raft/*.h) $(wildcard src/raft/compat/*/*.h)
RAFT_OBJ := $(B)/raft_raft.o $(B)/raft_raft_log.o $(B)/raft_raft_wire.o $(B)/raft_compat.o
# -Wno-stringop-overflow: at -O3 GCC 15 warns about raft.c's vote tally (`r->votes[i]` for i below config.n, which
# raft_config_decode bounds by RAFT_MEMBERS_MAX); the copy is not edited, so the false positive is silenced here.
$(B)/raft_%.o: src/raft/%.c $(RAFT_HDR) | $(B)
	$(CC) $(CFLAGS) -Wno-stringop-overflow -Isrc -Isrc/raft/compat -c -o $@ $<
$(B)/meta.o: server/meta.c server/meta.h src/repl.h | $(B)
	$(CC) $(CFLAGS) -Isrc -c -o $@ $<
# tsdbd's cluster thread (and the CLI's side of the admin ops); CLUSTER_OBJ goes into tsdbd, every binary that
# includes tsdbd.c, and the CLI
$(B)/cluster.o: server/cluster.c server/cluster.h server/cluster_int.h server/peerlink.h server/meta.h client/tsdb_crypto.h $(RAFT_HDR) | $(B)
	$(CC) $(CFLAGS) -Isrc -Isrc/raft/compat -Iclient -c -o $@ $<
# the data links (peerlink.h): in every binary that has the cluster thread, whose acceptor hands sockets to it; the
# frames' check (plcheck.h, PLCHECK_HDR above) is xxHash's XXH3 inline (server/xxhash, unchanged, UPSTREAM)
$(B)/peerlink.o: server/peerlink.c server/peerlink.h server/pltransport.h $(PLCHECK_HDR) client/tsdb_crypto.h src/compat.h | $(B)
	$(CC) $(CFLAGS) -Iclient -c -o $@ $<
# the verbs transport under peerlink (plrdma.h): no verbs header, every verb through struct plr_ops
$(B)/plrdma.o: server/plrdma.c server/plrdma.h server/pltransport.h | $(B)
	$(CC) $(CFLAGS) -c -o $@ $<
# rdma.flags holds RDMA_FLAGS and changes only when they do: headers installed later, RDMA_CFLAGS or NO_RDMA given,
# rebuild plrdma_dl.o (and tsdbd) instead of keeping the object built the other way
$(B)/rdma.flags: FORCE | $(B)
	@printf '%s\n' '$(RDMA_FLAGS)' | cmp -s - $@ || printf '%s\n' '$(RDMA_FLAGS)' >$@
$(B)/plrdma_dl.o: server/plrdma_dl.c server/plrdma.h $(B)/rdma.flags | $(B)
	$(CC) $(CFLAGS) $(RDMA_FLAGS) -c -o $@ $<
# the forwarding links (server/fwd.h): the transport in fwd.o (no cluster calls: test_peerlink and the fuzzer link
# it), tsdbd's instance over the cluster's member list in fwd_cluster.o
$(B)/fwd.o: server/fwd.c server/fwd.h server/peerlink.h | $(B)
	$(CC) $(CFLAGS) -c -o $@ $<
# forwarding one hop, the request half (server/fwdreq.h: envelope, tail, the entry node's rules; tests/test_fwd.c)
$(B)/fwdreq.o: server/fwdreq.c server/fwdreq.h server/fwd.h server/peerlink.h | $(B)
	$(CC) $(CFLAGS) -Iinclude -c -o $@ $<
$(B)/fwd_cluster.o: server/fwd_cluster.c server/fwd.h server/peerlink.h server/cluster.h server/meta.h client/tsdb_crypto.h | $(B)
	$(CC) $(CFLAGS) -Isrc -Isrc/raft/compat -Iclient -c -o $@ $<
# the chunk server (server/chunksrv.h): a member's chunk stores served on a peerlink of its own (kinds CHUNK, CHUNKB);
# no cluster calls (test_chunksrv and the fuzzer link it), tsdbd's instance over the member list in chunksrv_cluster.o
$(B)/chunksrv.o: server/chunksrv.c server/chunksrv.h server/peerlink.h src/chunk.h src/chunk_priv.h src/internal.h | $(B)
	$(CC) $(CFLAGS) -Isrc -c -o $@ $<
# the cold ops of a cluster ec volume over its stripe group (server/chunkpeer.h, r8f); no cluster calls (test_chunkpeer
# links it with chunksrv.o)
$(B)/chunkpeer.o: server/chunkpeer.c server/chunkpeer.h server/chunksrv.h src/coldops.h src/chunk.h | $(B)
	$(CC) $(CFLAGS) -Isrc -c -o $@ $<
$(B)/chunksrv_cluster.o: server/chunksrv_cluster.c server/chunksrv.h server/peerlink.h server/cluster.h server/meta.h src/chunk.h client/tsdb_crypto.h src/chunk_priv.h src/internal.h | $(B)
	$(CC) $(CFLAGS) -Isrc -Isrc/raft/compat -Iclient -c -o $@ $<
# meta.c's promotion is src/repl.c's repl_meta_promote (one function for the state machine and the simulator)
$(B)/repl.o: src/repl.c src/repl.h | $(B)
	$(CC) $(CFLAGS) -Isrc -c -o $@ $<
$(B)/crashpt.o: server/crashpt.c server/crashpt.h | $(B)
	$(CC) $(CFLAGS) -c -o $@ $<
# the replication driver's payloads on the data links (server/replwire.h)
$(B)/replwire.o: server/replwire.c server/replwire.h src/repl.h | $(B)
	$(CC) $(CFLAGS) -c -o $@ $<
# R4's files on the Raft leader's side (server/cluster_int.h): stubs until r4-transfer, r4-move, r4-setchg fill them
R4_CL_HDR := server/cluster_int.h server/cluster.h server/meta.h
$(B)/ctransfer.o: server/ctransfer.c $(R4_CL_HDR) | $(B)
	$(CC) $(CFLAGS) -Isrc -c -o $@ $<
$(B)/cmove.o: server/cmove.c $(R4_CL_HDR) | $(B)
	$(CC) $(CFLAGS) -Isrc -c -o $@ $<
$(B)/cset.o: server/cset.c $(R4_CL_HDR) | $(B)
	$(CC) $(CFLAGS) -Isrc -c -o $@ $<
CLUSTER_OBJ := $(B)/plrdma.o $(B)/plrdma_dl.o $(B)/ctransfer.o $(B)/cmove.o $(B)/cset.o $(B)/cluster.o $(B)/meta.o $(B)/repl.o $(B)/crashpt.o $(B)/peerlink.o $(B)/replwire.o $(B)/fwd.o $(B)/fwd_cluster.o $(B)/fwdreq.o $(B)/chunksrv.o $(B)/chunksrv_cluster.o $(RAFT_OBJ)
$(B)/tsdb: $(B)/cluster-cli.o $(B)/cluster-user-cli.o $(CLUSTER_OBJ)
$(B)/cluster-cli.o: tools/cluster-cli.c server/cluster.h server/meta.h $(RAFT_HDR) | $(B)
	$(CC) $(CFLAGS) -Iserver -Isrc -Isrc/raft/compat -c -o $@ $<
$(B)/cluster-user-cli.o: tools/cluster-user-cli.c server/cluster.h server/meta.h server/users.h $(RAFT_HDR) | $(B)
	$(CC) $(CFLAGS) -Iserver -Isrc -Isrc/raft/compat -c -o $@ $<
# the data links (server/peerlink.c): two endpoints in one process; includes src/internal.h for xxh64 (in the library)
$(B)/test_plrdma: tests/test_plrdma.c tests/plr_fake.h tests/pl_fake.h server/plrdma.h server/pltransport.h server/peerlink.h $(B)/plrdma.o $(B)/plrdma_dl.o $(B)/peerlink.o $(B)/tsdb_crypto.o $(LIB)
	$(CC) $(CFLAGS) -Iclient -Iserver -Isrc -o $@ $< $(B)/plrdma.o $(B)/plrdma_dl.o $(B)/peerlink.o $(B)/tsdb_crypto.o $(LIB) $(LDLIBS)

$(B)/test_peerlink: tests/test_peerlink.c tests/pl_fake.h server/pltransport.h server/peerlink.h $(PLCHECK_HDR) server/fwd.h $(B)/peerlink.o $(B)/fwd.o $(B)/tsdb_crypto.o $(LIB)
	$(CC) $(CFLAGS) -Iclient -Iserver -Isrc -o $@ $< $(B)/peerlink.o $(B)/fwd.o $(B)/tsdb_crypto.o $(LIB) $(LDLIBS)
# the cluster volumes' margins (server/margins.c) and jepsen's judgement of a failed volume, no server
$(B)/test_margins: tests/test_margins.c server/margins.c server/margins.h server/cluster.h server/meta.h tests/jepsen/outage_judge.h include/tsdb_protocol.h | $(B)
	$(CC) $(CFLAGS) -Iinclude -Iserver -o $@ $< server/margins.c
# the entry node's rules over a fake link (no sockets, no tsdbd)
$(B)/test_fwd: tests/test_fwd.c server/fwdreq.h server/fwd.h server/peerlink.h $(B)/fwdreq.o
	$(CC) $(CFLAGS) -Iinclude -Iserver -o $@ $< $(B)/fwdreq.o -lpthread
$(B)/test_meta: tests/test_meta.c server/meta.h $(B)/meta.o $(B)/repl.o $(RAFT_OBJ) $(B)/raft_raft_sim.o
	$(CC) $(CFLAGS) -Isrc -Isrc/raft/compat -o $@ $< $(B)/meta.o $(B)/repl.o $(RAFT_OBJ) $(B)/raft_raft_sim.o -lm
# test_meta with bugs planted in meta.c (MUT(n), -DMETA_MUTANTS): tests/meta_mutants.sh runs each with --mutant n and
# requires a failure (the unit tests or the simulator's checker must catch it), as sim_repl_mut does for repl.c.
$(B)/test_meta_mut: tests/test_meta.c server/meta.c server/meta.h $(B)/repl.o $(RAFT_OBJ) $(B)/raft_raft_sim.o
	$(CC) $(CFLAGS) -DMETA_MUTANTS -Isrc -Isrc/raft/compat -o $@ tests/test_meta.c server/meta.c $(B)/repl.o $(RAFT_OBJ) \
	    $(B)/raft_raft_sim.o -lm
# The Raft simulators for long: the copied core's own chaos runs and the metadata state machine under real cores,
# META_SOAK_SEEDS seeds of META_SOAK_SECONDS virtual seconds from META_SOAK_FIRST (a failure names its seed).
META_SOAK_SEEDS   ?= 5000
META_SOAK_SECONDS ?= 30
META_SOAK_FIRST   ?= 1000
# tsdbd --cluster end to end: five servers, init, node add, a leader killed and restarted (the cluster thread polls:
# one run covers both IO backends)
# cvol_check.o opens a replica directory in-process (the engine's headers; test_cluster speaks the client's)
$(B)/cvol_check.o: tests/cvol_check.c tests/cvol_check.h src/internal.h src/feed.h | $(B)
	$(CC) $(CFLAGS) -Isrc -Itests -c -o $@ $<
$(B)/test_cluster: tests/test_cluster.c tests/harness.h tests/cvol_check.h server/cluster.h server/meta.h server/peerlink.h \
    $(CLUSTER_OBJ) $(B)/cvol_check.o $(B)/users.o $(B)/tsdb_crypto.o $(B)/libtsdbclient.a $(LIB) \
    $(if $(filter Darwin,$(UNAME_S)),,$(B)/libcrashfs.so)
	$(CC) $(CFLAGS) -Iserver -Isrc -Isrc/raft/compat -Itests -Iclient -o $@ $< $(CLUSTER_OBJ) $(B)/cvol_check.o \
	    $(B)/users.o $(B)/tsdb_crypto.o $(CLIB) $(LIB) $(ZSTD_LIB) -lpthread -lm
rewind-mutants:
	tests/rewind_mutants.sh $(or $(REWIND_DIR),/var/tmp/rewind-mutants-$$$$)
meta-mutants: $(B)/test_meta_mut
	tests/meta_mutants.sh $(B)/test_meta_mut
# the replication driver's mutants (server/repld.c TSDB_REPLD_MUTANT): each must fail test_cluster replindet or
# replicate, or ackdurable (power loss of the replicas) or writerfail (D13 on the writer), or repcheck (floor, unsure
# REPL, admit check, fast path, a peer's cursor below what it acknowledged), or the ISR, keep, ship, backpressure, close and
# rejoin scenarios (tests/repld_mutants.sh pins each to its FAIL line); hbnoclamp is not listed: no scenario delays a link yet
repld-mutants: $(B)/tsdbd $(B)/test_cluster $(B)/tsdb $(B)/libcrashfs.so
	CRASHFS_SO=$(B)/libcrashfs.so TSDBD=$(B)/tsdbd TSDB_CLI=$(B)/tsdb tests/repld_mutants.sh $(SCRATCH) commitbyleader floornotsaved ackbeforesync nodepose noquiesce floornottold \
	    unsureignored admitnocheck nofastpath nocursorup commitpastremoved nominisr ackbelowminisr shipperfile nocap noseal \
	    noinboxbound nospill keepignorespeers keepwritten nologstartcheck stopnodepose lateclock syncbyrep inboxwaitall shipnoisr fastride \
	    nojoinhold inboxstarve opennohold promotenone tickstretch stepcurrentonly stallcatchup keepunanswered fencenosync nomoved deposemoved demotestaleidx \
	    refusedsilent divergeisr snapnoverify snapsamelink snapnospacecheck snapsrcpromoting snapnosrcmax snapsharedmax \
	    snapnodonebits snapmakedown snapnowaitdone snapnostale snapnostall snapnosrcdown \
	    snapstalekeep snapinstallstale snapservebeforeopen snapkeepdiverged snapsetnodefer snapclosekeep \
	    snapdropafter snapnoendafter snapnofence snapdivlogend snapnoleftovers replaystartlog nomsgcheck nometarefuse bulkframes \
	    metafenceonly noepochbound nodescepoch nodesccmp
# the replica set change mutants (server/rset.c, cvolset*.c, cset.c; tests/rset_mutants.sh pins each to its FAIL line)
rset-mutants: $(B)/tsdbd $(B)/test_cluster $(B)/tsdb
	TSDBD=$(B)/tsdbd TSDB_CLI=$(B)/tsdb tests/rset_mutants.sh $(SCRATCH)
# the lease and liveness mutants (server/cluster.c TSDB_CLUSTER_MUTANT): each must fail its test_cluster scenario
lease-mutants: $(B)/tsdbd $(B)/test_cluster $(B)/tsdb
	TSDBD=$(B)/tsdbd TSDB_CLI=$(B)/tsdb tests/lease_mutants.sh $(SCRATCH)
# r8f: the chunk peer's planted bugs in tsdbd (server/chunkpeer.c TSDB_CHUNKPEER_MUTANT), each must fail test_cluster
# ecbasic (nosumcmp only test_chunkpeer: a member that answers another sum needs its fake transport)
chunkpeer-mutants: $(B)/tsdbd $(B)/test_cluster $(B)/tsdb
	@$(if $(EC_OBJ),true,echo "chunkpeer-mutants: this build has no erasure codec (NO_EC=1): ecbasic skips, nothing to plant"; false)
	$(MUT) --name ecbasic --list "ownepoch staleraise delbeforegc" -- env TSDB_CHUNKPEER_MUTANT=@M@ TSDBD=$(B)/tsdbd \
	    TSDB_CLI=$(B)/tsdb ./$(B)/test_cluster @D@ ecbasic
# the planned leader transfer's mutants (server/rtransfer.c, server/ctransfer.c; tests/transfer_mutants.sh)
transfer-mutants: $(B)/tsdbd $(B)/test_cluster $(B)/tsdb
	TSDBD=$(B)/tsdbd TSDB_CLI=$(B)/tsdb tests/transfer_mutants.sh $(SCRATCH)
# the replica move's mutants (server/cmove.c TSDB_CLUSTER_MUTANT; tests/move_mutants.sh)
move-mutants: $(B)/tsdbd $(B)/test_cluster $(B)/tsdb
	TSDBD=$(B)/tsdbd TSDB_CLI=$(B)/tsdb tests/move_mutants.sh $(SCRATCH)
# the cluster login mutants (server/auth.c, tools/cluster-user-cli.c TSDB_AUTH_MUTANT; tests/auth_mutants.sh)
auth-mutants: $(B)/tsdbd $(B)/test_cluster $(B)/tsdb
	TSDBD=$(B)/tsdbd TSDB_CLI=$(B)/tsdb tests/auth_mutants.sh $(SCRATCH)
# the cluster volumes' margins (server/margins.c TSDBD_MARGIN_MUTANT; tests/margin_mutants.sh): test_cluster margins
margin-mutants: $(B)/tsdbd $(B)/test_cluster $(B)/tsdb
	TSDBD=$(B)/tsdbd TSDB_CLI=$(B)/tsdb tests/margin_mutants.sh $(SCRATCH)
# the metadata copy's snapshot, TOPOLOGY, STATUS, MOVED and the acceptor's table (tests/copy_mutants.sh)
copy-mutants: $(B)/tsdbd $(B)/test_cluster $(B)/tsdb
	TSDBD=$(B)/tsdbd TSDB_CLI=$(B)/tsdb tests/copy_mutants.sh $(SCRATCH)
# the forwarding mutants (server/tsdbd.c TSDBD_FWD_MUTANT; tests/fwd_mutants.sh)
fwd-mutants: $(B)/tsdbd $(B)/test_cluster $(B)/tsdb
	TSDBD=$(B)/tsdbd TSDB_CLI=$(B)/tsdb TEST_CLUSTER=$(B)/test_cluster tests/fwd_mutants.sh $(SCRATCH)
meta-soak: $(B)/test_meta
	./$(B)/test_meta $(SCRATCH) --seeds $(META_SOAK_SEEDS) --seconds $(META_SOAK_SECONDS) --first $(META_SOAK_FIRST)

# R4's files beside the replication driver and the volume manager (server/repld_int.h, cvolset.h, snapinst.h): stubs
# until r4-xferlink, r4-xferinst, r4-setchg, r4-transfer fill them; compiled with repld.c wherever it is (and so is
# server/margins.c, the cluster volumes' margins: every build of tsdbd.c)
R4_SRC := server/rsnap.c server/rset.c server/rtransfer.c server/cvolset.c server/cvolset_mgr.c server/snapinst.c \
          server/snapinst_run.c server/margins.c
R4_HDR := server/repld_int.h server/rsnap.h server/rset.h server/rtransfer.h server/cvolset.h server/snapinst.h \
          server/margins.h
# r8f: the peer cold ops and their cluster instance (server/chunkpeer.h), compiled with tsdbd (g_ec_cfg is tsdbd.c's)
CP_SRC := server/chunkpeer.c server/chunkpeer_cluster.c
TSDBD_SRC := $(CP_SRC) server/chunkpeer.h $(R4_SRC) $(R4_HDR) server/fwd.h server/fwdreq.h server/tsdbd_keys.h tools/cfg.h server/tsdbd.c server/metrics.c server/metrics.h server/auth.c server/auth.h server/tenants.c server/tenants.h server/users.h \
             server/cluster.h server/cvol.h server/cvol.c server/cvolfs.h server/cvolfs.c server/repld.h server/repld.c server/replwire.h src/cvol.h src/replica.h src/repl.h src/replfile.h server/meta.h server/domains.h server/epoll_compat.h include/tsdb_protocol.h include/tsdb_wire.h client/tsdb_pack.h client/tsdb_crypto.h
$(B)/tsdbd: server/tsdbd.c $(TSDBD_SRC) $(B)/tsdb_pack.o $(B)/users.o $(B)/tsdb_crypto.o $(CLUSTER_OBJ) $(CFG_OBJ) $(LIB)
	$(CC) $(CFLAGS) $(URING_FLAGS) -DTSDB_BUILD_VERSION='"$(TSDB_VERSION)"' -o $@ $< server/metrics.c server/auth.c server/tenants.c server/cvol.c server/cvolfs.c server/repld.c $(CP_SRC) $(R4_SRC) \
	    $(B)/tsdb_pack.o $(B)/users.o $(B)/tsdb_crypto.o $(CLUSTER_OBJ) $(CFG_OBJ) $(LIB) $(LDLIBS) $(URING_LIBS)

# A server with a bug planted in the Raft log (the copy in src/raft is upstream's and stays unedited, so the mutant is
# made here, from it, by sed): C2, the hard state (term and vote) saved without its fdatasync. test_cluster's powervote
# must fail it with its own message, every time (the test-trust review: it passed 2 runs in 5 before it made n2 vote
# for another candidate). `make cluster-mutants`, part of `make test`. The sed must change exactly one line.
$(B)/raft_log_c2.c: src/raft/raft_log.c | $(B)
	test "$$(grep -c ' || fdatasync(l->hard_fd) != 0' $<)" = 1
	sed 's/ || fdatasync(l->hard_fd) != 0//' $< > $@
	! cmp -s $< $@
$(B)/raft_log_c2.o: $(B)/raft_log_c2.c $(RAFT_HDR) | $(B)
	$(CC) $(CFLAGS) -Wno-stringop-overflow -Isrc -Isrc/raft -Isrc/raft/compat -c -o $@ $<
$(B)/tsdbd-mut-c2: server/tsdbd.c $(TSDBD_SRC) $(B)/tsdb_pack.o $(B)/users.o $(B)/tsdb_crypto.o $(CLUSTER_OBJ) $(CFG_OBJ) \
                   $(B)/raft_log_c2.o $(LIB)
	$(CC) $(CFLAGS) $(URING_FLAGS) -DTSDB_BUILD_VERSION='"$(TSDB_VERSION)"' -o $@ server/tsdbd.c server/metrics.c server/auth.c server/tenants.c server/cvol.c server/cvolfs.c server/repld.c $(CP_SRC) $(R4_SRC) \
	    $(B)/tsdb_pack.o $(B)/users.o $(B)/tsdb_crypto.o $(filter-out $(B)/raft_raft_log.o,$(CLUSTER_OBJ)) $(CFG_OBJ) \
	    $(B)/raft_log_c2.o $(LIB) $(LDLIBS) $(URING_LIBS)
# Under AddressSanitizer crashfs cannot be preloaded (test_cluster's powervote prints "skipped" and passes, so the planted
# bug would not fail anything): the target says so and does not run, instead of reporting a mutant that nothing caught.
cluster-mutants: $(B)/tsdbd-mut-c2 $(B)/test_cluster $(B)/tsdb $(CRASHFS_SO)
ifeq ($(findstring fsanitize=address,$(CFLAGS)),)
	tests/expect_fail.sh "hard state is term" env CRASHFS_SO=$(B)/libcrashfs.so TSDBD=$(B)/tsdbd-mut-c2 TSDB_CLI=$(B)/tsdb \
	    ./$(B)/test_cluster $(SCRATCH) powervote
else
	@echo "cluster-mutants: SKIPPED under AddressSanitizer (crashfs cannot be preloaded into it; 1 mutant not run)"
endif

# tests/fuzz/fuzz_frames.c includes server/tsdbd.c (epoll only: no ring needed without sockets). FRAMES_COV is
# -fsanitize-coverage=trace-pc in `make fuzz-frames`, which feeds its coverage-guided driver (auth.c, the login and
# the checks, is compiled with it too); empty in `make test` (FUZZ_COV is the on-disk harnesses' coverage flag and
# is always set).
$(B)/fuzz_frames: tests/fuzz/fuzz_frames.c $(TSDBD_SRC) $(B)/tsdb_pack.o $(B)/users.o $(B)/tsdb_crypto.o \
                  $(CLUSTER_OBJ) $(CFG_OBJ) $(LIB)
	$(CC) $(CFLAGS) $(FRAMES_COV) -DTSDBD_NO_URING -o $@ $< server/metrics.c server/auth.c server/tenants.c server/cvol.c server/cvolfs.c server/repld.c $(CP_SRC) $(R4_SRC) $(B)/tsdb_pack.o \
	    $(B)/users.o $(B)/tsdb_crypto.o $(CLUSTER_OBJ) $(CFG_OBJ) $(LIB) $(LDLIBS) $(PLAT_FUZZ_LDFLAGS)

# Mutates the request corpus for FUZZ_SECONDS (shared with `make fuzz`, default 60) under ASan and UBSan, in
# build-fuzz-frames/ (its own tree: `make fuzz` builds build-fuzz/ with other flags); new inputs go to
# build-fuzz-frames/frames-new/ and a failing one to build-fuzz-frames/frames-new/crash-*; the next run starts from
# both. A finding, once fixed, is committed to tests/fuzz/frames/ as its regression input (make test replays that
# directory).
# No allocation may exceed a frame (256 MB) and some slack: a request that makes the server allocate more than
# that from a few bytes is a finding (a crash), not a NULL the handler turns into TSDB_ERR_NOMEM.
FUZZ_MAX_ALLOC_MB ?= 300
fuzz-frames:
	$(MAKE) B=build-fuzz-frames ARCH= CFLAGS="$(ASAN_FLAGS)" FRAMES_COV=-fsanitize-coverage=trace-pc \
	    build-fuzz-frames/fuzz_frames
	UBSAN_OPTIONS="$(UBSAN_RUN) $$UBSAN_OPTIONS" \
	    ASAN_OPTIONS="allocator_may_return_null=0 max_allocation_size_mb=$(FUZZ_MAX_ALLOC_MB) $$ASAN_OPTIONS" \
	    ./build-fuzz-frames/fuzz_frames fuzz --seconds $(FUZZ_SECONDS) --out build-fuzz-frames/frames-new \
	    tests/fuzz/frames $$(test -d build-fuzz-frames/frames-new && echo build-fuzz-frames/frames-new)

$(B)/tsdb_client.o: client/tsdb_client.c client/tsdb_client.h client/tsdb_crypto.h include/tsdb_protocol.h \
                    include/tsdb.h include/tsdb_wire.h | $(B)
	$(CC) $(CFLAGS) -c -o $@ $<

$(B)/tsdb_pack.o: client/tsdb_pack.c client/tsdb_pack.h include/tsdb_protocol.h | $(B)
	$(CC) $(CFLAGS) -c -o $@ $<

$(CLIB): $(B)/tsdb_client.o $(B)/tsdb_pack.o $(B)/tsdb_calendar.o $(B)/tsdb_crypto.o
	$(AR) rcs $@ $^

$(B)/tsdbc-bench: tools/tsdbc-bench.c client/tsdb_client.h tools/common.h tools/datasets.h tools/cbench.h $(CLIB)
	$(CC) $(CFLAGS) -o $@ $< $(CLIB) $(ZSTD_LIB) -lpthread -lm $(LINK_EXTRA)

$(B)/outage-load: tools/outage-load.c client/tsdb_client.h $(CLIB)
	$(CC) $(CFLAGS) -Iclient -o $@ $< $(CLIB) $(ZSTD_LIB) -lpthread -lm $(LINK_EXTRA)

$(B)/type-bench: tools/type-bench.c tools/common.h tools/datasets.h src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

$(B)/fold-bench: tools/fold-bench.c tools/common.h src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# what a stream link costs for peerlink-shaped frames (TCP, or rsocket through a dlopen of librdmacm with --rsocket;
# tools/rdma-bench.sh, docs/design-rdma.md): no engine, and librdmacm is not linked
# The MAC candidates (tools/mac_cands.c, docs/design-rdma.md 6) with BLAKE3's reference C (tools/blake3, UPSTREAM): its
# kernels are built with their own -m flags and picked by its own cpuid dispatch (bench only, never in tsdbd). Their
# objects go in $(B)/b3/: tools/check-isa.sh reads every $(B)/*.o of make dist, and these are in no shipped file.
B3_SRC = tools/blake3/blake3.c tools/blake3/blake3_dispatch.c tools/blake3/blake3_portable.c
ifeq ($(shell uname -m),x86_64)
B3_OBJ = $(B)/b3/blake3_sse2.o $(B)/b3/blake3_sse41.o $(B)/b3/blake3_avx2.o $(B)/b3/blake3_avx512.o
B3_DEF =
else
B3_OBJ =
B3_DEF = -DBLAKE3_NO_SSE2 -DBLAKE3_NO_SSE41 -DBLAKE3_NO_AVX2 -DBLAKE3_NO_AVX512 -DBLAKE3_USE_NEON=0
endif
$(B)/b3/blake3_sse2.o: tools/blake3/blake3_sse2.c tools/blake3/blake3_impl.h | $(B)
	@mkdir -p $(@D)
	$(CC) -O3 -msse2 -c -o $@ $<
$(B)/b3/blake3_sse41.o: tools/blake3/blake3_sse41.c tools/blake3/blake3_impl.h | $(B)
	@mkdir -p $(@D)
	$(CC) -O3 -msse4.1 -c -o $@ $<
$(B)/b3/blake3_avx2.o: tools/blake3/blake3_avx2.c tools/blake3/blake3_impl.h | $(B)
	@mkdir -p $(@D)
	$(CC) -O3 -mavx2 -c -o $@ $<
$(B)/b3/blake3_avx512.o: tools/blake3/blake3_avx512.c tools/blake3/blake3_impl.h | $(B)
	@mkdir -p $(@D)
	$(CC) -O3 -mavx512f -mavx512vl -c -o $@ $<
$(B)/link-bench: tools/link-bench.c tools/mac_cands.c tools/mac_cands.h tools/mac_cands_kat.h $(PLCHECK_HDR) $(B3_SRC) $(B3_OBJ) $(B)/tsdb_crypto.o
	$(CC) $(CFLAGS) $(B3_DEF) -o $@ $< tools/mac_cands.c $(B3_SRC) $(B3_OBJ) $(B)/tsdb_crypto.o -ldl

$(B)/decode-bench: tools/decode-bench.c tools/common.h src/internal.h src/bitpack.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

ifneq ($(EC_OBJ),)
$(B)/ec-bench: tools/ec-bench.c tools/common.h src/internal.h $(EC_HDR) $(LIB)
	$(CC) $(CFLAGS) -Isrc -Isrc/ec/compat -o $@ $< $(LIB) $(LDLIBS) -lm
endif

$(B)/threads-bench: tools/threads-bench.c tools/common.h src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

$(B)/wtail-bench: tools/wtail-bench.c src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

$(B)/twa-bench: tools/twa-bench.c tools/common.h include/tsdb.h $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/scale-bench: tools/scale-bench.c client/tsdb_client.h tools/common.h tools/datasets.h $(CLIB) $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(CLIB) $(LIB) $(LDLIBS)

$(B)/test_stability: tests/test_stability.c tests/harness.h $(CLIB)
	$(CC) $(CFLAGS) -Iclient -o $@ $< $(CLIB) $(ZSTD_LIB) -lpthread -lm $(LINK_EXTRA)

$(B)/test_ports: tests/test_ports.c tests/harness.h $(CLIB)
	$(CC) $(CFLAGS) -Iclient -o $@ $< $(CLIB) $(ZSTD_LIB) -lpthread -lm $(LINK_EXTRA)

$(B)/test_client: tests/test_client.c $(CLIB)
	$(CC) $(CFLAGS) -Iclient -o $@ $< $(CLIB) $(ZSTD_LIB) -lpthread -lm $(LINK_EXTRA)

$(B)/test_wire: tests/test_wire.c include/tsdb_wire.h $(CLIB)
	$(CC) $(CFLAGS) -Iclient -o $@ $< $(CLIB) $(ZSTD_LIB) -lpthread -lm $(LINK_EXTRA)

# includes server/tsdbd.c, as fuzz_frames does (epoll only)
$(B)/test_out_send: tests/test_out_send.c $(TSDBD_SRC) $(B)/tsdb_pack.o $(B)/users.o $(B)/tsdb_crypto.o \
                    $(CLUSTER_OBJ) $(CFG_OBJ) $(LIB)
	$(CC) $(CFLAGS) -DTSDBD_NO_URING -o $@ $< server/metrics.c server/auth.c server/tenants.c server/cvol.c server/cvolfs.c server/repld.c $(CP_SRC) $(R4_SRC) $(B)/tsdb_pack.o $(B)/users.o \
	    $(B)/tsdb_crypto.o $(CLUSTER_OBJ) $(CFG_OBJ) $(LIB) $(LDLIBS)

$(B)/test_server_multi: tests/test_server_multi.c tests/harness.h $(CLIB)
	$(CC) $(CFLAGS) -Iclient -o $@ $< $(CLIB) $(LDLIBS)

$(B)/test_server: tests/test_server.c tests/harness.h tests/samekeys.h tests/lttb_ref.h server/metrics.c server/metrics.h $(CLIB) $(LIB)
	$(CC) $(CFLAGS) -Iclient -Iserver -o $@ $< server/metrics.c $(CLIB) $(LIB) $(LDLIBS)

$(B)/test_follow: tests/test_follow.c $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_export: tests/test_export.c $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_volumes: tests/test_volumes.c $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_damage: tests/test_damage.c src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# Crafted files in the data directory and extreme inputs (internals: -Isrc)
$(B)/test_crafted: tests/test_crafted.c src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# The packed granule index: round trips of every type and edge case, and through the engine (internals: -Isrc)
$(B)/test_gindex: tests/test_gindex.c src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS) -lm

# The memtable's series array and a flush's big buffers: what the other threads wait on (internals: -Isrc)
$(B)/test_flush_tail: tests/test_flush_tail.c src/internal.h src/compat.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# Crafted TZif files, rules and zone names; includes the calendar's source to see its cache
$(B)/test_stats: tests/test_stats.c src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS) -lm

# the tenant list of tsdbd when its source changes its mind while the engine takes a list (a hook in server/tenants.c)
$(B)/test_tn_race: tests/test_tn_race.c tests/harness.h server/tenants.c server/tenants.h server/users.h $(B)/users.o $(CLIB) $(LIB)
	$(CC) $(CFLAGS) -Iclient -Iserver -o $@ $< server/tenants.c $(B)/users.o $(CLIB) $(LIB) $(LDLIBS)

# Tenants, hot lists and residency (docs/design-stats.md steps 3 and 4)
$(B)/test_tenants: tests/test_tenants.c src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS) -lm

$(B)/test_tzfiles: tests/test_tzfiles.c client/tsdb_calendar.c include/tsdb_calendar.h
	$(CC) $(CFLAGS) -Iclient -o $@ $< -lpthread

$(B)/test_types: tests/test_types.c src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_delete: tests/test_delete.c src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_rs: tests/test_rs.c src/internal.h $(EC_HDR) $(LIB)
	$(CC) $(CFLAGS) -Isrc -Isrc/ec/compat -o $@ $< $(LIB) $(LDLIBS) -lm

# whether this build has the erasure codec (test-units-b checks that NO_EC=0 keeps it and NO_EC=1 drops it)
.PHONY: print-ec print-var
# the value of a variable (tests/check_ci_groups.sh): make -s print-var VAR=TEST_GROUPS
print-var:
	@echo '$($(VAR))'
print-ec:
	@echo $(if $(EC_OBJ),ec,noec)

# R7's interfaces (r7-iface): the names of .ec parts and settled sources, every near miss; TSDB_TEST_TIER's grammar and
# its refusal without TSDB_TEST_TIER_I_KNOW=1; the stubs leave a database as today's code does; eng_test_mark_ec
$(B)/test_ecnames: tests/test_ecnames.c src/internal.h $(R7_HDR) $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# settled answers equal unsettled answers bit for bit (R7, r7-oracle): two databases, one history, B settles some partitions;
# every query kind is compared. make oracle-soak: ORACLE_SEEDS seeds of ORACLE_STEPS steps from ORACLE_FIRST (a failure names its seed)
ORACLE_MUTANTS := skipdelete skipreplace skipretention nomark flipvalues flipagg flipgroup flipbounds flipspread fliptwa fliplttb fliplatest flipmulti flipstats noretry rankdown
ORACLE_SEEDS ?= 200
ORACLE_STEPS ?= 150
ORACLE_FIRST ?= 1000
$(B)/test_settle_oracle: tests/test_settle_oracle.c src/internal.h $(R7_HDR) $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS) -lm

.PHONY: oracle-soak
oracle-soak: $(B)/test_settle_oracle
	./$(B)/test_settle_oracle --seeds $(ORACLE_SEEDS) --steps $(ORACLE_STEPS) --first $(ORACLE_FIRST) $(SCRATCH)

# the swap of a settle (r7-swap): intent v4, the input check under db->lock against a drop, recovery at open; crash
# points and power loss (crashfs) at every step; readers across swaps; mutants must fail it
$(B)/test_ecswap: tests/test_ecswap.c src/internal.h $(R7_HDR) $(LIB) $(CRASHFS_SO)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# the settle (r7-settle, src/settle.c, src/ecpolicy.c): queries equal an oracle that never settled, damage aborts, the
# policy's ages and limits, close during an encode, 8 writers with merges and a drop, crash points and power loss
$(B)/test_settle: tests/test_settle.c src/internal.h $(R7_HDR) $(LIB) $(CRASHFS_SO)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# a leader's settle of a cluster ec volume (r8g-cut, src/eccold.c): the gate, the cut Z, F, Y, MK_COLD, a position
# DOWN without a fence, the TTL pass, a deposed leader, writes during a settle, the cluster policy; mutants must fail it
$(B)/test_eccold: tests/test_eccold.c src/internal.h src/eccold.h $(R7_HDR) $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_ecapply: tests/test_ecapply.c src/internal.h src/eccold.h src/ecapply.h src/ecdigest.h $(R7_HDR) $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# retired .ec parts and their objects (r7-gc, src/ecgc.c): holds across the deadline, the sweep at open, a store DOWN,
# power loss at every step of an open and a purge (crashfs); mutants gcunlinkfirst gcunrefleak gcnohold sweeplive
# sweepinflight
$(B)/test_ecgc: tests/test_ecgc.c src/internal.h $(R7_HDR) src/chunk_priv.h $(LIB) $(CRASHFS_SO)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# settled sources (r7-keep, src/eckeep.c): the keep from the record's settle time, the digest, DIVERGED; mutants
# digestskip digestraw digestnohold keepmtime noobjcmp openwait nolencheck orphannow nohaltstop must fail it
$(B)/test_keep: tests/test_keep.c src/internal.h $(R7_HDR) $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# the logical digest (r8j, src/ecdigest.c): one value for the same rows in any layout; mutants digestbytes
# digestnotomb digestnoretention must fail it
$(B)/test_ecdigest: tests/test_ecdigest.c src/internal.h src/ecdigest.h $(R7_HDR) $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# the seam of .ec parts (r7-seam, src/eccache.c): every query kind through it equals the maps, damaged cells, lost
# objects, stores DOWN, eviction under 32 threads, the fd budget; mutants seamnoverify seamnoretry cachekeyoff
$(B)/test_seam: tests/test_seam.c src/internal.h $(R7_HDR) $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# the .ec index file (r7-ecfile): its record codec, writer and open against the source part, damage to every region,
# the listing's refusals, WAL replay and fno_next over a settled partition; mutants must fail it
$(B)/test_ecpart: tests/test_ecpart.c src/internal.h $(R7_HDR) src/chunk.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# R8's marker bodies (COLD, GC, DIGEST: round trips, truncation, bounds), record_valid's cluster ec flag and the open's
# refusal of GC and DIGEST without requires=ec; mutants mkcoldbounds mkadmitall ecpposloose mkwriteany mkretired must fail it
$(B)/test_marker: tests/test_marker.c src/internal.h src/marker.h src/ecpart.h src/chunk.h src/tier.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# r7-skip: .ec and settling parts are merge barriers, never rewritten alone, by optimize or by the TTL work; drops
# retire them without waiting for a settle; .ec parts do not hold inserts (db_wait_merges). Marks by eng_test_mark_ec
$(B)/test_ecskip: tests/test_ecskip.c src/internal.h $(R7_HDR) $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# the tier of a handle (r7-tier): one backend for every volume, vuuid= and requires=tier in META (power loss at every
# step), stores DOWN at open and failed in service, the unsure EPOCH, the refusals; mutants must fail it
$(B)/test_tier: tests/test_tier.c src/internal.h src/ecgc.h src/tier.h src/chunk.h src/chunk_priv.h src/feed.h $(LIB) $(CRASHFS_SO)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# the chunk server through a fake link and two real peerlinks: puts, reads, fences, unsure stores, store binding, power
# loss in a put (crashfs); mutants must fail it
$(B)/test_chunksrv: tests/test_chunksrv.c server/chunksrv.h src/chunk.h src/chunk_priv.h $(B)/chunksrv.o $(B)/peerlink.o $(B)/tsdb_crypto.o $(LIB) $(CRASHFS_SO)
	$(CC) $(CFLAGS) -Isrc -Iserver -Iclient -o $@ $< $(B)/chunksrv.o $(B)/peerlink.o $(B)/tsdb_crypto.o $(LIB) $(LDLIBS)

# the cold ops of a cluster ec volume against six in-process chunk servers (a fake transport); mutants must fail it
$(B)/test_chunkpeer: tests/test_chunkpeer.c server/chunkpeer.h server/chunksrv.h src/coldops.h src/chunk.h $(B)/chunkpeer.o $(B)/chunksrv.o $(B)/peerlink.o $(B)/tsdb_crypto.o $(LIB)
	$(CC) $(CFLAGS) -Isrc -Iserver -Iclient -Iinclude -o $@ $< $(B)/chunkpeer.o $(B)/chunksrv.o $(B)/peerlink.o $(B)/tsdb_crypto.o $(LIB) $(LDLIBS)

# objects, chunk files, stores and the directory backend (R6): every geometry, every damage pattern, range reads at every
# boundary, epoch fences, power loss (crashfs) at every step of a chunk write and an EPOCH store; mutants must fail it
$(B)/test_chunk: tests/test_chunk.c src/internal.h src/chunk.h $(EC_HDR) $(LIB) $(CRASHFS_SO)
	$(CC) $(CFLAGS) -Isrc -Isrc/ec/compat -o $@ $< $(LIB) $(LDLIBS)

# the stores of a backend missing, damaged or replaced: open degraded, heal, replace, attach, power loss, threads
$(B)/test_chunk_heal: tests/test_chunk_heal.c src/internal.h src/chunk.h src/chunk_priv.h $(EC_HDR) $(LIB) $(CRASHFS_SO)
	$(CC) $(CFLAGS) -Isrc -Isrc/ec/compat -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_levels: tests/test_levels.c tools/datasets.h src/internal.h src/levels.h $(EC_HDR) $(LIB)
	$(CC) $(CFLAGS) -Isrc -Isrc/ec/compat -o $@ $< $(LIB) $(LDLIBS)

# links against libtsdb.so, as the embedded bindings do; run with LD_LIBRARY_PATH=$(B)
$(B)/test_compat: tests/test_compat.c src/compat.h tests/portable.h
	$(CC) $(CFLAGS) -o $@ $<

$(B)/test_shared: tests/test_shared.c $(B)/libtsdb.$(SOEXT)
	$(CC) $(CFLAGS) -o $@ $< -L$(B) -ltsdb -ldl $(if $(filter Darwin,$(UNAME_S)),-Wl$(comma)-rpath$(comma)@loader_path)

$(B)/test_lttb: tests/test_lttb.c tests/lttb_ref.h src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_group: tests/test_group.c $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_watchdog: tests/test_watchdog.c tests/watchdog.h | $(B)
	$(CC) $(CFLAGS) -o $@ $< $(LDLIBS)

# the test headers are prerequisites of every test that includes them (a changed watchdog or seed rebuilds them)
$(patsubst tests/%.c,$(B)/%,$(shell grep -l 'watchdog.h' tests/*.c)): tests/watchdog.h
$(patsubst tests/%.c,$(B)/%,$(shell grep -l 'seed.h' tests/*.c)): tests/seed.h

$(B)/test_twa: tests/test_twa.c $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_spread: tests/test_spread.c src/internal.h src/ext80.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_limits: tests/test_limits.c $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

# many small parts and every query kind against a model (desc_window_rows: internal)
$(B)/test_small_parts: tests/test_small_parts.c $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

# partition classes: bulk loads, idle-merge switches, overlap, WAL replay, CLASSES damage, retention (switch hook: internal)
$(B)/test_classes: tests/test_classes.c tools/datasets.h $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

# with ARCH: in `make dist` it carries the glibc-compat header, since the tarball's test_server preloads it
$(B)/libcrashfs.so: tests/jepsen/crashfs.c | $(B)
	$(CC) -O2 -g -std=gnu11 -Wall $(ARCH) -fPIC -shared -o $@ $< -ldl -lpthread

# the storage-outage shim (hang, error, slow, full and rollback faults per prefix; stacks with crashfs)
$(B)/liboutagefs.so: tests/jepsen/outagefs.c | $(B)
	$(CC) -O2 -g -std=gnu11 -Wall $(ARCH) -fPIC -shared -o $@ $< -ldl -lpthread

# a reader of a mapping that the shim's power loss cuts: the process must end with the loss's status (jepsen-selftest)
$(B)/test_crashfs_bus: tests/test_crashfs_bus.c | $(B)
	$(CC) $(CFLAGS) -o $@ $< -lpthread

# crashfs keeps the durable bytes of what a write-only descriptor rewrites or cuts (tests/test_crashfs_old.c)
$(B)/test_crashfs_old: tests/test_crashfs_old.c | $(B)
	$(CC) $(CFLAGS) -o $@ $<

$(B)/test_outagefs: tests/test_outagefs.c tests/outagefs_ctl.h | $(B)
	$(CC) $(CFLAGS) -o $@ $< -lpthread

$(B)/test_outage: tests/test_outage.c tests/harness.h tests/outagefs_ctl.h $(CLIB) $(LIB)
	$(CC) $(CFLAGS) -Iclient -o $@ $< $(CLIB) $(LIB) $(LDLIBS)

$(B)/test_unavailable: tests/test_unavailable.c tests/harness.h tests/outagefs_ctl.h tests/outage_status.h $(CLIB) $(LIB)
	$(CC) $(CFLAGS) -Iclient -o $@ $< $(CLIB) $(LIB) $(LDLIBS)

$(B)/outage_server: tests/outage_server.c tests/harness.h tests/outagefs_ctl.h tests/outage_status.h $(CLIB) $(LIB)
	$(CC) $(CFLAGS) -Iclient -o $@ $< $(CLIB) $(LIB) $(LDLIBS)

$(B)/test_health: tests/test_health.c tests/outagefs_ctl.h src/health_rules.h $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/jepsen: tests/jepsen/jepsen.c tests/jepsen/feedcheck.c tests/jepsen/outage_judge.h tests/harness.h src/internal.h src/feed.h src/health_rules.h $(CLIB) $(LIB)
	$(CC) $(CFLAGS) -Iclient -o $@ tests/jepsen/jepsen.c tests/jepsen/feedcheck.c $(CLIB) $(LIB) $(LDLIBS)

# Jepsen-style fault injection: crashes, simulated power loss, I/O errors and pauses while
# clients write and read; a checker verifies durability, atomicity and reads.
# One directory per tree: worktrees run make jepsen at the same time, and a shared directory turns the other run's
# activity into reported "lost writes". jepsen also refuses a directory another run holds.
JEPSEN_DIR ?= /tmp/tsdb-jepsen-$(shell printf %s "$(CURDIR)" | cksum | cut -d' ' -f1)
# Workloads: kv (keys every 2 s) and classes (rates that change, so series switch partition classes while
# large inserts, replaces and deletes race the switches; see tests/jepsen/jepsen.c).
JEPSEN_SEEDS ?= 1 2 3
JEPSEN_WORKLOADS ?= kv classes
# Storage outages (outage: one volume of two hung, failing or slow, or the server killed; no write lost or refused-yet-
# visible, the other volume keeps its pace) and misdirected I/O (with verify_checksums and without) and damage at rest
# run on the kv workload only: they test the
# engine's checks, which do not depend on the workload. JEPSEN_SECONDS, when set, is the length of every run (the
# weekly CI run sets it, with one power seed); otherwise each nemesis has its own short default.
JEPSEN_SECONDS ?=
# The outage nemesis's seeds: a seed fixes the fault schedule, so more seeds test more than repeats of one do (the weekly CI
# run sets six; a run with a flapping domain spends minutes waiting for its 60 s, and what it does in them is the seed's).
JEPSEN_OUTAGE_SEEDS ?= 1 2
# The directory tier (R7; tests/jepsen/jepsen.c, --tier N: N stores, 3 = 2+1, 6 = 4+2): kill, power, eio and
# settle-steps (a power cut at each step of a settle) on both workloads and both backends, under the eager policy, and
# the run fails when nothing settled. Empty until the tree has settles (src/ names TSDB_SETTLE_MUTANT, r7-settle's
# mutant): before that every tier run would fail its settle count, as it is meant to. JEPSEN_TIER= turns it off.
JEPSEN_TIER ?= $(if $(shell grep -l TSDB_SETTLE_MUTANT src/*.c 2>/dev/null),3 6,)
JEPSEN_TIER_SEEDS ?= 1 2
# Then kv once more with login (--auth: USERS made by $(B)/tsdb, every client logs in at every reconnect) under kill
# and one power seed, per backend.
jsec = $(if $(JEPSEN_SECONDS),$(JEPSEN_SECONDS),$(1))
# A run that jepsen ends INCONCLUSIVE (exit 3: its own liveness or coverage check failed, which says nothing about tsdb) is run again by
# tests/jepsen/rerun.sh with the next seed, twice at most; three in a row fail the gate with all three messages; a violation fails at once.
# The reruns are printed as they happen and counted in the summary line at the end (never silent).
JR = JEPSEN_RERUN_LOG=$(JEPSEN_DIR).reruns tests/jepsen/rerun.sh
# `make jepsen` is the selftest and then the runs (jepsen-run: CI runs the two apart, and the event loops of IO apart: make jepsen-run IO=uring)
jepsen: $(B)/jepsen $(B)/tsdbd $(B)/tsdb $(B)/libcrashfs.so $(B)/liboutagefs.so jepsen-selftest
	@$(MAKE) jepsen-run
jepsen-run: $(B)/jepsen $(B)/tsdbd $(B)/tsdb $(B)/libcrashfs.so $(B)/liboutagefs.so
	rm -f $(JEPSEN_DIR).reruns; trap 'tests/jepsen/rerun.sh --summary $(JEPSEN_DIR).reruns; rm -f $(JEPSEN_DIR).reruns' EXIT; \
	for io in $(IO); do for wl in $(JEPSEN_WORKLOADS); do echo "== tsdbd --io $$io, workload $$wl"; \
	    TSDBD_IO=$$io $(JR) ./$(B)/jepsen --dir $(JEPSEN_DIR) --workload $$wl --nemesis kill  --seconds $(call jsec,20) && \
	    for seed in $(JEPSEN_SEEDS); do \
	        TSDBD_IO=$$io $(JR) ./$(B)/jepsen --dir $(JEPSEN_DIR) --workload $$wl --nemesis power --seconds $(call jsec,20) \
	            --seed $$seed --crashfs $(B)/libcrashfs.so || exit 1; done && \
	    TSDBD_IO=$$io $(JR) ./$(B)/jepsen --dir $(JEPSEN_DIR) --workload $$wl --nemesis eio   --seconds $(call jsec,20) \
	        --crashfs $(B)/libcrashfs.so && \
	    TSDBD_IO=$$io $(JR) ./$(B)/jepsen --dir $(JEPSEN_DIR) --workload $$wl --nemesis pause --seconds $(call jsec,20) && \
	    for n in $(JEPSEN_TIER); do \
	        echo "== tsdbd --io $$io, workload $$wl, a tier of $$n stores" && \
	        TSDBD_IO=$$io $(JR) ./$(B)/jepsen --dir $(JEPSEN_DIR) --workload $$wl --tier $$n --nemesis kill \
	            --seconds $(call jsec,20) && \
	        for seed in $(JEPSEN_TIER_SEEDS); do \
	            TSDBD_IO=$$io $(JR) ./$(B)/jepsen --dir $(JEPSEN_DIR) --workload $$wl --tier $$n --nemesis power \
	                --seconds $(call jsec,20) --seed $$seed --crashfs $(B)/libcrashfs.so && \
	            TSDBD_IO=$$io $(JR) ./$(B)/jepsen --dir $(JEPSEN_DIR) --workload $$wl --tier $$n --nemesis settle-steps \
	                --seconds $(call jsec,30) --seed $$seed --crashfs $(B)/libcrashfs.so || exit 1; done && \
	        TSDBD_IO=$$io $(JR) ./$(B)/jepsen --dir $(JEPSEN_DIR) --workload $$wl --tier $$n --nemesis eio \
	            --seconds $(call jsec,20) --crashfs $(B)/libcrashfs.so || exit 1; done && \
	    if [ $$wl = kv ]; then \
	        for seed in $(JEPSEN_OUTAGE_SEEDS); do \
	            TSDBD_IO=$$io $(JR) ./$(B)/jepsen --dir $(JEPSEN_DIR) --nemesis outage --seconds $(call jsec,40) \
	                --seed $$seed --outagefs $(B)/liboutagefs.so || exit 1; done && \
	        TSDBD_IO=$$io $(JR) ./$(B)/jepsen --dir $(JEPSEN_DIR) --nemesis misdirect --seconds $(call jsec,20) \
	            --crashfs $(B)/libcrashfs.so --verify-checksums && \
	        TSDBD_IO=$$io $(JR) ./$(B)/jepsen --dir $(JEPSEN_DIR) --nemesis misdirect --seconds $(call jsec,20) \
	            --crashfs $(B)/libcrashfs.so && \
	        TSDBD_IO=$$io $(JR) ./$(B)/jepsen --dir $(JEPSEN_DIR) --nemesis rot --seconds $(call jsec,20) --verify-checksums; \
	    fi || exit 1; done; \
	    echo "== tsdbd --io $$io, workload kv with login" && \
	    TSDBD_IO=$$io JEPSEN_TSDB=./$(B)/tsdb $(JR) ./$(B)/jepsen --dir $(JEPSEN_DIR) --auth --nemesis kill \
	        --seconds $(call jsec,20) && \
	    TSDBD_IO=$$io JEPSEN_TSDB=./$(B)/tsdb $(JR) ./$(B)/jepsen --dir $(JEPSEN_DIR) --auth --nemesis power \
	        --seconds $(call jsec,20) --seed 1 --crashfs $(B)/libcrashfs.so && \
	    echo "== tsdbd --io $$io, workload kv with the change feed on" && \
	    TSDBD_IO=$$io JEPSEN_TSDB=./$(B)/tsdb $(JR) ./$(B)/jepsen --dir $(JEPSEN_DIR) --feed --nemesis kill \
	        --seconds $(call jsec,20) && \
	    for seed in $(JEPSEN_SEEDS); do \
	        TSDBD_IO=$$io JEPSEN_TSDB=./$(B)/tsdb $(JR) ./$(B)/jepsen --dir $(JEPSEN_DIR) --feed --nemesis power \
	            --seconds $(call jsec,20) --seed $$seed --crashfs $(B)/libcrashfs.so || exit 1; done && \
	    TSDBD_IO=$$io JEPSEN_TSDB=./$(B)/tsdb $(JR) ./$(B)/jepsen --dir $(JEPSEN_DIR) --feed --nemesis eio \
	        --seconds $(call jsec,20) --crashfs $(B)/libcrashfs.so || exit 1; done

# The harness has to notice what it is meant to: a server that crashes (SIGSEGV, not crashfs's exit 99) fails the
# power run, and so does a server that hangs (SIGSTOP: a pulled plug never ends it; it is reported, not counted as a
# pull), and a power run whose nemesis is idle: it does nothing, kills once and then sleeps, naps 9 s at a time, never
# pulls the plug (never, oncereact), sleeps through its naps (--mutant-idle, --mutant-longnap) or sleeps inside its fault
# step, with the server dead or with the new one up (deadsleep:MS, busysleep:MS; the idle one has a server die at the start of
# each long nap, as the shim's own odds do in a real run: left to those odds the mutant slipped past in 1 of 20 runs under 16
# busy loops). Each must fail (a lazy one as the gate failure of three INCONCLUSIVE attempts, a crash or hang as a violation at once). Also an eio nemesis that works once and sleeps, a burst (burst:10: faults enough for the floor, also under load, but none in the last third) and then asleep, naps that do
# not look at the server (nap:600: a server that died lies dead; one dies at the start of each nap, since left to the shim's own odds the case
# slipped past in 1 of 10 runs under 16 busy loops), and a shim that ignores SIGUSR2 (CRASHFS_IGNORE_USR2:
# the plugs are not counted), a nemesis that is asleep in the middle of the run with every third and the floor still met
# (hole:3:10 of 20 s, and hole:2:6 of 10 s for the pause nemesis, whose bound is tighter), an eio nemesis that never asks
# for the fault (eionosig: the shim's own odds are not its faults) and a pause nemesis that never continues the server
# (pausenocont: a stopped server is not a listening one, and its stop has a bound). The nemesis counts the faults it issues
# and a watcher thread measures the time a server lay dead, lay stopped and was listening with no fault issued or in
# progress (jepsen.c, nemesis_check). A tsdbd with a stale entry in its latest table (three mutants, built from a copy of the tree) fails the
# kv run's latest check.
# The nemesis check counts confirmed faults only (a kill: the server died of it and a new one listens; a pause: the watcher saw the
# server stopped; a plug: the shim logged it; an EIO: the shim's mark): killnoeffect and pausenoeffect signal nothing and are
# counted as nothing. Its floor is half the least honest rate per second of listening time with no fault being carried out (slow:MS,
# a longer nap, works at half or two thirds of it), its gap bound a tenth of the run (2 to 5 s, the stretch to the end of the run
# included: hole:15:100 stops at three quarters of a run of 20 s, hole:6:100 at six tenths of one of 10 s), and the dead bound
# catches a nemesis that does not look at the server when the shim, with high odds, is what kills it (blind:1000, no forced death).
# A step of the kill, power or eio nemesis that dawdles with the server live before its signal (killslowstep, powerslowstep,
# eioslowstep: 400 ms each) is idle time of the step (an honest step has microseconds of it, 0.00 s in every run measured), whatever
# the rate it leaves: 5% of the run is the bound. A pause run is 20 s (hole:16:100, four fifths of it, fails; of
# 10 s it did not), and a plug pulled when the clients are gone (lastpull) is an honest run: the harness gives the server a row to
# fail on (poke_server) and the case must pass. A failed liveness check of a run is INCONCLUSIVE (exit 3), which tests/jepsen/rerun.sh
# repeats with the next seed, twice at most; a lazy nemesis fails all three attempts (the gate failure naming its check, which
# tests/jepsen/expect.sh gate requires: a non-zero exit is not enough), and so does one that SPINS where an honest one sleeps (killspin,
# powerspinstep, eiospin, pullspin, waitspin, deadspin, norestartspin: no bound takes off the time a thread waits for the CPU, so a
# machine that starves the thread cannot hide a spinner). A hung server, a crash or a wrong answer is a VIOLATION (exit 1), at the
# first attempt (expect.sh violation), also beside a lazy nemesis: the history is
# always judged (mutant-loss.sh plants a loss in a copy of tsdbd and runs it with a spinning nemesis; before round 10 that ended rc 3,
# three times, with the history unjudged).
# A tsdbd that dies by itself or stops answering is a violation under every nemesis (mutant-crash.sh: JV_CRASH aborts on every Nth
# insert, JV_HANG blocks a worker for ever, each under kill, pause, none, eio, power, the hang also under outage, exit 1 at the first
# attempt, no tsdbd left behind), a tsdbd that cannot start for want of memory is a harness error (exit 2) and one whose open
# finds a damaged part a violation (failstart-tsdbd.sh), and the outage heal that takes long is INCONCLUSIVE unless the server reports the
# volume DIVERGED (mutant-slowprobe.sh). Round 12 adds to mutant-crash.sh: one abort per gate with a core dump (JV_CRASHONCE, JV_CRASH_MB: the
# core handler of the machine keeps the process from being a zombie when the kill nemesis' SIGKILL arrives; kv and classes), JV_EXIT under
# misdirect and rot (exit 0, and exit 1 with no damage in the log: no fail-stop on damage), JV_HANG under power and eio at the shim's default odds
# (the quiet tail's server is a clean one), and JV_DSTALL (a worker in D state, the kernel's stall: INCONCLUSIVE kernel-stall, never a violation).
# The limit that cannot be removed (a hang or a self-stop that a planned fault ends before the deadline looks like slowness) is in the header of
# jepsen.c. tests/test_crashfs_old.c (make test) holds the shim to what a disk may do: a rewritten or cut sector comes back as it was synced.
# What passes on purpose is listed at the end of the comment of nemesis_check.
# What each bound is set to, and the honest worst under load on tmpfs and disk, is in the comments of nemesis_check (jepsen.c).
# The selftest in five parts that CI runs on runners of their own (it took 97 minutes in one piece on 4 CPUs): the lines are those
# that were one recipe, cut between two statements; `make jepsen-selftest` runs them in this order. Each needs $(B)/tsdb (the CLI
# of mutant-crash.sh's login and feed cases): `make jepsen` had built it first, a part alone did not (round 3: the --auth and --feed cases failed).
JEPSEN_SELFTEST_PARTS := jepsen-selftest-1 jepsen-selftest-2 jepsen-selftest-3 jepsen-selftest-4 jepsen-selftest-5
.PHONY: $(JEPSEN_SELFTEST_PARTS) jepsen-run
jepsen-selftest: $(B)/jepsen $(B)/tsdbd $(B)/tsdb $(B)/libcrashfs.so $(B)/liboutagefs.so $(B)/test_crashfs_bus
	@$(MAKE) -j1 $(JEPSEN_SELFTEST_PARTS) && echo "jepsen-selftest: ok"
$(JEPSEN_SELFTEST_PARTS): $(B)/jepsen $(B)/tsdbd $(B)/tsdb $(B)/libcrashfs.so $(B)/liboutagefs.so $(B)/test_crashfs_bus
jepsen-selftest-1:
	@d=$$(mktemp -d $(JEPSEN_DIR)-bus.XXXXXX) || exit 1; \
	    LD_PRELOAD=$(CURDIR)/$(B)/libcrashfs.so CRASHFS_ROOT=$$d CRASHFS_PROB=0 ./$(B)/test_crashfs_bus $$d; rc=$$?; rm -r $$d; \
	    if [ $$rc -ne 99 ]; then \
	        echo "jepsen-selftest: a thread that read a mapping the shim's power loss cut ended the process with status $$rc, not the loss's 99"; exit 1; fi
	@out=$$(JEPSEN_TSDBD=tests/jepsen/segv-tsdbd.sh timeout 120 ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --nemesis power \
	    --seconds 5 --crashfs $(B)/libcrashfs.so 2>&1); rc=$$?; \
	    if [ $$rc -eq 0 ] || [ $$rc -eq 124 ] || \
	       ! echo "$$out" | grep -q 'ended with status 0x8\?b (signal 11\|died of signal 11 before it listened'; then \
	        echo "jepsen-selftest: a SIGSEGV in the server went unnoticed (rc $$rc, 124 is the timeout)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$(JEPSEN_TSDBD=tests/jepsen/segv-tsdbd.sh JEPSEN_SETUP_PAUSE_MS=1500 timeout 120 ./$(B)/jepsen \
	    --dir $(JEPSEN_DIR)-selftest --nemesis power --seconds 5 --crashfs $(B)/libcrashfs.so 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || \
	       ! echo "$$out" | grep -q 'ended with status 0x8\?b (signal 11\|died of signal 11 before it listened'; then \
	        echo "jepsen-selftest: a server that died before the setup connected hung or went unnoticed (rc $$rc, 124 is the timeout)"; \
	        echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$(JEPSEN_SETUP_PAUSE_MS=100000 JEPSEN_WATCHDOG_S=5 timeout 120 ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest \
	    --nemesis power --seconds 5 --crashfs $(B)/libcrashfs.so 2>&1); rc=$$?; \
	    if [ $$rc -ne 124 ] || ! echo "$$out" | grep -q 'jepsen --nemesis power: watchdog: still running after' || \
	       ! echo "$$out" | grep -q 'jepsen at the hang: phase setup: creating the series'; then \
	        echo "jepsen-selftest: a stalled run was not ended by the watchdog (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	tests/jepsen/expect.sh valid 'servers that start slowly made an honest nemesis look idle' \
	    env JEPSEN_TSDBD=tests/jepsen/slow-tsdbd.sh ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --nemesis power \
	    --seconds 20 --crashfs $(B)/libcrashfs.so || exit 1; \
	for spec in 'nothing|20' 'once|20' 'nap:9000|20' 'never|20' 'oncereact|20' 'deadsleep:1500|20' 'busysleep:1500|20' \
	    'idle|20' 'longnap|30'; do \
	    m=$${spec%%|*}; secs=$${spec#*|}; \
	    case $$m in idle) flag=--mutant-idle;; longnap) flag=--mutant-longnap;; *) flag="--mutant-nemesis $$m";; esac; \
	    tests/jepsen/expect.sh gate 'was idle' "a nemesis that is '$$m'" env JEPSEN_PULL_PROB=0.00003 ./$(B)/jepsen \
	        --dir $(JEPSEN_DIR)-selftest --nemesis power --seconds $$secs --crashfs $(B)/libcrashfs.so $$flag || exit 1; \
	done; \
	tests/jepsen/expect.sh gate 'was idle' 'an eio nemesis that works once and sleeps' ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest \
	    --nemesis eio --seconds 20 --crashfs $(B)/libcrashfs.so --mutant-nemesis once || exit 1; \
	tests/jepsen/expect.sh gate 'were not spread over it\|the nemesis issued [0-9]* faults in 20 s.*it was idle' \
	    'a nemesis that works in a burst and sleeps' ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --nemesis kill --seconds 20 \
	    --mutant-nemesis burst:12 || exit 1; \
	tests/jepsen/expect.sh gate 'listening for [0-9.]* s with no fault issued or in progress' 'a nemesis that sleeps through the middle of the run' \
	    ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --nemesis kill --seconds 20 --mutant-nemesis hole:3:10 || exit 1; \
	tests/jepsen/expect.sh gate 'listening for [0-9.]* s with no fault issued or in progress' 'a pause nemesis that sleeps through a stretch of the run' \
	    ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --nemesis pause --seconds 10 --mutant-nemesis hole:2:6 || exit 1; \
	tests/jepsen/expect.sh gate 'the nemesis issued [0-9]* faults in 20 s.*it was idle' "an eio nemesis that never asks for the fault (credited with the shim's own)" \
	    ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --nemesis eio --seconds 20 --crashfs $(B)/libcrashfs.so --mutant-nemesis eionosig || exit 1; \
	tests/jepsen/expect.sh gate 'a server lay stopped (SIGSTOP) for' 'a pause nemesis that never continues the server (or it hung the run: 124 is the timeout)' \
	    timeout 120 ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --nemesis pause --seconds 10 --mutant-nemesis pausenocont || exit 1; \
	tests/jepsen/expect.sh gate 'a server lay dead for' 'a nemesis that does not look at the server (it left it dead)' \
	    ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --nemesis power --seconds 20 --crashfs $(B)/libcrashfs.so --mutant-nemesis nap:600 || exit 1; \
	tests/jepsen/expect.sh gate 'classes: 0 switches, parts in 0 classes: the workload tested no change of class' \
	    'a classes workload whose clients are starved (they write nothing in the run: INCONCLUSIVE, not a violation: it was FAILED before)' \
	    env JEPSEN_CLIENT_NAP_MS=9000 ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --workload classes --nemesis none --seconds 8 --clients 1 || exit 1; \
	tests/jepsen/expect.sh gate 'the clients made [0-9]* acknowledged writes in 12 s (at least 100 expected)' \
	    'a kv workload whose clients are starved (8 acknowledged writes in a run: a tsdbd that drops every 20th would pass it)' \
	    env JEPSEN_CLIENT_NAP_MS=3000 ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --nemesis none --seconds 12 --clients 2 || exit 1; \
	tests/jepsen/expect.sh gate 'ran out of room for its history' \
	    'clients that fill their history before the run ends (they stopped quietly, and the rest of the run tested nothing: round 14)' \
	    env JEPSEN_MAX_OPS=1001 JEPSEN_OPS_PER_S=0 ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --nemesis none --seconds 20 || exit 1; \
	out=$$(JEPSEN_OPS_PER_S=400 ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --nemesis none --seconds 10 2>&1); rc=$$?; \
	    n=$$(echo "$$out" | sed -n 's/^jepsen none: .*; writes \([0-9]*\) ok .*reads \([0-9]*\) ok;.*/\1 \2/p' | awk '{print $$1+$$2}'); \
	    if [ $$rc -ne 0 ] || [ -z "$$n" ] || [ "$$n" -gt 6000 ] || [ "$$n" -lt 400 ]; then \
	        echo "jepsen-selftest: JEPSEN_OPS_PER_S=400 did not pace the clients ($$n writes and reads in 10 s, wanted 400 to 6000; rc $$rc)"; \
	        echo "$$out" | tail -n 12; exit 1; fi;
jepsen-selftest-2:
	@for spec in \
	    'kill@20@killnoeffect@-@the nemesis issued 0 faults in 20 s.*it was idle@a kill that kills nothing was counted as a fault' \
	    'pause@10@pausenoeffect@-@the nemesis issued 0 faults in 10 s.*it was idle@a pause that stops nothing was counted as a fault' \
	    'kill@20@slow:1500@-@the nemesis issued [0-9]* faults in 20 s.*it was idle@a kill nemesis at half the honest rate slipped past the floor' \
	    'pause@10@slow:1200@-@the nemesis issued [0-9]* faults in 10 s.*it was idle@a pause nemesis at two thirds of the honest rate slipped past the floor' \
	    'kill@20@killslowstep:1000@-@the nemesis issued [0-9]* faults in 20 s.*it was idle@a kill whose step dawdles with the server live (a third of the honest rate, in time that is no free time) slipped past the floor' \
	    'kill@20@killslowstep:400@-@the nemesis was idle in it@a kill whose step dawdles 400 ms with the server live (its time before the signal is idle time, which an honest step has none of)' \
	    'power@20@powerslowstep:400@-@the nemesis was idle in it@a power step that dawdles 400 ms with the server live and the fault in progress' \
	    'eio@20@eioslowstep:400@-@the nemesis was idle in it@an eio step that dawdles 400 ms with the server live and the fault in progress' \
	    'kill@20@killspin:400@-@the nemesis was idle in it@a kill step that SPINS 400 ms with the server live (no account of CPU waited for is taken off)' \
	    'power@20@powerspinstep:1000@-@the nemesis was idle in it@a power step that spins 1 s with the server live and the fault in progress' \
	    'eio@20@eiospin:700@-@the nemesis was idle in it@an eio step that spins 700 ms with the server live and the fault in progress' \
	    'power@20@pullspin:300@-@a server lay dead for\|the nemesis was idle in it@a power nemesis that spins 300 ms after the plug before it looks for the death' \
	    'power@20@waitspin:300@-@a server lay dead for@a power nemesis that spins 300 ms in its wait for the death (counted as the wait: only the dead stretch sees it)' \
	    'power@20@deadspin:800@-@a server lay dead for@a power nemesis that spins 800 ms with the server dead before the restart' \
	    'power@20@norestartspin:3@-@a server lay dead for@a power nemesis that spins, never restarting the server after its third pull' \
	    'power@20@norestart:3@-@a server lay dead for@a power nemesis that sleeps, never restarting the server after its third pull' \
	    'kill@20@hole:15:100@-@listening for [0-9.]* s with no fault issued or in progress@a kill nemesis that stops at three quarters of the run' \
	    'pause@10@hole:6:100@-@listening for [0-9.]* s with no fault issued or in progress@a pause nemesis that stops at seven tenths of the run' \
	    'pause@20@hole:16:100@-@listening for [0-9.]* s with no fault issued or in progress@a pause nemesis that stops at four fifths of the run (the length of the pause runs of make jepsen; of 10 s it was not caught)' \
	    'power@20@hole:15:100@-@was idle@a power nemesis that stops at three quarters of the run' \
	    'eio@20@hole:15:100@-@was idle@an eio nemesis that stops at three quarters of the run' \
	    'power@20@blind:1000@JEPSEN_PULL_PROB=0.003,JEPSEN_CRASH_PROB=0.003@a server lay dead for@a blind power nemesis (no server made to die in its naps: the shim kills them)' \
	    'eio@20@blind:1000@JEPSEN_PULL_PROB=0.003,JEPSEN_CRASH_PROB=0.003@a server lay dead for@a blind eio nemesis (no server made to die in its naps: the shim fails them)'; do \
	    oldifs=$$IFS; IFS=@; set -- $$spec; IFS=$$oldifs; nem=$$1; secs=$$2; mut=$$3; envs=$$(echo "$$4" | tr , ' '); pat=$$5; what=$$6; \
	    case $$envs in -) envs=X_NONE=1;; esac; \
	    shim=; case $$nem in power|eio) shim="--crashfs $(B)/libcrashfs.so";; esac; \
	    tests/jepsen/expect.sh gate "$$pat" "$$what" env $$envs ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --nemesis $$nem --seconds $$secs \
	        $$shim --mutant-nemesis $$mut || exit 1; \
	done;
jepsen-selftest-3:
	@tests/jepsen/expect.sh gate 'that the shim did not log are not faults' 'plugs that the shim made and did not log (counted as pulled)' \
	    env CRASHFS_IGNORE_USR2=2 ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --nemesis power --seconds 20 --crashfs $(B)/libcrashfs.so || exit 1; \
	tests/jepsen/expect.sh gate 'was idle' 'plugs that the shim ignored (counted as pulled)' \
	    env CRASHFS_IGNORE_USR2=1 JEPSEN_PULL_PROB=0.0003 ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --nemesis power --seconds 20 \
	    --crashfs $(B)/libcrashfs.so || exit 1; \
	tests/jepsen/expect.sh gate 'was idle' 'classes: plugs that the shim ignored (the poke rows of the harness are no rows of the workload: round 13)' \
	    env CRASHFS_IGNORE_USR2=1 JEPSEN_PULL_PROB=0.0003 ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --workload classes --nemesis power \
	    --seconds 20 --crashfs $(B)/libcrashfs.so || exit 1; \
	tests/jepsen/expect.sh violation 'in two partitions' 'a row of the classes workload in two partitions (planted in the count)' \
	    env JEPSEN_TEST_PLANT_ROW=1 ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --workload classes --nemesis none --seconds 8 || exit 1; \
	tests/jepsen/expect.sh valid 'a plug pulled when the clients are gone (no traffic for the power loss to wait on; called a hung server)' \
	    ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --nemesis power --seconds 20 --crashfs $(B)/libcrashfs.so --mutant-nemesis lastpull || exit 1; \
	tests/jepsen/expect.sh valid 'a shim slow to write its mark (an empty name read as a file the server must stop on) made an honest eio run fail' \
	    env CRASHFS_EIO_ONLY=tmp_ CRASHFS_MARK_SLOW_MS=300 ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --nemesis eio --seconds 20 \
	    --crashfs $(B)/libcrashfs.so || exit 1; \
	tests/jepsen/expect.sh violation 'it is hung' 'a server that hangs after a power pull (it went unnoticed or was counted as a pull)' \
	    env JEPSEN_TSDBD=tests/jepsen/hang-tsdbd.sh timeout 300 ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --nemesis power \
	    --seconds 20 --crashfs $(B)/libcrashfs.so || exit 1; \
	out=$$(JEPSEN_TSDBD=tests/jepsen/segv-tsdbd.sh ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --nemesis outage \
	    --seconds 8 --outagefs $(B)/liboutagefs.so 2>&1); rc=$$?; \
	    if [ $$rc -eq 0 ] || ! find $(JEPSEN_DIR)-selftest.outage-s1.server.log -size +0 -mmin -2 2>/dev/null | grep -q .; then \
	        echo "jepsen-selftest: a failed outage run did not keep its server log (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$(JEPSEN_TSDBD=tests/jepsen/dead-tsdbd.sh ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --nemesis outage \
	    --seconds 8 --outagefs $(B)/liboutagefs.so 2>&1); rc=$$?; \
	    if [ $$rc -ne 2 ] || ! find $(JEPSEN_DIR)-selftest.outage-s1.server.log -size +0 -mmin -2 2>/dev/null | grep -q .; then \
	        echo "jepsen-selftest: an outage run that ended at a server that never started did not keep its log (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$(JEPSEN_TSDBD=tests/jepsen/early-segv-tsdbd.sh timeout 120 ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest \
	    --nemesis kill --seconds 5 --mutant-slowcreate 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'cannot connect to create the series'; then \
	        echo "jepsen-selftest: a run whose server crashed before the series were created hung or went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$(tests/jepsen/rerun.sh ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --nemesis outage --seconds 20 --seed 2 \
	    --outagefs $(B)/liboutagefs.so 2>&1); rc=$$?; \
	    if [ $$rc -ne 0 ] || ! echo "$$out" | grep -q '^outage heals: [1-9][0-9]*, [0-9]* judged .*, 0 not judged$$'; then \
	        echo "jepsen-selftest: an outage run left a heal unjudged, or did not say how many (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	printf '%s\n' 'tsdb: [100.500] storage domain /d/vola: a write probe took 900 ms (more than 400 ms): x' \
	    'tsdb: [140.250] storage domain /d/volb: a write probe took 3000 ms (more than 400 ms): x' \
	    'tsdb: [150.750] storage domain /d/vola: a write probe took 700 ms (more than 400 ms): x' \
	    'tsdbd: [160.000] storage domain /d/vola: 2 -> 3' > $(JEPSEN_DIR)-selftest.scan.log; \
	    a=$$(./$(B)/jepsen --scan-slow-probes $(JEPSEN_DIR)-selftest.scan.log,/d/vola,120 2>&1); \
	    b=$$(./$(B)/jepsen --scan-slow-probes $(JEPSEN_DIR)-selftest.scan.log,/d/vola,151 2>&1); \
	    c=$$(./$(B)/jepsen --scan-slow-probes $(JEPSEN_DIR)-selftest.scan.log,/d/volb,100 2>&1); \
	    if [ "$$a" != 150.750 ] || [ "$$b" != 0.000 ] || [ "$$c" != 140.250 ]; then \
	        echo "jepsen-selftest: the scan of slow write probes in a server log is wrong ($$a $$b $$c)"; exit 1; fi;
jepsen-selftest-4:
	@    tests/jepsen/mutant-latest.sh $(JEPSEN_DIR)-selftest || { echo "jepsen-selftest: a stale latest table went unnoticed"; exit 1; }; \
	    tests/jepsen/mutant-loss.sh $(JEPSEN_DIR)-selftest || { echo "jepsen-selftest: data loss beside a lazy nemesis was hidden behind INCONCLUSIVE or went unnoticed"; exit 1; }; \
	    tests/jepsen/mutant-crash.sh $(JEPSEN_DIR)-selftest || { echo "jepsen-selftest: a server that dies by itself or stops answering was hidden behind INCONCLUSIVE, a rerun or a pass, or hung the harness"; exit 1; };
jepsen-selftest-5:
	@for spec in 'kill@3@--nemesis kill --seconds 5' 'power@4@--nemesis power --seconds 8 --crashfs $(B)/libcrashfs.so' \
	    'misdirect@3@--nemesis misdirect --seconds 8 --crashfs $(B)/libcrashfs.so' 'rot@3@--nemesis rot --seconds 8'; do \
	    oldifs=$$IFS; IFS=@; set -- $$spec; IFS=$$oldifs; nem=$$1; at=$$2; args=$$3; \
	    for dly in 0 400; do \
	    rm -f $(JEPSEN_DIR)-selftest.failstart-count; \
	    out=$$(FAILSTART_AT=$$at FAILSTART=env JEPSEN_START_AWAIT_DELAY_MS=$$dly JEPSEN_TSDBD=tests/jepsen/failstart-tsdbd.sh ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest $$args 2>&1); rc=$$?; \
	    if [ $$rc -ne 2 ] || ! echo "$$out" | grep -q 'the server does not come up (the environment, not a verdict about tsdb)' || echo "$$out" | grep -q 'FAILED'; then \
	        echo "jepsen-selftest: a server that fails to start for want of memory under the $$nem nemesis was a verdict, not a harness error (rc $$rc, wanted 2; the watcher saw it first: $$dly ms)"; echo "$$out" | tail -n 12; exit 1; fi; \
	    done; \
	    rm -f $(JEPSEN_DIR)-selftest.failstart-count; \
	    if [ $$nem = kill ] || [ $$nem = power ]; then tests/jepsen/expect.sh violation 'refuses to open the database after the faults: damaged data' \
	        "a part file damaged before a restart under the $$nem nemesis (the open fails with corrupt data)" \
	        env FAILSTART_AT=$$at FAILSTART=damage JEPSEN_TSDBD=tests/jepsen/failstart-tsdbd.sh ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest $$args || exit 1; fi; \
	    rm -f $(JEPSEN_DIR)-selftest.failstart-count; \
	done; \
	    d=$(JEPSEN_DIR)-selftest.tier; rm -rf $$d; mkdir -p $$d/tier; \
	    (cd $$d && touch p0_1_1_0.part p0_2_3_1.ec p1_1_1_0.ec settled_p0_1_1_0.part retired_p0_1_1_0.part \
	        retired_p2_1_1_0.ec tmp_1_1.part tier/p9_1_1_0.ec); \
	    printf '%s\n' 'tsdb: x' 'tsdb: settled p0 (3 granules)' 'tsdb: settled p1' 'tsdb: unsettled' > $$d.log; \
	    a=$$(./$(B)/jepsen --scan-tier $$d,$$d.log,'settled ' 2>&1); \
	    if [ "$$a" != "2 1 1 2 2 3" ]; then echo "jepsen-selftest: the count of .ec, settled_ and retired_ files and settle lines is wrong ($$a)"; exit 1; fi; \
	    out=$$(JEPSEN_TSDBD=tests/jepsen/nosettle-tsdbd.sh ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --nemesis kill \
	        --seconds 8 --tier 3 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'tier: 0 settle lines'; then \
	        echo "jepsen-selftest: a tier run in which nothing settled went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	    $(CC) -shared -fPIC -o $$d/fakestep.so tests/jepsen/fakestep.c -lpthread || exit 1; \
	    out=$$(FAKE_SO=$$(realpath $$d)/fakestep.so FAKE_EXIT=_exit JEPSEN_TSDBD=tests/jepsen/fakestep-tsdbd.sh JEPSEN_MIN_SETTLES=1 \
	        ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --nemesis settle-steps --crashfs $(B)/libcrashfs.so --seconds 25 --tier 3 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'crashfs did not roll back' || ! echo "$$out" | grep -q 'no .ec, settled_'; then \
	        echo "jepsen-selftest: a settle step that ends with _exit (no power cut) and a tier with settle lines but no file went unnoticed (rc $$rc)"; \
	        echo "$$out" | tail -n 20; exit 1; fi; \
	    out=$$(FAKE_SO=$$(realpath $$d)/fakestep.so FAKE_EXIT=exit FAKE_FILES=1 JEPSEN_TSDBD=tests/jepsen/fakestep-tsdbd.sh JEPSEN_MIN_SETTLES=1 \
	        ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --nemesis settle-steps --crashfs $(B)/libcrashfs.so --seconds 25 --tier 3 2>&1); rc=$$?; \
	    if [ $$rc -ne 0 ] || ! echo "$$out" | grep -Eq 'ended there:.* [1-9][0-9]*/[1-9]'; then \
	        echo "jepsen-selftest: the settle-steps nemesis fails a server that ends at its step with exit(99) (rc $$rc)"; \
	        echo "$$out" | tail -n 20; exit 1; fi; \
	    JEPSEN_REQUIRE_TIER=$(if $(strip $(JEPSEN_TIER)),1,0) tests/jepsen/mutant-settle.sh $(JEPSEN_DIR)-selftest || { echo "jepsen-selftest: a settle bug went unnoticed"; exit 1; }; \
	    tests/jepsen/mutant-slowprobe.sh $(JEPSEN_DIR)-selftest || { echo "jepsen-selftest: a domain that reports every probe slow went unnoticed"; exit 1; };

# jepsen-cluster (tests/jepsen/jepsen_cluster.c, docs/design-cluster.md 13.2): 3-8 tsdbd with a proxy of the harness in
# front of every peer port, nemeses kill, partition, pause, midcommit (crash points, server/crashpt.h) and power
# (crashfs), workloads meta and kv-local, a checker. One run per nemesis and backend; JEPSEN_SECONDS is the length of
# each (default 30), JEPSEN_SEED the seed, JEPSEN_NODES the size (default 5). Its directory is per tree, like jepsen's.
$(B)/jepsen_cluster: tests/jepsen/jepsen_cluster.c tests/jepsen/cluster_users.h tests/harness.h tests/cvol_check.h server/cluster.h server/meta.h \
    server/crashpt.h $(CLUSTER_OBJ) $(B)/cvol_check.o $(B)/tsdb_crypto.o $(CLIB) $(LIB)
	$(CC) $(CFLAGS) -Iclient -Iserver -Isrc -Isrc/raft/compat -Itests -o $@ $< $(CLUSTER_OBJ) $(B)/cvol_check.o \
	    $(B)/tsdb_crypto.o $(CLIB) $(LIB) $(LDLIBS)
# on a disk, never /tmp (a tmpfs: a failing run keeps the state of five nodes there, and a power run never syncs)
JEPSEN_CLUSTER_DIR ?= /var/tmp/tsdb-jepsen-cluster-$(shell printf %s "$(CURDIR)" | cksum | cut -d" " -f1)
JEPSEN_NEMESES ?= kill partition pause midcommit power
JEPSEN_SEED ?= 1
JEPSEN_NODES ?= 5
# kv-local runs its nodes with TSDB_CLUSTER_WRITES=1, kv and classes with the replication driver: separate runs
# users: every tsdbd requires a login, an admin thread adds, drops, re-adds users and changes their passwords and grants
# while one prober per node logs in and reads (tests/jepsen/cluster_users.h; the superuser is made with the CLI: $(B)/tsdb)
# kv-via: kv's clients on random members (every operation is forwarded to kvr's leader, s2f), a read of the series on the
# connection of each acknowledged write (read-your-writes), nemeses aimed at the members the clients are on, and the
# fwdstall nemesis (an entry node holds its forwards past fwd_max_age: an old frame must never be applied)
JEPSEN_CLUSTER_WORKLOADS ?= meta,kv-local meta,kv meta,kv-via meta,classes meta,users
JEPSEN_VIA_NEMESES ?= fwdstall
# R4's nemeses (kill2, slow, longdown, rebuild, churn: the header of jepsen_cluster.c): the ones whose tracks are on main
# (`jepsen_cluster --built-nemeses`; an unbuilt one is refused, never skipped silently when named), each on kv and classes
# (kvr moves under churn; classes at the default caps once r4-replay is on main). JEPSEN_R4_NEMESES names others;
# JEPSEN_R4_NEMESES=none leaves them out. They add runs: IO x JEPSEN_R4_WORKLOADS (2) x the built nemeses, each
# JEPSEN_SECONDS (20 runs and about 100 min at 300 s with all five built).
# JEPSEN_R4_TRY=1 runs the unbuilt ones too. longdown runs with repl_retain_s 3 (--retain-s S for another age).
JEPSEN_R4_WORKLOADS ?= meta,kv meta,classes
jepsen-cluster: $(B)/jepsen_cluster $(B)/tsdbd $(B)/tsdb $(B)/libcrashfs.so $(B)/liboutagefs.so
	for io in $(IO); do for wl in $(JEPSEN_CLUSTER_WORKLOADS); do for nem in $(JEPSEN_NEMESES); do \
	    echo "== tsdbd --io $$io, workloads $$wl, nemesis $$nem"; \
	    TSDBD=./$(B)/tsdbd TSDB_CLI=./$(B)/tsdb TSDBD_IO=$$io ./$(B)/jepsen_cluster --dir $(JEPSEN_CLUSTER_DIR) --nodes $(JEPSEN_NODES) \
	        --nemesis $$nem --workload $$wl --seconds $(call jsec,30) --seed $(JEPSEN_SEED) --crashfs $(B)/libcrashfs.so \
	        || exit 1; \
	    done; done; done
	for io in $(IO); do for nem in $(JEPSEN_VIA_NEMESES); do \
	    echo "== tsdbd --io $$io, workloads meta,kv-via, nemesis $$nem"; \
	    TSDBD=./$(B)/tsdbd TSDB_CLI=./$(B)/tsdb TSDBD_IO=$$io ./$(B)/jepsen_cluster --dir $(JEPSEN_CLUSTER_DIR) --nodes $(JEPSEN_NODES) \
	        --nemesis $$nem --workload meta,kv-via --seconds $(call jsec,30) --seed $(JEPSEN_SEED) --crashfs $(B)/libcrashfs.so \
	        || exit 1; \
	    done; done
	r4="$(JEPSEN_R4_NEMESES)"; case "$$r4" in none) r4=;; "") r4=$$(./$(B)/jepsen_cluster --built-nemeses);; esac; \
	for io in $(IO); do for wl in $(JEPSEN_R4_WORKLOADS); do for nem in $$r4; do \
	    echo "== tsdbd --io $$io, workloads $$wl, nemesis $$nem (R4)"; \
	    TSDBD=./$(B)/tsdbd TSDB_CLI=./$(B)/tsdb TSDBD_IO=$$io ./$(B)/jepsen_cluster --dir $(JEPSEN_CLUSTER_DIR) --nodes $(JEPSEN_NODES) \
	        --nemesis $$nem --workload $$wl --seconds $(call jsec,30) --seed $(JEPSEN_SEED) --crashfs $(B)/libcrashfs.so \
	        --outagefs $(B)/liboutagefs.so || exit 1; \
	    done; done; done
	$(MAKE) --no-print-directory jepsen-cluster-ec

# --ec (tests/jepsen/cluster_ec.h, row r8l-jc): kvr as an erasure-coded volume on JEPSEN_EC_NODES nodes with two chunk stores each.
# The nemeses default to kill partial diskloss, and, when this tsdbd settles ec volumes (it takes --ec-experimental: only then
# is there a chunk to rot and a crash point of the settle to reach) to partition pause rot midcommit power as well;
# JEPSEN_EC= (empty) leaves the runs out. Per io and workload (JEPSEN_EC_WORKLOADS).
JEPSEN_EC ?= 4+2
JEPSEN_EC_NODES ?= 7
JEPSEN_EC_WORKLOADS ?= meta,kv
JEPSEN_EC_NEMESES ?=
jepsen-cluster-ec: $(B)/jepsen_cluster $(B)/tsdbd $(B)/tsdb $(B)/libcrashfs.so
	@if [ -z "$(JEPSEN_EC)" ]; then echo "jepsen-cluster-ec: off (JEPSEN_EC is empty)"; exit 0; fi; \
	nems="$(JEPSEN_EC_NEMESES)"; if [ -z "$$nems" ]; then \
	    if ./$(B)/tsdbd --help 2>&1 | grep -q -- --ec-experimental; then nems="kill partition partial pause diskloss rot midcommit power"; \
	    else nems="kill partial diskloss"; fi; fi; \
	for io in $(IO); do for wl in $(JEPSEN_EC_WORKLOADS); do for nem in $$nems; do \
	    echo "== tsdbd --io $$io, workloads $$wl, --ec $(JEPSEN_EC) on $(JEPSEN_EC_NODES) nodes, nemesis $$nem"; \
	    TSDBD=./$(B)/tsdbd TSDB_CLI=./$(B)/tsdb TSDBD_IO=$$io ./$(B)/jepsen_cluster --dir $(JEPSEN_CLUSTER_DIR) \
	        --nodes $(JEPSEN_EC_NODES) --ec $(JEPSEN_EC) --nemesis $$nem --workload $$wl --seconds $(call jsec,30) \
	        --seed $(JEPSEN_SEED) --crashfs $(B)/libcrashfs.so || exit 1; \
	    done; done; done

# The harness has to notice what it is meant to: a port file that names another pid, a client that skips the check
# of the port it connected to while a foreign server holds the node's old one (and the same client with the check:
# refused, the run passes), a partition that is not applied, an armed crash point that never fires, a nemesis that
# sleeps through its naps, a write missing from the final state; and the comparison of replicas finds a row only one
# has. A crash point that fires is counted.
# Each check of the histories (validity, durability, atomic visibility, stale and monotonic reads) and the election
# check are fed a history made here with one thing wrong (--selftest-checker). A nemesis that does nothing (a kill that
# kills nobody, a pause that stops nobody, a delay that delays nothing, a heal that resumes held links) fails the run.
jepsen-cluster-selftest: $(B)/jepsen_cluster $(B)/tsdbd $(B)/tsdb $(B)/libcrashfs.so $(B)/liboutagefs.so
	@d=$(JEPSEN_CLUSTER_DIR)-selftest; j="env TSDBD=./$(B)/tsdbd TSDB_CLI=./$(B)/tsdb ./$(B)/jepsen_cluster --dir $$d"; \
	out=$$($$j --mutant foreign-pid --seconds 5 --nemesis kill 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'names pid'; then \
	        echo "jepsen-cluster-selftest: a port file that names another pid went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$($$j --mutant takeover --seconds 30 --nemesis kill --workload kv-local 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'durability'; then \
	        echo "jepsen-cluster-selftest: writes that went to a foreign server on the node's old port went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$($$j --mutant takeover-checked --seconds 30 --nemesis kill --workload kv-local 2>&1); rc=$$?; \
	    if [ $$rc -ne 0 ] || ! echo "$$out" | grep -q '[1-9][0-9]* refused for a port the node does not hold'; then \
	        echo "jepsen-cluster-selftest: the check of the port a client connected to did not refuse the foreign server (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$($$j --mutant noop-partition --seconds 20 --nemesis partition 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'while the pair was cut'; then \
	        echo "jepsen-cluster-selftest: a partition that was not applied went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$($$j --mutant unfired --seconds 15 --nemesis midcommit 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'never fired on node'; then \
	        echo "jepsen-cluster-selftest: an armed crash point that never fired went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$($$j --seconds 20 --nemesis midcommit 2>&1); rc=$$?; \
	    if [ $$rc -ne 0 ] || ! echo "$$out" | grep -q 'crash points hit [1-9]'; then \
	        echo "jepsen-cluster-selftest: a crash point that fires was not counted (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$($$j --mutant idle --seconds 25 --nemesis kill 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'the nemesis was idle'; then \
	        echo "jepsen-cluster-selftest: a nemesis that sleeps through its naps went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$($$j --mutant lost-write --seconds 12 --nemesis kill --workload kv-local 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'a lost write'; then \
	        echo "jepsen-cluster-selftest: a write missing from the final state went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$($$j --mutant rep-lost-write --seconds 30 --nemesis kill --workload kv 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q "the leader's final read"; then \
	        echo "jepsen-cluster-selftest: an acknowledged write missing from a replica of kvr went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$($$j --mutant rep-lost-write --seconds 20 --nemesis midcommit --workload kv 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q "terminal crash point .*ended there" || ! echo "$$out" | grep -q "the leader's final read"; then \
	        echo "jepsen-cluster-selftest: an acknowledged write missing from a replica of kvr after the terminal point and its failover went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$($$j --mutant rep-half-batch --seconds 30 --nemesis kill --workload kv 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'half of a large write'; then \
	        echo "jepsen-cluster-selftest: half of an acknowledged large write in a replica of kvr went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$($$j --selftest-checker 2>&1); rc=$$?; \
	    if [ $$rc -ne 0 ]; then \
	        echo "jepsen-cluster-selftest: a check of the histories or the election check does not find what it is for (rc $$rc)"; echo "$$out" | tail -n 30; exit 1; fi; \
	out=$$($$j --mutant nokill --seconds 15 --nemesis kill 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'kill nemesis.*had no effect'; then \
	        echo "jepsen-cluster-selftest: a kill that kills nobody went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$($$j --mutant nopause --seconds 20 --nemesis pause 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'pause nemesis.*had no effect'; then \
	        echo "jepsen-cluster-selftest: a pause that stops nobody went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$($$j --mutant nodelay --seconds 20 --nemesis partition 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'partition nemesis.*had no effect'; then \
	        echo "jepsen-cluster-selftest: a delay that delays nothing went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$($$j --mutant noreset --seconds 60 --nemesis partition 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'must be reset at the heal'; then \
	        echo "jepsen-cluster-selftest: a heal that resumes held links instead of resetting them went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$(TSDB_CLUSTER_MUTANT=dropnotcommitted $$j --seconds 30 --nemesis none --workload users 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'FAIL: users: '; then \
	        echo "jepsen-cluster-selftest: a user change answered before it committed (dropnotcommitted) went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$(TSDB_AUTH_MUTANT=loginbeforecatchup $$j --seconds 60 --nemesis kill --workload users 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'FAIL: users: '; then \
	        echo "jepsen-cluster-selftest: a restarted member that logs in before it caught up (loginbeforecatchup) went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$(TSDBD_FWD_MUTANT=fwdoldframe $$j --seconds 40 --nemesis fwdstall --workload kv-via 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'old frame: write'; then \
	        echo "jepsen-cluster-selftest: a frame held past the forward age at its entry node and then applied (fwdoldframe) went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$(TSDB_REPLICA_READS=1 $$j --seconds 30 --nemesis none --workload kv-via 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'read-your-writes: key'; then \
	        echo "jepsen-cluster-selftest: a read served from a replica's own copy (replica reads on, no mutant) went unnoticed by read-your-writes (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$($$j --mutant nokill2 --seconds 25 --nemesis kill2 --workload meta,kv 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'kill2 nemesis (round .*had no effect'; then \
	        echo "jepsen-cluster-selftest: a kill2 that kills one server went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$($$j --mutant noslow --seconds 25 --nemesis slow --workload meta,kv --outagefs $(B)/liboutagefs.so 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'slow nemesis (round .*had no effect'; then \
	        echo "jepsen-cluster-selftest: a slow that slows nothing went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$($$j --mutant plant-snap-left --seconds 15 --nemesis kill --workload meta,kv 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q '\.snap is left on node'; then \
	        echo "jepsen-cluster-selftest: a v<N>.snap left on a node went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$($$j --mutant plant-removed-left --seconds 15 --nemesis kill --workload meta,kv 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q '\.removed-77 is left on node'; then \
	        echo "jepsen-cluster-selftest: a v<N>.removed-* left on a node went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$(JEPSEN_R4_TRY=1 $$j --seconds 5 --nemesis churn --workload meta,kv-local 2>&1); rc=$$?; \
	    if [ $$rc -ne 2 ] || ! echo "$$out" | grep -q 'works on kvr'; then \
	        echo "jepsen-cluster-selftest: churn on a workload without kvr was not refused (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$($$j --seconds 5 --nemesis slow --workload meta,kv 2>&1); rc=$$?; \
	    if [ $$rc -ne 2 ] || ! echo "$$out" | grep -q 'needs --outagefs'; then \
	        echo "jepsen-cluster-selftest: slow without the shim was not refused (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	for n in longdown rebuild churn; do \
	    if ! ./$(B)/jepsen_cluster --built-nemeses | grep -qw $$n; then \
	        out=$$($$j --seconds 5 --nemesis $$n --workload meta,kv 2>&1); rc=$$?; \
	        if [ $$rc -ne 2 ] || ! echo "$$out" | grep -q "the $$n nemesis needs .* which is not on main yet"; then \
	            echo "jepsen-cluster-selftest: the unbuilt nemesis $$n was not refused with its line (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; fi; \
	    done; \
	out=$$($$j --mutant nolongdown --seconds 60 --nemesis longdown --workload meta,kv 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'longdown round(s) .* and only .* took a snapshot'; then \
	        echo "jepsen-cluster-selftest: a longdown that is back before its log is gone went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$($$j --mutant nosnapdone --seconds 60 --nemesis rebuild --workload meta,kv 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'rebuild round(s) and only .* were filled by a snapshot'; then \
	        echo "jepsen-cluster-selftest: a rebuild that no snapshot filled went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$(TSDB_CVOL_MUTANT=makemissing $$j --seconds 60 --nemesis rebuild --workload meta,kv 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'made it again before any'; then \
	        echo "jepsen-cluster-selftest: a volume made again where its directory was removed (makemissing) went unnoticed under rebuild (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	for n in longdown rebuild; do \
	    out=$$(TSDB_REPLD_MUTANT=snapdroppart $$j --seconds 120 --nemesis $$n --workload meta,kv --crashfs $(B)/libcrashfs.so 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'difference(s) between the replicas\|violation(s):'; then \
	        echo "jepsen-cluster-selftest: a snapshot that leaves a part out (snapdroppart) went unnoticed under $$n (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	    done; \
	if grep -qs moveshrinkfirst server/*.c src/*.c && ./$(B)/jepsen_cluster --built-nemeses | grep -qw churn; then \
	    out=$$(TSDB_CLUSTER_MUTANT=moveshrinkfirst TSDB_REPLD_MUTANT=moveshrinkfirst $$j --seconds 120 --nemesis churn --workload meta,kv --crashfs $(B)/libcrashfs.so 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'difference(s) between the replicas\|violation(s):'; then \
	        echo "jepsen-cluster-selftest: a move that shrinks first (moveshrinkfirst) went unnoticed under churn (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	else echo "jepsen-cluster-selftest: moveshrinkfirst under churn skipped (the mutant or the nemesis is not on main yet)"; fi; \
	ecj="$$j --nodes 7 --ec 4+2 --workload meta,kv --nemesis kill --seconds 20"; \
	out=$$($$ecj --seconds 40 --mutant ec-diverged 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'FAIL: a volume was DIVERGED'; then \
	        echo "jepsen-cluster-selftest: a DIVERGED volume in an ec run without rot or disk loss went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$($$ecj --mutant ec-member-differs 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'FAIL: ec: .* through member node'; then \
	        echo "jepsen-cluster-selftest: a member that answers differently after a transfer went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$($$ecj --mutant ec-notransfer 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'FAIL: ec: kvr did not move to node'; then \
	        echo "jepsen-cluster-selftest: per-member reads with no transfer went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	out=$$($$ecj --nemesis partial --mutant noop-partition 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'exchanged'; then \
	        echo "jepsen-cluster-selftest: a partial partition that cut nothing went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	if ./$(B)/tsdbd --help 2>&1 | grep -q -- --ec-experimental; then \
	    out=$$($$ecj --mutant ec-nosettle 2>&1); rc=$$?; \
	    if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'nothing settled'; then \
	        echo "jepsen-cluster-selftest: an ec run in which nothing settled went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	    for mv in TSDB_ECAPPLY_MUTANT=ycold TSDB_ECAPPLY_MUTANT=ecabovey TSDB_ECCOLD_MUTANT=cutnogate; do \
	        var=$${mv%%=*}; m=$${mv#*=}; \
	        if grep -qs "\"$$var\"" server/*.c src/*.c; then \
	            out=$$(env $$mv $$j --nodes 7 --ec 4+2 --workload meta,kv --nemesis kill,midcommit --seconds 120 --crashfs $(B)/libcrashfs.so 2>&1); rc=$$?; \
	            if [ $$rc -ne 1 ] || ! echo "$$out" | grep -q 'FAIL: a volume was DIVERGED\|through member node\|violation(s):\|difference(s) between the replicas'; then \
	                echo "jepsen-cluster-selftest: the server mutant $$m ($$var) went unnoticed in an ec run, or failed for another reason (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	        else echo "jepsen-cluster-selftest: the server mutant $$m skipped (tsdbd reads no $$var)"; fi; \
	    done; \
	else echo "jepsen-cluster-selftest: ec-nosettle, ycold, ecabovey, cutnogate skipped (this tsdbd has no --ec-experimental)"; fi; \
	out=$$($$j --selftest-replicas 2>&1); rc=$$?; \
	    if [ $$rc -ne 0 ]; then \
	        echo "jepsen-cluster-selftest: the comparison of replicas is wrong (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	    echo "jepsen-cluster-selftest: ok"

# The harness itself (one thread per proxied link, a sampler, the workloads) under ThreadSanitizer and under ASan+UBSan,
# against the normal tsdbd: a short run of the kill, partition and pause nemeses (JEPSEN_SANITIZE_SECONDS). The objects
# of the sanitized builds go to build-jc-tsan/ and build-jc-asan/.
JEPSEN_SANITIZE_SECONDS ?= 45
jepsen-cluster-sanitized: $(B)/tsdbd
	$(MAKE) B=build-jc-tsan ARCH= CFLAGS="-O1 -g -fsanitize=thread -fno-omit-frame-pointer" LDFLAGS="-fsanitize=thread" \
	    build-jc-tsan/jepsen_cluster
	$(MAKE) B=build-jc-asan ARCH= CFLAGS="-O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer" \
	    LDFLAGS="-fsanitize=address,undefined" build-jc-asan/jepsen_cluster
	@set -e; for k in tsan asan; do \
	    out=$$(TSAN_OPTIONS="halt_on_error=0 second_deadlock_stack=1" ASAN_OPTIONS=detect_leaks=0 UBSAN_OPTIONS=print_stacktrace=1 \
	        TSDBD=./$(B)/tsdbd ./build-jc-$$k/jepsen_cluster --dir $(JEPSEN_CLUSTER_DIR)-$$k --nemesis kill,partition,pause \
	        --seconds $(JEPSEN_SANITIZE_SECONDS) 2>&1) && rc=0 || rc=$$?; \
	    if [ $$rc -ne 0 ] || echo "$$out" | grep -qE 'WARNING: ThreadSanitizer|ERROR: AddressSanitizer|runtime error'; then \
	        echo "jepsen-cluster-sanitized ($$k): rc $$rc"; echo "$$out" | tail -n 40; exit 1; fi; \
	    echo "jepsen-cluster-sanitized ($$k): ok"; done

# Build and test on AlmaLinux 10 (the main target OS) in Podman; takes minutes, logs in $LOGDIR.
# URING=0 keeps Podman's stock seccomp profile, which blocks io_uring as production does: tsdbd then runs on epoll.
test-almalinux10:
	tests/distro/almalinux10.sh

# The programs in examples/ are what the README's quick start builds and runs.
$(B)/embedded-example: examples/embedded.c include/tsdb.h include/tsdb_calendar.h $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/client-example: examples/client.c client/tsdb_client.h include/tsdb_calendar.h $(CLIB)
	$(CC) $(CFLAGS) -Iclient -o $@ $< $(CLIB) $(ZSTD_LIB) -lpthread -lm $(LINK_EXTRA)

all: $(B)/embedded-example $(B)/client-example

# Runs the README's quick start, word for word, in a scratch directory.
test: test-quickstart test-seeds-selftest test-cli-stats test-cli-stats-net test-cli-cluster test-cluster-tutorial
# The random-heavy tests under a seed that is not their constant (tests/seed.h): `make test-seeds` picks one and prints it,
# `make test-seeds TSDB_TEST_SEED=N` repeats a run. A failure is rerun with the same N on the one test.
SEED_TESTS = test_users test_latest test_buckets test_delete test_limits
.PHONY: test-seeds test-seeds-selftest
test-seeds: $(SEED_TESTS:%=$(B)/%) $(B)/test_wire
	@seed=$${TSDB_TEST_SEED:-$$(od -An -N4 -tu4 /dev/urandom | tr -d ' ')}; echo "TSDB_TEST_SEED=$$seed"; \
	TSDB_TEST_SEED=$$seed ./$(B)/test_wire tests/data/wire13-vectors.txt || { echo "FAILED: TSDB_TEST_SEED=$$seed ./$(B)/test_wire"; exit 1; }; \
	for t in $(SEED_TESTS); do TSDB_TEST_SEED=$$seed ./$(B)/$$t $(SCRATCH) || { echo "FAILED: TSDB_TEST_SEED=$$seed ./$(B)/$$t"; exit 1; }; done; \
	echo "test-seeds passed (TSDB_TEST_SEED=$$seed)"
test-seeds-selftest: $(B)/test_wire
	B=$(B) tests/test_seeds.sh
test-cli-stats: $(B)/tsdb
	B=$(B) tests/test_cli_stats.sh $(TMPDIR)
test-cli-stats-net: $(B)/tsdb $(B)/tsdbd
	B=$(B) tests/test_cli_stats_net.sh $(TMPDIR)
test-cli-cluster: $(B)/tsdb
	B=$(B) tests/test_cli_cluster.sh $(TMPDIR)
# The cluster tutorial's command block, word for word (docs/cluster-preview.md), on ports the kernel picks.
test-cluster-tutorial: $(B)/tsdb $(B)/tsdbd $(B)/client-example
	B=$(B) tests/test_cluster_tutorial.sh
test-quickstart: $(B)/tsdb $(B)/tsdbd $(B)/embedded-example $(B)/client-example
	B=$(B) tests/test_quickstart.sh

# The outage shim's own test (Linux only: LD_PRELOAD)
ifneq ($(UNAME_S),Darwin)
test: test-outagefs
test-outagefs: $(B)/test_outagefs $(B)/test_health $(B)/test_outage $(B)/test_unavailable $(B)/liboutagefs.so $(B)/tsdbd $(B)/tsdb
	OUTAGEFS_LIB=$(B)/liboutagefs.so ./$(B)/test_outagefs $(SCRATCH)
	OUTAGEFS_LIB=$(B)/liboutagefs.so ./$(B)/test_health $(SCRATCH)
	for io in $(IO); do echo "== outage, tsdbd --io $$io"; \
	    OUTAGEFS_LIB=$(B)/liboutagefs.so TSDBD=$(TSDBD) TSDBD_IO=$$io ./$(B)/test_outage $(SCRATCH) || exit 1; \
	    OUTAGEFS_LIB=$(B)/liboutagefs.so TSDBD=$(TSDBD) TSDBD_IO=$$io ./$(B)/test_unavailable $(SCRATCH) || exit 1; done
endif

# `make` with no target must build `all` (the .DEFAULT_GOAL line at the top): a rule for one object placed above
# `all:` took the default once.
test: check-default-goal
check-default-goal:
	@g=$$($(MAKE) --no-print-directory -pnq -f $(firstword $(MAKEFILE_LIST)) 2>/dev/null | sed -n 's/^\.DEFAULT_GOAL := //p' | tail -n 1); \
	if [ "$$g" != all ]; then echo "check-default-goal: make alone builds '$$g', not all"; exit 1; fi

# The version is in VERSION, include/tsdb.h, the binaries and the client manifests; they must agree.
test: check-version check-rdma-syms
# tsdbd names no ibv_ or rdma_ symbol and needs no RDMA library (dlopen, docs/design-rdma.md §5.4); --rdma: the
# transport is in it
check-rdma-syms: $(B)/tsdbd
	tools/check-rdma-syms.sh $(if $(filter 1,$(NO_RDMA)),,--rdma) $(B)/tsdbd
check-version: $(B)/tsdbd $(B)/tsdb
	tools/check-version.sh $(B)

# The GitHub Pages site (site/): well formed, no broken links, every code block the tested program or README
# section it names, every chart number in the published results. --fix copies the code blocks again.
test: check-site
check-site:
	python3 tests/check_site.py

# The release workflow's scripts (.github/workflows/release.yml): the release text (tools/release-notes.sh), and the
# GitHub API calls of .github/scripts/release-publish.sh against a local mock of the API (needs curl and jq).
test: test-release
test-release:
	tests/test_release_notes.sh
	python3 -I tests/test_release_publish.py

# The build fleet of the subagent-economy scripts (SE_REMOTES: bgrun.sh picks a host with a free slot, rx.sh, await.sh and
# watchdog.sh use the job's host) against two fake hosts, and the planted bugs it must catch (se-fleet-mutants, weekly).
test: test-se-fleet
.PHONY: test-se-fleet se-fleet-mutants
test-se-fleet:
	tests/test_se_fleet.sh $(SCRATCH)
SE_FLEET_MUTANTS = nohost rxdefault rxjob overbook noprobe downfree fallbacklast slotsignored rjobhost notag ownglobal awaithost awaitone wdfirst wdhost wdone overflow pushfirst backfirst gitfirst nolock pinlock noprobetimeout claimneedspid claimslot claimnowait nowaitprobe rrunsticky rrunnonote endnote badlocal slotcap ownmsg
se-fleet-mutants:
	$(MUT) --name se-fleet --rc 1 --timeout 600 --detail 'FAIL|SURVIVED' --list "$(SE_FLEET_MUTANTS)" -- tests/test_se_fleet.sh --mutant @M@ @D@
# tools/link-bench on loopback (the tool behind docs/design-rdma.md's numbers): tags checked, a wrong one caught, round
# trips timed whole. LINK_BENCH=path runs the test against another binary (a planted bug). Seconds.
test: test-link-bench
.PHONY: test-link-bench
test-link-bench: $(B)/link-bench
	B=$(B) tests/test_link_bench.sh

# The licence of every source file (tools/check-spdx.sh) and the test of that check (it must fail on each kind of slip).
test: test-spdx
test-spdx:
	tools/check-spdx.sh
	tests/test_check_spdx.sh

# The helper that runs a test binary's planted bugs in parallel (tools/mutants.sh), with fake programs: seconds.
test: test-mutants-helper
test-mutants-helper:
	tests/test_mutants_helper.sh

# every group above is a matrix entry of the sanitizer jobs in .github/workflows/ci.yml
test: check-ci-groups
check-ci-groups:
	tests/check_ci_groups.sh

.PHONY: test-quickstart check-site test-release test-spdx check-ci-groups test-mutants-helper tsan-par
