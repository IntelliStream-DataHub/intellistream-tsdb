CC      ?= cc
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
# `make dist` links these statically (-l:libzstd.a); glibc stays dynamic.
ZSTD_LIB  ?= -lzstd
URING_LIB ?= -luring
LDLIBS  += $(ZSTD_LIB) -lpthread -lm $(LINK_EXTRA)
# Output directory; `make tsan` builds into its own, next to a normal build.
B       ?= build
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
LIB  := $(B)/libtsdb.a
BINS := $(B)/tsdb $(B)/tsdb-bench $(B)/test_tsdb

all: $(LIB) $(B)/libtsdb.so $(B)/tsdb $(B)/tsdb-bench $(B)/codec-bench $(B)/insert-bench $(B)/tsdbd $(CLIB) $(B)/tsdbc-bench $(B)/scale-bench $(B)/type-bench $(B)/twa-bench

$(B)/%.o: src/%.c src/internal.h src/levels.h include/tsdb.h | $(B)
	$(CC) $(CFLAGS) -c -o $@ $<

$(B)/%.base.o: src/%.c src/internal.h src/levels.h include/tsdb.h | $(B)
	$(CC) $(CFLAGS) -DTSDB_LV=base -c -o $@ $<

$(B)/%.v3.o: src/%.c src/internal.h src/levels.h include/tsdb.h | $(B)
	$(CC) $(CFLAGS) $(LV_FLAGS_v3) -DTSDB_LV=v3 -c -o $@ $<

$(B)/%.v4.o: src/%.c src/internal.h src/levels.h include/tsdb.h | $(B)
	$(CC) $(CFLAGS) $(LV_FLAGS_v4) -DTSDB_LV=v4 -c -o $@ $<

# the calendar helper is client-side code, in both libraries: the engine itself knows UTC only
$(LIB): $(OBJ) $(B)/tsdb_calendar.o
	$(AR) rcs $@ $^

# The engine as a shared library for the embedded bindings (Rust, Python, Java FFM).
# Only the public tsdb_* API is exported (tools/libtsdb.map); zstd is linked in
# statically when ZSTD_LIB says so (make dist), else dynamically.
$(B)/libtsdb.so: $(OBJ) $(B)/tsdb_calendar.o tools/libtsdb.map
	$(CC) $(CFLAGS) -shared -Wl,--version-script=tools/libtsdb.map -Wl,-soname,libtsdb.so -o $@ \
	    $(OBJ) $(B)/tsdb_calendar.o $(LDLIBS)

$(B)/tsdb_calendar.o: client/tsdb_calendar.c include/tsdb_calendar.h include/tsdb.h | $(B)
	$(CC) $(CFLAGS) -c -o $@ $<

$(B)/tsdb: tools/tsdb-cli.c tools/common.h $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/tsdb-bench: tools/tsdb-bench.c tools/common.h tools/datasets.h tools/cbench.h $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_tsdb: tests/test_tsdb.c tests/samekeys.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# its own write/fsync/fdatasync/pwrite/renameat inject storage errors into the library
$(B)/test_failstop: tests/test_failstop.c $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

test: $(B)/test_tsdb $(B)/test_shared $(B)/test_failstop $(B)/test_damage $(B)/test_types $(B)/test_levels $(B)/test_server $(B)/test_stability $(B)/test_follow $(B)/test_volumes $(B)/test_group $(B)/test_lttb $(B)/test_twa $(B)/tsdbd $(B)/libcrashfs.so $(B)/tsdb
	./$(B)/test_tsdb
	LD_LIBRARY_PATH=$(B) ./$(B)/test_shared
	./$(B)/test_failstop
	./$(B)/test_damage
	./$(B)/test_types
	./$(B)/test_levels
	./$(B)/test_group
	./$(B)/test_lttb
	B=$(B) tests/test_cli_lttb.sh
	./$(B)/test_twa
	./$(B)/test_follow
	./$(B)/test_volumes
	for io in $(IO); do echo "== tsdbd --io $$io"; \
	    TSDBD=$(TSDBD) TSDBD_IO=$$io ./$(B)/test_server && \
	    TSDBD=$(TSDBD) TSDBD_IO=$$io ./$(B)/test_stability || exit 1; done

# Without -march=native: with it, GCC 15.2's AddressSanitizer crashes in memcpy into
# fake-stack frames (use-after-return detection), in code that did not change.
# The runtime-dispatched AVX-512 kernels are still tested.
# UBSan only prints by default and the run still exits 0: halt_on_error makes the first report fail
# it. `ubsan-check` proves that with a program that overflows on purpose.
ASAN_FLAGS := -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer
UBSAN_RUN  := halt_on_error=1:print_stacktrace=1
asan: ubsan-check
	$(MAKE) clean
	UBSAN_OPTIONS="$(UBSAN_RUN) $$UBSAN_OPTIONS" \
	$(MAKE) ARCH= CFLAGS="$(ASAN_FLAGS)" LDFLAGS="-fsanitize=address,undefined" test

ubsan-check:
	@mkdir -p $(B)
	$(CC) $(ASAN_FLAGS) -o $(B)/ubsan_overflow tests/ubsan_overflow.c
	@if UBSAN_OPTIONS="$(UBSAN_RUN)" ./$(B)/ubsan_overflow >/dev/null 2>&1; then \
	    echo "ubsan-check: an undefined behaviour report did not fail the run"; exit 1; fi
	@echo "ubsan-check: ok"

# ThreadSanitizer, in build-tsan/ (no `make clean`: a normal build in build/
# stays), the server under test included. -O1 keeps reports readable;
# halt_on_error makes the first race fail the run. Not with ASan: the two do
# not combine.
TSAN_FLAGS := -O1 -g -fsanitize=thread -fno-omit-frame-pointer
tsan:
	TSAN_OPTIONS="halt_on_error=1 second_deadlock_stack=1 $$TSAN_OPTIONS" \
	$(MAKE) B=build-tsan TSDBD=build-tsan/tsdbd ARCH= CFLAGS="$(TSAN_FLAGS)" test

$(B):
	mkdir -p $(B)

clean:
	rm -rf build build-tsan $(DIST_B)

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
DIST_BINS   = tsdbd tsdb codec-bench type-bench twa-bench tsdb-bench insert-bench tsdbc-bench scale-bench
DIST_TESTS  = test_tsdb test_types test_levels test_group test_lttb test_twa test_follow test_volumes test_server test_stability
DIST_FILES  = $(addprefix $(DIST_B)/,$(DIST_BINS) $(DIST_TESTS))

DIST_SIG    = $$($(CC) --version | head -n 1) | $(CC) | $(CFLAGS) | $(subst ",,$(DIST_VARS)) | $(LV_FLAGS_v3) | $(LV_FLAGS_v4) \
              | $$(cksum <tools/glibc-compat.h)

dist:
	@sig="$(DIST_SIG)"; \
	if [ "$$(cat $(DIST_B)/.flags 2>/dev/null)" != "$$sig" ]; then \
	    [ -d $(DIST_B) ] && echo "dist: compiler or flags changed since the last build: rebuilding $(DIST_B)"; \
	    rm -rf $(DIST_B) && mkdir -p $(DIST_B) && printf '%s\n' "$$sig" >$(DIST_B)/.flags; \
	fi
	$(MAKE) $(DIST_VARS) all $(DIST_TESTS:%=$(DIST_B)/%)
	tools/check-glibc.sh -c $(GLIBC_MAX) $(DIST_FILES)
	tools/check-isa.sh $(DIST_B) $(DIST_FILES)
	CC="$(CC)" tools/mkdist.sh $(DIST_B)

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

test-rust: $(B)/tsdbd
	cd client/rust && cargo test

test-python: $(B)/tsdbd
	test -x $(PYVENV)/bin/maturin || (python3 -m venv $(PYVENV) && $(PYVENV)/bin/pip install -q maturin pytest)
	cd client/python && VIRTUAL_ENV=$(abspath $(PYVENV)) $(abspath $(PYVENV))/bin/maturin develop --release \
	    && $(abspath $(PYVENV))/bin/python -m pytest tests

test-java: $(B)/tsdbd
	cd client/java && mvn -q test

test-clients: test-rust test-python test-java

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
	javac -d $(INTEG)/java -cp "$$(ls client/java/target/tsdb-client-*.jar | head -n 1)" tests/integration/java/Itj.java

integration-tests: $(B)/tsdbd $(B)/tsdb $(INTEG)/itc integ-rust integ-python integ-java
	python3 tests/integration/run.py --bin-dir $(B) $(INTEG_ARGS)

# The harness's own tests (seconds): a broken io_uring backend fails the run, signals, mutate.py's verdicts.
integration-selftest: $(B)/tsdbd $(B)/tsdb
	python3 tests/integration/selftest.py

# The same against the binaries of `make dist`: its tsdbd and tsdb, and the C client linked with its libtsdbclient.a.
integration-tests-dist: dist integ-rust integ-python integ-java
	$(CC) -O2 -g -std=gnu11 -Iinclude -Iclient -o $(DIST_B)/itc tests/integration/c/itc.c $(DIST_B)/libtsdbclient.a \
	    $(ZSTD_LIB) -lpthread -lm
	python3 tests/integration/run.py --bin-dir $(DIST_B) --itc $(DIST_B)/itc $(INTEG_ARGS)

.PHONY: bench-clients-smoke hooks all test asan ubsan-check tsan clean jepsen test-almalinux10 test-rust test-python test-java test-clients dist dist-test dist-check-almalinux10 dist-check-sandybridge integ-rust integ-python integ-java integration-tests integration-tests-dist integration-selftest

$(B)/codec-bench: tools/codec-bench.c tools/common.h tools/datasets.h $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/insert-bench: tools/insert-bench.c tools/common.h tools/datasets.h $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

# The version shown by tsdb_build_info; part of the tsdbd.o flags, so a change rebuilds only that.
TSDB_VERSION ?= $(shell git describe --always --dirty 2>/dev/null || echo dev)

$(B)/tsdbd: server/tsdbd.c server/metrics.c server/metrics.h include/tsdb_protocol.h client/tsdb_pack.h $(B)/tsdb_pack.o $(LIB)
	$(CC) $(CFLAGS) $(URING_FLAGS) -DTSDB_BUILD_VERSION='"$(TSDB_VERSION)"' -o $@ $< server/metrics.c $(B)/tsdb_pack.o $(LIB) $(LDLIBS) $(URING_LIBS)

CLIB := $(B)/libtsdbclient.a

$(B)/tsdb_client.o: client/tsdb_client.c client/tsdb_client.h include/tsdb_protocol.h include/tsdb.h | $(B)
	$(CC) $(CFLAGS) -c -o $@ $<

$(B)/tsdb_pack.o: client/tsdb_pack.c client/tsdb_pack.h include/tsdb_protocol.h | $(B)
	$(CC) $(CFLAGS) -c -o $@ $<

$(CLIB): $(B)/tsdb_client.o $(B)/tsdb_pack.o $(B)/tsdb_calendar.o
	$(AR) rcs $@ $^

$(B)/tsdbc-bench: tools/tsdbc-bench.c client/tsdb_client.h tools/common.h tools/datasets.h tools/cbench.h $(CLIB)
	$(CC) $(CFLAGS) -o $@ $< $(CLIB) $(ZSTD_LIB) -lpthread -lm $(LINK_EXTRA)

$(B)/type-bench: tools/type-bench.c tools/common.h tools/datasets.h src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

$(B)/twa-bench: tools/twa-bench.c tools/common.h include/tsdb.h $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/scale-bench: tools/scale-bench.c client/tsdb_client.h tools/common.h tools/datasets.h $(CLIB) $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(CLIB) $(LIB) $(LDLIBS)

$(B)/test_stability: tests/test_stability.c $(CLIB)
	$(CC) $(CFLAGS) -Iclient -o $@ $< $(CLIB) $(ZSTD_LIB) -lpthread -lm $(LINK_EXTRA)

$(B)/test_server: tests/test_server.c tests/samekeys.h tests/lttb_ref.h server/metrics.c server/metrics.h $(CLIB) $(LIB)
	$(CC) $(CFLAGS) -Iclient -Iserver -o $@ $< server/metrics.c $(CLIB) $(LIB) $(LDLIBS)

$(B)/test_follow: tests/test_follow.c $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_volumes: tests/test_volumes.c $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_damage: tests/test_damage.c src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_types: tests/test_types.c src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_levels: tests/test_levels.c tools/datasets.h src/internal.h src/levels.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# links against libtsdb.so, as the embedded bindings do; run with LD_LIBRARY_PATH=$(B)
$(B)/test_shared: tests/test_shared.c $(B)/libtsdb.so
	$(CC) $(CFLAGS) -o $@ $< -L$(B) -ltsdb -ldl

$(B)/test_lttb: tests/test_lttb.c tests/lttb_ref.h src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_group: tests/test_group.c $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_twa: tests/test_twa.c $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/libcrashfs.so: tests/jepsen/crashfs.c | $(B)
	$(CC) -O2 -g -std=gnu11 -Wall -fPIC -shared -o $@ $< -ldl -lpthread

$(B)/jepsen: tests/jepsen/jepsen.c $(CLIB)
	$(CC) $(CFLAGS) -Iclient -o $@ $< $(CLIB) $(ZSTD_LIB) -lpthread -lm $(LINK_EXTRA)

# Jepsen-style fault injection: crashes, simulated power loss, I/O errors and pauses while
# clients write and read; a checker verifies durability, atomicity and reads.
JEPSEN_DIR ?= /tmp/tsdb-jepsen
JEPSEN_SEEDS ?= 1 2 3
jepsen: $(B)/jepsen $(B)/tsdbd $(B)/libcrashfs.so
	for io in $(IO); do echo "== tsdbd --io $$io"; \
	    TSDBD_IO=$$io ./$(B)/jepsen --dir $(JEPSEN_DIR) --nemesis kill  --seconds 20 && \
	    for seed in $(JEPSEN_SEEDS); do \
	        TSDBD_IO=$$io ./$(B)/jepsen --dir $(JEPSEN_DIR) --nemesis power --seconds 20 --seed $$seed \
	            --crashfs $(B)/libcrashfs.so || exit 1; done && \
	    TSDBD_IO=$$io ./$(B)/jepsen --dir $(JEPSEN_DIR) --nemesis eio   --seconds 20 --crashfs $(B)/libcrashfs.so && \
	    TSDBD_IO=$$io ./$(B)/jepsen --dir $(JEPSEN_DIR) --nemesis pause --seconds 10 || exit 1; done

# Build and test on AlmaLinux 10 (the production OS) in Podman; takes minutes, logs in $LOGDIR.
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
test: test-quickstart
test-quickstart: $(B)/tsdb $(B)/tsdbd $(B)/embedded-example $(B)/client-example
	B=$(B) tests/test_quickstart.sh

.PHONY: test-quickstart
