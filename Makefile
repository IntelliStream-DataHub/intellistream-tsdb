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
# the fuzz harnesses (tests/fuzz/fuzz_<name>.c; see `make fuzz`)
FUZZ_HARNESSES := codec part manifest wal dict volumes tz
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

all: $(LIB) $(B)/libtsdb.so $(B)/tsdb $(B)/tsdb-bench $(B)/codec-bench $(B)/insert-bench $(B)/tsdbd $(CLIB) $(B)/tsdbc-bench $(B)/scale-bench $(B)/type-bench $(B)/twa-bench $(B)/fold-bench

$(B)/%.o: src/%.c src/internal.h src/levels.h include/tsdb.h | $(B)
	$(CC) $(CFLAGS) -c -o $@ $<
# simd.c includes its kernels twice each (SSE and AVX, AVX and AVX2)
$(B)/simd.o: src/simd_avx.inc src/simd_sse.inc
# Its lanes start at zero (arrays of 16 and 32 doubles): GCC's generic tuning (the dist build) clears 33 to 8192
# known bytes with `rep stosq`, whose startup (some 30 cycles) was a third of a short run's fold (fold-bench --rows
# 32, sse kernels); -march=native picks vector stores itself. Vector stores up to 512 bytes, memset beyond.
MEMSET_FLAG := $(shell $(CC) -mmemset-strategy=vector_loop:512:noalign,libcall:-1:noalign -E -x c /dev/null \
                 >/dev/null 2>&1 && echo -mmemset-strategy=vector_loop:512:noalign,libcall:-1:noalign)
$(B)/simd.o: override CFLAGS += $(MEMSET_FLAG)

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
	$(CC) $(CFLAGS) -DTSDB_BUILD_VERSION='"$(TSDB_VERSION)"' -o $@ $< $(LIB) $(LDLIBS)

$(B)/tsdb-bench: tools/tsdb-bench.c tools/common.h tools/datasets.h tools/cbench.h $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_tsdb: tests/test_tsdb.c tests/samekeys.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# its own write/fsync/fdatasync/pwrite/renameat inject storage errors into the library
$(B)/test_failstop: tests/test_failstop.c $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

test: $(B)/test_tsdb $(B)/test_shared $(B)/test_failstop $(B)/test_damage $(B)/test_types $(B)/test_delete $(B)/test_levels $(B)/test_server $(B)/test_stability $(B)/test_ports $(B)/test_client $(B)/test_follow $(B)/test_volumes $(B)/test_group $(B)/test_lttb $(B)/test_twa $(B)/test_spread $(B)/test_limits $(B)/test_crafted $(B)/test_tzfiles $(FUZZ_HARNESSES:%=$(B)/fuzz-%) $(B)/tsdbd $(B)/libcrashfs.so $(B)/tsdb $(B)/fuzz_frames
	./$(B)/test_tsdb
	LD_LIBRARY_PATH=$(B) ./$(B)/test_shared
	./$(B)/test_failstop
	./$(B)/test_damage
	./$(B)/test_crafted
	./$(B)/test_tzfiles
	for h in $(FUZZ_HARNESSES); do ./$(B)/fuzz-$$h -replay tests/fuzz/corpus/$$h || exit 1; done
	./$(B)/test_types
	./$(B)/test_delete
	./$(B)/test_levels
	./$(B)/test_group
	./$(B)/test_lttb
	B=$(B) tests/test_cli_lttb.sh
	./$(B)/test_twa
	./$(B)/test_spread
	B=$(B) tests/test_cli_spread.sh
	B=$(B) tests/test_cli_input.sh
	./$(B)/test_limits
	./$(B)/test_follow
	./$(B)/test_volumes
	./$(B)/fuzz_frames replay tests/fuzz/frames
	TSDBD=$(TSDBD) ./$(B)/test_ports
	./$(B)/test_client
	for io in $(IO); do echo "== tsdbd --io $$io"; \
	    TSDBD=$(TSDBD) TSDBD_IO=$$io ./$(B)/test_server && \
	    TSDBD=$(TSDBD) TSDBD_IO=$$io ./$(B)/test_stability || exit 1; done

# ------------------------------------------------------------------ fuzzing
# tests/fuzz/: one harness per decoder or loader of what lies on disk (driver.c has the details).
# `make fuzz FUZZ_SECONDS=N [FUZZ="codec part"]` runs each for N seconds, built in build-fuzz/ with
# ASan + UBSan and GCC's trace-pc coverage (or libFuzzer when FUZZ_CC=clang); new inputs and
# crash-*/timeout-* files go to build-fuzz/out/<harness>/. `make test` replays the committed corpus
# (tests/fuzz/corpus/<harness>/) through the normal build; `make asan` does so under the sanitizers.
# Every input a run found a bug with is copied into the corpus with its fix.
FUZZ           ?= $(FUZZ_HARNESSES)
FUZZ_SECONDS   ?= 60
FUZZ_B         := build-fuzz
FUZZ_CC        ?= $(CC)
FUZZ_SAN       := -O1 -g -fsanitize=address,undefined -fno-sanitize-recover=undefined -fno-omit-frame-pointer
FUZZ_COMMON    := -std=gnu11 -Wall -Wextra -Wno-unused-parameter -fno-math-errno -ffp-contract=off -Iinclude -Isrc -Iclient
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
	$(CC) $(CFLAGS) -Isrc -Iclient -o $@ $< tests/fuzz/driver.c $(LIB) $(LDLIBS)

fuzz-replay: $(FUZZ_HARNESSES:%=$(B)/fuzz-%)
	for h in $(FUZZ_HARNESSES); do FUZZ_TMP=$(or $(TMPDIR),/tmp) ./$(B)/fuzz-$$h -replay tests/fuzz/corpus/$$h || exit 1; done

ifneq ($(B),$(FUZZ_B))
$(FUZZ_B)/libtsdb.a: FORCE
	$(MAKE) B=$(FUZZ_B) CC=$(FUZZ_CC) ARCH= CFLAGS="$(FUZZ_SAN) $(FUZZ_COV)" $@

# the driver is built without coverage: it holds the hook
$(FUZZ_B)/driver.o: tests/fuzz/driver.c $(FUZZ_B)/libtsdb.a
	$(FUZZ_CC) $(FUZZ_COMMON) $(FUZZ_SAN) -c -o $@ $<

$(FUZZ_B)/fuzz-%: tests/fuzz/fuzz_%.c tests/fuzz/fuzz.h tests/fuzz/template.h src/internal.h $(FUZZ_B)/libtsdb.a \
                  $(if $(FUZZ_DRV),$(FUZZ_B)/driver.o)
	$(FUZZ_CC) $(FUZZ_COMMON) $(FUZZ_SAN) $(FUZZ_COV) -c -o $@.o $<
	$(FUZZ_CC) $(FUZZ_SAN) $(FUZZ_LINK) -o $@ $@.o $(if $(FUZZ_DRV),$(FUZZ_B)/driver.o) $(FUZZ_B)/libtsdb.a $(LDLIBS)

fuzz: $(FUZZ:%=$(FUZZ_B)/fuzz-%)
	for h in $(FUZZ); do echo "== fuzz $$h, $(FUZZ_SECONDS) s"; mkdir -p $(FUZZ_B)/out/$$h; \
	    ASAN_OPTIONS=abort_on_error=1:detect_leaks=0 UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
	    $(FUZZ_B)/fuzz-$$h $(if $(FUZZ_DRV),-t $(FUZZ_SECONDS) -o $(FUZZ_B)/out/$$h,-max_total_time=$(FUZZ_SECONDS) \
	        -artifact_prefix=$(FUZZ_B)/out/$$h/) $(FUZZ_B)/out/$$h tests/fuzz/corpus/$$h || exit 1; done

# writes each harness's generated seeds into tests/fuzz/corpus/<harness>/ (after a format change)
fuzz-seeds: $(FUZZ_HARNESSES:%=$(B)/fuzz-%)
	for h in $(FUZZ_HARNESSES); do FUZZ_TMP=$(or $(TMPDIR),/tmp) ./$(B)/fuzz-$$h -seeds tests/fuzz/corpus/$$h || exit 1; done

endif
FORCE:
.PHONY: fuzz fuzz-replay fuzz-seeds FORCE

# Without -march=native: with it, GCC 15.2's AddressSanitizer crashes in memcpy into
# fake-stack frames (use-after-return detection), in code that did not change.
# The runtime-dispatched AVX-512 kernels are still tested.
# UBSan only prints by default and the run still exits 0: halt_on_error makes the first report fail
# it. `ubsan-check` proves that with a program that overflows on purpose, then runs the CLI's parsers on
# out-of-range input with float-cast-overflow added (GCC's `undefined` leaves it out).
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
	$(CC) $(ASAN_FLAGS) -fsanitize=float-cast-overflow -o $(B)/ubsan_cli_parse tests/ubsan_cli_parse.c
	UBSAN_OPTIONS="$(UBSAN_RUN)" ./$(B)/ubsan_cli_parse

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
	rm -rf build build-tsan build-fuzz build-fuzz-frames $(DIST_B)

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
DIST_TESTS  = test_tsdb test_types test_levels test_group test_lttb test_twa test_spread test_limits test_follow test_volumes test_server test_stability test_ports
DIST_FILES  = $(addprefix $(DIST_B)/,$(DIST_BINS) $(DIST_TESTS) libcrashfs.so)

DIST_SIG    = $$($(CC) --version | head -n 1) | $(CC) | $(CFLAGS) | $(subst ",,$(DIST_VARS)) | $(LV_FLAGS_v3) | $(LV_FLAGS_v4) \
              | $$(cksum <tools/glibc-compat.h)

dist:
	@sig="$(DIST_SIG)"; \
	if [ "$$(cat $(DIST_B)/.flags 2>/dev/null)" != "$$sig" ]; then \
	    [ -d $(DIST_B) ] && echo "dist: compiler or flags changed since the last build: rebuilding $(DIST_B)"; \
	    rm -rf $(DIST_B) && mkdir -p $(DIST_B) && printf '%s\n' "$$sig" >$(DIST_B)/.flags; \
	fi
	$(MAKE) $(DIST_VARS) all $(DIST_TESTS:%=$(DIST_B)/%) $(DIST_B)/libcrashfs.so
	tools/check-glibc.sh -c $(GLIBC_MAX) $(DIST_FILES)
	tools/check-isa.sh $(DIST_B) $(DIST_FILES)
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

test-rust: $(B)/tsdbd
	cd client/rust && cargo test --features embedded
	cd client/rust && cargo build --all-targets  # the TCP-only crate (no features) must still build
	cd client/rust-sys && cargo test

test-python: $(B)/tsdbd
	test -x $(PYVENV)/bin/maturin || (python3 -m venv $(PYVENV) && $(PYVENV)/bin/pip install -q maturin pytest)
	cd client/python && VIRTUAL_ENV=$(abspath $(PYVENV)) $(abspath $(PYVENV))/bin/maturin develop --release \
	    && $(abspath $(PYVENV))/bin/python -m pytest tests

# The Python sdist carries tsdb-sys as a path dependency, so the engine's C sources travel in it as the copy
# client/rust-sys/vendor.sh makes (csrc/). test-python-sdist builds a wheel from the sdist alone and imports it.
python-sdist:
	test -x $(PYVENV)/bin/maturin || (python3 -m venv $(PYVENV) && $(PYVENV)/bin/pip install -q maturin pytest)
	client/rust-sys/vendor.sh
	rm -rf $(B)/sdist
	cd client/python && VIRTUAL_ENV=$(abspath $(PYVENV)) $(abspath $(PYVENV))/bin/maturin sdist -o $(abspath $(B))/sdist

test-python-sdist: python-sdist
	rm -rf $(B)/sdist-check && mkdir -p $(B)/sdist-check/whl
	PATH=$(abspath $(PYVENV))/bin:$$PATH $(PYVENV)/bin/pip wheel -q --no-build-isolation --no-deps -w $(B)/sdist-check/whl $(B)/sdist/tsdb_client-*.tar.gz
	cd $(B)/sdist-check && unzip -q whl/tsdb_client-*.whl -d site \
	    && $(abspath $(PYVENV))/bin/python -I -c "import os, sys; sys.path.insert(0, os.path.abspath('site')); import tsdb_client; \
	    assert tsdb_client.__file__.startswith('$(abspath $(B))/sdist-check/site'), tsdb_client.__file__; tsdb_client.Db"

test-java: $(B)/tsdbd $(B)/tsdb
	cd client/java && mvn -q test

# The embedded Java binding (Java 22+, FFM) needs the TCP client's jar for the shared types. It is installed into a
# local repository of this tree's build (read through to ~/.m2), never into the ~/.m2 that other trees share.
JAVA_M2 := -Dmaven.repo.local=$(abspath $(B))/m2 -Dmaven.repo.local.tail=$(HOME)/.m2/repository
test-java-embedded: $(B)/tsdbd $(B)/libtsdb.so
	cd client/java && mvn -q $(JAVA_M2) install -DskipTests
	cd client/java-embedded && mvn -q $(JAVA_M2) test

test-clients: test-rust test-python test-python-sdist test-java test-java-embedded

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

.PHONY: check-version fuzz-frames jepsen-selftest bench-clients-smoke hooks all test asan ubsan-check tsan clean jepsen test-almalinux10 test-rust test-python test-java test-java-embedded test-clients python-sdist test-python-sdist dist dist-test dist-check-almalinux10 dist-check-sandybridge integ-rust integ-python integ-java integration-tests integration-tests-dist integration-selftest

$(B)/codec-bench: tools/codec-bench.c tools/common.h tools/datasets.h $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/insert-bench: tools/insert-bench.c tools/common.h tools/datasets.h $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

# The release version is the file VERSION (also in include/tsdb.h and the client manifests: `make check-version`).
# TSDB_VERSION is the build id shown next to it by `tsdbd --version`, `tsdb --version` and tsdb_build_info.
RELEASE_VERSION := $(shell cat VERSION)
TSDB_VERSION ?= $(shell git describe --always --dirty 2>/dev/null || echo dev)

$(B)/tsdbd: server/tsdbd.c server/metrics.c server/metrics.h include/tsdb_protocol.h client/tsdb_pack.h $(B)/tsdb_pack.o $(LIB)
	$(CC) $(CFLAGS) $(URING_FLAGS) -DTSDB_BUILD_VERSION='"$(TSDB_VERSION)"' -o $@ $< server/metrics.c $(B)/tsdb_pack.o $(LIB) $(LDLIBS) $(URING_LIBS)

# tests/fuzz/fuzz_frames.c includes server/tsdbd.c (epoll only: no ring needed without sockets). FRAMES_COV is
# -fsanitize-coverage=trace-pc in `make fuzz-frames`, which feeds its coverage-guided driver; empty in `make test`
# (FUZZ_COV is the on-disk harnesses' coverage flag and is always set).
$(B)/fuzz_frames: tests/fuzz/fuzz_frames.c server/tsdbd.c server/metrics.c server/metrics.h include/tsdb_protocol.h \
                  client/tsdb_pack.h $(B)/tsdb_pack.o $(LIB)
	$(CC) $(CFLAGS) $(FRAMES_COV) -DTSDBD_NO_URING -o $@ $< server/metrics.c $(B)/tsdb_pack.o $(LIB) $(LDLIBS)

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

$(B)/fold-bench: tools/fold-bench.c tools/common.h src/internal.h $(LIB)
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

$(B)/test_server: tests/test_server.c tests/harness.h tests/samekeys.h tests/lttb_ref.h server/metrics.c server/metrics.h $(CLIB) $(LIB)
	$(CC) $(CFLAGS) -Iclient -Iserver -o $@ $< server/metrics.c $(CLIB) $(LIB) $(LDLIBS)

$(B)/test_follow: tests/test_follow.c $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_volumes: tests/test_volumes.c $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_damage: tests/test_damage.c src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# Crafted files in the data directory and extreme inputs (internals: -Isrc)
$(B)/test_crafted: tests/test_crafted.c src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

# Crafted TZif files, rules and zone names; includes the calendar's source to see its cache
$(B)/test_tzfiles: tests/test_tzfiles.c client/tsdb_calendar.c include/tsdb_calendar.h
	$(CC) $(CFLAGS) -Iclient -o $@ $< -lpthread

$(B)/test_types: tests/test_types.c src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_delete: tests/test_delete.c src/internal.h $(LIB)
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

$(B)/test_spread: tests/test_spread.c src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_limits: tests/test_limits.c $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

# with ARCH: in `make dist` it carries the glibc-compat header, since the tarball's test_server preloads it
$(B)/libcrashfs.so: tests/jepsen/crashfs.c | $(B)
	$(CC) -O2 -g -std=gnu11 -Wall $(ARCH) -fPIC -shared -o $@ $< -ldl -lpthread

$(B)/jepsen: tests/jepsen/jepsen.c tests/harness.h $(CLIB)
	$(CC) $(CFLAGS) -Iclient -o $@ $< $(CLIB) $(ZSTD_LIB) -lpthread -lm $(LINK_EXTRA)

# Jepsen-style fault injection: crashes, simulated power loss, I/O errors and pauses while
# clients write and read; a checker verifies durability, atomicity and reads.
# One directory per tree: worktrees run make jepsen at the same time, and a shared directory turns the other run's
# activity into reported "lost writes". jepsen also refuses a directory another run holds.
JEPSEN_DIR ?= /tmp/tsdb-jepsen-$(shell printf %s "$(CURDIR)" | cksum | cut -d' ' -f1)
JEPSEN_SEEDS ?= 1 2 3
jepsen: $(B)/jepsen $(B)/tsdbd $(B)/libcrashfs.so jepsen-selftest
	for io in $(IO); do echo "== tsdbd --io $$io"; \
	    TSDBD_IO=$$io ./$(B)/jepsen --dir $(JEPSEN_DIR) --nemesis kill  --seconds 20 && \
	    for seed in $(JEPSEN_SEEDS); do \
	        TSDBD_IO=$$io ./$(B)/jepsen --dir $(JEPSEN_DIR) --nemesis power --seconds 20 --seed $$seed \
	            --crashfs $(B)/libcrashfs.so || exit 1; done && \
	    TSDBD_IO=$$io ./$(B)/jepsen --dir $(JEPSEN_DIR) --nemesis eio   --seconds 20 --crashfs $(B)/libcrashfs.so && \
	    TSDBD_IO=$$io ./$(B)/jepsen --dir $(JEPSEN_DIR) --nemesis pause --seconds 10 || exit 1; done

# The harness has to notice what it is meant to: a server that crashes (SIGSEGV, not crashfs's exit 99) fails the
# power run, and a power run that restarts too rarely fails too.
jepsen-selftest: $(B)/jepsen $(B)/tsdbd $(B)/libcrashfs.so
	@out=$$(JEPSEN_TSDBD=tests/jepsen/segv-tsdbd.sh ./$(B)/jepsen --dir $(JEPSEN_DIR)-selftest --nemesis power \
	    --seconds 5 --crashfs $(B)/libcrashfs.so 2>&1); rc=$$?; \
	    if [ $$rc -eq 0 ] || ! echo "$$out" | grep -q 'ended with status 0x8\?b, not a power loss'; then \
	        echo "jepsen-selftest: a SIGSEGV in the server went unnoticed (rc $$rc)"; echo "$$out" | tail -n 20; exit 1; fi; \
	    echo "jepsen-selftest: ok"

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

# The version is in VERSION, include/tsdb.h, the binaries and the client manifests; they must agree.
test: check-version
check-version: $(B)/tsdbd $(B)/tsdb
	tools/check-version.sh $(B)

.PHONY: test-quickstart
