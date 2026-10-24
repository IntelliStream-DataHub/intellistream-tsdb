CC      ?= cc
# -march=native gives the codecs BMI/LZCNT; the aggregation kernels pick
# AVX-512/AVX2/scalar at runtime either way. `make PORTABLE=1` for a generic build.
ifeq ($(PORTABLE),1)
ARCH    ?=
else
ARCH    ?= -march=native
endif
CFLAGS  ?= -O3 -g
override CFLAGS += -std=gnu11 -Wall -Wextra -Wno-unused-parameter -fno-math-errno $(ARCH) -Iinclude -fPIC
LDLIBS  += -lzstd -lpthread -lm
# Output directory; `make tsan` builds into its own, next to a normal build.
B       ?= build
TSDBD   ?= $(B)/tsdbd

SRC  := $(wildcard src/*.c)
OBJ  := $(SRC:src/%.c=$(B)/%.o)
LIB  := $(B)/libtsdb.a
BINS := $(B)/tsdb $(B)/tsdb-bench $(B)/test_tsdb

all: $(LIB) $(B)/tsdb $(B)/tsdb-bench $(B)/codec-bench $(B)/insert-bench $(B)/tsdbd $(CLIB) $(B)/tsdbc-bench $(B)/scale-bench $(B)/type-bench

$(B)/%.o: src/%.c src/internal.h include/tsdb.h | $(B)
	$(CC) $(CFLAGS) -c -o $@ $<

# the calendar helper is client-side code, in both libraries: the engine itself knows UTC only
$(LIB): $(OBJ) $(B)/tsdb_calendar.o
	$(AR) rcs $@ $^

$(B)/tsdb_calendar.o: client/tsdb_calendar.c include/tsdb_calendar.h include/tsdb.h | $(B)
	$(CC) $(CFLAGS) -c -o $@ $<

$(B)/tsdb: tools/tsdb-cli.c tools/common.h $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/tsdb-bench: tools/tsdb-bench.c tools/common.h tools/datasets.h $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_tsdb: tests/test_tsdb.c tests/samekeys.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

test: $(B)/test_tsdb $(B)/test_types $(B)/test_server $(B)/test_stability $(B)/test_follow $(B)/test_volumes $(B)/test_group $(B)/tsdbd
	./$(B)/test_tsdb
	./$(B)/test_types
	./$(B)/test_group
	./$(B)/test_follow
	./$(B)/test_volumes
	TSDBD=$(TSDBD) ./$(B)/test_server
	TSDBD=$(TSDBD) ./$(B)/test_stability

# Without -march=native: with it, GCC 15.2's AddressSanitizer crashes in memcpy into
# fake-stack frames (use-after-return detection), in code that did not change.
# The runtime-dispatched AVX-512 kernels are still tested.
asan:
	$(MAKE) clean
	$(MAKE) ARCH= CFLAGS="-O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer" \
	        LDFLAGS="-fsanitize=address,undefined" test

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
	rm -rf build build-tsan

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

.PHONY: all test asan tsan clean jepsen test-rust test-python test-java test-clients

$(B)/codec-bench: tools/codec-bench.c tools/common.h tools/datasets.h $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/insert-bench: tools/insert-bench.c tools/common.h tools/datasets.h $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/tsdbd: server/tsdbd.c include/tsdb_protocol.h client/tsdb_pack.h $(B)/tsdb_pack.o $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(B)/tsdb_pack.o $(LIB) $(LDLIBS) -luring

CLIB := $(B)/libtsdbclient.a

$(B)/tsdb_client.o: client/tsdb_client.c client/tsdb_client.h include/tsdb_protocol.h include/tsdb.h | $(B)
	$(CC) $(CFLAGS) -c -o $@ $<

$(B)/tsdb_pack.o: client/tsdb_pack.c client/tsdb_pack.h include/tsdb_protocol.h | $(B)
	$(CC) $(CFLAGS) -c -o $@ $<

$(CLIB): $(B)/tsdb_client.o $(B)/tsdb_pack.o $(B)/tsdb_calendar.o
	$(AR) rcs $@ $^

$(B)/tsdbc-bench: tools/tsdbc-bench.c client/tsdb_client.h tools/common.h tools/datasets.h $(CLIB)
	$(CC) $(CFLAGS) -o $@ $< $(CLIB) -lzstd -lpthread -lm

$(B)/type-bench: tools/type-bench.c tools/common.h tools/datasets.h src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

$(B)/scale-bench: tools/scale-bench.c client/tsdb_client.h tools/common.h tools/datasets.h $(CLIB) $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(CLIB) $(LIB) $(LDLIBS)

$(B)/test_stability: tests/test_stability.c $(CLIB)
	$(CC) $(CFLAGS) -Iclient -o $@ $< $(CLIB) -lzstd -lpthread -lm

$(B)/test_server: tests/test_server.c tests/samekeys.h $(CLIB) $(LIB)
	$(CC) $(CFLAGS) -Iclient -o $@ $< $(CLIB) $(LIB) $(LDLIBS)

$(B)/test_follow: tests/test_follow.c $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_volumes: tests/test_volumes.c $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_types: tests/test_types.c src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

$(B)/test_group: tests/test_group.c $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

$(B)/libcrashfs.so: tests/jepsen/crashfs.c | $(B)
	$(CC) -O2 -g -std=gnu11 -Wall -fPIC -shared -o $@ $< -ldl -lpthread

$(B)/jepsen: tests/jepsen/jepsen.c $(CLIB)
	$(CC) $(CFLAGS) -Iclient -o $@ $< $(CLIB) -lzstd -lpthread -lm

# Jepsen-style fault injection: crashes, simulated power loss and pauses while
# clients write and read; a checker verifies durability, atomicity and reads.
JEPSEN_DIR ?= /tmp/tsdb-jepsen
jepsen: $(B)/jepsen $(B)/tsdbd $(B)/libcrashfs.so
	./$(B)/jepsen --dir $(JEPSEN_DIR) --nemesis kill  --seconds 20
	./$(B)/jepsen --dir $(JEPSEN_DIR) --nemesis power --seconds 20 --crashfs $(B)/libcrashfs.so
	./$(B)/jepsen --dir $(JEPSEN_DIR) --nemesis pause --seconds 10
