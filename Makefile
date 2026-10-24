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

SRC  := $(wildcard src/*.c)
OBJ  := $(SRC:src/%.c=build/%.o)
LIB  := build/libtsdb.a
BINS := build/tsdb build/tsdb-bench build/test_tsdb

all: $(LIB) build/tsdb build/tsdb-bench build/codec-bench build/insert-bench build/tsdbd $(CLIB) build/tsdbc-bench build/scale-bench build/type-bench

build/%.o: src/%.c src/internal.h include/tsdb.h | build
	$(CC) $(CFLAGS) -c -o $@ $<

$(LIB): $(OBJ)
	$(AR) rcs $@ $^

build/tsdb: tools/tsdb-cli.c tools/common.h $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

build/tsdb-bench: tools/tsdb-bench.c tools/common.h tools/datasets.h $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

build/test_tsdb: tests/test_tsdb.c $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

test: build/test_tsdb build/test_types build/test_server build/test_stability build/test_follow build/test_volumes build/test_group build/tsdbd
	./build/test_tsdb
	./build/test_types
	./build/test_group
	./build/test_follow
	./build/test_volumes
	./build/test_server
	./build/test_stability

# Without -march=native: with it, GCC 15.2's AddressSanitizer crashes in memcpy into
# fake-stack frames (use-after-return detection), in code that did not change.
# The runtime-dispatched AVX-512 kernels are still tested.
asan:
	$(MAKE) clean
	$(MAKE) ARCH= CFLAGS="-O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer" \
	        LDFLAGS="-fsanitize=address,undefined" test

build:
	mkdir -p build

clean:
	rm -rf build

# Clients in other languages; not part of `make test`, so the C build needs no
# cargo, Python or JDK. Each runs its tests against build/tsdbd.
PYVENV ?= build/pyvenv

test-rust: build/tsdbd
	cd client/rust && cargo test

test-python: build/tsdbd
	test -x $(PYVENV)/bin/maturin || (python3 -m venv $(PYVENV) && $(PYVENV)/bin/pip install -q maturin pytest)
	cd client/python && VIRTUAL_ENV=$(abspath $(PYVENV)) $(abspath $(PYVENV))/bin/maturin develop --release \
	    && $(abspath $(PYVENV))/bin/python -m pytest tests

test-java: build/tsdbd
	cd client/java && mvn -q test

test-clients: test-rust test-python test-java

.PHONY: all test asan clean jepsen test-rust test-python test-java test-clients

build/codec-bench: tools/codec-bench.c tools/common.h tools/datasets.h $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

build/insert-bench: tools/insert-bench.c tools/common.h tools/datasets.h $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

build/tsdbd: server/tsdbd.c include/tsdb_protocol.h client/tsdb_pack.h build/tsdb_pack.o $(LIB)
	$(CC) $(CFLAGS) -o $@ $< build/tsdb_pack.o $(LIB) $(LDLIBS) -luring

CLIB := build/libtsdbclient.a

build/tsdb_client.o: client/tsdb_client.c client/tsdb_client.h include/tsdb_protocol.h include/tsdb.h | build
	$(CC) $(CFLAGS) -c -o $@ $<

build/tsdb_pack.o: client/tsdb_pack.c client/tsdb_pack.h include/tsdb_protocol.h | build
	$(CC) $(CFLAGS) -c -o $@ $<

$(CLIB): build/tsdb_client.o build/tsdb_pack.o
	$(AR) rcs $@ $^

build/tsdbc-bench: tools/tsdbc-bench.c client/tsdb_client.h tools/common.h tools/datasets.h $(CLIB)
	$(CC) $(CFLAGS) -o $@ $< $(CLIB) -lzstd -lpthread -lm

build/type-bench: tools/type-bench.c tools/common.h tools/datasets.h src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

build/scale-bench: tools/scale-bench.c client/tsdb_client.h tools/common.h tools/datasets.h $(CLIB) $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(CLIB) $(LIB) $(LDLIBS)

build/test_stability: tests/test_stability.c $(CLIB)
	$(CC) $(CFLAGS) -Iclient -o $@ $< $(CLIB) -lzstd -lpthread -lm

build/test_server: tests/test_server.c $(CLIB) $(LIB)
	$(CC) $(CFLAGS) -Iclient -o $@ $< $(CLIB) $(LIB) $(LDLIBS)

build/test_follow: tests/test_follow.c $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

build/test_volumes: tests/test_volumes.c $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

build/test_types: tests/test_types.c src/internal.h $(LIB)
	$(CC) $(CFLAGS) -Isrc -o $@ $< $(LIB) $(LDLIBS)

build/test_group: tests/test_group.c $(LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LIB) $(LDLIBS)

build/libcrashfs.so: tests/jepsen/crashfs.c | build
	$(CC) -O2 -g -std=gnu11 -Wall -fPIC -shared -o $@ $< -ldl -lpthread

build/jepsen: tests/jepsen/jepsen.c $(CLIB)
	$(CC) $(CFLAGS) -Iclient -o $@ $< $(CLIB) -lzstd -lpthread -lm

# Jepsen-style fault injection: crashes, simulated power loss and pauses while
# clients write and read; a checker verifies durability, atomicity and reads.
JEPSEN_DIR ?= /tmp/tsdb-jepsen
jepsen: build/jepsen build/tsdbd build/libcrashfs.so
	./build/jepsen --dir $(JEPSEN_DIR) --nemesis kill  --seconds 20
	./build/jepsen --dir $(JEPSEN_DIR) --nemesis power --seconds 20 --crashfs build/libcrashfs.so
	./build/jepsen --dir $(JEPSEN_DIR) --nemesis pause --seconds 10
