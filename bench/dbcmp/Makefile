# Builds the comparison harness into build/dbcmp at the top of the repository (ignored by git).
#
# tsdbq and tsdbstat link the client library, which the top-level Makefile builds. qdrive needs
# only libpq's shared library, not its headers: it declares the few functions it calls. Without
# pkg-config it links libpq.so.5 by name (Debian/Ubuntu: libpq5; Fedora: libpq).
#
# embq, the embedded tsdb / DuckDB driver, is built only when DUCKDB names a directory holding
# duckdb.h and libduckdb.so from a DuckDB C library release (see README.md); the path to the
# library is recorded in the binary, so nothing is installed.
ROOT    := ../..
OUT     := $(ROOT)/build/dbcmp
CC      ?= cc
CFLAGS  ?= -O2 -g
override CFLAGS += -std=gnu11 -Wall -Wextra -Wno-unused-parameter
LIBPQ   ?= $(shell pkg-config --libs libpq 2>/dev/null || echo -l:libpq.so.5)
CLIB    := $(ROOT)/build/libtsdbclient.a
LIB     := $(ROOT)/build/libtsdb.a
DUCKDB  ?= /var/tmp/dbcmp/duckdb/v1.5.6/lib

PROGS := $(addprefix $(OUT)/,conv loadlp qdrive tsdbq tsdbstat)
ifneq ($(wildcard $(DUCKDB)/duckdb.h),)
PROGS += $(OUT)/embq
endif

all: $(PROGS)

$(OUT):
	mkdir -p $@

$(OUT)/conv: conv.c | $(OUT)
	$(CC) $(CFLAGS) -o $@ $<

$(OUT)/loadlp: loadlp.c httpc.h | $(OUT)
	$(CC) $(CFLAGS) -o $@ $< -lpthread

$(OUT)/qdrive: qdrive.c httpc.h | $(OUT)
	$(CC) $(CFLAGS) -o $@ $< $(LIBPQ) -lpthread

$(OUT)/tsdbq: tsdbq.c $(CLIB) | $(OUT)
	$(CC) $(CFLAGS) -I$(ROOT)/client -I$(ROOT)/include -o $@ $< $(CLIB) -lzstd -lpthread -lm

$(OUT)/tsdbstat: tsdbstat.c $(CLIB) | $(OUT)
	$(CC) $(CFLAGS) -I$(ROOT)/client -I$(ROOT)/include -o $@ $< $(CLIB) -lzstd -lpthread

$(OUT)/embq: embq.c $(LIB) | $(OUT)
	$(CC) $(CFLAGS) -I$(ROOT)/include -I$(DUCKDB) -o $@ $< $(LIB) -L$(DUCKDB) -lduckdb -Wl,-rpath,$(abspath $(DUCKDB)) \
		-lzstd -lpthread -lm

$(CLIB) $(LIB):
	$(MAKE) -C $(ROOT) $(patsubst $(ROOT)/%,%,$@)

clean:
	rm -rf $(OUT)

.PHONY: all clean
