# Builds the comparison harness into build/dbcmp at the top of the repository (ignored by git).
#
# tsdbq and tsdbstat link the client library, which the top-level Makefile builds. qdrive needs
# only libpq's shared library, not its headers: it declares the few functions it calls. Without
# pkg-config it links libpq.so.5 by name (Debian/Ubuntu: libpq5; Fedora: libpq).
ROOT    := ../..
OUT     := $(ROOT)/build/dbcmp
CC      ?= cc
CFLAGS  ?= -O2 -g
override CFLAGS += -std=gnu11 -Wall -Wextra -Wno-unused-parameter
LIBPQ   ?= $(shell pkg-config --libs libpq 2>/dev/null || echo -l:libpq.so.5)
CLIB    := $(ROOT)/build/libtsdbclient.a

PROGS := $(addprefix $(OUT)/,conv loadlp qdrive tsdbq tsdbstat)

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
	$(CC) $(CFLAGS) -I$(ROOT)/client -I$(ROOT)/include -o $@ $< $(CLIB) -lpthread -lm

$(OUT)/tsdbstat: tsdbstat.c $(CLIB) | $(OUT)
	$(CC) $(CFLAGS) -I$(ROOT)/client -I$(ROOT)/include -o $@ $< $(CLIB) -lpthread

$(CLIB):
	$(MAKE) -C $(ROOT) build/libtsdbclient.a

clean:
	rm -rf $(OUT)

.PHONY: all clean
