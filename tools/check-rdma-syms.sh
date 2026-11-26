#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# tools/check-rdma-syms.sh [--rdma] [--rdma-cm] BINARY...: the verbs and rdma_cm transports load libibverbs and
# librdmacm with dlopen (server/plrdma_dl.c, docs/design-rdma.md §5.4, D4), so a binary must name no symbol of
# rdma-core (ibv_*, rdma_*, the providers' mlx*dv_*) and need no RDMA library: one binary runs with and without
# rdma-core. verbs.h has macros and inlines that call exported entry points (ibv_get_device_list, ibv_reg_mr,
# ibv_query_port): a call of one by its name breaks this.
# --rdma: the verbs transport must be in the binary (libibverbs.so.1).
# --rdma-cm: Option C rdma_cm must be in the binary (librdmacm.so.1).
set -u
want_rdma=0
want_cm=0
while [ $# -gt 0 ]; do
    case "$1" in
    --rdma) want_rdma=1; shift ;;
    --rdma-cm) want_cm=1; shift ;;
    *) break ;;
    esac
done
[ $# -ge 1 ] || { echo "usage: $0 [--rdma] [--rdma-cm] BINARY..." >&2; exit 2; }
rc=0
for f in "$@"; do
    [ -f "$f" ] || { echo "check-rdma-syms: $f: no such file" >&2; rc=1; continue; }
    syms=$(nm -u -P "$f" 2>/dev/null) || { echo "check-rdma-syms: nm failed on $f" >&2; rc=1; continue; }
    bad=$(printf '%s\n' "$syms" | awk '{print $1}' | grep -E '^(__)?(ibv|rdma|mlx[0-9]*dv|efadv|hnsdv)_' || true)
    if [ -n "$bad" ]; then
        echo "check-rdma-syms: $f names rdma-core symbols (call them through dlsym, server/plrdma_dl.c):" >&2
        printf '  %s\n' $bad >&2
        rc=1
    fi
    if command -v readelf >/dev/null 2>&1; then
        need=$(readelf -d "$f" 2>/dev/null | grep -E 'NEEDED.*(libibverbs|librdmacm|libmlx)' || true)
        [ -z "$need" ] || { echo "check-rdma-syms: $f links an RDMA library: $need" >&2; rc=1; }
    fi
    if [ $want_rdma = 1 ] && ! grep -q -a 'libibverbs.so.1' "$f"; then
        echo "check-rdma-syms: $f was to have the verbs transport (TSDB_RDMA) and does not load libibverbs" >&2
        rc=1
    fi
    if [ $want_cm = 1 ] && ! grep -q -a 'librdmacm.so.1' "$f"; then
        echo "check-rdma-syms: $f was to have rdma_cm (TSDB_RDMA_CM) and does not load librdmacm" >&2
        rc=1
    fi
done
[ $rc = 0 ] && echo "check-rdma-syms: ok ($*$([ $want_rdma = 1 ] && echo ', with the transport')$([ $want_cm = 1 ] && echo '+cm'))"
exit $rc
