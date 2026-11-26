#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# Soft-RoCE (rdma_rxe) check and setup helper for developer and weekly-gate hosts (docs/design-rdma.md §8, rd-rxe).
#
# Usage:
#   tools/setup-rxe.sh [--check]                   inspect RDMA devices, kernel module, and memlock limit (no root)
#   sudo tools/setup-rxe.sh --setup [--dev rxe0] [--netdev IF]
#                                                  load rdma_rxe and add an rxe link (creates a veth pair if needed)
#   sudo tools/setup-rxe.sh --teardown [--dev rxe0]
#                                                  remove the rxe link (and tsdb-rxe0 veth pair if created by --setup)
#
# Tests never create kernel devices themselves: run this once per boot on a host with rdma_rxe (e.g. Ubuntu with
# linux-modules-extra), then run `TSDB_RDMA_REQUIRED=1 make test-rdma`.
set -eu

mode=check
dev=rxe0
netdev=
veth_a=tsdb-rxe0
veth_b=tsdb-rxe1

usage() {
    sed -n '3,13p' "$0" | sed 's/^# \{0,1\}//'
}

while [ $# -gt 0 ]; do
    case "$1" in
    --check) mode=check ;;
    --setup) mode=setup ;;
    --teardown) mode=teardown ;;
    --dev)
        [ $# -ge 2 ] || { echo "setup-rxe: --dev needs a name" >&2; exit 2; }
        dev=$2; shift
        if ! [[ "$dev" =~ ^[a-zA-Z0-9._-]+$ ]] || [ "$dev" = "." ] || [ "$dev" = ".." ]; then
            echo "setup-rxe: invalid --dev name '$dev'" >&2; exit 2
        fi ;;
    --netdev)
        [ $# -ge 2 ] || { echo "setup-rxe: --netdev needs an interface" >&2; exit 2; }
        netdev=$2; shift
        if ! [[ "$netdev" =~ ^[a-zA-Z0-9._-]+$ ]] || [ "$netdev" = "." ] || [ "$netdev" = ".." ]; then
            echo "setup-rxe: invalid --netdev name '$netdev'" >&2; exit 2
        fi ;;
    -h | --help) usage; exit 0 ;;
    *) echo "setup-rxe: unknown option '$1'" >&2; usage >&2; exit 2 ;;
    esac
    shift
done

check_memlock() {
    local lim
    lim=$(ulimit -l 2>/dev/null || echo unknown)
    echo "memlock limit (ulimit -l): $lim KiB"
    if [ "$lim" != unlimited ] && [ "$lim" != unknown ]; then
        if [ "$lim" -lt 4096 ] 2>/dev/null; then
            echo "  WARNING: RLIMIT_MEMLOCK ($lim KiB) is very low; raise LimitMEMLOCK or ulimit -l"
        elif [ "$lim" -le 8192 ] 2>/dev/null; then
            echo "  note: tests use --peer-rdma-pool-mb 1 to fit within an 8 MiB memlock limit; production tsdbd needs LimitMEMLOCK=infinity"
        fi
    fi
}

do_check() {
    echo "kernel: $(uname -r)"
    check_memlock
    if [ -d /sys/class/infiniband ] && [ -n "$(ls -A /sys/class/infiniband 2>/dev/null)" ]; then
        echo "RDMA devices in /sys/class/infiniband:"
        for d in /sys/class/infiniband/*; do
            [ -e "$d" ] || continue
            local name p pnum st ll
            name=$(basename "$d")
            for p in "$d"/ports/*; do
                [ -e "$p" ] || continue
                pnum=$(basename "$p")
                st=$(cat "$p/state" 2>/dev/null || echo unknown)
                ll=$(cat "$p/link_layer" 2>/dev/null || echo unknown)
                echo "  $name: port $pnum state=[$st] link_layer=[$ll]"
            done
        done
    else
        echo "RDMA devices in /sys/class/infiniband: none"
    fi
    if command -v rdma >/dev/null 2>&1; then
        echo "rdma link:"
        local rlinks
        rlinks=$(rdma link show 2>/dev/null || true)
        if [ -n "$rlinks" ]; then
            printf '%s\n' "$rlinks" | sed 's/^/  /'
        else
            echo "  (none)"
        fi
    else
        echo "rdma tool: not installed (install iproute2)"
    fi
    if modinfo rdma_rxe >/dev/null 2>&1; then
        echo "rdma_rxe module: available ($(modinfo -n rdma_rxe 2>/dev/null))"
    else
        echo "rdma_rxe module: not found for kernel $(uname -r) (on Ubuntu: install linux-modules-extra-$(uname -r); AlmaLinux 10 omits rdma_rxe)"
    fi
    if modinfo siw >/dev/null 2>&1; then
        echo "siw module: available"
    else
        echo "siw module: not found"
    fi
}

pick_netdev() {
    if [ -n "$netdev" ]; then
        return 0
    fi
    # Prefer an existing UP non-loopback Ethernet interface with a non-tentative IP address.
    local iface
    for iface in $(ip -o link show up 2>/dev/null | awk -F': ' '{print $2}' | cut -d@ -f1); do
        case "$iface" in lo | docker* | podman* | br-* | veth* | ib* | virbr* | cni* | flannel* | tun* | tap*) continue ;; esac
        if [ -r "/sys/class/net/$iface/type" ] && [ "$(cat "/sys/class/net/$iface/type")" = 1 ]; then
            if ip -o addr show dev "$iface" 2>/dev/null | grep -v 'tentative' | grep -qE '\binet6?\b'; then
                netdev=$iface
                return 0
            fi
        fi
    done
    # Fallback: create a dedicated veth pair with link-local IPv4 addresses (avoids IPv6-disabled hosts and DAD races).
    if ! ip link show "$veth_a" >/dev/null 2>&1; then
        ip link add "$veth_a" type veth peer name "$veth_b"
    fi
    ip addr replace 169.254.99.1/30 dev "$veth_a"
    ip addr replace 169.254.99.2/30 dev "$veth_b"
    ip link set "$veth_a" up
    ip link set "$veth_b" up
    netdev=$veth_a
}

do_setup() {
    [ "$(id -u)" = 0 ] || { echo "setup-rxe: --setup requires root (run with sudo)" >&2; exit 1; }
    command -v rdma >/dev/null 2>&1 || { echo "setup-rxe: 'rdma' command not found (install iproute2)" >&2; exit 1; }
    if ! modinfo rdma_rxe >/dev/null 2>&1; then
        echo "setup-rxe: kernel module rdma_rxe not found for $(uname -r)" >&2
        echo "  On Ubuntu: sudo apt-get install linux-modules-extra-$(uname -r)" >&2
        exit 1
    fi
    modprobe rdma_rxe
    if [ -d "/sys/class/infiniband/$dev" ]; then
        echo "setup-rxe: $dev already exists"
    else
        pick_netdev
        echo "setup-rxe: adding $dev on netdev $netdev"
        rdma link add "$dev" type rxe netdev "$netdev"
    fi
    local st
    st=$(cat "/sys/class/infiniband/$dev/ports/1/state" 2>/dev/null || echo unknown)
    echo "setup-rxe: $dev port 1 state=[$st]"
    check_memlock
    echo "ready: run 'TSDB_RDMA_REQUIRED=1 TSDB_RDMA_DEV=$dev:1 make test-rdma'"
}

do_teardown() {
    [ "$(id -u)" = 0 ] || { echo "setup-rxe: --teardown requires root (run with sudo)" >&2; exit 1; }
    if command -v rdma >/dev/null 2>&1 && [ -d "/sys/class/infiniband/$dev" ]; then
        rdma link delete "$dev"
        echo "setup-rxe: deleted $dev"
    else
        echo "setup-rxe: $dev not present"
    fi
    if ip link show "$veth_a" >/dev/null 2>&1; then
        ip link delete "$veth_a"
        echo "setup-rxe: deleted veth $veth_a"
    fi
}

case "$mode" in
check) do_check ;;
setup) do_setup ;;
teardown) do_teardown ;;
esac
