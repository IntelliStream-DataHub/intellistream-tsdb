#!/bin/sh
# SPDX-License-Identifier: AGPL-3.0-only
# Installs what the Makefile and the tests need on an ubuntu-24.04 runner (apt only).
#
#   .github/scripts/install-deps.sh [clients]
#
# Always: the C build (gcc, make, binutils for objdump in `make dist`), libzstd (the dev package also has the static
# libzstd.a that `make dist` links), liburing (liburing.a for `make dist` too), pkg-config and python3 (the
# integration harness and several helpers), curl and jq (the release workflow's API calls and their test in
# `make test`; the runner has both, this only says so).
# With "clients": what `make test-clients` and `make integration-tests` use on top: a venv for maturin and pytest,
# and the Python headers PyO3 looks for. Rust, Java and Maven come from their own steps in the workflow
# (rust-toolchain, setup-java; Maven is preinstalled on the runner).
set -eu
pkgs="build-essential binutils libzstd-dev liburing-dev pkg-config python3 curl jq"
for g in "$@"; do
    case $g in
    clients) pkgs="$pkgs python3-venv python3-dev python3-pip" ;;
    *) echo "install-deps: unknown group '$g'" >&2; exit 2 ;;
    esac
done
sudo apt-get update -q
# shellcheck disable=SC2086
sudo apt-get install -y -q --no-install-recommends $pkgs
