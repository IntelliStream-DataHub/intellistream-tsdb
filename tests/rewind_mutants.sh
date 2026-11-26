#!/bin/bash
# SPDX-License-Identifier: AGPL-3.0-only
# Mutation run for the directory scans: every `rewinddir(` in src/, server/, and tools/ is replaced with `(void)0;`,
# one at a time, and the test suite must fail (a dup shares its offset with the underlying fd, so a scan without
# rewinddir misses entries once an earlier scan moved the offset). Usage: tests/rewind_mutants.sh NEWDIR
#
# Builds a copy of HEAD in NEWDIR (which must not exist: nothing is deleted), with build/libcrashfs.so (without it
# crashfs-based tests skip or fail on the clean tree, and every mutant would "die" of that), runs every test once on
# the clean copy (a control: it must be green, or no result means anything), then tests every discovered rewinddir site.
# Sites are discovered dynamically across *.c, *.h, and *.inc in src/, server/, and tools/ and identified stably by
# file:function (plus #N when a function has more than one), never by raw line numbers.
#
# A site in EQUIVALENT is one whose descriptor was opened immediately before the scan and never scanned (or whose
# readdir output is unused / redundant / dead / unobservable under quiescent local-FS tests), so removing its rewinddir
# changes no observable behaviour and no test can fail. The run fails if any non-equivalent site survives, if any
# equivalent site is killed, or if any EQUIVALENT entry no longer matches a discovered site.
set -uo pipefail

dir=${1:?usage: rewind_mutants.sh NEWDIR}
[ ! -e "$dir" ] || { echo "rewind_mutants: $dir exists" >&2; exit 2; }
root=$(cd "$(dirname "$0")/.." && pwd) || exit 2

# Safe process termination helpers (explicitly refuse pid 0, -1, or non-numeric values).
kill_pid() {
    local sig=$1 p=$2
    [[ $p =~ ^[1-9][0-9]*$ ]] || return 0
    kill -"$sig" "$p" 2>/dev/null || true
}

descendants_of() {
    local p=$1 c
    [[ $p =~ ^[1-9][0-9]*$ ]] || return 0
    if command -v pgrep >/dev/null 2>&1; then
        for c in $(pgrep -P "$p" 2>/dev/null || true); do
            descendants_of "$c"
            printf '%s\n' "$c"
        done
    else
        local pid ppid
        while read -r pid ppid; do
            if [ "$ppid" = "$p" ]; then
                descendants_of "$pid"
                printf '%s\n' "$pid"
            fi
        done < <(ps -eo pid=,ppid= 2>/dev/null || true)
    fi
}

terminate_pids() {
    local -a roots=() all=()
    local p d alive=0
    for p in "$@"; do
        [[ $p =~ ^[1-9][0-9]*$ ]] || continue
        kill -0 "$p" 2>/dev/null || continue
        roots+=("$p")
    done
    [ "${#roots[@]}" -gt 0 ] || return 0
    for p in "${roots[@]}"; do
        while IFS= read -r d; do
            [[ $d =~ ^[1-9][0-9]*$ ]] && all+=("$d")
        done < <(descendants_of "$p")
        all+=("$p")
    done
    for p in "${all[@]}"; do kill_pid TERM "$p"; done
    for p in "${all[@]}"; do
        if kill -0 "$p" 2>/dev/null; then
            alive=1
            break
        fi
    done
    if [ "$alive" -eq 1 ]; then
        sleep 0.2
        for p in "${all[@]}"; do kill_pid KILL "$p"; done
    fi
    for p in "${roots[@]}"; do wait "$p" 2>/dev/null || true; done
}

active_pids=()
tmp_files=()
cleanup_done=0

cleanup_all() {
    [ "$cleanup_done" -eq 1 ] && return 0
    cleanup_done=1
    local -a live=("${active_pids[@]}")
    local j
    while IFS= read -r j; do
        [[ $j =~ ^[1-9][0-9]*$ ]] && live+=("$j")
    done < <(jobs -rp 2>/dev/null || true)
    terminate_pids "${live[@]}"
    local tf
    for tf in "${tmp_files[@]}"; do
        [ -n "$tf" ] && rm -rf "$tf" 2>/dev/null || true
    done
}

trap 'cleanup_all' EXIT
trap 'cleanup_all; exit 130' INT
trap 'cleanup_all; exit 143' TERM
trap 'cleanup_all; exit 129' HUP

mkdir -p "$dir" || exit 2
dir=$(cd "$dir" && pwd) || exit 2
(cd "$root" && git archive HEAD) | tar -x -C "$dir" || exit 2
cd "$dir" || exit 2
export TMPDIR=$dir/scratch
mkdir -p "$TMPDIR"

cpus=$(getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || echo 6)
: "${JOBS:=$cpus}"
: "${MUTANT_JOBS:=$((JOBS > 4 ? 4 : JOBS))}"
[ "$JOBS" -ge 1 ] 2>/dev/null || JOBS=1
[ "$MUTANT_JOBS" -ge 1 ] 2>/dev/null || MUTANT_JOBS=1
ctrl_jobs=$((JOBS > 8 ? 8 : JOBS))

# Wait for one background job to finish without sleeping on non-zero child exits.
wait_one() {
    local rc=0
    wait -n 2>/dev/null || rc=$?
    # wait -n returns 2 if unsupported (bash < 4.3); any other rc means a job finished (or 127 = no jobs).
    [ "$rc" -eq 2 ] && sleep 0.2
    return 0
}

# C-aware scanner shared by discovery and targeted mutation (POSIX awk).
# Preserves exact character columns when stripping comments/strings/char literals so mutations operate on
# exact code spans in the raw line and never touch comments or strings.
read -r -d '' AWK_C_SCAN <<'AWK_EOF' || true
    function is_ident(ch) {
        return ch ~ /^[A-Za-z0-9_]$/
    }
    function strip_c(s,    out, i, n, c, c2, cont) {
        out = ""
        n = length(s)
        cont = (s ~ /\\[ \t\r]*$/)
        for (i = 1; i <= n; i++) {
            c = substr(s, i, 1)
            c2 = substr(s, i, 2)
            if (in_cmt) {
                if (c2 == "*/") {
                    out = out "  "
                    in_cmt = 0
                    i++
                } else {
                    out = out " "
                }
            } else if (in_str != "") {
                if (c == "\\" && i < n) {
                    out = out "  "
                    i++
                } else if (c == in_str) {
                    out = out " "
                    in_str = ""
                } else {
                    out = out " "
                }
            } else {
                if (c2 == "/*") {
                    out = out "  "
                    in_cmt = 1
                    i++
                } else if (c2 == "//") {
                    out = out sprintf("%" (n - i + 1) "s", "")
                    break
                } else if (c == "\"" || c == "\047") {
                    out = out " "
                    in_str = c
                } else {
                    out = out c
                }
            }
        }
        if (in_str != "" && !cont)
            in_str = ""
        return out
    }
    function strip_attrs(s,    pos, p, n, d, pre, rest) {
        while (match(s, /(^|[^A-Za-z0-9_])__attribute__[ \t]*\(/)) {
            pos = RSTART
            if (substr(s, pos, 1) != "_")
                pos++
            p = index(substr(s, pos), "(")
            if (p == 0)
                break
            p = pos + p - 1
            n = length(s)
            d = 0
            while (p <= n) {
                if (substr(s, p, 1) == "(") {
                    d++
                } else if (substr(s, p, 1) == ")") {
                    d--
                    if (d == 0)
                        break
                }
                p++
            }
            if (d != 0)
                break
            pre = substr(s, 1, pos - 1)
            rest = substr(s, p + 1)
            s = pre " " rest
        }
        return s
    }
    function check_eof_state(fname) {
        if (fname != "" && (in_cmt || in_str != "")) {
            printf "rewind_mutants: unterminated comment/string state at EOF of %s\n", fname > "/dev/stderr"
            parse_err = 1
        }
    }
    FNR == 1 {
        if (NR > 1)
            check_eof_state(prev_file)
        prev_file = FILENAME
        fn = ""
        cand = ""
        brace = 0
        in_cmt = 0
        in_str = ""
        in_pp = 0
        pp_depth = 0
    }
    mode == "mutate" {
        clean = strip_c($0)
        if (FNR != target_line) {
            print $0
            next
        }
        pos = 1
        len = length(clean)
        occ = 0
        out_line = $0
        while (pos <= len) {
            rel = index(substr(clean, pos), "rewinddir")
            if (rel == 0)
                break
            col = pos + rel - 1
            before = (col > 1) ? substr(clean, col - 1, 1) : ""
            after = (col + 9 <= len) ? substr(clean, col + 9, 1) : ""
            if (is_ident(before) || is_ident(after)) {
                pos = col + 9
                continue
            }
            p = col + 9
            while (p <= len && substr(clean, p, 1) ~ /^[ \t\r]$/) p++
            if (p > len || substr(clean, p, 1) != "(") {
                pos = col + 9
                continue
            }
            depth = 0
            arg_nonws = 0
            close_p = 0
            while (p <= len) {
                ch = substr(clean, p, 1)
                if (ch == "(") {
                    depth++
                } else if (ch == ")") {
                    depth--
                    if (depth == 0) {
                        close_p = p
                        break
                    }
                } else if (depth >= 1 && ch !~ /^[ \t\r]$/) {
                    arg_nonws = 1
                }
                p++
            }
            if (close_p == 0 || !arg_nonws) {
                pos = col + 9
                continue
            }
            semi_p = close_p + 1
            while (semi_p <= len && substr(clean, semi_p, 1) ~ /^[ \t\r]$/) semi_p++
            if (semi_p > len || substr(clean, semi_p, 1) != ";") {
                pos = close_p + 1
                continue
            }
            occ++
            if (occ == target_occ) {
                out_line = substr($0, 1, col - 1) "(void)0;" substr($0, semi_p + 1)
                replaced = 1
                break
            }
            pos = semi_p + 1
        }
        print out_line
        next
    }
    mode == "discover" {
        clean = strip_c($0)
        pp_cont = ($0 ~ /\\[ \t\r]*$/)
        pp_line = (clean ~ /^[ \t]*#/)
        skip_pp = (pp_line || in_pp)

        if (!skip_pp && brace == 0) {
            hdr = strip_attrs(clean)
            sub(/^[ \t]+/, "", hdr)
            if (hdr ~ /^[A-Za-z_]/ && hdr ~ /\(/ && hdr !~ /=/ && cand == "") {
                c = hdr
                sub(/\(.*/, "", c)
                sub(/[ \t]+$/, "", c)
                sub(/.*[ \t*]/, "", c)
                if (c != "" && c != "__attribute__" && c != "typeof" && c != "_Static_assert" && c != "static_assert" && c != "_Alignas")
                    cand = c
            }
            if (clean ~ /;/)
                cand = ""
            if (clean ~ /\{/ && cand != "") {
                fn = cand
                cand = ""
            }
        }

        # Scan every non-comment/non-string identifier `rewinddir` on this line.
        pos = 1
        len = length(clean)
        occ_on_line = 0
        while (pos <= len) {
            rel = index(substr(clean, pos), "rewinddir")
            if (rel == 0)
                break
            col = pos + rel - 1
            before = (col > 1) ? substr(clean, col - 1, 1) : ""
            after = (col + 9 <= len) ? substr(clean, col + 9, 1) : ""
            if (is_ident(before) || is_ident(after)) {
                pos = col + 9
                continue
            }
            clean_tokens++
            if (skip_pp) {
                printf "rewind_mutants: rewinddir inside preprocessor directive at %s:%d: %s\n", FILENAME, FNR, $0 > "/dev/stderr"
                parse_err = 1
                pos = col + 9
                continue
            }
            p = col + 9
            while (p <= len && substr(clean, p, 1) ~ /^[ \t\r]$/) p++
            if (p > len || substr(clean, p, 1) != "(") {
                printf "rewind_mutants: unparsed rewinddir token at %s:%d: %s\n", FILENAME, FNR, $0 > "/dev/stderr"
                parse_err = 1
                pos = col + 9
                continue
            }
            depth = 0
            arg_nonws = 0
            close_p = 0
            while (p <= len) {
                ch = substr(clean, p, 1)
                if (ch == "(") {
                    depth++
                } else if (ch == ")") {
                    depth--
                    if (depth == 0) {
                        close_p = p
                        break
                    }
                } else if (depth >= 1 && ch !~ /^[ \t\r]$/) {
                    arg_nonws = 1
                }
                p++
            }
            if (close_p == 0 || !arg_nonws) {
                printf "rewind_mutants: unparsed rewinddir(...) call at %s:%d: %s\n", FILENAME, FNR, $0 > "/dev/stderr"
                parse_err = 1
                pos = col + 9
                continue
            }
            semi_p = close_p + 1
            while (semi_p <= len && substr(clean, semi_p, 1) ~ /^[ \t\r]$/) semi_p++
            if (semi_p > len || substr(clean, semi_p, 1) != ";") {
                printf "rewind_mutants: rewinddir(...) not terminated by semicolon on same line at %s:%d: %s\n", FILENAME, FNR, $0 > "/dev/stderr"
                parse_err = 1
                pos = close_p + 1
                continue
            }
            if (fn == "") {
                printf "rewind_mutants: could not attribute enclosing function (<top>) at %s:%d: %s\n", FILENAME, FNR, $0 > "/dev/stderr"
                parse_err = 1
                pos = semi_p + 1
                continue
            }
            occ_on_line++
            ns++
            s_file[ns] = FILENAME
            s_line[ns] = FNR
            s_occ[ns] = occ_on_line
            s_fn[ns] = fn
            k = FILENAME ":" fn
            total[k]++
            fn_idx[ns] = total[k]
            pos = semi_p + 1
        }

        if (pp_line) {
            if (clean ~ /^[ \t]*#[ \t]*(if|ifdef|ifndef)([^A-Za-z0-9_]|$)/) {
                pp_depth++
                pp_brace[pp_depth] = brace
                pp_fn[pp_depth] = fn
                pp_cand[pp_depth] = cand
            } else if (clean ~ /^[ \t]*#[ \t]*(else|elif)([^A-Za-z0-9_]|$)/) {
                if (pp_depth > 0) {
                    brace = pp_brace[pp_depth]
                    fn = pp_fn[pp_depth]
                    cand = pp_cand[pp_depth]
                }
            } else if (clean ~ /^[ \t]*#[ \t]*endif([^A-Za-z0-9_]|$)/) {
                if (pp_depth > 0)
                    pp_depth--
            }
        }
        if (skip_pp) {
            in_pp = pp_cont
        } else {
            n1 = gsub(/\{/, "{", clean)
            n2 = gsub(/\}/, "}", clean)
            brace += n1 - n2
            if (brace <= 0) {
                brace = 0
                if (n2 > 0)
                    fn = ""
            }
        }
    }
    END {
        check_eof_state(prev_file)
        if (mode == "mutate") {
            if (parse_err || !replaced) {
                printf "rewind_mutants: failed to mutate %s:%d (occ %d)\n", prev_file, target_line, target_occ > "/dev/stderr"
                exit 2
            }
            exit 0
        }
        if (ns != clean_tokens) {
            printf "rewind_mutants: discovery mismatch (parsed %d calls vs %d non-comment rewinddir tokens)\n", ns + 0, clean_tokens + 0 > "/dev/stderr"
            parse_err = 1
        }
        if (parse_err)
            exit 2
        for (i = 1; i <= ns; i++) {
            k = s_file[i] ":" s_fn[i]
            id = (total[k] > 1) ? (k "#" fn_idx[i]) : k
            printf "%s\t%s\t%d\t%d\n", id, s_file[i], s_line[i], s_occ[i]
        }
    }
AWK_EOF

discover_sites() {
    awk -v mode=discover "$AWK_C_SCAN" "$@"
}

mutate_site() {
    local src=$1 line=$2 occ=$3 dst=$4
    awk -v mode=mutate -v target_line="$line" -v target_occ="$occ" "$AWK_C_SCAN" "$src" >"$dst"
}

# Sites whose rewinddir is equivalent (fd opened just before and never scanned, or scan result unused/redundant/dead):
# - src/chunkstore.c:dir_scan_volume: vfd opened in vol_get 9 lines prior, never scanned
# - src/chunkstore.c:scan_volumes: st->dirfd opened in store_open immediately prior, never scanned
# - src/feedfile.c:feed_dir_state: in feed_open_reconcile db->dirfd is fresh; in feed_dir_enable readdir fields (any_fwal, junk) are unread
# - src/part.c:intents_recover#1: immediately overwritten by intents_recover#2 (top of the 2-pass loop) before any readdir(d)
# - src/snapshot.c:link_parts: inside `if (snap_mutant == SNAP_MUT_CARRYSETTLED)`, dead code in normal runs
# - src/snapshot.c:rm_dir_depth: unobservable under quiescent local-FS tests (dfd opened at entry; pass 0 reads/unlinks all entries and fsyncs, so pass 1 sees an empty dir)
# - src/snapshot.c:inst_verify_files: sdfd opened in snap_install 10 lines prior, never scanned
# - src/volumes.c:engine_file_find: fd opened in volume_dir_prepare 4 lines prior, never scanned
# - src/volumes.c:up_scan: d->dirfd opened in up_open immediately prior, never scanned
# - tools/dump.c:vol_drop: fd opened 17 lines prior, never scanned
EQUIVALENT="
src/chunkstore.c:dir_scan_volume
src/chunkstore.c:scan_volumes
src/feedfile.c:feed_dir_state
src/part.c:intents_recover#1
src/snapshot.c:link_parts
src/snapshot.c:rm_dir_depth
src/snapshot.c:inst_verify_files
src/volumes.c:engine_file_find
src/volumes.c:up_scan
tools/dump.c:vol_drop
"

is_eq() {
    local id=$1 e
    for e in $EQUIVALENT; do
        [ "$id" = "$e" ] && return 0
    done
    return 1
}

# Discover sites and validate EQUIVALENT before spending time on the clean build and control run.
cfiles_out=$(find src server tools -type f \( -name '*.c' -o -name '*.h' -o -name '*.inc' \) | LC_ALL=C sort) || {
    echo "rewind_mutants: failed to list source files under src/ server/ tools/" >&2
    exit 2
}
if [ -z "$cfiles_out" ]; then
    echo "rewind_mutants: no C source files found under src/ server/ tools/" >&2
    exit 2
fi
mapfile -t cfiles <<<"$cfiles_out"

disc_out=$(discover_sites "${cfiles[@]}") || {
    echo "rewind_mutants: rewinddir site discovery failed" >&2
    exit 2
}

site_ids=() site_files=() site_lines=() site_occs=() site_eq=()
if [ -n "$disc_out" ]; then
    while IFS=$'\t' read -r id f l occ; do
        [ -n "$id" ] || continue
        if [[ $id == *":<top>"* ]] || ! [[ $l =~ ^[1-9][0-9]*$ ]] || ! [[ $occ =~ ^[1-9][0-9]*$ ]]; then
            echo "rewind_mutants: invalid discovered site record: '$id' '$f' '$l' '$occ'" >&2
            exit 2
        fi
        site_ids+=("$id")
        site_files+=("$f")
        site_lines+=("$l")
        site_occs+=("$occ")
        if is_eq "$id"; then site_eq+=(1); else site_eq+=(0); fi
    done <<<"$disc_out"
fi

nsites=${#site_ids[@]}
if [ "$nsites" -eq 0 ]; then
    echo "rewind_mutants: no rewinddir sites discovered" >&2
    exit 2
fi

# Cross-check against raw rewinddir( grep count: discovery cannot exceed raw occurrences.
raw_hits=$(( $( (grep -Eo '(^|[^A-Za-z0-9_])rewinddir[[:space:]]*\(' "${cfiles[@]}" || true) | wc -l ) ))
if [ "$nsites" -gt "$raw_hits" ]; then
    echo "rewind_mutants: discovery sanity check failed ($nsites discovered > $raw_hits raw rewinddir( matches)" >&2
    exit 2
fi

stale_eq=0
for e in $EQUIVALENT; do
    found=0
    for id in "${site_ids[@]}"; do
        [ "$id" = "$e" ] && { found=1; break; }
    done
    if [ "$found" -eq 0 ]; then
        echo "rewind_mutants: EQUIVALENT entry '$e' did not match any discovered site (the list is stale)" >&2
        stale_eq=1
    fi
done
if [ "$stale_eq" -ne 0 ]; then
    echo "rewind_mutants: FAILED (stale EQUIVALENT list)" >&2
    exit 1
fi

# Ordered roughly fastest-first; test_volumes last (it has a minor-fault threshold sensitive to heavy parallel load).
T="test_compat test_flush_tail test_graveyard test_follow test_upgrade test_recover test_ecgc test_keep test_fno test_failstop test_feedstore test_export test_ecswap test_chunk test_drops test_tsdb test_delete test_damage test_snapshot test_ecapply test_intents test_classes test_replica test_volumes"

build_in() {
    local d=$1 j=$2
    local tgts="" t
    for t in $T; do tgts="$tgts build/$t"; done
    env -u MAKEFLAGS -u MFLAGS -u MAKELEVEL -u B make -C "$d" B=build -j"$j" $tgts build/tsdb build/libcrashfs.so >"$d/build.log" 2>&1
}

clean_objs_for() {
    local wdir=$1 f=$2 base
    case $f in
    *.h|*.inc)
        rm -f "$wdir/build/"*.o "$wdir/build/libtsdb.a" "$wdir/build/tsdb"
        ;;
    *)
        base=${f##*/}
        base=${base%.c}
        rm -f "$wdir/build/$base.o" "$wdir/build/$base".*.o \
              "$wdir/build/ec_$base.o" "$wdir/build/raft_$base.o" \
              "$wdir/build/libtsdb.a" "$wdir/build/tsdb"
        ;;
    esac
}

run_test() {
    local b=$1 t=$2 scratch=$3 log=$4
    mkdir -p "$scratch"
    case $t in
    test_chunk|test_ecgc|test_ecswap)
        TMPDIR="$scratch" TSDB_CLI="$b/tsdb" TSDB_REQUIRE_CRASHFS=1 timeout 900 "$b/$t" --quick "$scratch" >"$log" 2>&1 ;;
    test_volumes)
        # Serialize test_volumes so concurrent 9M-row inserts do not exceed its minor-fault budget
        flock "$dir/vol.lock" env TMPDIR="$scratch" TSDB_CLI="$b/tsdb" TSDB_REQUIRE_CRASHFS=1 timeout 900 "$b/$t" "$scratch" >"$log" 2>&1 ;;
    *)
        TMPDIR="$scratch" TSDB_CLI="$b/tsdb" TSDB_REQUIRE_CRASHFS=1 timeout 900 "$b/$t" "$scratch" >"$log" 2>&1 ;;
    esac
}

# Symmetric flake-confirmation retry serialized globally across workers via retry.lock
# (lock order is strictly retry.lock -> vol.lock, matching run_test's vol.lock).
# Sets retry_err=1 if locking itself fails, so the caller fails closed rather than misclassifying.
retry_test() {
    local b=$1 t=$2 scratch=$3 log=$4 rc
    retry_err=0
    if ! exec 9>"$dir/retry.lock"; then
        retry_err=1
        return 1
    fi
    if ! flock 9; then
        exec 9>&-
        retry_err=1
        return 1
    fi
    rm -rf "$scratch"
    run_test "$b" "$t" "$scratch" "$log"
    rc=$?
    flock -u 9 2>/dev/null || true
    exec 9>&-
    return "$rc"
}

# Run a foreground command so bash can interrupt wait and trigger INT/TERM/HUP cleanup immediately.
run_cancellable() {
    local cpid rc=0
    "$@" &
    cpid=$!
    active_pids+=("$cpid")
    wait "$cpid" || rc=$?
    active_pids=()
    return "$rc"
}

run_cancellable build_in "$dir" "$JOBS" || { echo "rewind_mutants: the clean build failed (see $dir/build.log)" >&2; exit 2; }

# Control run: run all tests in parallel (bounded by ctrl_jobs), each in its own scratch subdir
mkdir -p "$TMPDIR/ctrl" "$dir/ctrl_logs"
active_pids=()
for t in $T; do
    while [ "$(jobs -rp | wc -l)" -ge "$ctrl_jobs" ]; do wait_one; done
    (
        run_test "$dir/build" "$t" "$TMPDIR/ctrl/$t" "$dir/ctrl_logs/$t.log"
        echo $? >"$dir/ctrl_logs/$t.rc"
    ) &
    active_pids+=($!)
done
for p in "${active_pids[@]}"; do wait "$p" || true; done
active_pids=()
control=""
for t in $T; do
    rc=$(cat "$dir/ctrl_logs/$t.rc" 2>/dev/null || echo 1)
    if [ "$rc" -ne 0 ]; then
        # Retry once serially after all parallel control tests have finished (rules out parallel load flakes)
        rm -rf "$TMPDIR/ctrl/$t"
        run_cancellable run_test "$dir/build" "$t" "$TMPDIR/ctrl/$t" "$dir/ctrl_logs/$t.log" || control="$control $t"
    fi
done
if [ -n "$control" ]; then
    echo "rewind_mutants: the control run (no mutant) fails in:$control (logs $dir/ctrl_logs/*.log): the result would mean nothing" >&2
    exit 2
fi
echo "control run: green"

nworkers=$((MUTANT_JOBS < nsites ? MUTANT_JOBS : nsites))
bjobs=$((JOBS / nworkers))
[ "$bjobs" -ge 1 ] || bjobs=1
# Cap total concurrent test suites across all workers (~8) so heavy crashfs/multi-thread tests do not thrash
tjobs=$((bjobs > 2 ? 2 : bjobs))

write_res() {
    local resf=$1 st=$2 val=$3
    printf '%s\t%s\n' "$st" "$val" >"$resf.tmp" && mv -f "$resf.tmp" "$resf"
}

run_mutant() {
    local wid=$1 idx=$2
    local wdir=$dir/w.$wid
    local id=${site_ids[$idx]} f=${site_files[$idx]} l=${site_lines[$idx]} occ=${site_occs[$idx]} eq=${site_eq[$idx]}
    local resf=$dir/res/$idx
    local backup=$wdir/orig.bak

    if ! cp "$wdir/$f" "$backup"; then
        write_res "$resf" "ERR" "build-backup-failed" || true
        return 1
    fi
    if ! mutate_site "$wdir/$f" "$l" "$occ" "$wdir/$f.mut" || ! mv -f "$wdir/$f.mut" "$wdir/$f"; then
        rm -f "$wdir/$f.mut"
        cp "$backup" "$wdir/$f" || { write_res "$resf" "ERR" "restore-failed" || true; return 1; }
        write_res "$resf" "ERR" "mutation-failed" || true
        return 0
    fi
    if cmp -s "$wdir/$f" "$backup"; then
        cp "$backup" "$wdir/$f" || { write_res "$resf" "ERR" "restore-failed" || true; return 1; }
        write_res "$resf" "ERR" "no-substitution" || true
        return 0
    fi
    # Force recompilation of the mutated source even within the same filesystem mtime tick.
    clean_objs_for "$wdir" "$f"

    local killed="" fatal=0
    if build_in "$wdir" "$bjobs"; then
        local sroot=$TMPDIR/w.$wid
        local -a t_arr=($T)
        if [ "$eq" -eq 1 ]; then
            # Equivalent: run all tests pipelined up to tjobs at a time; confirm any failure via serialized retry.
            local t
            rm -f "$wdir"/rc.*
            for t in "${t_arr[@]}"; do
                while [ "$(jobs -rp | wc -l)" -ge "$tjobs" ]; do wait_one; done
                rm -rf "$sroot/$t"
                (
                    run_test "$wdir/build" "$t" "$sroot/$t" "$wdir/m.$t.log"
                    echo $? >"$wdir/rc.$t"
                ) &
            done
            wait
            for t in "${t_arr[@]}"; do
                local rc
                rc=$(cat "$wdir/rc.$t" 2>/dev/null || echo 1)
                if [ "$rc" -ne 0 ]; then
                    # Retry once (serialized across workers via retry.lock) to rule out parallel load/memory flakes
                    if ! retry_test "$wdir/build" "$t" "$sroot/$t" "$wdir/m.$t.log"; then
                        if [ "$retry_err" -ne 0 ]; then
                            fatal=1
                            break
                        fi
                        killed="$killed $t"
                    fi
                fi
            done
        else
            # Non-equivalent: run in batches of tjobs; confirm every candidate kill via the same serialized retry
            # so a transient parallel flake can never falsely kill a surviving mutant.
            local k=0 nt=${#t_arr[@]}
            while [ "$k" -lt "$nt" ] && [ -z "$killed" ] && [ "$fatal" -eq 0 ]; do
                local -a bpids=() bnames=() failed_batch=()
                local b=0
                while [ "$b" -lt "$tjobs" ] && [ "$k" -lt "$nt" ]; do
                    local t=${t_arr[$k]}
                    rm -rf "$sroot/$t"
                    run_test "$wdir/build" "$t" "$sroot/$t" "$wdir/m.$t.log" &
                    bpids+=($!)
                    bnames+=("$t")
                    b=$((b + 1))
                    k=$((k + 1))
                done
                local j
                for j in "${!bpids[@]}"; do
                    if ! wait "${bpids[$j]}"; then
                        failed_batch+=("${bnames[$j]}")
                    fi
                done
                local ft
                for ft in "${failed_batch[@]}"; do
                    if ! retry_test "$wdir/build" "$ft" "$sroot/$ft" "$wdir/m.$ft.log"; then
                        if [ "$retry_err" -ne 0 ]; then
                            fatal=1
                            break
                        fi
                        killed="$killed $ft"
                    fi
                done
            done
        fi
    else
        killed=" build-failed"
    fi

    if ! cp "$backup" "$wdir/$f"; then
        echo "rewind_mutants: worker $wid failed to restore $wdir/$f from $backup" >&2
        write_res "$resf" "ERR" "restore-failed" || true
        return 1
    fi
    # Force the next build_in in this worker to recompile the restored clean source file.
    clean_objs_for "$wdir" "$f"

    if [ "$fatal" -ne 0 ]; then
        write_res "$resf" "ERR" "retry-lock-failed" || true
        return 1
    fi
    write_res "$resf" "OK" "$killed" || return 1
    return 0
}

mkdir -p "$dir/res"
for ((w = 1; w <= nworkers; w++)); do
    wdir=$dir/w.$w
    mkdir -p "$wdir"
    # Copy clean built tree (excluding scratch and logs) for each persistent worker
    tar -C "$dir" --exclude='./scratch' --exclude='./ctrl_logs' --exclude='./res' --exclude='./w.*' -cf - . | tar -C "$wdir" -xf - || exit 2
done

# Worker pool: each worker pulls the next site index via an atomic flock counter
next_file=$dir/next_idx
echo 0 >"$next_file"
worker_loop() {
    local wid=$1 idx rc
    worker_cleanup() {
        local -a jps=()
        local j
        while IFS= read -r j; do
            [[ $j =~ ^[1-9][0-9]*$ ]] && jps+=("$j")
        done < <(jobs -rp 2>/dev/null || true)
        terminate_pids "${jps[@]}"
    }
    trap 'worker_cleanup' EXIT
    trap 'worker_cleanup; exit 130' INT
    trap 'worker_cleanup; exit 143' TERM
    trap 'worker_cleanup; exit 129' HUP

    while :; do
        idx=$(flock "$dir/queue.lock" bash -c 'n=$(cat "$1") || exit 1; [[ $n =~ ^[0-9]+$ ]] || exit 1; printf "%s\n" "$((n + 1))" >"$1" || exit 1; printf "%s\n" "$n"' _ "$next_file")
        rc=$?
        if [ "$rc" -ne 0 ] || ! [[ $idx =~ ^[0-9]+$ ]]; then
            echo "rewind_mutants: worker $wid failed to pop queue index (rc=$rc idx='$idx')" >&2
            return 1
        fi
        [ "$idx" -lt "$nsites" ] || break
        run_mutant "$wid" "$idx" || return 1
    done
}

wpids=()
for ((w = 1; w <= nworkers; w++)); do
    worker_loop "$w" &
    wpids+=($!)
done
active_pids=("${wpids[@]}")

bad=0
for p in "${wpids[@]}"; do
    if ! wait "$p"; then
        echo "rewind_mutants: worker pid $p exited non-zero" >&2
        bad=1
    fi
done
active_pids=()

for ((i = 0; i < nsites; i++)); do
    id=${site_ids[$i]}
    l=${site_lines[$i]}
    eq=${site_eq[$i]}
    st=""
    killed=""
    resf=$dir/res/$i
    if [ ! -r "$resf" ] || ! IFS=$'\t' read -r st killed <"$resf"; then
        echo "MUTANT $id (line $l): ERROR (missing or unreadable result file $resf)"
        bad=1
        continue
    fi
    if [ "$st" != "OK" ]; then
        echo "MUTANT $id (line $l): ERROR ($killed)"
        bad=1
        continue
    fi
    if [ -n "$killed" ]; then
        echo "MUTANT $id (line $l): killed by:$killed"
        if [ "$eq" -eq 1 ]; then
            echo "  an equivalent site was killed: the list is stale"
            bad=1
        fi
    else
        if [ "$eq" -eq 1 ]; then
            echo "MUTANT $id (line $l): SURVIVED (expected: equivalent)"
        else
            echo "MUTANT $id (line $l): SURVIVED"
            bad=1
        fi
    fi
done

[ "$bad" -eq 0 ] && echo "rewind_mutants: every site that matters is killed ($nsites sites checked)" || echo "rewind_mutants: FAILED"
exit "$bad"
