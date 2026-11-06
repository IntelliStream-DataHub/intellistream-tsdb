# Task: check the macOS port on a Mac

The macOS port (docs/macos.md) was rebased onto 0.9.0 and the code main gained since (login, USERS, thread names,
the new tests) was ported on Linux, where it could only be checked to compile out of the way: every Linux object is
the same machine code as before. Nothing of it has run on a Mac since the rebase. This task runs it on one, on
Apple silicon first and on an Intel Mac too if you have one, and sends the output back.

About 30 minutes on Apple silicon, most of it `make test` and `make test-clients`.

## Set up

```sh
xcode-select --install                 # cc, make, git (skip if `cc --version` works)
brew install zstd rust python maven    # zstd for tsdb; the rest only for make test-clients (a JDK comes with maven)
git clone <repo> tsdb-mac && cd tsdb-mac
git checkout macos-support             # the port; main once it is merged
bash                                   # the commands below are bash; macOS's own shell is zsh
ulimit -n 10240                        # in the shell that runs the tests below (the default 256 is too low)
mkdir -p ~/tsdb-mac-report
```

## Steps

Run them in order, in the same shell, from the checkout. Each writes a log into `~/tsdb-mac-report`.

1. **The machine.**

   ```sh
   { sw_vers; uname -m; sysctl -n machdep.cpu.brand_string; cc --version; brew --prefix zstd; git log -1 --oneline
     grep -n 'F_OFD_SETLK' "$(xcrun --show-sdk-path)/usr/include/sys/fcntl.h"; } > ~/tsdb-mac-report/machine.txt 2>&1
   ```

   The `grep` line must find `F_OFD_SETLK` and `F_OFD_SETLKW`: tsdb locks VOLUMES and USERS with them.

2. **Build.** No warnings expected.

   ```sh
   make -j"$(sysctl -n hw.ncpu)" 2>&1 | tee ~/tsdb-mac-report/make.log
   grep -c 'warning:' ~/tsdb-mac-report/make.log
   ```

3. **The tests**, on the one backend macOS has (about 10 minutes):

   ```sh
   time make test IO=epoll 2>&1 | tee ~/tsdb-mac-report/test.log
   echo "rc=${PIPESTATUS[0]}" >> ~/tsdb-mac-report/test.log
   grep -n 'FAIL\|skipped\|not applicable' ~/tsdb-mac-report/test.log
   ```

   It must end with `rc=0`. The skipped lines should be exactly the ones docs/macos.md lists. If a test hangs (no
   output for 5 minutes), run `ps -ax -o pid,ppid,etime,command | grep -E 'test_|tsdbd'` in another terminal, save
   that into `~/tsdb-mac-report/hang.txt`, then press Ctrl-C.

   The tests new since the port first ran on a Mac, to look at above all: `test_users`, `test_auth`,
   `test_auth_ops`, `test_out_send`, `test_compat` (run with and without `TSDB_FAST_FSYNC`), `test_cli_users.sh`,
   `test_fs_warning.sh`, and the login tests of `test_server` and `test_ports`. Three lines in the log answer
   questions only a Mac can:

   ```sh
   grep -n 'F_FULLFSYNC\|int variances\|test_fs_warning' ~/tsdb-mac-report/test.log
   ```

   - `test_compat: F_FULLFSYNC taken by APFS on a directory and a file`. tsdb takes a refusal of the drive cache
     flush (ENOTSUP, EINVAL, ENOTTY) for a file system without one and goes on, so if APFS refused it on
     directories, every rename and new file would stop at the drive's cache without a word. This line says APFS
     takes it. A line `F_FULLFSYNC on APFS: directory -1 (...)` instead is the answer to send back. If it says
     `not on APFS`, /tmp is on another file system: run `./build/test_compat /var/tmp` and save its output.
   - `int variances: 5 known answers (x87's, through src/ext80.h here)`, then the rest of `test_spread` skipped.
     On Apple silicon the integer variances go through `src/ext80.h`, and these answers must be x87's bits.
   - `test_fs_warning: ok (apfs, warning shown)`: tsdbd warns at startup on APFS without `--verify-checksums`.

4. **test_multi, twenty times.** It failed about 1 run in 12 on a Mac before: the check of series 0 assumed the
   caller always takes it, when a helper thread may claim it first. main corrected that check in 742f4eb
   (2026-11-05), so all twenty must pass:

   ```sh
   for i in $(seq 20); do ./build/test_multi /tmp > /dev/null 2>&1 || echo "run $i failed"; done \
       | tee ~/tsdb-mac-report/multi.txt
   ```

5. **The clients** (Rust, Python, Java, and the license check of every package; about 10 minutes):

   ```sh
   time make test-clients 2>&1 | tee ~/tsdb-mac-report/clients.log
   echo "rc=${PIPESTATUS[0]}" >> ~/tsdb-mac-report/clients.log
   ```

   `test-licenses` is new since the port's first run on a Mac; it reads the jars and the tarball with `unzip` and
   `tar`, which macOS has.

6. **A server with login, by hand.** It checks USERS and its lock outside the test harness.

   ```sh
   d=$(mktemp -d); printf 'secret-pw\n' > "$d/pw"
   build/tsdb "$d/db" user add admin --superuser --password-file "$d/pw" 2>&1 | tee ~/tsdb-mac-report/server.txt
   build/tsdbd --dir "$d/db" --port 0 --port-file "$d/port" >> ~/tsdb-mac-report/server.txt 2>&1 &
   sleep 2; cat "$d/port" >> ~/tsdb-mac-report/server.txt
   build/tsdb "$d/db" user list >> ~/tsdb-mac-report/server.txt 2>&1   # beside the running server: must work
   kill %1; wait; tail -3 ~/tsdb-mac-report/server.txt
   ```

   The port file holds "PORT PID", `user list` names `admin`, and tsdbd's last line says it shut down.

7. **A fresh import is durable.** On Linux an import into a new directory writes without syncing file by file and
   syncs each file system once at the end; macOS has no way to do that, so there it must sync every file as it
   writes it. A small library counts the drive cache flushes (`F_FULLFSYNC`) the import asks for:

   ```sh
   d=$(mktemp -d)
   cat > "$d/count.c" << 'EOF'
   #include <fcntl.h>
   #include <stdarg.h>
   #include <stdio.h>
   static int n;
   static int count_fcntl(int fd, int cmd, ...)
   {
       va_list ap;
       va_start(ap, cmd);
       long arg = va_arg(ap, long);
       va_end(ap);
       n += cmd == F_FULLFSYNC;
       return fcntl(fd, cmd, arg);
   }
   __attribute__((used, section("__DATA,__interpose"))) static struct { void *with, *of; } ip = {
       (void *)count_fcntl, (void *)fcntl};
   __attribute__((destructor)) static void done(void) { fprintf(stderr, "F_FULLFSYNC calls: %d\n", n); }
   EOF
   cc -dynamiclib -o "$d/count.dylib" "$d/count.c"
   awk 'BEGIN { for (s = 0; s < 200; s++) for (t = 1; t <= 50; t++) printf "s%d,%d,%d.5\n", s, 1000 * t, t }' \
       > "$d/in.csv"
   unset TSDB_FAST_FSYNC
   build/tsdb "$d/src" import "$d/in.csv" && build/tsdb "$d/src" export > "$d/x.tsdbdump"  # CSV: synced as usual
   DYLD_INSERT_LIBRARIES="$d/count.dylib" build/tsdb "$d/db" import "$d/x.tsdbdump" 2>&1 \
       | tee ~/tsdb-mac-report/import.txt
   build/tsdb "$d/db" parts >> ~/tsdb-mac-report/import.txt 2>&1
   ```

   The import must say `imported 200 series` and `F_FULLFSYNC calls:` with at least 200 (one per new series, plus
   the parts and directories; the same import on Linux, with fsync and fdatasync counted, does 210 under this
   rule). Before this was fixed it asked for fewer than 10, the directories only. If there is no
   `F_FULLFSYNC calls:` line at all, macOS did not load the library: say so.

8. **An Intel Mac**, if you have one: steps 1 to 5 again into another directory (`~/tsdb-mac-report-intel`). There
   the AVX kernels are chosen at startup as on Linux, which has never run on macOS.

## Send back

The whole `~/tsdb-mac-report` directory (`tar czf tsdb-mac-report.tgz -C ~ tsdb-mac-report`), or if that is too
much: `machine.txt`, the last 30 lines of `test.log` and `clients.log`, the output of the `grep` in step 3,
`multi.txt`, `server.txt`, `import.txt`, and `hang.txt` if there is one. Also say how long steps 3 and 5 took.
