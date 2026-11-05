# Login and access control: before and after

Mean of the runs, then (standard deviation in percent of the mean; lowest to highest run). Written by tools/auth-bench-summary.py from raw/; every number can be found there.

**Inserts measured without fsync, so they compare the server's code, not the disk** (tsdbd --no-fsync, each workload and repetition on a fresh copy of the database, 10 s each).

## The run

- **date**: 2026-11-05 14:52 CET
- **mode**: standard
- **reps**: 5
- **groups**: inserts
- **seconds per workload**: 3
- **inserts**: tsdbd --no-fsync, each workload and repetition on a fresh copy of the database, 10 s each; inserts measured without fsync, so they compare the server's code, not the disk
- **host**: olav-b650mprors
- **cpu**: AMD Ryzen 9 7950X 16-Core Processor
- **cpu topology**: 1 socket(s) x 16 cores x 2 threads = 32 logical CPUs
- **memory**: 61 GB
- **kernel**: Linux 7.0.0-38-generic
- **os**: Ubuntu 26.04.1 LTS
- **governor**: performance (32 CPUs)
- **compiler**: cc (Ubuntu 15.2.0-16ubuntu1) 15.2.0
- **scratch**: /var/tmp/auth-build/s5/scratch-ins (ext4, 274G free)
- **lock**: /home/olav/.local/state/subagent-economy/BENCH.lock
- **busy at start**: 1.05 of 32 CPUs over 2 s
- **git clone**: 9d01490
- **tree main**: ba4f6c2 = ba4f6c2: tsdbd 0.9.0 (build dev); cflags: -O3 -g -std=gnu11 -Wall -Wextra -Wno-unused-parameter -fno-math-errno -ffp-contract=off -march=native -Iinclude -fPIC; client defs: none
- **tree auth**: HEAD = 9d01490: tsdbd 0.9.0 (build dev); cflags: -O3 -g -std=gnu11 -Wall -Wextra -Wno-unused-parameter -fno-math-errno -ffp-contract=off -march=native -Iinclude -fPIC; client defs: -DAB_LOGIN -DAB_ADMIN
- **tsdbd flags**: --wal-sync-ms 10 --port 0 --port-file (and --no-auth for noauth; --no-fsync for the insert workloads)
- **workload sizes**: 100 series x 1000000 points; 3 s per workload; inserts: 1000 rows a request over a pool of 1000 series; QUERY_MULTI: 100 series a request
- **server glob64**: simd avx512, io io_uring
- **server main1**: simd avx512, io io_uring
- **server name5000**: simd avx512, io io_uring
- **server noauth**: simd avx512, io io_uring
- **server prefix**: simd avx512, io io_uring
- **server super**: simd avx512, io io_uring

## Inserts, 1,000 rows a request, without fsync, on a fresh database for every run

Unit: rows/s.

| configuration | one series per request, 1 connection | one series per request, 32 connections | a series per row, 1 connection | a series per row, 32 connections |
|---|---:|---:|---:|---:|
| main ba4f6c2 without auth, the baseline | 25,650,896 (2.8%; 24,957,101 to 26,780,800) | 26,027,727 (1.0%; 25,609,599 to 26,221,931) | 13,810,044 (5.3%; 12,971,200 to 14,677,453) | 16,125,759 (3.7%; 15,637,132 to 17,175,718) |
| auth build, --no-auth | 26,059,099 (1.9%; 25,570,400 to 26,779,197) | 26,060,817 (1.5%; 25,562,772 to 26,622,801) | 13,158,187 (7.1%; 12,376,812 to 14,636,654) | 15,977,275 (1.4%; 15,734,832 to 16,307,713) |
| auth build, logged in as a superuser | 25,734,378 (4.7%; 25,074,149 to 27,875,721) | 25,981,325 (0.9%; 25,618,966 to 26,170,729) | 13,336,542 (6.1%; 12,593,311 to 14,700,048) | 16,394,119 (1.7%; 16,032,639 to 16,657,970) |
| auth build, a user with 8 prefix: grants | 26,639,144 (3.3%; 26,093,339 to 28,181,036) | 26,025,715 (1.8%; 25,367,654 to 26,692,557) | 13,176,223 (10.0%; 11,385,265 to 14,725,011) | 16,262,414 (2.9%; 15,894,714 to 17,014,449) |
| auth build, a user with 5,000 name: grants | 25,613,075 (3.7%; 24,365,600 to 26,356,809) | 26,048,198 (1.0%; 25,639,603 to 26,333,825) | 13,074,740 (8.9%; 12,060,117 to 14,654,907) | 15,791,940 (1.7%; 15,543,476 to 16,217,507) |
| auth build, a user with 64 glob: grants | 26,379,109 (2.0%; 25,842,942 to 27,284,000) | 25,972,917 (1.1%; 25,693,338 to 26,395,382) | 13,154,847 (4.2%; 12,407,559 to 13,959,060) | 16,193,747 (3.9%; 15,641,227 to 17,012,821) |

Change against ba4f6c2 (the same code without auth), in percent of its mean:

| configuration | one series per request, 1 connection | one series per request, 32 connections | a series per row, 1 connection | a series per row, 32 connections |
|---|---:|---:|---:|---:|
| auth build, --no-auth | +1.6% | +0.1% | -4.7% | -0.9% |
| auth build, logged in as a superuser | +0.3% | -0.2% | -3.4% | +1.7% |
| auth build, a user with 8 prefix: grants | +3.9% | -0.0% | -4.6% | +0.8% |
| auth build, a user with 5,000 name: grants | -0.1% | +0.1% | -5.3% | -2.1% |
| auth build, a user with 64 glob: grants | +2.8% | -0.2% | -4.7% | +0.4% |

