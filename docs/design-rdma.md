# RDMA for the peer links

Status: design and measurements, no transport code. Branch `rdma-design` from main at a06d4bb7, written on 2026-11-24.
Target release: **v0.11.0** (the build is the tracks of §9). Revised after an adversarial review the same day: the rate
caps, the port shared with NFS over RDMA, the MAC's CPU, the upgrade's state machine, credits, registration, the
build's header detection and where the RDMA tests can run.

The owner's request (2026-11-24, verbatim): "all servers should have infiniband too, but I do not think everyone is
configure with ipoib yet. We should support RDMA links". Every link between two tsdbd today is TCP
(`server/peerlink.c`), whatever network it runs on. This document measures what InfiniBand and the Ethernet RDMA
flavours give tsdb's frames on the test servers, maps each link kind to what RDMA would change for it, compares the
ways to get there and proposes a plan.

Related: [design-cluster.md](design-cluster.md) §5.1.1 (data links, C25), [design-cluster-data.md](design-cluster-data.md)
§5.7 (CHUNK and CHUNKB), [ha.md](ha.md) §15 and [design-ha-modes.md](design-ha-modes.md) (production: NFSv4/RDMA to ZFS on
AlmaLinux 10), [design-auth.md](design-auth.md) (the threat model: a backend on an isolated network).

## Summary

1. **The first limits are not the network: they are the frame MAC and the rate caps.** Every data-link frame carries
   an HMAC-SHA256 tag over all of its bytes (`frame_tag`, peerlink.c). On the test servers' Xeons (no SHA extensions)
   that costs about 4.2 CPU seconds per GB on each side and caps **one link at about 1.9 Gbit/s (230 MB/s) on every
   transport**: TCP on 40GbE, rsocket and TCP on loopback alike (§3.3). Without the tag the same link moves 30-33
   Gbit/s over TCP on 40GbE. Below the MAC, the bulk kinds are held by **node-wide token buckets**, one per kind and
   shared by every link of that kind on the node: 256 MiB/s for BULK (`PL_BULK_RATE_DEFAULT`), 200 MiB/s for SNAP
   (`rebuild_mb_s`), 256 MiB/s for CHUNKB (`--chunk-rate-mb-s`). At the defaults a snapshot runs at 210 MB/s whatever
   the MAC, and a COMMIT shipped to two replicas at about 134 MB/s each. Fixing the MAC (§6, D1) is the first track
   and helps TCP right away; raising the caps is a decision of its own, bounded by the disks, the NFS storage on the
   same fabric and the LOG link's latency (D11); RDMA matters for bulk only after both (§4).
2. **Native verbs on the FDR InfiniBand** move 50 Gbit/s from 16 KiB messages up, at 0.02-0.09 CPU seconds per GB
   (event-driven completions, §3.1), and a 1 KiB frame and its answer take about 5 µs busy-polling or about 15 µs
   sleeping on completion events, against about 23 µs over TCP on 40GbE (p50, untagged). rsocket (librdmacm's socket
   emulation) reaches 44-46 Gbit/s with 16-64 KiB frames but about 30 Gbit/s with 1 MiB ones, and a 6 µs round
   trip, at TCP's CPU per GB (§3.2). RoCE and iWARP on the 40GbE ports reach its line rate too (§3.5). These CPU
   numbers leave the MAC out: with a MAC of a few GB/s per core added to both transports, verbs save about a fifth of
   the per-byte CPU, not three to six times (§4), and one link's MAC runs on one core per side.
3. **IPoIB is not needed for RDMA if the connection is set up over the TCP link we already have.** rdma_cm and rsocket
   resolve IP addresses, so they need IPoIB on InfiniBand. A queue pair can instead be set up by exchanging its
   numbers (LID or GID, QPN, PSN, MTU) over the authenticated TCP data link, which runs on whatever network the peer
   address is on. That is how the recommended design works on servers without IPoIB (§5, D2).
4. **Recommendation**: a verbs transport *under* peerlink (RC queue pairs, SEND/RECV with credits, one QP per link
   connection), reached by **upgrading** a TCP data link after its handshake; the TCP connection stays as the link's
   anchor (liveness, fallback, firewall control). The metadata link (kind 0, Raft) stays TCP. libibverbs and
   librdmacm are loaded with `dlopen`, so one binary runs with and without rdma-core; `NO_RDMA=1` builds without it;
   macOS never has it. About 12 track-days (§9); after the first (the MAC), §3 and §4 are measured again and the
   owner decides the order and scope of the rest with those numbers (D14).
5. **Operational prerequisites**: `LimitMEMLOCK` for tsdbd (the test servers' 8 MB limit already refuses perftest's
   4 MiB messages, §2), and a test path without hardware: Soft-RoCE (`rdma_rxe`) and `siw` exist in Ubuntu kernels
   (the dev machine has both modules) but **not in AlmaLinux 10's kernel** (§8).
6. **The peer links would share the InfiniBand port with production's NFS over RDMA** (design-ha-modes.md): a bulk
   stream at line rate competes with the node's storage I/O, the WAL's `fdatasync` on the commit path included. The
   design gives the queue pairs a configurable service level and keeps the rate caps; rd-hw measures NFS latency
   under peer load before any cap is raised (§4 "Sharing the port", D11, D13).

## 1. What travels on the peer links today

A peerlink (`server/peerlink.h`) is one TCP connection per (peer, kind), one thread per connection that owns the
socket (a tsdbd has three that carry data: repld's for LOG, BULK and SNAP, fwd's for FWD and FWDQ, chunksrv's for CHUNK
and CHUNKB; the metadata link is the cluster thread's), a bounded send queue (8 MiB), `poll()` on the socket and an eventfd, the handshake (HELLO, HELLO_ACK, AUTH:
HMAC over the transcript under the cluster secret), then frames `[u32 len][u8 type][payload][tag 32]` with the tag
`HMAC-SHA256(key of the direction, seq | type | payload)`. Log frames are zstd-1 compressed. Bulk files go as
`BEGIN`, `RESUME`, `CHUNK` (256 KiB) at the rate of a token bucket per kind, `END` (xxh64), `DONE`. The bucket is
**node-wide**: every link of one kind shares it (peerlink.h, R4 D7), so one peer gets the whole rate and two share it.
BULK's is `PL_BULK_RATE_DEFAULT`, 256 MiB/s; SNAP's is the cluster setting `rebuild_mb_s`, 200 MiB/s by default
(rsnap.c); CHUNKB's is `cluster.chunk.rate` (`--chunk-rate-mb-s`), 256 MiB/s.
`on_sent` reports a tracked frame WRITTEN once its first byte reached the socket, NEVER_WRITTEN otherwise (fwd's rule).

| kind | owner | what | frame sizes | what matters |
|---|---|---|---|---|
| 0 META | cluster.c | Raft, heartbeats, watch, admin ops | small | liveness, the failure detector |
| 1 LOG | repld.c | WAL records, ACKs, heartbeats per volume | 64 B (ACK) to 1 MiB, typical 1-64 KiB zstd'd | latency: on the commit path of every replicated insert |
| 2 BULK | repld.c | a COMMIT's part files | 256 KiB chunks of files up to GBs | throughput, CPU; must not delay LOG |
| 3 FWD | fwd.c | forwarded requests and replies | up to 8 MiB, typical < 4 KiB | latency: one extra hop on a client's request; NEVER_WRITTEN vs WRITTEN |
| 4 FWDQ | fwd.c | forwarded queries, streamed replies under CREDIT | 1 MiB chunks | throughput and latency of a query through an entry node |
| 5 SNAP | rsnap.c | snapshots (R4) | 256 KiB chunks, GBs | throughput: rebuild time (2 TB in 30 min would be 1.1 GB/s; at today's default `rebuild_mb_s` of 200 MiB/s, 2 TB take 2.6 h) |
| 6 CHUNK | chunksrv.c | chunk store requests (read up to 4 MiB, sums, list) | requests small, reads 64 KiB-1 MiB cells | latency of a cold read: k reads in parallel |
| 7 CHUNKB | chunksrv.c | chunk puts as bulk files | cells of 64 KiB-1 MiB | throughput of a settle |

## 2. The test servers

Six AlmaLinux 10.3 servers (kernel 6.12.0-264), two Xeon sockets each (E5-2697 v4 or Gold 6148: AVX2, the Gold also
AVX-512; **no SHA extensions** on either), 376-503 GB RAM. Each has a Mellanox ConnectX-3 (`mlx4_0`): port 1
InfiniBand FDR, 56 Gbit/s signalling (about 54 Gbit/s of data); on four of them port 2 is 40GbE (RoCE possible). Two
have a Chelsio T5 40GbE pair cabled back to back (`cxgb4_0`, iWARP). The management ports are Intel NICs whose RDMA
function (`irdma`) is also listed. rdma-core 61, perftest 25.10, librdmacm with `librspreload.so`. What was read, without
changing anything:

- **IPoIB** is configured on the test servers, in **datagram mode with an MTU of 2044** (connected mode, which allows an
  MTU of 65520 and is what TCP over IPoIB needs to be fast, is not set). In production the owner expects some servers
  without IPoIB at all.
- **No TCP over IPoIB** (the owner's rule for these servers, 2026-11-24): firewalld admits TCP between the servers only
  from their Ethernet addresses, and nothing here sends TCP to an IPoIB address. The IPoIB addresses serve rdma_cm's
  address resolution only (perftest `-R`, rsocket). TCP, and every TCP fallback in this design, runs on Ethernet.
  **RDMA traffic bypasses netfilter**: rdma_cm and RC queue pairs connected through a port
  firewalld closes for TCP. A design that relies on the firewall to keep strangers off the RDMA path is wrong (§5.5).
- **`ulimit -l` is 8192 KiB** (8 MB of locked memory per process). Memory registered for RDMA is locked: perftest
  could not register its buffers for 4 MiB messages (`Cannot allocate memory`). io_uring's rings count against the
  same limit, and so do the queue and completion buffers the provider library allocates per QP. tsdbd's need is the
  registered pool (one per tsdbd, §4), those buffers for every link and the rings (§4 "Registration").
- **Production shares this fabric with storage**: the volumes are NFSv4.1/4.2 over RDMA exports of ZFS
  (design-ha-modes.md), so peer links on the IB port compete with the node's NFS traffic (§4 "Sharing the port").
  The test servers have no NFS/RDMA mount; that measurement needs a production-like node (rd-hw).
- **The CPUs here are not production's**: these Xeons have no SHA extensions; whether production's do is unknown and
  matters for the MAC (§6).
- **No Soft-RoCE**: AlmaLinux 10's kernel has neither `rdma_rxe` nor `siw` (`modinfo` finds neither).

## 3. Measurements

Tools: `tools/rdma-bench.sh` (drives a pair of servers over ssh; hosts and addresses are arguments) runs perftest
through rdma_cm and `tools/link-bench` (new: peerlink's frame shape and HMAC tag over a stream socket, so the same
binary measures TCP and, with `--rsocket`, rsocket: librdmacm's `rsocket`/`rconnect`/`rread`... called through
`dlopen`. Not `LD_PRELOAD=librspreload.so`: the preload falls back to a kernel socket when `rconnect` fails, which would
send TCP to the IPoIB address and report TCP under the rsocket label). Each number is the mean of 3 runs of 3 s
(perftest: a fixed count of messages, about 3 s at line rate); the spread is the lowest and highest run. CPU is user +
system time of each side's process (bash `times` for perftest, `getrusage` in link-bench) divided by the payload
moved. perftest's CPU includes its connection setup and registration (a fixed 0.01-0.2 s), so its small numbers are
upper bounds. Raw results stay outside the repository. Pair: two Gold 6148 servers, back to back through the IB
switch.

### 3.1 Native verbs (perftest, rdma_cm, InfiniBand FDR)

Bandwidth in Gbit/s, mean (lowest-highest run), then CPU seconds per GB on the sending and the receiving side.
`send_bw` and `read_bw` sleep on completion events; `write_bw` busy-polls (perftest has no event mode for it).

| message | SEND (events) | RDMA WRITE (busy poll) | RDMA READ (events) |
|---|---|---|---|
| 64 B | 2.74 (2.69-2.81); 6.7 / 6.6 | 2.48 (2.36-2.54); 7.1 / 7.1 | 2.61 (2.57-2.63); 6.9 / 6.9 |
| 1 KiB | 32.5 (32.0-33.1); 0.44 / 0.43 | 39.4 (37.3-41.5); 0.44 / 0.45 | 37.9 (37.0-38.7); 0.44 / 0.45 |
| 16 KiB | 49.2 (49.1-49.2); 0.039 / 0.090 | 49.8 (49.8-49.9); 0.18 / 0.18 | 50.1 (50.1-50.2); 0.041 / 0.18 |
| 64 KiB | 50.1 (50.0-50.3); 0.022 / 0.057 | 50.1 (50.1-50.2); 0.18 / 0.18 | 50.2 (50.1-50.4); 0.022 / 0.18 |
| 256 KiB | 50.1 (50.0-50.1); 0.018 / 0.031 | 50.3 (50.2-50.3); 0.18 / 0.18 | 50.1 (50.0-50.1); 0.019 / 0.18 |
| 1 MiB | 50.0 (50.0-50.1); 0.018 / 0.021 | 50.4 (50.4-50.4); 0.22 / 0.22 | 50.3 (50.3-50.4); 0.018 / 0.18 |
| 4 MiB | failed in every run: `Cannot allocate memory` (8 MB memlock, §2) | failed | failed |

Up to 1 KiB the NIC's message rate is the limit (about 4-5 million messages a second); the CPU there is about 0.45 µs
per message per side. From 16 KiB on the link is at line rate. The 0.18 s/GB of the busy-polling sides and of READ's
passive side is one core spinning for the length of the run, not work per byte: a sleeping receiver of SENDs costs
0.02-0.09.

Latency in µs, typical (p99). perftest reports **one way** for SEND and WRITE (half its ping-pong round trip) and a
**whole round trip** for READ (a request and its answer). Every run was within 3% of the mean.

| message | SEND, busy poll | SEND, events | WRITE, busy poll | READ, busy poll |
|---|---|---|---|---|
| 64 B | 1.22 (1.77) | 5.99 (13.6) | 1.14 (1.23) | 2.03 (2.25) |
| 1 KiB | 2.53 (3.08) | 7.51 (14.4) | 2.43 (2.57) | 2.55 (2.75) |
| 16 KiB | 5.91 (6.69) | 11.1 (17.4) | 5.77 (5.98) | 5.86 (6.19) |
| 64 KiB | 14.0 (14.3) | 19.1 (25.9) | 13.4 (13.6) | 13.4 (13.7) |
| 256 KiB | 45.3 (45.7) | 53.0 (58.9) | 44.7 (45.1) | 44.8 (45.3) |
| 1 MiB | 170 (171) | 180 (186) | 170 (172) | 170 (171) |

So a 1 KiB frame and its answer over SEND take about **5 µs busy-polling and about 15 µs sleeping on events**: a
sleeping side adds about 5 µs per message, which matters below 64 KiB only.

**Without rdma_cm and without IPoIB**: perftest run without `-R` exchanges the queue pair's numbers (LID, QPN, PSN)
over a TCP connection and addresses the peer by LID. With that TCP connection on the 40GbE Ethernet addresses and the
data on the InfiniBand port, one run each gave 50.8 Gbit/s at 64 KiB and 2.22 µs (typical, one way; p99 2.67) for a
1 KiB SEND busy-polling: the same as through rdma_cm. This is the set-up of option D (§5.1).

### 3.2 rsocket (link-bench --rsocket, InfiniBand)

`link-bench --rsocket`: peerlink's frame shape over librdmacm's rsocket API (addresses resolved by rdma_cm on the
IPoIB addresses; the data is RDMA, never TCP), untagged (the tag is §3.3's subject). Bandwidth in Gbit/s, mean
(lowest-highest), CPU s/GB sender / receiver; round trip of a frame and an equal answer, p50 (p99) in µs.

| frame | bandwidth | CPU s/GB | round trip |
|---|---|---|---|
| 64 B | 0.91 (0.65-1.15) | 4.3 / 4.3 | 3.04 (4.97) |
| 1 KiB | 26.5 (26.4-26.6) | 0.30 / 0.30 | 5.96 (7.08) |
| 16 KiB | 43.8 (43.8-43.8) | 0.18 / 0.18 | 15.6 (33.6) |
| 64 KiB | 46.0 (45.8-46.1) | 0.17 / 0.17 | 34.5 (44.4) |
| 256 KiB | 31.4 (31.3-31.4) | 0.18 / 0.20 | 117 (156) |
| 1 MiB | 30.3 (30.1-30.4) | 0.17 / 0.21 | 492 (601) |
| 4 MiB | 30.6 (28.8-33.8) | 0.18 / 0.21 | |

rsocket is best at 16-64 KiB frames (44-46 Gbit/s) and falls to about 30 Gbit/s for the large ones, which copy
through its own buffers. The `rsocket-buf1m` runs asked for 1 MiB buffers (`--buf`, `SO_SNDBUF`/`SO_RCVBUF` through
`rsetsockopt`), but on the client only: the server set them on the accepted socket, after its buffers were sized, so
its side kept rsocket's default. One-sided, the bandwidth at 1 MiB is the same (30.6) and the 1 MiB round trip falls
from 492 to 394 µs p50 (-20%). Buffers on both sides were not measured; link-bench now sets them on the listening
socket and rdma-bench.sh passes `--buf` to both sides (rd-hw takes the numbers).
Its CPU per GB is the same as TCP's (§3.3), because it copies every byte on both sides, and its round trip at 1 KiB
(6 µs) is that of busy-polled verbs, because it polls before it sleeps. With the tag every size falls to 1.8-1.9
Gbit/s, as on TCP.

### 3.3 TCP, and what the frame MAC costs

`link-bench` over TCP on the 40GbE ports (ConnectX-3 port 2, the same pair as §3.1), with and without peerlink's
tag (`HMAC-SHA256` over every frame, sender and receiver). Gbit/s mean (lowest-highest), CPU s/GB sender / receiver.

| frame | untagged | CPU s/GB | tagged (today's peerlink) | CPU s/GB |
|---|---|---|---|---|
| 64 B | 0.70 (0.70-0.70) | 6.5 / 11.4 | 0.30 (0.28-0.32) | 25 / 27 |
| 1 KiB | 7.97 (7.11-8.44) | 0.65 / 1.01 | 1.26 (1.14-1.38) | 6.1 / 6.1 |
| 16 KiB | 28.7 (21.3-32.8) | 0.18 / 0.27 | 1.81 (1.76-1.88) | 4.2 / 4.3 |
| 64 KiB | 32.8 (32.6-33.0) | 0.14 / 0.17 | 1.83 (1.82-1.83) | 4.1 / 4.3 |
| 256 KiB | 32.1 (31.9-32.5) | 0.12 / 0.15 | 1.86 (1.83-1.92) | 4.1 / 4.3 |
| 1 MiB | 30.5 (29.8-31.1) | 0.13 / 0.16 | 1.90 (1.89-1.92) | 4.2 / 4.2 |
| 4 MiB | 30.1 (29.9-30.2) | 0.15 / 0.16 | 1.89 (1.89-1.90) | 4.2 / 4.2 |

Round trip, p50 (p99) in µs:

| frame | untagged | tagged |
|---|---|---|
| 64 B | 23.5 (33.8) | 24.9 (33.1) |
| 1 KiB | 22.5 (34.1) | 48.9 (116) |
| 16 KiB | 43.8 (91.9) | 362 (1,020) |
| 64 KiB | 108 (346) | 1,460 (1,880) |
| 1 MiB | 1,100 (1,460) | 17,800 (18,700) |

The tag is the limit, not the network: on loopback (no NIC at all) the same link moves 41.8 Gbit/s untagged at 1 MiB
and 1.81 tagged; over rsocket (§3.2) 1.83; over TCP on the Chelsio pair (E5-2697 v4) 37.3 untagged and 1.80 tagged.
4.2 CPU seconds per GB is about 240 MB/s per core: tsdb's own portable SHA-256 (`client/tsdb_crypto.c`), one pass
per side. OpenSSL's assembly SHA-256 (through Python's `hashlib` on one of these servers) does 479 MB/s, so even the
best SHA-256 here would leave a link near 3.8 Gbit/s; BLAKE2b there does 926 MB/s (§6). At 1 KiB the four MACs of a round trip (sign and check, each way) add about 26 µs, more than the network.
The cap is the code, not the build flags or the CPU's extensions: on the build machine (an i5-13400, which has SHA
extensions) link-bench built `-O2` (as rdma-bench.sh builds it) and `-O3 -march=native` (as tsdbd is built) moves
2.25 and 2.33 Gbit/s tagged at 1 MiB on loopback (3 runs each), because `tsdb_crypto.c` is portable C only (§6).

The 64 B and 1 KiB rows are one `write()` per frame with `TCP_NODELAY` (one `rwrite` per frame on rsocket): a system
call per frame. peerlink appends frames to its write buffer and sends many per call, so these rows overstate its CPU
per small frame on TCP and are not the basis for a per-frame comparison. `link-bench --batch N` writes N frames per
call (rdma-bench.sh's `eth-tcp-batch` and `rsocket-batch` suites); not run yet (rd-hw).

### 3.4 Not measured

- **TCP over IPoIB**: not measured, and never to be: the owner's rule is that no TCP runs over IPoIB (§2).
  `tools/rdma-bench.sh` has no such suite. With datagram mode and MTU 2044 it would be below the 40GbE TCP numbers
  anyway, and the MAC limit of §3.3 applies to every stream.
- **RoCE and iWARP**: see §3.5.

### 3.5 RoCE (ConnectX-3 port 2, 40GbE) and iWARP (Chelsio T5, 40GbE)

Both on 40GbE (about 37 Gbit/s of payload), perftest through rdma_cm. RoCE: ConnectX-3 port 2, the Gold 6148 pair of
§3.1. iWARP: the Chelsio T5 pair (E5-2697 v4), back to back. Gbit/s mean (lowest-highest); latency typical one way
(SEND, WRITE) or round trip (READ) in µs, as in §3.1.

| | RoCE | iWARP |
|---|---|---|
| SEND 64 KiB / 1 MiB (events) | 36.6 / 36.6 (36.6-36.6) | 37.2 / 37.3 (37.2-37.3) |
| CPU s/GB, SEND 1 MiB, sender / receiver | 0.018 / 0.022 | 0.017 / 0.023 |
| WRITE 1 KiB (busy poll) | 35.4 (35.2-35.6) | 26.9 (26.9-26.9) |
| SEND 1 KiB (events) | 29.6 (29.5-29.7) | 32.4 in 1 run; 2 of 3 runs failed |
| SEND one way 64 B / 1 KiB, busy poll | 1.54 / 3.01 | 3.50 / 4.50 |
| SEND one way 1 KiB, events | 8.0 (p99 14.8) | not trusted (below) |
| WRITE one way 64 B, busy poll | 1.54 | 3.18 |
| READ round trip 1 KiB | 3.41 | 6.21 |
| rsocket 64 KiB / 1 MiB | 36.2 / 31.1 | 20.7 / 25.5 |
| rsocket round trip 1 KiB, p50 (p99) | 6.15 (6.67) | 15.1 (18.6) |
| TCP on the same port, 1 MiB untagged | 30.5 (§3.3) | 37.3 (37.2-37.3) |

Both reach the 40GbE line rate from 64 KiB at the same CPU per GB as InfiniBand. RoCE's latency is about 20-30%
above InfiniBand's; iWARP's is twice to three times it (TCP offload in the NIC). **iWARP failed runs**: two of three
1 KiB SEND bandwidth runs and every 64 KiB SEND latency run in event mode ended with a completion error (status 5, a
flushed work request: the queue pair had gone to the error state). iWARP has no receiver-not-ready retry, so a SEND
that finds no posted receive ends the connection at once: a transport over iWARP must never send without a credit
(§4 has credits for every transport). perftest's event-mode SEND latency on iWARP (1.64 µs, below its own busy-poll
4.50) comes from the two runs that did not fail and is not believed. RoCE without lossless Ethernet (PFC) is fine
back to back but can lose throughput to retransmissions on a busy switch; RoCE v1 (all a plain ConnectX-3 does; the GID type says which) is not routed across IP subnets.

## 4. What RDMA would give each kind, and at what cost

The gains, from §3 on these servers. Today every link is capped at about 230 MB/s per direction by the MAC whatever
the transport, and the bulk kinds are held lower still by their node-wide rate caps (§1). At the default caps, fixing
the MAC alone (D1) buys nothing for SNAP (200 MiB/s, 210 MB/s, is below the MAC's 230), about 15% for a BULK file to
one replica (256 MiB/s, 268 MB/s), nothing for BULK to two replicas (134 MB/s each), and lifts CHUNKB only where one
peer takes the whole 268 MB/s. The line-rate entries below need the MAC fixed **and** the caps raised (D11), which
rd-hw is to justify with the disks' and the storage's numbers:

| kind | gain | cost and care |
|---|---|---|
| 0 META | none worth having (a few frames a second) | **stays TCP** (D3): the failure detector and Raft must not depend on the new code or on the fabric's health. A volume whose RDMA path fails falls back to TCP; the metadata group never notices |
| 1 LOG | a 1 KiB round trip of about 15 µs (events; 5 µs busy-polling) instead of about 23 µs, and less CPU per frame (how much is open: §3.3's small-frame TCP rows are a system call per frame, peerlink batches); zstd stays (it saves bytes on the wire and in the receiver's WAL alike) | the commit path also holds the replica's `fdatasync` (0.7 ms on NVMe), so the saving is a few % of a replicated insert's latency (target p50 ≤ 0.9 ms). Worth it, not decisive |
| 2 BULK, 5 SNAP, 7 CHUNKB | up to line rate (50 Gbit/s) once the caps allow it (D11); the transport's own CPU is 0.02-0.09 s/GB against 0.12-0.17 for TCP, with the MAC on top of both ("CPU with the MAC" below) | registered buffers on both sides (pread into a registered slot, SEND, the receiver writes the slot to the file); the token buckets count what is posted; the receiver's write to NFS, not the wire, bounds a rebuild ("Sharing the port") |
| 3 FWD | one hop of about 15 µs (events) instead of about 23 µs on every forwarded request | WRITTEN must mean "posted" (D5), so fwd's NEVER_WRITTEN stays definite |
| 4 FWDQ | streamed replies at line rate (within the MAC's per-link bound) | fwd's CREDIT is an application frame that travels as data under the link's credits (§4 "Flow control"): the reply's credits and the link's are separate layers, never merged |
| 6 CHUNK | a 1 MiB cell in about 0.2 ms of wire time instead of about 0.3 ms over TCP on 40GbE; k cells of a cold read in parallel | RDMA READ straight from the store's page cache would need registered file memory (ODP: not on ConnectX-3): SEND/RECV with a copy is the design; READ is later work (§10) |

**CPU with the MAC.** peerlink runs one thread per connection, so a link's MAC runs on one core per side: 50 Gbit/s
is 6.25 GB/s of MAC per side on one core, beyond every candidate of §6 on these CPUs. At a MAC of 3 GB/s per core (to
measure) a single link stops near 24 Gbit/s on any transport, and the MAC costs 0.33 CPU s/GB per side: TCP's
per-byte CPU becomes 0.45-0.50 s/GB and verbs' 0.35-0.42, a saving of about a fifth, not the three to six times of
the transport columns. Several links (kinds, peers) run on separate threads, so a node's total can still exceed one
link's. This is why the measurements after rd-mac decide the order of the verbs tracks (D14).

**Registration.** Every byte that RDMA moves must lie in registered (locked) memory, and registering on the data path
costs tens of µs and fails unpredictably under a memlock limit, so memory is registered once at start (D7). A tsdbd
has three peerlinks that upgrade (§1: repld's, fwd's, chunksrv's), so there is **one pool per tsdbd and device**,
shared by the three (one protection domain), not one per peerlink. Receive slots are the bulk of it, and per-link
receive rings do not add up: 7 kinds x 8 peers x 16 slots x 64 KiB is 56 MiB of posted receives alone, most of them
idle and most 64 KiB slots holding a 64-byte ACK. The design therefore uses **two slot sizes** (4 KiB for control and
small frames, 64 KiB for the rest; a larger frame spans several) and a **shared receive queue (SRQ) per peerlink and
slot size**, whose depth follows what arrives rather than how many links exist; credits (below) are granted per link
out of the SRQ's posted slots. For 8 peers: 128 large and 512 small receive slots per peerlink, 3 x (8 + 2) = 30 MiB,
plus send slots taken from a shared free list only while a send is in flight (16 MiB): about 46 MiB, under the
default `pool_mb` of 64, which is per tsdbd. Whether SRQs or small per-link rings are simpler for the credit rule is
settled in rd-verbs with the fake transport (§8). tsdbd's memlock need is the pool, plus the provider's queue and
completion buffers per QP (56 QPs at 8 peers), plus io_uring's rings: it needs `LimitMEMLOCK=infinity` (or that sum
with headroom) in its systemd unit; with a lower limit it says so in one line and stays on TCP (`auto`) or refuses to
start (`on`). Tests run with a pool of 1-2 MiB (§8).

**Flow control.** A SEND needs a posted receive at the peer. What happens to one that finds none depends on the
fabric: on InfiniBand with `rnr_retry` 7 (infinite) and on rxe the NIC retries for ever, so a credit bug is a silent
stall; on iWARP the QP fails at once (§3.5 saw that). The transport never relies on RNR: production sets a small
finite `rnr_retry`, and the tests set 0 so that a SEND without a receive fails the QP on every fabric, as the fake does
(§8). The rule, with two classes of credits per link and direction:

- **Data credits**: one per data receive slot granted to this link. Every frame of the link's protocol (the kind's
  frames and peerlink's own BEGIN, CHUNK, END...) takes one per slot it spans. The receiver returns them in a field of
  the header of every frame it sends, and in an `RDMA_CREDIT` frame when it owes a quarter of the window and has
  nothing else to send. `plink_pause` stops returning data credits, which stops the sender as a closed TCP window does.
- **Control credits**: K = 4 small receive slots per link reserved for PING, PONG and `RDMA_CREDIT`, counted apart from
  the data credits. A control frame takes one control credit; a side sends one only while it holds one, so at most K
  are in flight per direction and they cannot storm. Control credits are returned only piggybacked, in the header of
  the next frame of either class, never by a frame of their own; an `RDMA_CREDIT` that would return only control
  credits is sent only when at least K/2 are owed, so a credit exchange dies out instead of answering itself.
  `plink_pause` never withholds control credits: PINGs and PONGs go on, and a paused link is never counted dead
  (peerlink.h's rule for paused links).
- Both sides at zero data credits with frames queued is not a deadlock: each still holds control credits, and an
  `RDMA_CREDIT` returns data credits as soon as the receiver reposts slots.
- The link-level `RDMA_CREDIT` is not fwd's FWDQ `CREDIT`: that is an application frame of kind 4 that travels as data
  under the link's credits. Two layers, kept apart.

**Completions in the event loop.** The link thread keeps its `poll()`: the completion channel's fd joins the socket
and the eventfd (`ibv_req_notify_cq`, then `ibv_get_cq_event` and drain the CQ when it is readable). Sleeping on
events costs about 5 µs per message against busy polling (§3.1, one way: 6.0-7.5 µs against 1.2-2.5 µs for 64 B-1 KiB); a short
busy poll before sleeping (`rdma.poll_us`, default 0, rsocket's `polling_time`) buys it back on the LOG link when CPUs
are free.

**Keepalive and failure.** An RC queue pair learns of a dead peer only when it sends (retry count x timeout: about 0.5 s
with retry 7 and timeout 14), so the link's PING every `ping_ms` and the `dead_ms` rule stay as they are, sent over the
QP so that they test the path the data takes. A QP in the error state ends the connection, exactly as a TCP error does:
the queue is dropped, `on_link(down)` runs, the dialer redials (with the upgrade, §5). The anchor TCP connection's end
ends the connection too. A QP that fails soon after it settled counts against the offer backoff like a failed offer,
so a flapping port puts the link on TCP for a while instead of taking it down again and again (§5.3).

**Written and lost.** A frame posted to the send queue may or may not reach the peer when the QP fails afterwards,
just as bytes in a TCP socket buffer. The rule stays: WRITTEN from the moment the first work request of the frame is
posted, NEVER_WRITTEN only while it is still in the link's queue (D5). A completion with `IBV_WC_WR_FLUSH_ERR` does
not make it NEVER_WRITTEN: a work request ahead of it may have carried part of the frame, and a flush is not a
statement about what the peer received. fwd's outcomes (`FWD_NEVER_WRITTEN` definite, `FWD_WRITTEN` indeterminate until
a reply) carry over unchanged.

**Fencing.** One QP belongs to one connection (one `plink_gen`). When a connection ends, its QP is moved to the error
state, every work request it posted is drained until its completion has come back (flushed or not: a last signalled
marker request on each queue, the `ib_drain_qp` pattern), the QP is destroyed, and only then do its slots return to
the pool, so nothing an old connection posted can land in memory a newer one uses. A send slot returns only after its
send completion; with unsignalled sends (every n-th signalled), a slot returns only on the next signalled completion
after it. With an SRQ the receive slots it holds for the dead QP are those of the links' credit shares, returned when
the QP's last receive completion has been seen; with
SEND/RECV only (no remote keys handed out, D9) a peer can never write into our memory outside the receive slots we
posted for that QP. `plink_send_gen` keeps meaning "this connection only": a frame of generation G is posted only to
G's QP.

**Token buckets.** Unchanged in mechanism: a bulk chunk takes its tokens before it is posted, so a kind's rate is the
same on both transports. At the defaults they, not the transport, set bulk throughput (§1); raising them is D11.

**Sharing the port.** In production the volumes are NFSv4.1/4.2 over RDMA exports of ZFS (design-ha-modes.md: an
"RDMA port" fault is a storage fault), on the same InfiniBand fabric and in all likelihood the same HCA port. A peer
bulk stream at line rate competes with every NFS read and write of the node, including the replica's write of the
very bytes it receives and the WAL `fdatasync` that the LOG row above counts as the commit path's dominant latency. Three
measures: (a) the queue pairs' **service level** is a setting (`cluster.peer.rdma.sl`, put in the QP's address
vector, `ah_attr.sl`), one SL for every kind or one per kind, so that an operator whose subnet manager maps SLs to
virtual lanes with arbitration weights can rank peer bulk below storage and LOG above it (the SM's tables are
operations, D13); (b) the **rate caps stay** and are sized against the port, whatever the transport (D11); (c) rd-hw
measures NFS/RDMA read and write latency, and the WAL's `fdatasync`, under peer bulk load at several caps, and the
end-to-end rebuild rate, which the receiver's NFS write rate bounds. 50 Gbit/s is a wire number, not a rebuild rate.

## 5. The options

| | what | IPoIB needed | code | for tsdb |
|---|---|---|---|---|
| A. IPoIB only | the peer address names the IPoIB interface; nothing changes in tsdb | yes | none | **ruled out**: the owner allows no TCP over IPoIB. It would also gain nothing: the MAC cap first, then TCP over IPoIB (datagram, MTU 2044) is slower and costlier than TCP on 40GbE, no latency gain, and servers without IPoIB cannot take part |
| B. rsocket under plink | `rsocket`/`rpoll` calls instead of the socket calls in peerlink.c (or LD_PRELOAD) | yes (rdma_cm resolves IP addresses) | small | 30-46 Gbit/s, 6 µs, TCP's CPU per GB; but rsocket's own threads, buffers and keepalive sit under peerlink's, fork is unsafe, fallback to TCP is silent under preload, and the library sees little upkeep. Hides what §4 needs to control |
| C. verbs under peerlink, connected by rdma_cm | RC QPs through rdma_cm on the RDMA address | yes on IB (not on RoCE/iWARP: they have Ethernet IPs) | medium | the full gain; the only way for iWARP |
| D. verbs under peerlink, upgraded from the TCP link | the TCP data link authenticates as today, then both sides exchange QP numbers in MAC'd frames and move the link's frames to the QP | **no** (the TCP link runs on any network; the QP is addressed by LID/GID) | medium (C plus the exchange) | the full gain, on every server with an IB port; the TCP link is the anchor and the fallback; firewall policy keeps working, because no RDMA link exists without its TCP link |
| E. RoCE on the 40GbE ports | C or D on port 2 of the ConnectX-3 | no | same code as C/D | needs a lossless or ECN-configured Ethernet to behave under load; the switch configuration is the cost |
| F. iWARP on the Chelsio ports | C (iWARP connects only through rdma_cm) | no (Ethernet IPs) | same code as C | only two test servers have it; iWARP runs TCP in the NIC |

**Recommendation: D, with C for iWARP and as an option where IPoIB exists** (D2). One verbs code path serves IB, RoCE
and iWARP; only the connection step differs.

### 5.1 The upgrade

The upgrade is a state machine with **one decider, the dialer**, and one rule that removes every ordering question:
**from the offer until the dialer's decision, each side sends nothing on TCP but the upgrade's own frames, and after the
decision a side sends on exactly one transport**. No frame of the link ever needs to be ordered against a frame on the
other transport.

1. The dialer connects TCP to the peer address and runs the handshake exactly as today (HELLO with the kind,
   HELLO_ACK, AUTH). Nothing about authentication changes. HELLO and HELLO_ACK carry a capability bit `PL_CAP_RDMA`,
   set by every build of cluster version 8 or later (rd-mac bumps `PL_CL_VERSION` to 8, and peerlink refuses a HELLO
   of another version, so an older build never sees an offer). A `NO_RDMA` build sets the bit too: it decodes
   `RDMA_OFFER` and answers `RDMA_DECLINE{not_built}`.
2. If its `cluster.peer.rdma` is `auto` or `on`, the kind is one it moves (all but META), the peer set the bit and no
   backoff holds (§5.3), the dialer stops sending on TCP (frames queue; the upgrade takes milliseconds and at most the
   2 s timeout, below `dead_ms`) and sends `RDMA_OFFER` (a link frame, type 0x77): its device, port, LID, GID, the QP
   number and first PSN of a QP it created for this connection, the path MTU, its receive credits, a random 64-bit
   nonce.
3. The acceptor, if it can, creates its QP, moves it to RTR with the dialer's numbers, posts its receives and answers
   `RDMA_ACCEPT` with its own numbers on TCP; else `RDMA_DECLINE{reason}` on TCP, after which both sides are settled on
   TCP (`not_built`, `off` and `kind` declines stop offers to that peer and kind for the connection's life; the others
   count for the backoff). After ACCEPT the acceptor sends nothing on TCP, and nothing on the QP but READY, until it settles (step 6).
4. The dialer moves its QP to RTR and RTS and posts `RDMA_HELLO{nonce}` on the QP.
5. The acceptor checks HELLO and posts `RDMA_READY` on the QP.
6. **The dialer decides.** On READY within 2 s of the offer it settles on the QP and posts `RDMA_GO` there as its first
   frame (its queued frames follow on the QP). On anything else (a timeout, a QP error, a refused registration, no
   route to the GID) it sends `RDMA_ABORT` on TCP and settles on TCP, destroying its QP as in "Fencing" (§4); a READY
   that arrives later is discarded with the QP. The acceptor settles on the QP when GO arrives there, or on TCP when
   ABORT arrives on TCP, whichever comes: the dialer sends exactly one of the two, so the acceptor cannot settle
   differently. An acceptor that sees neither within 4 s of its ACCEPT ends the connection (the dialer redials). With
   `on`, the dialer ends the connection instead of sending ABORT.
7. **Sequence numbers.** OFFER, ACCEPT, DECLINE, GO and ABORT take the next number of their direction as every frame
   does. HELLO and READY are tagged under the link's key with the number of the OFFER (dialer) and of the ACCEPT
   (acceptor) and with their own types, and take no number: whether they were delivered or not, the stream goes on
   at the same number on whichever transport is settled, and nothing is skipped or reused. The link's keys are this
   connection's (derived from the handshake's transcript, `keys_derive`), so a HELLO cannot be replayed into another
   connection.
8. Once settled on the QP, the TCP connection carries nothing but the kernel's keepalive, and its end is the end of
   the connection. The same machine runs on a link that is already up (a re-offer, §5.3): the queued frames wait at
   most the timeout, and the drivers see no `on_link` change.

`on_link(up)` runs on each side once that side has settled (the dialer at its decision, the acceptor at GO or ABORT),
so a driver never sees the change of transport. On InfiniBand without IPoIB the QP's address comes from the LID (and the SM's path) exchanged in
step 2 (measured, §3.1: numbers exchanged over TCP on the Ethernet addresses, data on the IB port by LID, 50.8 Gbit/s
and 2.22 µs, the same as through rdma_cm); on RoCE it is the GID that carries the Ethernet IP; iWARP cannot do this and uses C: step 2 then names an
rdma_cm address and port, and the dialer connects there with the nonce in the connection's private data.

### 5.2 Discovery and configuration

No change to the metadata group (no member record field): a member advertises nothing new; capabilities are offered
per link in step 2. New keys (tsdbd_keys.h rows, docs/config.md):

| key | flag | default | meaning |
|---|---|---|---|
| `cluster.peer.rdma` | `--peer-rdma` | `auto` | `off`, `auto` (use where both ends can, else TCP), `on` (a data link that cannot upgrade ends; for tests and for operators who want to know) |
| `cluster.peer.rdma.device` | `--peer-rdma-device` | the first active InfiniBand port | `mlx4_0:1`: device and port; a list for servers with more than one. Unset, only ports whose link layer is InfiniBand (`IBV_LINK_LAYER_INFINIBAND`) are taken: a RoCE or iWARP port (port 2 of the test servers' ConnectX-3 is 40GbE) is used only when named (D10) |
| `cluster.peer.rdma.kinds` | `--peer-rdma-kinds` | `log,bulk,fwd,fwdq,snap,chunk,chunkb` | the kinds that upgrade |
| `cluster.peer.rdma.pool_mb` | `--peer-rdma-pool-mb` | 64 | registered memory per tsdbd and device, shared by its three peerlinks (§4) |
| `cluster.peer.rdma.sl` | `--peer-rdma-sl` | `0` | the InfiniBand service level of the QPs: one number, or `KIND=SL,...` (`log=1,bulk=2,snap=2`); effective only where the subnet manager maps SLs to VLs (§4 "Sharing the port", D13) |
| `cluster.peer.rdma.retry_s` | `--peer-rdma-retry-s` | 600 | a link that settled on TCP after a failure (not a decline for `not_built`, `off` or `kind`) offers again after this long (§5.3) |
| `cluster.peer.rdma.poll_us` | `--peer-rdma-poll-us` | 0 | busy poll before sleeping on completion events |
| `cluster.peer.rdma.cm` | `--peer-rdma-cm` | (unset) | `ADDR[:PORT]`: also listen with rdma_cm (option C; iWARP) |
| `cluster.peer.allow_ipoib` | `--peer-allow-ipoib` | `false` | `true` lets the TCP peer address be on an IPoIB interface (§5.3); off by default because no TCP should run over IPoIB |

An offer is declined (`subnet`) when the offered GID's subnet prefix matches none of the acceptor's chosen ports: a host
on two IB fabrics then falls back at once instead of after the 2 s timeout of an unreachable LID.

Metrics (docs/metrics.md): per kind the links on RDMA and on TCP, fallbacks with their reason, credits stalls, RNR
retries, QP errors. `tsdb cluster status` names each link's transport.

### 5.3 Fallback

TCP is always there: the anchor is the TCP link, on the Ethernet address the member advertises, never an IPoIB
address (no TCP runs over IPoIB, §2). tsdbd checks this at start: a peer address bound to an interface of type
InfiniBand (`/sys/class/net/IF/type` 32, ARPHRD_INFINIBAND) is refused with a line naming the interface, unless
`cluster.peer.allow_ipoib=true` says the operator wants it (D12, track rd-upgrade). A node knows its own interfaces,
not the peer's, so the check is made where each end can make it: at start on the address tsdbd listens on for peers,
and at every dial on the local address of the connected socket (`getsockname`, looked up in `getifaddrs`): a dial
whose route leaves through an IPoIB interface is ended with that line.

A failure of the RDMA path ends the connection; the redial upgrades again. **Backoff**: a failed offer, and a QP that
fails within 60 s of settling, each count as a failure of that peer and kind; three in a row and no offer is made to
it for a minute (doubling to 10), logged once per change; a link that stays on RDMA for 60 s resets the count. So a
port that flaps after READY puts the link on TCP for a while instead of taking it down every few seconds (each time
a resume of repld). **Way back**: a link that settled on TCP because of a failure re-offers on the live link after
`cluster.peer.rdma.retry_s` (600 s) once the backoff allows it (§5.1 step 8: no `on_link` change); a decline for
`not_built`, `off` or `kind` is final for the connection. The link's drivers see the same `on_link` down and up they see after a TCP failure today, so every resume path
already tested for TCP is the one RDMA uses.

### 5.4 Build

`NO_RDMA=1` builds without any of it (like `NO_URING`). The default build **detects** rdma-core's headers
(`infiniband/verbs.h`, `rdma/rdma_cma.h`: `libibverbs-devel`/`librdmacm-devel`, `libibverbs-dev`/`librdmacm-dev` on
Ubuntu): present, it builds the transport; absent, it builds as `NO_RDMA=1` with one line from make (neither the dev
machine nor the build machine has them today, and the default build must not break there). `make dist`, CI
(ci.yml's packages) and the release image install them, so the tarball always has the transport; AGENTS.md's
Requirements, docs/macos.md and the release image get a line (rd-verbs). It links neither library: `server/plrdma_dl.c` loads `libibverbs.so.1` and
`librdmacm.so.1` with `dlopen` at the first use, so the tarball of `make dist` runs on hosts without rdma-core (one
line: RDMA not available) and `tools/check-glibc.sh` is unaffected. The fast-path calls (`ibv_post_send`,
`ibv_poll_cq`) are inline in `verbs.h` and call through the context's function table, so they need no symbol. Some
set-up calls are macros or inlines that reference exported symbols (to verify on the track against rdma-core 61's
`verbs.h`: `ibv_reg_mr` expands to `__ibv_reg_mr`, which calls `ibv_reg_mr` or `ibv_reg_mr_iova2`; `ibv_query_port`
to `___ibv_query_port` with the compat port struct): the shim calls the exported entry points it took with `dlsym`,
never those macros, and rd-verbs begins with a spike and a check that `nm -u build/tsdbd` names no `ibv_` or `rdma_`
symbol (`tools/check-rdma-syms.sh`, in `make test` when the transport is built). macOS
builds with `NO_RDMA` forced (the code is Linux-only and behind `#ifdef TSDB_RDMA`; docs/macos.md gets a line).
Licences: the new files are AGPL-3.0-only like the rest of `server/`; rdma-core's headers are used, not copied.

### 5.5 Security

The threat model is a backend on an isolated network (design-auth.md). Two facts change with RDMA: the firewall does
not see it (§2), and a peer that can reach a QP can send to it. In D the QP exists only for a connection whose TCP
handshake passed AUTH. An RC packet names only its destination QP; the responder checks its PSN and partition key
(P_Key), not who sent it, so a host on the fabric that learns a QP number and guesses the PSN could inject packets.
The protection is that every frame keeps its MAC (with the faster MAC of §6), so a forged packet on the fabric ends the link instead of
reaching a driver. No remote key is ever handed out (D9), so no peer can read or write our memory. Option C's rdma_cm
listener is reachable by anyone on the fabric; it accepts only a connection whose private data carries a nonce the
TCP handshake issued.

## 6. The frame MAC (D1)

§3.3 shows that `frame_tag` limits every link to about 230 MB/s and costs about 4 CPU s/GB per side on these servers;
OpenSSL's SHA-256 on the same CPU is about 480 MB/s and BLAKE2b about 930 MB/s (Python's hashlib, single core, one
run each): a faster SHA-256 does not lift the cap enough. The servers have no `openssl` command, so AES-GMAC and BLAKE3
are not measured here yet. Options, to be measured with `link-bench --mac` in the first track:

| MAC | speed on these CPUs | notes |
|---|---|---|
| HMAC-SHA256 (today) | about 230 MB/s per side (§3.3) | portable C, tsdb_crypto.c |
| HMAC-SHA256 with SHA extensions (SHA-NI) | none here: these Xeons lack them; 1-2 GB/s per core where present (to measure) | **no protocol change** (no new cluster version), only a dispatched kernel; helps only CPUs that have the extensions. The build machine has them and still moves 2.3 Gbit/s, because tsdb_crypto.c never uses them (§3.3). Whether production's CPUs have them is unknown: read `/proc/cpuinfo` (`sha_ni`) there before choosing |
| AES-128-GMAC (AES-NI + PCLMULQDQ) | several GB/s per core (to measure) | needs a nonce per frame (the sequence number is one); OpenSSL's libcrypto or own code; FIPS-friendly |
| BLAKE3 keyed | several GB/s per core with AVX2/AVX-512 (to measure) | CC0/Apache-2.0 reference code with SIMD kernels; fits `src/levels.h` style dispatch |
| Poly1305 with a per-frame key from ChaCha20 | 1-2 GB/s in portable 64-bit C (to measure) | no SIMD needed, small code |

**Measured (rd-mac, 2026-11-24)**: `link-bench mac` (tools/mac_cands.c; `tools/rdma-bench.sh --only mac` on the test
servers) tags frames in memory on one core: payload MB/s, mean of 3 runs (lowest-highest), built `-O3` without
`-march` as `make dist` builds (each kernel picks its instructions at run time). Every candidate is checked against
known answers first (Python's `hmac`, the `cryptography` package, BLAKE3's own vectors). The input of each tag is
peerlink's: sequence number, type, payload; GMAC and Poly1305 take sequence number and type as their nonce.

| MAC (tag bytes) | frame | Xeon E5-2697 v4 (test servers: AVX2, no SHA-NI) | i5-13400 (build machine: SHA-NI) | Ryzen 9 7950X (dev: AVX-512, SHA-NI) |
|---|---|---:|---:|---:|
| HMAC-SHA256, portable (32), today | 64 B | 71 (71-71) | 99 (99-99) | 132 (132-132) |
| | 1 KiB | 207 (207-207) | 285 (284-286) | 390 (388-391) |
| | 16 KiB | 235 (235-235) | 324 (323-326) | 446 (444-447) |
| | 1 MiB | 234 (234-234) | 320 (320-321) | 451 (451-452) |
| HMAC-SHA256, SHA-NI (32) | 64 B | not on this CPU | 387 (385-392) | 502 (502-503) |
| | 1 KiB | | 1,573 (1,571-1,576) | 2,111 (2,101-2,118) |
| | 16 KiB | | 1,960 (1,960-1,960) | 2,678 (2,676-2,680) |
| | 1 MiB | | 1,991 (1,991-1,992) | 2,723 (2,722-2,724) |
| **AES-128-GMAC** (16) | 64 B | **739 (639-789)** | **1,445 (1,440-1,448)** | **1,576 (1,574-1,578)** |
| | 1 KiB | **5,312 (5,300-5,333)** | **8,782 (8,756-8,812)** | **8,127 (8,108-8,144)** |
| | 16 KiB | **7,331 (7,301-7,379)** | **11,033 (10,986-11,067)** | **10,348 (10,280-10,476)** |
| | 1 MiB | **7,540 (7,516-7,559)** | **11,297 (11,282-11,307)** | **10,469 (10,446-10,500)** |
| Poly1305, ChaCha20 per-frame key (16) | 64 B | 329 (329-329) | 409 (409-410) | 475 (473-476) |
| | 1 KiB | 1,465 (1,464-1,465) | 1,782 (1,780-1,784) | 1,574 (1,572-1,576) |
| | 16 KiB | 1,870 (1,870-1,871) | 2,243 (2,231-2,251) | 1,824 (1,813-1,831) |
| | 1 MiB | 1,908 (1,908-1,909) | 2,290 (2,266-2,314) | 1,853 (1,850-1,856) |
| BLAKE3 keyed (16 of its output) | 64 B | 334 (332-335) | 403 (400-405) | 537 (535-538) |
| | 1 KiB | 766 (766-767) | 889 (889-890) | 1,158 (1,157-1,158) |
| | 16 KiB | 1,436 (1,435-1,436) | 1,739 (1,704-1,760) | 2,788 (2,785-2,790) |
| | 1 MiB | 2,603 (2,600-2,607) | 3,261 (3,134-3,353) | 8,595 (8,581-8,603) |

The second test server gave the same numbers within 1% (its 64 B GMAC 789). The build machine ran beside other jobs
(load about 2 on 16 threads); its hybrid cores make the spread of BLAKE3's 1 MiB row.

**The choice: AES-128-GMAC** (`server/plmac.c`). It is the fastest at every frame size on all three CPUs: on the test
servers 32 times today's HMAC at 1 MiB (7.5 GB/s per core, more than a 40GbE link carries) and 26 times at 1 KiB,
where BLAKE3 needs whole 1 KiB chunks in parallel to use its SIMD width and Poly1305 pays a ChaCha20 block per frame.
SHA-NI would not help the test servers at all and is 4-5 times slower than GMAC where it exists. AES-NI and
PCLMULQDQ are in every x86-64-v2 CPU tsdb's tarball runs on in practice (Westmere and later), but not in the v2 level
itself, so the kernel is picked at run time and a CPU without them is refused at start. GMAC's one hazard is a nonce
used twice under one key: the nonce is the frame's sequence number and type, the key is per connection and direction
(derived from both sides' handshake nonces), so a sequence number is never reused under a key unless the counter
wraps, which peerlink refuses (2^64 frames).

Default proposal (before the measurements): the fastest that keeps a 128-bit tag and a keyed construction, negotiated in HELLO (a new cluster
version: before 1.0 no compatibility promise, design-cluster-data.md E40). If production's CPUs have SHA extensions,
dispatching HMAC-SHA256 to them is the cheapest first step and changes no protocol. Whatever is chosen, one link's
MAC runs on one core per side (§4 "CPU with the MAC"): line rate on one link needs about 6 GB/s per core, which no
candidate is expected to reach on these CPUs; spreading one link's MAC over threads is not in this design.

What rd-mac must carry (it is more than a function): a SIMD kernel (BLAKE3, SHA-NI, AES-GMAC) needs CPU-level
dispatch under `make dist` (the `src/levels.h` pattern or a runtime check like `src/cpu.c`'s, `tools/check-isa.sh`'s
v2 baseline and its allow list of wide kernels); OpenSSL's libcrypto instead would be a new linked library
(`tools/check-glibc.sh`, the tarball, AGENTS.md's Requirements). The link MAC is server-only, so it goes in a new
AGPL `server/plmac.c` (vendored reference code under its own directory with its `UPSTREAM` and licence, as `src/ec/`),
not in the Apache-2.0 `client/tsdb_crypto.c`; test vectors of the chosen MAC; `PL_CL_VERSION` 8 and the capability
bit of §5.1; test_peerlink's tag mutants against the new MAC; the fuzz-replframe corpus regenerated; and
`link-bench --mac` to take the numbers of D14. Not proposed: MAC'ing only the frame's
xxh64 (not collision resistant: a tag over it protects nothing against a peer that can change bytes), or no MAC on
RDMA (InfiniBand's ICRC catches corruption, but §5.5 needs the MAC against forged packets).

## 7. Decisions (defaults; the owner may change them)

| | decision | alternative | why |
|---|---|---|---|
| D1 | **Replace the data links' HMAC-SHA256 first**, before any RDMA code | RDMA first | §3.3: the MAC caps every link at about 1.9 Gbit/s on every transport; RDMA alone would move nothing faster for bulk. At the default rate caps D1 alone buys little for bulk (§4): it is the precondition for D11, and it halves the 1 KiB round trip's cost on LOG and FWD (26 µs of MAC, §3.3) |
| D2 | **Verbs under peerlink, upgraded from the TCP data link** (option D), rdma_cm only for iWARP and as an option | rsocket (B); rdma_cm everywhere (C); IPoIB only (A) | works without IPoIB; one auth path; firewall still governs who may connect; fallback is the link we have |
| D3 | **The metadata link stays TCP** | everything on RDMA | Raft and the failure detector stay independent of the fabric and of new code |
| D4 | **`dlopen` of libibverbs and librdmacm**; `NO_RDMA=1` to build without; macOS never | link them | one binary and one tarball for hosts with and without rdma-core |
| D5 | **WRITTEN = posted** | WRITTEN on completion | a completion says the peer's NIC acknowledged, not what the peer did; a flushed WR is not proof of nothing sent |
| D6 | **`auto` is the default** | `off` until proven | a cluster with IB uses it without configuration; `off` stays one key away |
| D7 | **One registered pool per tsdbd and device**, shared by its three peerlinks; slots of 4 KiB and 64 KiB; receives on an SRQ per peerlink and slot size, credits granted per link (SRQ against small per-link rings settled in rd-verbs); 64 MiB by default; memlock checked at start | a pool per peerlink; registration per frame or per file | registration on the data path costs tens of µs and fails unpredictably under a memlock limit; per-link 64 KiB rings do not add up (§4) |
| D8 | **One RC QP per link connection** (per peer and kind) | one shared QP per peer | keeps kinds independent (C25, R4 D7: a snapshot never delays a COMMIT file) and generations simple; ConnectX-3 has QPs to spare |
| D9 | **SEND/RECV only** in 0.11.0, no RDMA WRITE/READ, no remote keys | WRITE for bulk, READ for chunk cells | §3.1: SEND reaches line rate from 16 KiB; no rkey means no fencing of remote access to get wrong |
| D10 | **RoCE supported by the same code, not tested as a production path** until a lossless Ethernet setup exists; `auto` takes InfiniBand ports only | RoCE as a target; any active port | the production fabric is InfiniBand; a RoCE v1 link on a lossy Ethernet must never be picked silently |
| D11 | **The bulk rate caps keep their defaults** (BULK 256 MiB/s, `rebuild_mb_s` 200, CHUNKB 256, node-wide per kind) until rd-hw has measured, with the MAC fixed, the LOG link's latency and NFS/RDMA latency under bulk load; then they are raised on that evidence, per kind | raise them with D1 | the caps, not the transport, set bulk throughput today (§1); they also protect the storage that shares the port (§4) |
| D12 | **tsdbd refuses a peer address that leaves through an IPoIB interface**, unless `cluster.peer.allow_ipoib=true` (§5.3) | warn only; no check | the owner's rule: no TCP over IPoIB; a misconfigured member is found at start or dial, not by its traffic |
| D13 | **A service level per kind** (`cluster.peer.rdma.sl`, default 0) and no other QoS in tsdb | rate caps only | the SM's SL-to-VL tables and VL arbitration decide what an SL means; tsdb only has to let the operator mark its traffic (§4 "Sharing the port") |
| D14 | **A decision point after rd-mac**: §3.3 (TCP, batched and tagged) and §4 are measured again with the new MAC, on the test servers and on production's CPUs if possible, and the owner sets the order and scope of rd-seam onwards from those numbers | build all tracks in order | the case for the verbs tracks rests on numbers §6 has not taken; the owner asked for RDMA links, so the point decides order and scope, not whether |

## 8. Testing

- **Without hardware, a fake transport**: the verbs calls go through an ops table (as `src/coldops.h` does for the
  cold tier); `tests/test_plrdma.c` runs peerlink over an in-memory fake that delivers SENDs between two QPs and can
  plant every fault: a completion error at the n-th WR, a flush mid-frame, RNR, a QP error between two frames of a
  file, a peer that stops returning credits, an offer answered late, a decline, a registration that fails. Planted
  bugs (`--mutant`): WRITTEN reported on completion, a flushed WR reported NEVER_WRITTEN, an old generation's frame
  posted to the new QP, credits not reserved for PING (a paused link counted dead), slots returned before the QP is
  destroyed, the upgrade accepted without the MAC check, a fallback that keeps the old connection's queue.
  The fake fails the QP at once on a SEND that finds no posted receive (as iWARP does, and as `rnr_retry` 0 does on
  the hardware runs): a credit bug must be an error, never a stall. Further cases: both sides at zero data credits
  with frames queued; a paused link that only PINGs for 10 x `dead_ms`; READY in flight when the dialer's timeout
  fires (the dialer ABORTs, both settle on TCP, the late READY is discarded); ABORT after ACCEPT before HELLO; HELLO
  posted and then a fallback (the next TCP frame verifies: HELLO and READY took no sequence number); a GO lost to a QP
  error; the acceptor's 4 s timeout; a QP that fails after READY three times (the backoff, then a re-offer after
  `retry_s` on the live link); an offer from another IB subnet. Further mutants: a control credit returned by a frame
  of its own (a storm), a pause that withholds control credits, a slot returned before its flush completion, an
  unsignalled send's slot returned early, the acceptor settling on READY instead of GO.
- **Soft-RoCE where it exists**: the dev machine (Ubuntu kernel) has `rdma_rxe` and `siw` (neither loaded today), so
  `test_peerlink` and `test_cluster` run every existing case once more with `--peer-rdma on` over an rxe device. Making
  the device needs root (`modprobe rdma_rxe; rdma link add rxe0 type rxe netdev IF` on an Ethernet or veth interface),
  so the tests never make it: they use a device that exists and print that they are skipped otherwise. A suite that
  skips everywhere has no teeth, so the setup is named: the **weekly gate on the dev machine** makes the device once
  per boot (a step of the gate's script run with sudo by the owner, or a systemd unit) and runs with
  `TSDB_RDMA_REQUIRED=1`, which turns "skipped" into a failure. rxe pins registered pages against `RLIMIT_MEMLOCK`
  (8 MB hard on the dev machine), and test_cluster starts 3-5 tsdbd: the tests run with `pool_mb` 1 and
  `rnr_retry` 0. CI (ubuntu-24.04, Azure kernel) may not load `rdma_rxe` (it lives in linux-modules-extra): rd-rxe
  finds out; if it cannot, CI runs the fake only and says so. AlmaLinux 10 has neither module: those runs say skipped.
- **The test servers** for real hardware, by hand and in the weekly gate when they are free: `test_cluster` scenarios
  over IB (replicate, fwd, chunksrv, a snapshot), `jepsen-cluster --peer-rdma on` with a nemesis that breaks only the
  RDMA path (a QP forced to error through a test hook, `TSDB_PLRDMA_TEST`), and the §10.3 measurements of
  design-cluster.md over TCP and over IB.
- `make test-spdx`, `make check-version` unchanged; `tools/check-glibc.sh` must still pass (no new linked library).

## 9. Track plan

| key | risk | depends on | files | days |
|---|---|---|---|---|
| rd-mac | high | main | server/peerlink.c (`frame_tag` and its key schedule), new server/plmac.c (AGPL; vendored kernel code with its `UPSTREAM`), CPU-level dispatch (`src/levels.h` pattern or a runtime check; tools/check-isa.sh's allow list), `PL_CL_VERSION` 8 and `PL_CAP_RDMA`, test vectors, tools/link-bench.c `--mac`, tests/test_peerlink.c (tag mutants against the new MAC), fuzz-replframe corpus; then D14's measurements | 3 |
| rd-seam | medium | rd-mac | server/peerlink.c split: the TCP transport behind `struct pl_transport` (read, write, poll fds, close) with no change in behaviour; test_peerlink unchanged and green; `objdump` not needed (behaviour, not codegen) | 1 |
| rd-verbs | high | rd-seam, D14 | a spike first (dlopen of rdma-core 61, `tools/check-rdma-syms.sh`); new server/plrdma.[ch] (the pool per tsdbd, SRQs or rings, QP setup from exchanged numbers, data and control credits, SEND/RECV slots, frames split over slots, the drain before destroy, completion fd in the link's poll), server/plrdma_dl.c, Makefile header detection and `NO_RDMA`, ci.yml packages, the release image, AGENTS.md Requirements, docs/macos.md; tests/test_plrdma.c with the fake and its mutants | 3 |
| rd-upgrade | high | rd-verbs | peerlink.c: the state machine of §5.1 (`RDMA_OFFER/ACCEPT/DECLINE/HELLO/READY/GO/ABORT`), fallback, the backoff that counts QP failures after READY, the re-offer on a live link, the subnet decline, the SL, on_link after each side settles; the refusal of a peer address on an IPoIB interface (§5.3); tsdbd_keys.h rows, docs/config.md, metrics, `tsdb cluster status`; test_peerlink cases over the fake | 2 |
| rd-cm | medium | rd-upgrade | option C: the rdma_cm listener and dial (`cluster.peer.rdma.cm`), the nonce in private data; tests over rxe/siw (siw is iWARP-like) | 0.75 |
| rd-rxe | medium | rd-upgrade | tests: a helper that finds an rxe device (or says skipped; `TSDB_RDMA_REQUIRED=1` fails instead), the weekly gate's setup step on the dev machine (the device, memlock), test_peerlink and test_cluster `rdma` runs with a tiny pool and `rnr_retry` 0, whether CI can load rdma_rxe and the CI job if it can | 1 |
| rd-hw | low | rd-rxe | runs on the test servers: test_cluster and jepsen-cluster over IB, tsdbc-bench `via` and insert-bench on 3 members, TCP against IB, this document's §3 again with the MAC fixed (batched, buffers on both sides); on a production-like node NFS/RDMA and WAL `fdatasync` latency under peer bulk load at several caps, and the end-to-end rebuild rate (D11); results here | 1.5 |

About 12 days; rd-mac stands alone and is worth merging first even if the rest waits, and D14's decision point
follows it. The gates: the targeted tests of
each track, `make asan` and `make tsan` of test_peerlink and test_plrdma (memory registered and freed across threads),
the cluster gate and `make jepsen-cluster` once rd-upgrade is in, and a weekly run over rxe.

## 10. Not in this design

- RDMA READ of chunk cells straight from a store (needs registered page cache: ODP, which ConnectX-3 lacks, or a
  registered cell cache) and RDMA WRITE for bulk files: after the 0.11.0 numbers show the copy matters.
- NFS over RDMA is the kernel's (docs/nfs-server.md) and this design does not change it, but the peer links share
  its fabric and port: §4 "Sharing the port", D11, D13.
- Clients over RDMA: clients talk to tsdbd over TCP; out of scope.
- Configuring IPoIB connected mode, the SM, PFC/ECN for RoCE: operations, not tsdb.
