# RDMA for the peer links

Status: design and measurements, no transport code. Branch `rdma-design` from main at a06d4bb7, written on 2026-11-24.
Target release: **v0.11.0** (the build is the tracks of §9).

The owner's request (2026-11-24, verbatim): "all servers should have infiniband too, but I do not think everyone is
configure with ipoib yet. We should support RDMA links". Every link between two tsdbd today is TCP
(`server/peerlink.c`), whatever network it runs on. This document measures what InfiniBand and the Ethernet RDMA
flavours give tsdb's frames on the test servers, maps each link kind to what RDMA would change for it, compares the
ways to get there and proposes a plan.

Related: [design-cluster.md](design-cluster.md) §5.1.1 (data links, C25), [design-cluster-data.md](design-cluster-data.md)
§5.7 (CHUNK and CHUNKB), [ha.md](ha.md) §15 and [design-ha-modes.md](design-ha-modes.md) (production: NFSv4/RDMA to ZFS on
AlmaLinux 10), [design-auth.md](design-auth.md) (the threat model: a backend on an isolated network).

## Summary

1. **The first limit is not the network: it is the frame MAC.** Every data-link frame carries an HMAC-SHA256 tag over
   all of its bytes (`frame_tag`, peerlink.c). On the test servers' Xeons (no SHA extensions) that costs about 4.2 CPU
   seconds per GB on each side and caps **one link at about 1.9 Gbit/s on every transport**: TCP on 40GbE, rsocket
   and TCP on loopback alike (§3.3). Without the tag the same link moves about 30 Gbit/s over TCP on 40GbE. A
   replicated large insert, a snapshot or a rebuild therefore runs at about 230 MB/s per peer today, wherever it runs.
   Fixing the MAC (§6, D1) is the first track, helps TCP right away, and is a precondition for RDMA to matter for bulk.
2. **Native verbs on the FDR InfiniBand** move 50 Gbit/s from 16 KiB messages up, at 0.02-0.06 CPU seconds per GB
   (event-driven completions, §3.1), and a 1 KiB round trip takes 2.5 µs busy-polling or about 8 µs sleeping on
   completion events, against about 28 µs over TCP on 40GbE. rsocket (librdmacm's socket emulation) reaches about
   30 Gbit/s and a 6 µs round trip (§3.2).
3. **IPoIB is not needed for RDMA if the connection is set up over the TCP link we already have.** rdma_cm and rsocket
   resolve IP addresses, so they need IPoIB on InfiniBand. A queue pair can instead be set up by exchanging its
   numbers (LID or GID, QPN, PSN, MTU) over the authenticated TCP data link, which runs on whatever network the peer
   address is on. That is how the recommended design works on servers without IPoIB (§5, D2).
4. **Recommendation**: a verbs transport *under* peerlink (RC queue pairs, SEND/RECV with credits, one QP per link
   connection), reached by **upgrading** a TCP data link after its handshake; the TCP connection stays as the link's
   anchor (liveness, fallback, firewall control). The metadata link (kind 0, Raft) stays TCP. libibverbs and
   librdmacm are loaded with `dlopen`, so one binary runs with and without rdma-core; `NO_RDMA=1` builds without it;
   macOS never has it. About 8 track-days (§9).
5. **Operational prerequisites**: `LimitMEMLOCK` for tsdbd (the test servers' 8 MB limit already refuses perftest's
   4 MiB messages, §2), and a test path without hardware: Soft-RoCE (`rdma_rxe`) and `siw` exist in Ubuntu kernels
   (the dev machine has both modules) but **not in AlmaLinux 10's kernel** (§8).

## 1. What travels on the peer links today

A peerlink (`server/peerlink.h`) is one TCP connection per (peer, kind), one thread per connection that owns the
socket, a bounded send queue (8 MiB), `poll()` on the socket and an eventfd, the handshake (HELLO, HELLO_ACK, AUTH:
HMAC over the transcript under the cluster secret), then frames `[u32 len][u8 type][payload][tag 32]` with the tag
`HMAC-SHA256(key of the direction, seq | type | payload)`. Log frames are zstd-1 compressed. Bulk files go as
`BEGIN`, `RESUME`, `CHUNK` (256 KiB) at the rate of a token bucket per kind (256 MiB/s by default), `END` (xxh64), `DONE`.
`on_sent` reports a tracked frame WRITTEN once its first byte reached the socket, NEVER_WRITTEN otherwise (fwd's rule).

| kind | owner | what | frame sizes | what matters |
|---|---|---|---|---|
| 0 META | cluster.c | Raft, heartbeats, watch, admin ops | small | liveness, the failure detector |
| 1 LOG | repld.c | WAL records, ACKs, heartbeats per volume | 64 B (ACK) to 1 MiB, typical 1-64 KiB zstd'd | latency: on the commit path of every replicated insert |
| 2 BULK | repld.c | a COMMIT's part files | 256 KiB chunks of files up to GBs | throughput, CPU; must not delay LOG |
| 3 FWD | fwd.c | forwarded requests and replies | up to 8 MiB, typical < 4 KiB | latency: one extra hop on a client's request; NEVER_WRITTEN vs WRITTEN |
| 4 FWDQ | fwd.c | forwarded queries, streamed replies under CREDIT | 1 MiB chunks | throughput and latency of a query through an entry node |
| 5 SNAP | rsnap.c | snapshots (R4) | 256 KiB chunks, GBs | throughput: rebuild time (2 TB in 30 min is 1.1 GB/s) |
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
- **firewalld** lets only ssh in on the IPoIB interfaces, so TCP over IPoIB could not be measured (§3.4). TCP on the
  40GbE ports is reachable. **RDMA traffic bypasses netfilter**: rdma_cm and RC queue pairs connected through a port
  firewalld closes for TCP. A design that relies on the firewall to keep strangers off the RDMA path is wrong (§5.5).
- **`ulimit -l` is 8192 KiB** (8 MB of locked memory per process). Memory registered for RDMA is locked: perftest
  could not register its buffers for 4 MiB messages (`Cannot allocate memory`). io_uring's rings count against the
  same limit.
- **No Soft-RoCE**: AlmaLinux 10's kernel has neither `rdma_rxe` nor `siw` (`modinfo` finds neither).

## 3. Measurements

Tools: `tools/rdma-bench.sh` (drives a pair of servers over ssh; hosts and addresses are arguments) runs perftest
through rdma_cm and `tools/link-bench` (new: peerlink's frame shape and HMAC tag over a stream socket, so the same
binary measures TCP and, under `LD_PRELOAD=librspreload.so`, rsocket). Each number is the mean of 3 runs of 3 s
(perftest: a fixed count of messages, about 3 s at line rate); the spread is the lowest and highest run. CPU is user +
system time of each side's process (bash `times` for perftest, `getrusage` in link-bench) divided by the payload
moved. perftest's CPU includes its connection setup and registration (a fixed 0.01-0.2 s), so its small numbers are
upper bounds. Raw results stay outside the repository. Pair: two Gold 6148 servers, back to back through the IB
switch.

### 3.1 Native verbs (perftest, rdma_cm, InfiniBand FDR)

<!-- results: verbs -->

### 3.2 rsocket (librspreload under link-bench, InfiniBand)

<!-- results: rsocket -->

### 3.3 TCP, and what the frame MAC costs

<!-- results: tcp -->

### 3.4 Not measured

- **TCP over IPoIB**: refused by the servers' firewall; the run says "not measured" until the owner opens it. With
  datagram mode and MTU 2044 it is expected to be well below the 40GbE TCP numbers; either way the MAC limit of §3.3
  applies to it as to every stream.
- **RoCE and iWARP**: see §3.5.

### 3.5 RoCE (ConnectX-3 port 2, 40GbE) and iWARP (Chelsio T5, 40GbE)

<!-- results: roce iwarp -->

## 4. What RDMA would give each kind, and at what cost

The gains, from §3 on these servers (the MAC fixed as in §6; with today's MAC every kind is capped at about 230 MB/s
whatever the transport):

| kind | gain | cost and care |
|---|---|---|
| 0 META | none worth having (a few frames a second) | **stays TCP** (D3): the failure detector and Raft must not depend on the new code or on the fabric's health. A volume whose RDMA path fails falls back to TCP; the metadata group never notices |
| 1 LOG | a 1 KiB round trip of about 8 µs (events) instead of 28 µs, and a third of the CPU per frame; zstd stays (it saves bytes on the wire and in the receiver's WAL alike) | the commit path also holds the replica's `fdatasync` (0.7 ms on NVMe), so the saving is a few % of a replicated insert's latency (target p50 ≤ 0.9 ms). Worth it, not decisive |
| 2 BULK, 5 SNAP, 7 CHUNKB | line rate (50 Gbit/s) at 0.02-0.06 CPU s/GB against 0.14-0.17 for TCP: the CPU left for merges and queries during a rebuild | registered buffers on both sides (pread into a registered slot, SEND, the receiver writes the slot to the file); the token buckets count what is posted, unchanged |
| 3 FWD | one hop of about 8 µs instead of 28 µs on every forwarded request | WRITTEN must mean "posted" (D5), so fwd's NEVER_WRITTEN stays definite |
| 4 FWDQ | streamed replies at line rate; CREDIT maps directly onto RDMA credits | the reply's credits and the link's credits are separate layers; keep them separate |
| 6 CHUNK | a 1 MiB cell in about 0.2 ms of wire time instead of about 0.3 ms over TCP on 40GbE; k cells of a cold read in parallel | RDMA READ straight from the store's page cache would need registered file memory (ODP: not on ConnectX-3): SEND/RECV with a copy is the design; READ is later work (§10) |

**Registration.** Every byte that RDMA moves must lie in registered (locked) memory. The design registers one pool
per peerlink at start (D7): slots of 64 KiB for frames, used by every link of that peerlink, so registration never
happens on the data path. A pool of 64 MiB serves 7 kinds x 8 peers with 16 slots per direction per link of the
busiest kinds; the sizing is a setting. tsdbd needs `LimitMEMLOCK=infinity` (or a value above the pool plus the
io_uring rings) in its systemd unit; with a lower limit it says so in one line and stays on TCP (`auto`) or refuses to
start (`on`).

**Flow control.** SEND needs a posted receive at the peer, and a SEND that finds none is retried by the NIC (RNR) and
then fails the QP. So the receiver grants credits: one per posted receive slot, returned in every frame's header and in
a small CREDIT frame when it has nothing else to send (rsocket does the same). `plink_pause` stops returning credits,
which stops the sender exactly as a closed TCP window does; two slots per direction are kept for the link's own
PING, PONG and CREDIT so that a paused link is never counted dead (peerlink.h's rule for paused links).

**Completions in the event loop.** The link thread keeps its `poll()`: the completion channel's fd joins the socket
and the eventfd (`ibv_req_notify_cq`, then `ibv_get_cq_event` and drain the CQ when it is readable). Sleeping on
events costs about 5 µs per message against busy polling (§3.1: 6-8 µs against 1.2-2.5 µs for 64 B-1 KiB); a short
busy poll before sleeping (`rdma.poll_us`, default 0, rsocket's `polling_time`) buys it back on the LOG link when CPUs
are free.

**Keepalive and failure.** An RC queue pair learns of a dead peer only when it sends (retry count x timeout: about 0.5 s
with retry 7 and timeout 14), so the link's PING every `ping_ms` and the `dead_ms` rule stay as they are, sent over the
QP so that they test the path the data takes. A QP in the error state ends the connection, exactly as a TCP error does:
the queue is dropped, `on_link(down)` runs, the dialer redials (with the upgrade, §5). The anchor TCP connection's end
ends the connection too.

**Written and lost.** A frame posted to the send queue may or may not reach the peer when the QP fails afterwards,
just as bytes in a TCP socket buffer. The rule stays: WRITTEN from the moment the first work request of the frame is
posted, NEVER_WRITTEN only while it is still in the link's queue (D5). A completion with `IBV_WC_WR_FLUSH_ERR` does
not make it NEVER_WRITTEN: a work request ahead of it may have carried part of the frame, and a flush is not a
statement about what the peer received. fwd's outcomes (`FWD_NEVER_WRITTEN` definite, `FWD_WRITTEN` indeterminate until
a reply) carry over unchanged.

**Fencing.** One QP belongs to one connection (one `plink_gen`). When a connection ends, its QP is destroyed before
its slots return to the pool, so nothing an old connection posted can land in memory a newer one uses; with
SEND/RECV only (no remote keys handed out, D9) a peer can never write into our memory outside the receive slots we
posted for that QP. `plink_send_gen` keeps meaning "this connection only": a frame of generation G is posted only to
G's QP.

**Token buckets.** Unchanged: a bulk chunk takes its tokens before it is posted, so a kind's rate is the same on both
transports.

## 5. The options

| | what | IPoIB needed | code | for tsdb |
|---|---|---|---|---|
| A. IPoIB only | the peer address names the IPoIB interface; nothing changes in tsdb | yes | none | the MAC cap first; then TCP over IPoIB (datagram, MTU 2044) is slower and costlier than TCP on 40GbE; no latency gain; servers without IPoIB cannot take part |
| B. rsocket under plink | `rsocket`/`rpoll` calls instead of the socket calls in peerlink.c (or LD_PRELOAD) | yes (rdma_cm resolves IP addresses) | small | 30 Gbit/s, 6 µs; but rsocket's own threads, buffers and keepalive sit under peerlink's, fork is unsafe, fallback to TCP is silent under preload, and the library sees little upkeep. Hides what §4 needs to control |
| C. verbs under peerlink, connected by rdma_cm | RC QPs through rdma_cm on the RDMA address | yes on IB (not on RoCE/iWARP: they have Ethernet IPs) | medium | the full gain; the only way for iWARP |
| D. verbs under peerlink, upgraded from the TCP link | the TCP data link authenticates as today, then both sides exchange QP numbers in MAC'd frames and move the link's frames to the QP | **no** (the TCP link runs on any network; the QP is addressed by LID/GID) | medium (C plus the exchange) | the full gain, on every server with an IB port; the TCP link is the anchor and the fallback; firewall policy keeps working, because no RDMA link exists without its TCP link |
| E. RoCE on the 40GbE ports | C or D on port 2 of the ConnectX-3 | no | same code as C/D | needs a lossless or ECN-configured Ethernet to behave under load; the switch configuration is the cost |
| F. iWARP on the Chelsio ports | C (iWARP connects only through rdma_cm) | no (Ethernet IPs) | same code as C | only two test servers have it; iWARP runs TCP in the NIC |

**Recommendation: D, with C for iWARP and as an option where IPoIB exists** (D2). One verbs code path serves IB, RoCE
and iWARP; only the connection step differs.

### 5.1 The upgrade

1. The dialer connects TCP to the peer address and runs the handshake exactly as today (HELLO with the kind,
   HELLO_ACK, AUTH). Nothing about authentication changes.
2. If its `cluster.peer.rdma` is `auto` or `on` and the kind is one it moves (all but META), it sends `RDMA_OFFER`
   (a link frame, type 0x77): its device, port, LID, GID, the QP number and first PSN of a QP it created for this
   connection, the path MTU, the number of receive slots it posted and their size, a random 64-bit nonce.
3. The acceptor, if it can, creates its QP, moves it to RTR with the dialer's numbers, posts its receives and answers
   `RDMA_ACCEPT` with its own numbers; else `RDMA_DECLINE{reason}`. A peer of an older build ends the link on an
   unknown frame type, which the dialer sees as a decline: next time it does not offer to that peer for 10 minutes.
4. The dialer moves its QP to RTR and RTS and sends a first frame over the QP: `RDMA_HELLO{nonce}` with the frame MAC
   under the link's key and sequence numbers continuing from the TCP stream. The acceptor checks it and answers
   `RDMA_READY` over the QP. From then on every frame of the link goes over the QP; the TCP connection carries nothing
   but the kernel's keepalive, and its end is the end of the connection.
5. Any failure before `RDMA_READY` (no route to the GID, a refused registration, a timeout of 2 s) leaves the link on
   TCP for this connection, with one log line; `on` instead ends the connection.

`on_link(up)` runs once the link's transport is settled (after step 4 or the fallback), so a driver never sees the
change of transport. On InfiniBand without IPoIB the QP's address comes from the LID (and the SM's path) exchanged in
step 2; on RoCE it is the GID that carries the Ethernet IP; iWARP cannot do this and uses C: step 2 then names an
rdma_cm address and port, and the dialer connects there with the nonce in the connection's private data.

### 5.2 Discovery and configuration

No change to the metadata group (no member record field): a member advertises nothing new; capabilities are offered
per link in step 2. New keys (tsdbd_keys.h rows, docs/config.md):

| key | flag | default | meaning |
|---|---|---|---|
| `cluster.peer.rdma` | `--peer-rdma` | `auto` | `off`, `auto` (use where both ends can, else TCP), `on` (a data link that cannot upgrade ends; for tests and for operators who want to know) |
| `cluster.peer.rdma.device` | `--peer-rdma-device` | first active port | `mlx4_0:1`: device and port; a list for servers with more than one |
| `cluster.peer.rdma.kinds` | `--peer-rdma-kinds` | `log,bulk,fwd,fwdq,snap,chunk,chunkb` | the kinds that upgrade |
| `cluster.peer.rdma.pool_mb` | `--peer-rdma-pool-mb` | 64 | registered memory per peerlink |
| `cluster.peer.rdma.poll_us` | `--peer-rdma-poll-us` | 0 | busy poll before sleeping on completion events |
| `cluster.peer.rdma.cm` | `--peer-rdma-cm` | (unset) | `ADDR[:PORT]`: also listen with rdma_cm (option C; iWARP) |

Metrics (docs/metrics.md): per kind the links on RDMA and on TCP, fallbacks with their reason, credits stalls, RNR
retries, QP errors. `tsdb cluster status` names each link's transport.

### 5.3 Fallback

TCP is always there: the anchor is the TCP link. A failure of the RDMA path ends the connection; the redial upgrades
again; a peer whose offers fail three times in a row is not offered to for a minute (doubling to 10), logged once per
change. The link's drivers see the same `on_link` down and up they see after a TCP failure today, so every resume path
already tested for TCP is the one RDMA uses.

### 5.4 Build

`NO_RDMA=1` builds without any of it (like `NO_URING`); the default build needs only rdma-core's headers
(`libibverbs-devel`, `librdmacm-devel`) and links neither library: `server/plrdma_dl.c` loads `libibverbs.so.1` and
`librdmacm.so.1` with `dlopen` at the first use, so the tarball of `make dist` runs on hosts without rdma-core (one
line: RDMA not available) and `tools/check-glibc.sh` is unaffected. The fast-path calls (`ibv_post_send`,
`ibv_poll_cq`) are inline in `verbs.h` and call through the context's function table, so they need no symbol. macOS
builds with `NO_RDMA` forced (the code is Linux-only and behind `#ifdef TSDB_RDMA`; docs/macos.md gets a line).
Licences: the new files are AGPL-3.0-only like the rest of `server/`; rdma-core's headers are used, not copied.

### 5.5 Security

The threat model is a backend on an isolated network (design-auth.md). Two facts change with RDMA: the firewall does
not see it (§2), and a peer that can reach a QP can send to it. In D the QP exists only for a connection whose TCP
handshake passed AUTH; an RC QP in RTR accepts packets only from the remote QP number and address it was given; and
every frame keeps its MAC (with the faster MAC of §6), so a forged packet on the fabric ends the link instead of
reaching a driver. No remote key is ever handed out (D9), so no peer can read or write our memory. Option C's rdma_cm
listener is reachable by anyone on the fabric; it accepts only a connection whose private data carries a nonce the
TCP handshake issued.

## 6. The frame MAC (D1)

§3.3 shows that `frame_tag` limits every link to about 230 MB/s and costs about 4 CPU s/GB per side on these servers;
OpenSSL's SHA-256 on the same CPU is about 480 MB/s (Python's hashlib, single core): a faster SHA-256 does not lift the
cap enough. Options, to be measured with `link-bench --mac` in the first track:

| MAC | speed on these CPUs | notes |
|---|---|---|
| HMAC-SHA256 (today) | about 230 MB/s per side (§3.3) | portable C, tsdb_crypto.c |
| AES-128-GMAC (AES-NI + PCLMULQDQ) | several GB/s per core (to measure) | needs a nonce per frame (the sequence number is one); OpenSSL's libcrypto or own code; FIPS-friendly |
| BLAKE3 keyed | several GB/s per core with AVX2/AVX-512 (to measure) | CC0/Apache-2.0 reference code with SIMD kernels; fits `src/levels.h` style dispatch |
| Poly1305 with a per-frame key from ChaCha20 | 1-2 GB/s in portable 64-bit C (to measure) | no SIMD needed, small code |

Default proposal: the fastest that keeps a 128-bit tag and a keyed construction, negotiated in HELLO (a new cluster
version: before 1.0 no compatibility promise, design-cluster-data.md E40). Not proposed: MAC'ing only the frame's
xxh64 (not collision resistant: a tag over it protects nothing against a peer that can change bytes), or no MAC on
RDMA (InfiniBand's ICRC catches corruption, but §5.5 needs the MAC against forged packets).

## 7. Decisions (defaults; the owner may change them)

| | decision | alternative | why |
|---|---|---|---|
| D1 | **Replace the data links' HMAC-SHA256 first**, before any RDMA code | RDMA first | §3.3: the MAC caps every transport at about 1.9 Gbit/s; RDMA alone would move nothing faster for bulk |
| D2 | **Verbs under peerlink, upgraded from the TCP data link** (option D), rdma_cm only for iWARP and as an option | rsocket (B); rdma_cm everywhere (C); IPoIB only (A) | works without IPoIB; one auth path; firewall still governs who may connect; fallback is the link we have |
| D3 | **The metadata link stays TCP** | everything on RDMA | Raft and the failure detector stay independent of the fabric and of new code |
| D4 | **`dlopen` of libibverbs and librdmacm**; `NO_RDMA=1` to build without; macOS never | link them | one binary and one tarball for hosts with and without rdma-core |
| D5 | **WRITTEN = posted** | WRITTEN on completion | a completion says the peer's NIC acknowledged, not what the peer did; a flushed WR is not proof of nothing sent |
| D6 | **`auto` is the default** | `off` until proven | a cluster with IB uses it without configuration; `off` stays one key away |
| D7 | **One registered pool per peerlink**, slots of 64 KiB, sized by a setting (64 MiB); memlock checked at start | registration per frame or per file | registration on the data path costs tens of µs and fails unpredictably under a memlock limit |
| D8 | **One RC QP per link connection** (per peer and kind) | one shared QP per peer | keeps kinds independent (C25, R4 D7: a snapshot never delays a COMMIT file) and generations simple; ConnectX-3 has QPs to spare |
| D9 | **SEND/RECV only** in 0.11.0, no RDMA WRITE/READ, no remote keys | WRITE for bulk, READ for chunk cells | §3.1: SEND reaches line rate from 16 KiB; no rkey means no fencing of remote access to get wrong |
| D10 | **RoCE supported by the same code, not tested as a production path** until a lossless Ethernet setup exists | RoCE as a target | the production fabric is InfiniBand |

## 8. Testing

- **Without hardware, a fake transport**: the verbs calls go through an ops table (as `src/coldops.h` does for the
  cold tier); `tests/test_plrdma.c` runs peerlink over an in-memory fake that delivers SENDs between two QPs and can
  plant every fault: a completion error at the n-th WR, a flush mid-frame, RNR, a QP error between two frames of a
  file, a peer that stops returning credits, an offer answered late, a decline, a registration that fails. Planted
  bugs (`--mutant`): WRITTEN reported on completion, a flushed WR reported NEVER_WRITTEN, an old generation's frame
  posted to the new QP, credits not reserved for PING (a paused link counted dead), slots returned before the QP is
  destroyed, the upgrade accepted without the MAC check, a fallback that keeps the old connection's queue.
- **Soft-RoCE where it exists**: the dev machine (Ubuntu kernel) has `rdma_rxe` and `siw`, so `test_peerlink` and
  `test_cluster` run every existing case once more with `--peer-rdma on` over an rxe device on loopback (made by the
  test script with `rdma link add`, which needs root: the tests print that they are skipped otherwise). CI (Ubuntu
  runners) does the same. AlmaLinux 10 has neither module: those runs say skipped.
- **The test servers** for real hardware, by hand and in the weekly gate when they are free: `test_cluster` scenarios
  over IB (replicate, fwd, chunksrv, a snapshot), `jepsen-cluster --peer-rdma on` with a nemesis that breaks only the
  RDMA path (a QP forced to error through a test hook, `TSDB_PLRDMA_TEST`), and the §10.3 measurements of
  design-cluster.md over TCP and over IB.
- `make test-spdx`, `make check-version` unchanged; `tools/check-glibc.sh` must still pass (no new linked library).

## 9. Track plan

| key | risk | depends on | files | days |
|---|---|---|---|---|
| rd-mac | high | main | server/peerlink.c (`frame_tag` and its key schedule), client/tsdb_crypto.c or a new server/plmac.c, `PL_CL_VERSION` 8, tools/link-bench.c `--mac`, tests/test_peerlink.c (tag mutants against the new MAC), fuzz-replframe corpus | 1 |
| rd-seam | medium | rd-mac | server/peerlink.c split: the TCP transport behind `struct pl_transport` (read, write, poll fds, close) with no change in behaviour; test_peerlink unchanged and green; `objdump` not needed (behaviour, not codegen) | 1 |
| rd-verbs | high | rd-seam | new server/plrdma.[ch] (pool, QP setup from exchanged numbers, credits, SEND/RECV slots, frames split over slots, completion fd in the link's poll), server/plrdma_dl.c, Makefile `NO_RDMA`, tests/test_plrdma.c with the fake and its mutants | 2 |
| rd-upgrade | high | rd-verbs | peerlink.c: `RDMA_OFFER/ACCEPT/DECLINE/HELLO/READY`, fallback and offer backoff, on_link after the transport settles; tsdbd_keys.h rows, docs/config.md, metrics, `tsdb cluster status`; test_peerlink cases over the fake | 1.5 |
| rd-cm | medium | rd-upgrade | option C: the rdma_cm listener and dial (`cluster.peer.rdma.cm`), the nonce in private data; tests over rxe/siw (siw is iWARP-like) | 0.75 |
| rd-rxe | medium | rd-upgrade | tests: a helper that makes an rxe device (or says skipped), test_peerlink and test_cluster `rdma` runs, the CI job | 0.75 |
| rd-hw | low | rd-rxe | runs on the test servers: test_cluster and jepsen-cluster over IB, tsdbc-bench `via` and insert-bench on 3 members, TCP against IB, this document's §3 again with the MAC fixed; results here | 1 |

About 8 days; rd-mac stands alone and is worth merging first even if the rest waits. The gates: the targeted tests of
each track, `make asan` and `make tsan` of test_peerlink and test_plrdma (memory registered and freed across threads),
the cluster gate and `make jepsen-cluster` once rd-upgrade is in, and a weekly run over rxe.

## 10. Not in this design

- RDMA READ of chunk cells straight from a store (needs registered page cache: ODP, which ConnectX-3 lacks, or a
  registered cell cache) and RDMA WRITE for bulk files: after the 0.11.0 numbers show the copy matters.
- NFS over RDMA is the kernel's (docs/nfs-server.md); nothing here touches it.
- Clients over RDMA: clients talk to tsdbd over TCP; out of scope.
- Configuring IPoIB connected mode, the SM, PFC/ECN for RoCE: operations, not tsdb.
