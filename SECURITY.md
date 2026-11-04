# Security policy

## Supported versions

| version | supported |
|---|---|
| 0.9.x | yes: security fixes go into the latest 0.9.x release |
| older | there are none; 0.9.0 is the first release |

When 1.0 is released, this table will say which versions get fixes.

## What tsdb is, and what it is not

tsdb is a database for the **backend of a system, on a trusted network**. It is made to sit behind network
isolation (a private network, a firewall, a VPN) next to the programs that write to it and read from it. It is
not made to face the internet or any network you do not trust. Speed is a goal of the project, and the design
leaves out what would cost speed and is not needed behind isolation.

### Logins and rights

`tsdbd` lets in only clients that log in, and gives each user only the rights it was granted:

- **Login.** A client proves its password with SCRAM-SHA-256 (as PostgreSQL does). The password itself never
  crosses the network, and the server stores only a salted value made from it (the file `USERS` in the database
  directory, mode 0600). Whoever reads that file cannot log in with it alone, but can with it and one recorded
  login of the same user (see below): keep `USERS` as secret as the data it protects. The client checks the server
  too: a server that does not hold the user's stored value cannot finish the login.
- **Rights.** A user holds `read`, `insert`, `delete` and `admin` on all series, on a name prefix (a tenant such as
  `acme/`), on one series, or on the names a pattern matches. A series a user may not touch answers exactly like a
  series that does not exist, so users cannot find out which series other tenants store (only roughly how many,
  see below). A superuser may do everything.
- **Before login** a connection may send only the login messages, each at most 4 KiB, and must finish within
  `--login-timeout` seconds (10 by default). A failed login closes the connection and is logged with the user
  name and the address (at most one line a second; never a password).
- **Without a superuser `tsdbd` does not start.** `--no-auth` turns logins off: then anyone who can open a
  connection can read, write and delete any data, as before 0.9. Use it only for tests and on your own machine.

What logins do not protect against:

- **Someone who can read or change the network.** There is no TLS yet ([roadmap](docs/roadmap.md)). Data and
  queries travel in the clear. A recorded login does not show the password, but:
  - it lets the eavesdropper guess passwords offline at their own speed: use long random passwords;
  - a recorded password change plus one later recorded login of the same user is enough to log in as that user,
    with no guessing: `user add` and `user passwd` through the server send the value that `USERS` keeps, and that
    value together with a recorded login gives what the client proves. The same goes for whoever has a copy of
    `USERS` and records one login of the user;
  - someone who can change traffic, not only read it, can take over a connection after it has logged in.

  So on a network you do not trust, a login protects nothing until TLS is there. Keep the backend network
  isolated, protect `USERS` like the data, and run password changes on the server host where you can
  (`tsdb <dir> user ...` changes the file there and sends nothing over the network). TLS, which is planned,
  closes all three.
- **The metrics port** (`--metrics-port`) is plain HTTP without a login. It shows counts for the whole server, and
  per volume its directory path with its rows and bytes on disk; no series names or values. Where a volume is
  named after a tenant, anyone who reaches the port sees that name and how much the tenant stores. Bind it to a
  trusted interface (`--metrics-bind`).
- **Someone with access to the database directory or the host.** `tsdb <dir> ...` commands and programs that use
  tsdb as a library open the files directly and need no login: the file permissions decide. Whoever can write
  the directory can add a superuser.
- **State names are shared within a volume.** Series of the `state` type store labels such as `RUNNING` in one
  list per volume. A user with a right on any series of a volume can look up the labels other tenants added
  there. Where those labels are sensitive, give each tenant its own volume (its path then shows on the metrics
  port, see above).
- **How many series a volume holds.** Series ids are numbered in the order series are created, one count per
  volume. A user who creates a series sees from its id how many series the volume held before, and from two such
  ids how many other users created in between. Give each tenant its own volume where this matters.
- **Passwords a tenant admin already knows.** A user with `admin` on `acme/` can create users under `acme/` and set
  their passwords. If a superuser later lets `acme/bob` read another tenant's series, the admin can no longer
  change bob's password, but still knows the one they set before. Give such a share to a user whose password the
  tenant admins never set, or reset the password when you share.
- **Denial of service by a user who has logged in**, such as one opening thousands of connections.

So, even with logins:

- **Do not expose `tsdbd` to an untrusted network.** `tsdbd` listens on `127.0.0.1` unless you pass `--listen`
  with another address; keep it that way, or bind it to an interface that only your own systems can reach.
- Put the database directory where only the `tsdbd` user can write. The files are not signed: anyone who can
  write to them can change your data.
- If you need access from outside, put a proxy, a VPN or an SSH tunnel in front.

We treat a way around a login or a grant as a vulnerability: a client that is not logged in doing anything but
log in, a user reading, changing or learning about series it holds no right on, or a user gaining rights or
taking over another user it does not manage. With `--no-auth` every client may do everything, by design.

## What we do defend against

Even on a trusted network, a bad client, a broken program or a damaged disk must not hurt the database. These
are in scope, and we want to hear about a way around any of them:

- **Memory corruption or code execution from input:** network frames, files on disk (parts, the WAL, dictionaries),
  the export/import stream, and replies the clients read from the server. The frame decoders, the file loaders and
  the codecs are fuzzed (`make fuzz`, `make fuzz-frames`) and must be safe on any bytes.
- **One request taking the server down:** a crash, a hang or an unbounded use of memory from a single request or
  a single file. Request size, query memory and the time to deliver a frame are bounded.
- **Data loss or corruption:** a failed write or fsync stops the engine (fail-stop) instead of acknowledging
  data that may not be on disk; damaged files fail the open and are never skipped quietly; inserts are all or
  nothing. `make jepsen` tests this with crashes, simulated power loss, disk errors, misdirected reads and
  writes, and files damaged at rest. Two limits are known: without `--verify-checksums` damaged or misdirected
  data blocks of a part are read as wrong values (tsdbd says so at startup on ext4 and XFS; ZFS checks its data
  itself), as is a part's index damaged after the server opened it unless `--pin-index` keeps it in memory, and
  damage to the very last record of the WAL or of an append-only file looks like a write cut short
  by a crash and is cut, with a log line. See "Damage on disk" in [docs/design.md](docs/design.md).
- **Writes outside the database's directories:** volume paths are checked (absolute, no `.` or `..`, not in or
  around another database), and a client can never make the server add a volume or create a directory.

The details are in the "Invariants" section of [AGENTS.md](AGENTS.md); earlier security-related fixes are in the
[CHANGELOG](CHANGELOG.md) ("Security hardening").

Out of scope for now: what logins do not protect against (see above; TLS is on the roadmap), anything a client
of a `tsdbd` started with `--no-auth` can do, denial of service that needs sustained abuse by a logged-in or
trusted peer (such as a client opening thousands of connections), and
hardening ideas without a concrete way to go wrong. You are still welcome to open an ordinary issue for those.

## Reporting a vulnerability

**Please report it privately. Do not open a public issue or pull request for a vulnerability.**

Send an email to **olav@intellistream.ai**. You can also use GitHub's "Report a vulnerability" button on the
repository's Security tab, if it is enabled.

Please include:

- the version (`tsdbd --version`) and how you built it,
- what you found and what it lets an attacker do,
- steps to reproduce it: the request, the file or the program that triggers it (a crashing input from a fuzzer is
  perfect),
- whether you plan to publish it, and when.

What you can expect from us:

- an answer that we received the report, within a few working days;
- an assessment, and a fix or a plan for one, as soon as we can: we aim for a fix within 90 days, sooner for
  data loss or code execution;
- a credit in the release notes, if you want one;
- a request to keep the details private until a fixed release is out. We will not take legal action against
  someone who reports in good faith and follows this policy.

tsdb is maintained by [IntelliStream AS](https://intellistream.ai). This is a small project, so please be patient.
