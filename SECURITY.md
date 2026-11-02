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

Today that means:

- **No authentication.** Anyone who can open a connection to `tsdbd` can read, write and delete any data.
- **No encryption.** There is no TLS. Data and queries travel in the clear. The metrics port (`--metrics-port`)
  is plain HTTP and also has no login.
- **No access control.** There are no users, roles or per-series permissions.

These three are on the [roadmap](docs/roadmap.md) for after 1.0. Until then:

- **Do not expose `tsdbd` to an untrusted network.** `tsdbd` listens on `127.0.0.1` unless you pass `--listen`
  with another address; keep it that way, or bind it to an interface that only your own systems can reach.
- Put the database directory where only the `tsdbd` user can write. The files are not signed: anyone who can
  write to them can change your data.
- If you need access from outside, put a proxy, a VPN or an SSH tunnel in front, and do the access control there.

Because of this, we treat "an unauthenticated client can do X" as expected behaviour, not as a vulnerability.

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
  nothing. `make jepsen` tests this with crashes, simulated power loss and disk errors.
- **Writes outside the database's directories:** volume paths are checked (absolute, no `.` or `..`, not in or
  around another database), and a client can never make the server add a volume or create a directory.

The details are in the "Invariants" section of [AGENTS.md](AGENTS.md); earlier security-related fixes are in the
[CHANGELOG](CHANGELOG.md) ("Security hardening").

Out of scope for now: missing authentication, TLS or access control (see above), denial of service that needs
sustained abuse by an authenticated or trusted peer (such as a client opening thousands of connections), and
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
