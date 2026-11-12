// SPDX-License-Identifier: Apache-2.0
//! Login and users (protocol 12, `docs/design-auth.md`): SCRAM-SHA-256 as the client (RFC 5802, RFC 7677), the
//! verifiers the admin calls send, the process-wide key cache, and the types of users and grants.
//!
//! The password never travels (A18). A login proves it with HMACs over the exchange, and the client checks the
//! server's signature before the connect returns: a server that does not hold the user's verifier cannot pass.
//! Creating a user or setting a password sends a verifier computed here from a fresh salt. PBKDF2 is the
//! expensive step (4096 iterations or more, by design); its keys are cached per process by user, salt,
//! iterations and password, so every connection of a [`Pool`](crate::Pool), and every reconnect, pays for it
//! once. Passwords are their UTF-8 bytes, without SASLprep (A20), as in the other clients.

use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard};

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};
use crate::protocol::{Cursor, USER_SUPERUSER};

type HmacSha256 = Hmac<Sha256>;

/// The iteration count of a verifier made without one, and the fewest a server may ask for at login: fewer would
/// be a downgrade a fake server could force on the client.
pub const SCRAM_ITERATIONS: u32 = 4096;
/// More would let a fake server pin the client's CPU for minutes (as the C client).
const ITER_MAX: u32 = 100_000_000;
/// The longest password, in bytes, in every client and the CLI (design-auth.md §6); a password is at least 1 byte
/// and holds no NUL.
pub const PASSWORD_MAX: usize = 1023;
const SALT_LEN: usize = 16;
const KEY_CACHE: usize = 16;

// ------------------------------------------------------------ users and grants

/// The rights a grant gives on the series its selector matches (`TSDB_RIGHT_*`); combine them with `|`.
///
/// `admin` on a selector also lets the holder manage users and grants inside it (design §2) and implies the
/// other three there. Parsed from and shown as the CLI writes them: `read,insert`, `write` (insert and delete).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Rights(u8);

impl Rights {
    /// Reading the series' rows, aggregates and metadata.
    pub const READ: Rights = Rights(1);
    /// Inserting rows (and creating series the selector matches).
    pub const INSERT: Rights = Rights(2);
    /// Deleting rows, replacing ranges and setting retention.
    pub const DELETE: Rights = Rights(4);
    /// Managing the users and grants inside the selector.
    pub const ADMIN: Rights = Rights(8);
    /// Insert and delete.
    pub const WRITE: Rights = Rights(2 | 4);

    /// The bits as the wire carries them.
    pub const fn bits(self) -> u8 {
        self.0
    }

    /// Rights from their wire bits, unknown bits included (a newer server may know more).
    pub const fn from_bits(bits: u8) -> Rights {
        Rights(bits)
    }

    /// Whether every right of `other` is among these.
    pub const fn contains(self, other: Rights) -> bool {
        self.0 & other.0 == other.0
    }
}

impl std::ops::BitOr for Rights {
    type Output = Rights;
    fn bitor(self, rhs: Rights) -> Rights {
        Rights(self.0 | rhs.0)
    }
}

impl std::ops::BitOrAssign for Rights {
    fn bitor_assign(&mut self, rhs: Rights) {
        self.0 |= rhs.0;
    }
}

const RIGHT_NAMES: [(&str, Rights); 4] =
    [("read", Rights::READ), ("insert", Rights::INSERT), ("delete", Rights::DELETE), ("admin", Rights::ADMIN)];

impl fmt::Display for Rights {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut sep = "";
        for (name, r) in RIGHT_NAMES {
            if self.contains(r) {
                write!(f, "{sep}{name}")?;
                sep = ",";
            }
        }
        let rest = self.0 & !0xf;
        if rest != 0 {
            write!(f, "{sep}0x{rest:02x}")?;
        } else if sep.is_empty() {
            f.write_str("none")?;
        }
        Ok(())
    }
}

impl FromStr for Rights {
    type Err = Error;

    /// The grammar of tsdbd, the CLI and every client (design §2): names separated by commas, spaces around a name
    /// ignored; `read`, `insert`, `delete`, `admin`, `write` (insert and delete), `none` (no rights) and `0xHH`
    /// (two hex digits: bits a later server may name, as [`Display`](fmt::Display) shows them). Whatever
    /// `Display` prints parses back to the same bits.
    fn from_str(s: &str) -> Result<Rights> {
        let mut r = Rights::default();
        for word in s.split(',') {
            r |= match word.trim_matches(' ') {
                "write" => Rights::WRITE,
                "none" => Rights::default(),
                w => match RIGHT_NAMES.iter().find(|(n, _)| *n == w) {
                    Some(&(_, x)) => x,
                    None => match w.strip_prefix("0x") {
                        Some(h) if h.len() == 2 && h.bytes().all(|b| b.is_ascii_hexdigit()) => {
                            Rights(u8::from_str_radix(h, 16).expect("two hex digits"))
                        }
                        _ => {
                            return Err(Error::InvalidArgument(format!(
                                "rights {s:?}: {w:?} is none of read, insert, delete, admin, write, none, 0xHH"
                            )));
                        }
                    },
                },
            };
        }
        Ok(r)
    }
}

/// Which series a grant covers (`TSDB_SEL_*`). Parsed from and shown as the CLI writes them: `all`, `prefix:P`,
/// `name:N`, `glob:G`.
///
/// A prefix is a byte prefix of the name (`prefix:acme` also matches `acmecorp/x`; tenants end theirs in `/`).
/// In a glob `*` matches any run of bytes, `?` one byte and `\` makes the next byte literal; the server stores a
/// glob without a wildcard as a name and one ending in a single final `*` as a prefix.
///
/// A later server may add kinds (docs/compatibility.md): a grant of a kind this client does not know is read as
/// [`Selector::Other`], shown as `kind#N:TEXT`, and cannot be granted or revoked from here. Match with a `_` arm.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Selector {
    /// Every series.
    All,
    /// The names that start with these bytes.
    Prefix(String),
    /// One series name.
    Name(String),
    /// The names the glob matches.
    Glob(String),
    /// A kind this client does not know, as the server sent it.
    Other {
        /// The kind on the wire.
        kind: u8,
        /// Its text.
        text: String,
    },
}

impl Selector {
    /// The kind on the wire (`TSDB_SEL_*`).
    fn kind(&self) -> u8 {
        match self {
            Selector::All => 0,
            Selector::Prefix(_) => 1,
            Selector::Name(_) => 2,
            Selector::Glob(_) => 3,
            Selector::Other { kind, .. } => *kind,
        }
    }

    /// The text after the kind ("" for all).
    pub fn text(&self) -> &str {
        match self {
            Selector::All => "",
            Selector::Prefix(s) | Selector::Name(s) | Selector::Glob(s) | Selector::Other { text: s, .. } => s,
        }
    }

    fn from_wire(kind: u8, text: String) -> Selector {
        match kind {
            0 => Selector::All,
            1 => Selector::Prefix(text),
            2 => Selector::Name(text),
            3 => Selector::Glob(text),
            kind => Selector::Other { kind, text },
        }
    }
}

impl fmt::Display for Selector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Selector::All => f.write_str("all"),
            Selector::Prefix(s) => write!(f, "prefix:{s}"),
            Selector::Name(s) => write!(f, "name:{s}"),
            Selector::Glob(s) => write!(f, "glob:{s}"),
            Selector::Other { kind, text } => write!(f, "kind#{kind}:{text}"),
        }
    }
}

impl FromStr for Selector {
    type Err = Error;

    fn from_str(s: &str) -> Result<Selector> {
        if s == "all" {
            return Ok(Selector::All);
        }
        let (kind, text) = s.split_once(':').unwrap_or(("", ""));
        let text = text.to_string();
        match kind {
            "prefix" => Ok(Selector::Prefix(text)),
            "name" => Ok(Selector::Name(text)),
            "glob" => Ok(Selector::Glob(text)),
            _ => Err(Error::InvalidArgument(format!("selector {s:?} is none of all, prefix:P, name:N, glob:G"))),
        }
    }
}

/// One grant of a user: `rights` on the series `selector` matches.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Grant {
    /// The series it covers.
    pub selector: Selector,
    /// What it allows there.
    pub rights: Rights,
}

/// A user as [`whoami`](crate::Client::whoami) and [`user_list`](crate::Client::user_list) show it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct User {
    /// The login name; "" from a server without login (`--no-auth`).
    pub name: String,
    /// Every right on every series, user management and the database-wide operations.
    pub superuser: bool,
    /// The grants; a user's rights on a series are the union of those whose selector matches its name.
    pub grants: Vec<Grant>,
}

/// One user at `r`: u16 len, name, u8 flags, u32 g, g x (u8 kind, u8 rights, u16 len, selector).
pub(crate) fn read_user(r: &mut Cursor<'_>) -> Result<User> {
    let n = r.u16()? as usize;
    let name = String::from_utf8_lossy(r.take(n)?).into_owned();
    let flags = r.u8()?;
    let g = r.u32()? as usize;
    if g > r.remaining() / 4 {
        return Err(Error::Protocol(format!("{g} grants in {} bytes", r.remaining())));
    }
    let mut grants = Vec::with_capacity(g);
    for _ in 0..g {
        let (kind, rights) = (r.u8()?, r.u8()?);
        let n = r.u16()? as usize;
        let text = String::from_utf8_lossy(r.take(n)?).into_owned();
        grants.push(Grant { selector: Selector::from_wire(kind, text), rights: Rights(rights) });
    }
    Ok(User { name, superuser: flags & USER_SUPERUSER != 0, grants })
}

/// The u8 kind, u8 rights, u16 len, selector of GRANT and REVOKE.
pub(crate) fn grant_body(selector: &Selector, rights: Rights) -> Result<Vec<u8>> {
    if let Selector::Other { kind, .. } = selector {
        return Err(Error::InvalidArgument(format!("a selector of kind {kind} is unknown to this client")));
    }
    let t = selector.text().as_bytes();
    if t.len() > u16::MAX as usize {
        return Err(Error::InvalidArgument("a selector is longer than 65535 bytes".into()));
    }
    let mut b = vec![selector.kind(), rights.0];
    b.extend_from_slice(&(t.len() as u16).to_le_bytes());
    b.extend_from_slice(t);
    Ok(b)
}

// ------------------------------------------------------------ crypto

fn hmac(key: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut m = HmacSha256::new_from_slice(key).expect("HMAC takes any key length");
    for p in parts {
        m.update(p);
    }
    m.finalize().into_bytes().into()
}

static PBKDF2_RUNS: AtomicU64 = AtomicU64::new(0);

/// PBKDF2-HMAC-SHA-256 with one 32-byte block: SaltedPassword of RFC 5802.
fn pbkdf2(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    PBKDF2_RUNS.fetch_add(1, Ordering::Relaxed);
    let key = HmacSha256::new_from_slice(password).expect("HMAC takes any key length");
    let mut m = key.clone();
    m.update(salt);
    m.update(&1u32.to_be_bytes());
    let mut u: [u8; 32] = m.finalize().into_bytes().into();
    let mut t = u;
    for _ in 1..iterations {
        let mut m = key.clone(); // the key's pads are hashed once, not per iteration
        m.update(&u);
        u = m.finalize().into_bytes().into();
        for (a, b) in t.iter_mut().zip(u) {
            *a ^= b;
        }
    }
    wipe(&mut u);
    t
}

/// ClientKey and ServerKey of a password.
fn keys(password: &[u8], salt: &[u8], iterations: u32) -> ([u8; 32], [u8; 32]) {
    let mut salted = pbkdf2(password, salt, iterations);
    let ck = hmac(&salted, &[b"Client Key"]);
    let sk = hmac(&salted, &[b"Server Key"]);
    wipe(&mut salted);
    (ck, sk)
}

fn sha256(b: &[u8]) -> [u8; 32] {
    Sha256::digest(b).into()
}

fn ct_equal(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && std::hint::black_box(a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y))) == 0
}

fn wipe(b: &mut [u8]) {
    for x in b.iter_mut() {
        // volatile, so the compiler cannot drop the stores to a buffer that is not read again
        unsafe { std::ptr::write_volatile(x, 0) };
    }
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

pub(crate) fn base64(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len().div_ceil(3) * 4);
    for c in b.chunks(3) {
        let w = u32::from(c[0]) << 16 | u32::from(*c.get(1).unwrap_or(&0)) << 8 | u32::from(*c.get(2).unwrap_or(&0));
        for i in 0..4 {
            s.push(if i <= c.len() { B64[(w >> (18 - 6 * i) & 63) as usize] as char } else { '=' });
        }
    }
    s
}

/// Strict base64: whole quads, padding only at the end, nothing outside the alphabet.
pub(crate) fn unbase64(s: &[u8]) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(4) {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 4 * 3);
    for (q, quad) in s.chunks(4).enumerate() {
        let last = q == s.len() / 4 - 1;
        let pad = quad.iter().rev().take_while(|&&c| c == b'=').count();
        if pad > 2 || (pad > 0 && !last) {
            return None;
        }
        let mut w = 0u32;
        for &c in &quad[..4 - pad] {
            w = w << 6 | B64.iter().position(|&x| x == c)? as u32;
        }
        w <<= 6 * pad;
        out.extend_from_slice(&w.to_be_bytes()[1..4 - pad]);
    }
    Some(out)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn random(buf: &mut [u8]) -> Result<()> {
    let mut at = 0;
    while at < buf.len() {
        let rest = &mut buf[at..];
        // SAFETY: the kernel writes at most rest.len() bytes into rest
        let n = unsafe { libc::getrandom(rest.as_mut_ptr().cast(), rest.len(), 0) };
        if n < 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(Error::Io(e));
        }
        at += n as usize;
    }
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn random(buf: &mut [u8]) -> Result<()> {
    use std::io::Read;
    std::fs::File::open("/dev/urandom")?.read_exact(buf)?;
    Ok(())
}

// ------------------------------------------------------------ key cache

struct KeyEntry {
    user: String,
    salt: Vec<u8>,
    /// 0 for an entry whose computation failed: it matches no login (they ask for 4096 or more).
    iterations: u32,
    /// HMAC(salt, password): tells one password from another without keeping it.
    pw_mac: [u8; 32],
    /// ClientKey and ServerKey; None while one login computes them and others wait for it.
    keys: Option<([u8; 32], [u8; 32])>,
}

impl Drop for KeyEntry {
    fn drop(&mut self) {
        if let Some((ck, sk)) = &mut self.keys {
            wipe(ck);
            wipe(sk);
        }
    }
}

struct KeyCache {
    entries: Vec<KeyEntry>,
    next: usize,
}

static KEYS: Mutex<KeyCache> = Mutex::new(KeyCache { entries: Vec::new(), next: 0 });
/// Signalled when a computation ends: the logins waiting for that key look again.
static KEYS_DONE: Condvar = Condvar::new();

fn lock_keys() -> MutexGuard<'static, KeyCache> {
    KEYS.lock().unwrap_or_else(|e| e.into_inner())
}

/// The slot a computation claimed; if the computation unwinds, the slot is given up so its waiters do not wait
/// for ever (they then compute the key themselves).
struct Claim(Option<usize>);

impl Drop for Claim {
    fn drop(&mut self) {
        if let Some(i) = self.0 {
            let mut cache = lock_keys();
            cache.entries[i].iterations = 0;
            cache.entries[i].keys = Some(([0; 32], [0; 32]));
            KEYS_DONE.notify_all();
        }
    }
}

/// ClientKey and ServerKey, from the cache or computed once. The connections of a pool start together: the first
/// to miss claims an entry and computes it outside the lock, the others wait for it (one PBKDF2 per key, not one
/// per connection, and one entry, so other users' keys are not evicted by copies). A pending entry is never
/// evicted; with KEY_CACHE keys in flight, one more is computed without being cached.
fn keys_cached(user: &str, password: &[u8], salt: &[u8], iterations: u32) -> ([u8; 32], [u8; 32]) {
    let mut mac = hmac(salt, &[password]);
    let mut cache = lock_keys();
    let claim = loop {
        let found = cache
            .entries
            .iter()
            .find(|e| e.iterations == iterations && e.user == user && e.salt == salt && ct_equal(&e.pw_mac, &mac))
            .map(|e| e.keys);
        match found {
            Some(Some(k)) => {
                wipe(&mut mac);
                return k;
            }
            Some(None) => cache = KEYS_DONE.wait(cache).unwrap_or_else(|e| e.into_inner()),
            None => break claim_slot(&mut cache),
        }
    };
    if let Some(i) = claim {
        cache.entries[i].user = user.to_string();
        cache.entries[i].salt = salt.to_vec();
        cache.entries[i].iterations = iterations;
        cache.entries[i].pw_mac = mac;
    }
    wipe(&mut mac);
    drop(cache);
    let mut claim = Claim(claim);
    let k = keys(password, salt, iterations);
    if let Some(i) = claim.0.take() {
        let mut cache = lock_keys();
        cache.entries[i].keys = Some(k);
        KEYS_DONE.notify_all();
    }
    k
}

/// A pending entry for a new key: a new one while there is room, else the next in the ring that nobody waits for.
fn claim_slot(cache: &mut KeyCache) -> Option<usize> {
    let pending = || KeyEntry { user: String::new(), salt: Vec::new(), iterations: 0, pw_mac: [0; 32], keys: None };
    if cache.entries.len() < KEY_CACHE {
        cache.entries.push(pending());
        return Some(cache.entries.len() - 1);
    }
    for _ in 0..KEY_CACHE {
        let i = cache.next % KEY_CACHE;
        cache.next = cache.next.wrapping_add(1);
        if cache.entries[i].keys.is_some() {
            cache.entries[i] = pending();
            return Some(i);
        }
    }
    None
}

// ------------------------------------------------------------ the exchange

#[cfg(feature = "test-hooks")]
static TEST_NONCE: Mutex<Option<String>> = Mutex::new(None);

/// Test hooks, only with the feature `test-hooks` (the crate's own tests and the Python module's test build turn
/// it on; nothing else should): a fixed client nonce (the RFC 7677 test replays a fixed exchange) and the number
/// of PBKDF2 runs in this process (the key cache test).
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub mod test_hooks {
    /// Every later login of this process uses `nonce` as its client nonce; `None` goes back to random ones.
    pub fn set_client_nonce(nonce: Option<&str>) {
        *super::TEST_NONCE.lock().unwrap_or_else(|e| e.into_inner()) = nonce.map(str::to_string);
    }

    /// How many times this process ran PBKDF2 (logins and verifiers).
    pub fn pbkdf2_runs() -> u64 {
        super::PBKDF2_RUNS.load(std::sync::atomic::Ordering::Relaxed)
    }
}

fn refused(why: &str) -> Error {
    Error::Auth(why.to_string())
}

/// The client's side of one login, between its two messages.
pub(crate) struct Scram {
    user: String,
    nonce: String,
    /// client-first-message with its gs2 header "n,,"
    first: String,
}

impl Scram {
    /// Starts a login of `user`: the client-first-message, sent in AUTH_START.
    pub(crate) fn start(user: &str) -> Result<Scram> {
        // user names never hold ',' or '=' (USERS refuses them), so n= needs no escaping
        if user.is_empty() || user.len() > 255 || user.contains([',', '=', '\0']) {
            return Err(Error::InvalidArgument(format!("user name {user:?}: 1 to 255 bytes without , = or NUL")));
        }
        #[cfg(feature = "test-hooks")]
        let fixed = TEST_NONCE.lock().unwrap_or_else(|e| e.into_inner()).clone();
        #[cfg(not(feature = "test-hooks"))]
        let fixed: Option<String> = None;
        let nonce = match fixed {
            Some(n) => n,
            None => {
                let mut r = [0u8; 18];
                random(&mut r)?;
                base64(&r)
            }
        };
        let first = format!("n,,n={user},r={nonce}");
        Ok(Scram { user: user.to_string(), nonce, first })
    }

    /// AUTH_START's payload: u8 len, mechanism, u16 len, client-first-message.
    pub(crate) fn start_body(&self) -> Vec<u8> {
        let mech = crate::protocol::SCRAM_SHA256.as_bytes();
        let mut b = vec![mech.len() as u8];
        b.extend_from_slice(mech);
        b.extend_from_slice(&(self.first.len() as u16).to_le_bytes());
        b.extend_from_slice(self.first.as_bytes());
        b
    }

    /// From the server-first-message (`r=<nonce>,s=<salt>,i=<iterations>`, nothing else, strictly as the C
    /// client reads it): AUTH_CONTINUE's payload, and the ServerSignature the server must answer with.
    pub(crate) fn finish(&self, password: &[u8], server_first: &[u8]) -> Result<(Vec<u8>, [u8; 32])> {
        let bad = || refused("the server's first message does not follow SCRAM-SHA-256");
        let sf = std::str::from_utf8(server_first).map_err(|_| bad())?;
        let rest = sf.strip_prefix("r=").ok_or_else(bad)?;
        let (nonce, rest) = rest.split_once(",s=").ok_or_else(bad)?;
        let (salt, iter) = rest.split_once(",i=").ok_or_else(bad)?;
        // the server's nonce continues ours, printable without ','
        if nonce.len() <= self.nonce.len()
            || !nonce.starts_with(&self.nonce)
            || !nonce.bytes().all(|c| (0x21..=0x7e).contains(&c) && c != b',')
        {
            return Err(bad());
        }
        let salt = unbase64(salt.as_bytes()).filter(|s| !s.is_empty() && s.len() <= 64).ok_or_else(bad)?;
        if !iter.starts_with(|c: char| ('1'..='9').contains(&c)) || !iter.bytes().all(|c| c.is_ascii_digit()) {
            return Err(bad());
        }
        let iterations = match iter.parse::<u32>() {
            Ok(i) if (SCRAM_ITERATIONS..=ITER_MAX).contains(&i) => i,
            _ => return Err(refused("the server asks for an iteration count outside 4096 to 100,000,000")),
        };
        let (mut ck, mut sk) = keys_cached(&self.user, password, &salt, iterations);
        // AuthMessage = client-first-message-bare "," server-first-message "," client-final-message-without-proof
        let without_proof = format!("c=biws,r={nonce}");
        let am = [&self.first.as_bytes()[3..], b",", server_first, b",", without_proof.as_bytes()];
        let sig = hmac(&sha256(&ck), &am);
        let mut proof = [0u8; 32];
        for i in 0..32 {
            proof[i] = ck[i] ^ sig[i];
        }
        let server_sig = hmac(&sk, &am);
        wipe(&mut ck);
        wipe(&mut sk);
        let msg = format!("{without_proof},p={}", base64(&proof));
        wipe(&mut proof);
        let mut b = (msg.len() as u16).to_le_bytes().to_vec();
        b.extend_from_slice(msg.as_bytes());
        Ok((b, server_sig))
    }
}

/// Checks the server-final-message: `v=` and the ServerSignature, compared in constant time. An `e=` (or anything
/// else) fails: the server did not prove it holds this user's verifier.
pub(crate) fn check_server_final(msg: &[u8], expected: &[u8; 32]) -> Result<()> {
    match msg.strip_prefix(b"v=").and_then(unbase64) {
        Some(v) if ct_equal(&v, expected) => Ok(()),
        _ => Err(refused("the server's signature is wrong: it does not hold this user's verifier")),
    }
}

/// The body of a server message: u16 len and exactly that many bytes.
pub(crate) fn server_message(body: &[u8]) -> Result<&[u8]> {
    match body {
        [a, b, rest @ ..] if rest.len() == usize::from(u16::from_le_bytes([*a, *b])) => Ok(rest),
        _ => Err(refused("the server's SCRAM message is malformed")),
    }
}

/// A password's verifier with a fresh salt (A18): u32 iterations, u8 salt_len, salt, StoredKey, ServerKey.
pub(crate) fn verifier(password: &str, iterations: Option<u32>) -> Result<Vec<u8>> {
    let it = iterations.unwrap_or(SCRAM_ITERATIONS);
    crate::client::check_password(password.as_bytes())?;
    if !(SCRAM_ITERATIONS..=ITER_MAX).contains(&it) {
        return Err(Error::InvalidArgument(format!("{it} iterations: 4096 to 100,000,000")));
    }
    let mut salt = [0u8; SALT_LEN];
    random(&mut salt)?;
    let (mut ck, mut sk) = keys(password.as_bytes(), &salt, it);
    let mut v = it.to_le_bytes().to_vec();
    v.push(SALT_LEN as u8);
    v.extend_from_slice(&salt);
    v.extend_from_slice(&sha256(&ck));
    v.extend_from_slice(&sk);
    wipe(&mut ck);
    wipe(&mut sk);
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_rfc4648() {
        for (raw, enc) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(base64(raw.as_bytes()), enc);
            assert_eq!(unbase64(enc.as_bytes()).unwrap(), raw.as_bytes());
        }
        for bad in ["Zg=", "Z===", "Zg==Zg==", "Zm9v!A==", "Zm=v"] {
            assert!(unbase64(bad.as_bytes()).is_none(), "{bad}");
        }
    }

    #[test]
    fn pbkdf2_rfc7914() {
        // RFC 7914 §11, the first 32 bytes of each
        let hex = |s: &str| {
            (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect::<Vec<_>>()
        };
        assert_eq!(
            pbkdf2(b"passwd", b"salt", 1).to_vec(),
            hex("55ac046e56e3089fec1691c22544b605f94185216dde0465e68b9d57c20dacbc")
        );
        assert_eq!(
            pbkdf2(b"Password", b"NaCl", 80000).to_vec(),
            hex("4ddcd8f60b98be21830cee5ef22701f9641a4418d04c0414aeff08876b34ab56")
        );
    }

    #[test]
    fn rights_and_selectors_parse_as_the_cli_writes_them() {
        assert_eq!("read,write".parse::<Rights>().unwrap(), Rights::READ | Rights::INSERT | Rights::DELETE);
        assert_eq!((Rights::READ | Rights::ADMIN).to_string(), "read,admin");
        assert_eq!(Rights::default().to_string(), "none");
        assert!("read,fly".parse::<Rights>().is_err());
        for s in ["all", "prefix:acme/", "name:acme/x", "glob:acme/*/t?", "prefix:"] {
            assert_eq!(s.parse::<Selector>().unwrap().to_string(), s);
        }
        assert!("acme/".parse::<Selector>().is_err());
    }

    /// REVIEW3 #7: the grammar of server/users.c rights_parse, and every value prints as text that parses back.
    #[test]
    fn rights_text_round_trips_in_the_servers_grammar() {
        for bits in 0..=255u8 {
            let r = Rights::from_bits(bits);
            assert_eq!(r.to_string().parse::<Rights>().unwrap(), r, "{r}");
        }
        assert_eq!(Rights::from_bits(0x45).to_string(), "read,delete,0x40");
        let ok = [
            ("read, insert", 3),
            (" read ,  write ", 7),
            ("none", 0),
            ("none,read", 1),
            ("0x30", 0x30),
            ("read,0xF0", 0xf1),
            ("0x00", 0),
        ];
        for (text, bits) in ok {
            assert_eq!(text.parse::<Rights>().unwrap().bits(), bits, "{text}");
        }
        for bad in
            ["", "read,", "reads", "read,,insert", "READ", " ", "re ad", "read\t", "\tread", "0x1", "0x100", "0xg0"]
        {
            assert!(bad.parse::<Rights>().is_err(), "{bad:?}");
        }
    }

    /// REVIEW3 #6: a kind a later server adds is kept and shown, never a broken reply, and cannot be granted.
    #[test]
    fn unknown_selector_kinds_are_kept() {
        let s = Selector::from_wire(9, "x/*".into());
        assert_eq!(s, Selector::Other { kind: 9, text: "x/*".into() });
        assert_eq!(s.to_string(), "kind#9:x/*");
        assert!(s.to_string().parse::<Selector>().is_err());
        assert!(matches!(grant_body(&s, Rights::READ), Err(Error::InvalidArgument(_))));
    }
}
