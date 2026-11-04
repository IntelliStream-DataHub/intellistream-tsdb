//! The client's side of SCRAM-SHA-256 against fake servers: the RFC 7677 §3 exchange byte for byte (its values
//! read from tests/rfc7677.h, which the C tests use too), and the server-first and server-final messages it
//! must refuse. A test binary of its own: the fixed client nonce is process-wide.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Mutex;
use std::thread;

use tsdb_client::{Client, ConnectOptions, Error, Status};

/// The fixed nonce is process-wide; the tests that set it take turns.
static NONCE: Mutex<()> = Mutex::new(());

/// The string macros of tests/rfc7677.h: `#define NAME "text"`, adjacent literals and earlier macros
/// concatenated, `\` continuations joined.
fn rfc7677() -> HashMap<String, String> {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/rfc7677.h");
    let text = std::fs::read_to_string(path).expect("tests/rfc7677.h").replace("\\\n", " ");
    let mut m: HashMap<String, String> = HashMap::new();
    for line in text.lines() {
        let Some(rest) = line.trim().strip_prefix("#define ") else { continue };
        let (name, mut rest) = rest.split_once(char::is_whitespace).unwrap_or((rest, ""));
        let mut value = String::new();
        loop {
            rest = rest.trim_start();
            if let Some(r) = rest.strip_prefix('"') {
                let end = r.find('"').expect("closed literal");
                value.push_str(&r[..end]);
                rest = &r[end + 1..];
            } else if let Some(id) =
                rest.split(|c: char| !(c.is_alphanumeric() || c == '_')).next().filter(|s| !s.is_empty())
            {
                match m.get(id) {
                    Some(v) => value.push_str(v),
                    None => value.push_str(id), // a number
                }
                rest = &rest[id.len()..];
            } else {
                break; // the end, or a comment
            }
        }
        m.insert(name.to_string(), value);
    }
    m
}

/// A fake tsdbd of protocol 12 that asks for a login: answers HELLO, then AUTH_START with `first` and
/// AUTH_CONTINUE with `last` (u16 length, message). Returns the address and the thread, which gives the
/// client-first and client-final messages it got.
fn fake(first: String, last: String) -> (String, thread::JoinHandle<(String, String)>) {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    let h = thread::spawn(move || {
        let (mut s, _) = l.accept().unwrap();
        let (mut got_first, mut got_final) = (String::new(), String::new());
        let mut h = [0u8; 9];
        while s.read_exact(&mut h).is_ok() {
            let len = u32::from_le_bytes(h[0..4].try_into().unwrap()) as usize;
            let mut p = vec![0; len - 5];
            s.read_exact(&mut p).unwrap();
            let msg = |b: &[u8]| String::from_utf8(b.to_vec()).unwrap();
            let reply_msg = |m: &str| [(m.len() as u16).to_le_bytes().to_vec(), m.as_bytes().to_vec()].concat();
            let body = match h[8] {
                1 => {
                    let mut b = 12u32.to_le_bytes().to_vec();
                    b.extend_from_slice(&1u16.to_le_bytes()); // TSDB_HELLO_AUTH
                    b.extend_from_slice(&2u16.to_le_bytes());
                    b.extend_from_slice(b"\x0bSCRAM-SHA-1\x0dSCRAM-SHA-256");
                    b
                }
                29 => {
                    assert_eq!(&p[..14], b"\x0dSCRAM-SHA-256");
                    let n = u16::from_le_bytes([p[14], p[15]]) as usize;
                    assert_eq!(p.len(), 16 + n);
                    got_first = msg(&p[16..]);
                    reply_msg(&first)
                }
                30 => {
                    let n = u16::from_le_bytes([p[0], p[1]]) as usize;
                    assert_eq!(p.len(), 2 + n);
                    got_final = msg(&p[2..]);
                    reply_msg(&last)
                }
                op => panic!("op {op} before the login finished"),
            };
            let mut reply = ((9 + body.len()) as u32).to_le_bytes().to_vec();
            reply.extend_from_slice(&h[4..8]);
            reply.extend_from_slice(&[0; 5]);
            reply.extend_from_slice(&body);
            if s.write_all(&reply).is_err() {
                break;
            }
            if h[8] == 30 {
                break;
            }
        }
        (got_first, got_final)
    });
    (addr, h)
}

fn login(addr: &str, user: &str, pw: &str) -> tsdb_client::Result<Client> {
    Client::connect_opts(addr, &ConnectOptions::new().user(user).password(pw))
}

#[test]
fn rfc7677_exchange_byte_for_byte() {
    let v = rfc7677();
    let _g = NONCE.lock().unwrap();
    tsdb_client::test_hooks::set_client_nonce(Some(&v["RFC7677_CNONCE"]));
    let (addr, h) = fake(v["RFC7677_SERVER_FIRST"].clone(), v["RFC7677_SERVER_FINAL"].clone());
    let c = login(&addr, &v["RFC7677_USER"], &v["RFC7677_PASSWORD"]);
    tsdb_client::test_hooks::set_client_nonce(None);
    let (first, last) = h.join().unwrap();
    assert_eq!(first, v["RFC7677_CLIENT_FIRST"]);
    assert_eq!(last, v["RFC7677_CLIENT_FINAL"]);
    assert!(c.is_ok(), "{:?}", c.err());
    assert_eq!(v["RFC7677_NONCE"], format!("{}{}", v["RFC7677_CNONCE"], v["RFC7677_SNONCE"]));
}

/// A server that does not hold the verifier cannot pass: a wrong ServerSignature, an `e=`, a malformed
/// server-first all fail the connect with Error::Auth.
#[test]
fn bad_server_messages_fail_the_login() {
    let v = rfc7677();
    let sf = v["RFC7677_SERVER_FIRST"].clone();
    let fin = v["RFC7677_SERVER_FINAL"].clone();
    let mut wrong_sig = fin.clone().into_bytes();
    wrong_sig[5] ^= 1; // another base64 letter
    let cases: Vec<(String, String)> = vec![
        (sf.clone(), String::from_utf8(wrong_sig).unwrap()),
        (sf.clone(), "e=invalid-proof".into()),
        (sf.clone(), String::new()),
        (sf.clone(), format!("{fin},x=1")),
        (sf.replace(",i=4096", ",i=4095"), fin.clone()), // a downgrade
        (sf.replace(",i=4096", ",i=04096"), fin.clone()),
        (sf.replace(",i=4096", ",i=4096,x=1"), fin.clone()), // an extension
        (sf.replace("r=rOprNGfwEbeRWgbNEkqO", "r=xOprNGfwEbeRWgbNEkqO"), fin.clone()), // not our nonce
        (format!("r={},s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096", v["RFC7677_CNONCE"]), fin.clone()), // no server part
        (sf.replace(",s=W22ZaJ0SNY7soEsUEjb6gQ==", ",s=W22Z!J0SNY7soEsUEjb6gQ=="), fin.clone()),
        (sf.replace(",s=W22ZaJ0SNY7soEsUEjb6gQ==", ",s="), fin.clone()),
        (format!("m=x,{sf}"), fin.clone()),
    ];
    let _g = NONCE.lock().unwrap();
    tsdb_client::test_hooks::set_client_nonce(Some(&v["RFC7677_CNONCE"]));
    for (first, last) in cases {
        let (addr, h) = fake(first.clone(), last.clone());
        let e = login(&addr, "user", "pencil").err().unwrap_or_else(|| panic!("{first:?} / {last:?} must fail"));
        assert!(matches!(e, Error::Auth(_)), "{first:?} / {last:?}: {e:?}");
        assert_eq!(e.status(), Some(Status::Auth));
        assert_eq!(e.code(), -17);
        h.join().unwrap();
    }
    tsdb_client::test_hooks::set_client_nonce(None);
}

#[test]
fn a_server_asking_for_a_login_gets_none_without_a_user() {
    let (addr, h) = fake(String::new(), String::new());
    let e = Client::connect(&addr).unwrap_err();
    assert!(matches!(&e, Error::Auth(m) if m.contains("requires a login")), "{e:?}");
    let (first, _) = h.join().unwrap();
    assert!(first.is_empty(), "nothing was sent after HELLO");
    // a user without a password is refused before anything is sent
    let e = Client::connect_opts(&addr, &ConnectOptions::new().user("u")).unwrap_err();
    assert!(matches!(e, Error::InvalidArgument(_)), "{e:?}");
}
