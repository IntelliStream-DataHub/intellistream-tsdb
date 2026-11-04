//! The Rust client of the integration tests: same command line and output as c/itc.c, python/itp.py and
//! java/Itj.java (see run.py).
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::ops::ControlFlow;
use tsdb_client::{Bucket, Client, Column, ConnectOptions, Group, QueryOpts, Unit, ValueType, Values};

type R<T> = Result<T, Box<dyn std::error::Error>>;

fn parse_type(s: &str) -> ValueType {
    match s {
        "float32" => ValueType::Float32,
        "float64" => ValueType::Float64,
        "int64" => ValueType::Int64,
        "bool" => ValueType::Bool,
        "state" => ValueType::State,
        _ => panic!("unknown type {s}"),
    }
}

struct Series {
    id: u64,
    ty: ValueType,
}

struct Ctx {
    c: Client,
    series: std::collections::HashMap<String, Series>,
    names: std::collections::HashMap<(u64, u32), String>,
}

impl Ctx {
    fn find(&mut self, name: &str, create: Option<ValueType>) -> Option<(u64, ValueType)> {
        if let Some(s) = self.series.get(name) {
            return Some((s.id, s.ty));
        }
        let (id, ty) = match create {
            Some(t) => (self.c.series_create(name, t, None).unwrap(), t),
            None => {
                let id = self.c.series_lookup(name).ok()?;
                (id, self.c.series_type(id).unwrap())
            }
        };
        self.series.insert(name.to_string(), Series { id, ty });
        Some((id, ty))
    }
}

/// One parsed token of a batch: None is null.
enum Tok {
    F32(u32),
    F64(u64),
    I64(i64),
    B(bool),
    St(String),
    Null,
}

fn token(ty: ValueType, t: &str) -> Tok {
    if t == "N" {
        return Tok::Null;
    }
    match ty {
        ValueType::Float32 => Tok::F32(u32::from_str_radix(t, 16).unwrap()),
        ValueType::Float64 => Tok::F64(u64::from_str_radix(t, 16).unwrap()),
        ValueType::Int64 => Tok::I64(t.parse().unwrap()),
        ValueType::Bool => Tok::B(t == "1"),
        _ => Tok::St(t.to_string()),
    }
}

fn build(cx: &mut Ctx, series: u64, ty: ValueType, toks: &[Tok]) -> Column {
    macro_rules! col {
        ($t:ty, $variant:ident, $conv:expr) => {
            Column::from_options::<$t>(toks.iter().map(|t| match t {
                Tok::$variant(x) => Some($conv(x)),
                Tok::Null => None,
                _ => panic!("token of the wrong type"),
            }))
        };
    }
    match ty {
        ValueType::Float32 => col!(f32, F32, |x: &u32| f32::from_bits(*x)),
        ValueType::Float64 => col!(f64, F64, |x: &u64| f64::from_bits(*x)),
        ValueType::Int64 => col!(i64, I64, |x: &i64| *x),
        ValueType::Bool => col!(bool, B, |x: &bool| *x),
        _ => {
            let mut codes = Vec::new();
            for t in toks {
                codes.push(match t {
                    Tok::St(s) => Some(cx.c.state(series, s).unwrap()),
                    Tok::Null => None,
                    _ => panic!("token of the wrong type"),
                });
            }
            Column::from_options::<u32>(codes)
        }
    }
}

fn write(cx: &mut Ctx, path: &str) -> R<()> {
    let f = BufReader::new(std::fs::File::open(path)?);
    let mut lines = f.lines();
    while let Some(line) = lines.next() {
        let line = line?;
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.is_empty() {
            continue;
        }
        if p[0] == "S" {
            cx.find(p[1], Some(parse_type(p[2])));
            continue;
        }
        let multi = p[0] == "M";
        let level: i32 = p[2].parse()?;
        let flags = p[3];
        let n: usize = p[4].parse()?;
        let (ty, one) = if multi {
            (parse_type(p[1]), 0u64)
        } else {
            let (id, t) = cx.find(p[1], None).expect("series");
            (t, id)
        };
        let (mut ts, mut ids, mut toks) = (Vec::new(), Vec::new(), Vec::new());
        for _ in 0..n {
            let row = lines.next().expect("short script")?;
            let r: Vec<&str> = row.split_whitespace().collect();
            let (id, t) = if multi { (cx.find(r[0], None).expect("series").0, &r[1..]) } else { (one, &r[..]) };
            ids.push(id);
            ts.push(t[0].parse::<i64>()?);
            toks.push(token(ty, t[1]));
        }
        cx.c.set_compression(level)?;
        let is_async = flags.contains("async");
        if flags.contains("legacy") {
            let vals: Vec<f32> = toks
                .iter()
                .map(|t| match t {
                    Tok::F32(x) => f32::from_bits(*x),
                    _ => panic!("legacy batch is not plain float32"),
                })
                .collect();
            match (multi, is_async) {
                (false, false) => cx.c.insert_series(one, &ts, &vals)?,
                (false, true) => cx.c.insert_series_async(one, &ts, &vals)?,
                (true, false) => cx.c.insert(&ids, &ts, &vals)?,
                (true, true) => cx.c.insert_async(&ids, &ts, &vals)?,
            }
            continue;
        }
        // a batch without nulls goes in as plain slices, the way a user would write it
        let col = build(cx, one, ty, &toks);
        let plain = col.null_count() == 0;
        macro_rules! send {
            ($m:ident, $ma:ident, $ms:ident, $msa:ident) => {{
                match (multi, is_async) {
                    (false, false) => cx.c.$ms(one, &ts, &col)?,
                    (false, true) => cx.c.$msa(one, &ts, &col)?,
                    (true, false) => cx.c.$m(&ids, &ts, &col)?,
                    (true, true) => cx.c.$ma(&ids, &ts, &col)?,
                }
            }};
        }
        if plain {
            macro_rules! sendp {
                ($v:expr) => {{
                    let v = $v;
                    match (multi, is_async) {
                        (false, false) => cx.c.insert_series_values(one, &ts, v)?,
                        (false, true) => cx.c.insert_series_values_async(one, &ts, v)?,
                        (true, false) => cx.c.insert_values(&ids, &ts, v)?,
                        (true, true) => cx.c.insert_values_async(&ids, &ts, v)?,
                    }
                }};
            }
            match &col.values {
                Values::Float32(v) => sendp!(v),
                Values::Float64(v) => sendp!(v),
                Values::Int64(v) => sendp!(v),
                Values::Bool(v) => sendp!(v),
                Values::State(v) => sendp!(v),
                _ => unreachable!(),
            }
        } else {
            send!(insert_values, insert_values_async, insert_series_values, insert_series_values_async);
        }
    }
    cx.c.wait()?;
    Ok(())
}

fn state_name(cx: &mut Ctx, series: u64, code: u32) -> String {
    if let Some(n) = cx.names.get(&(series, code)) {
        return n.clone();
    }
    let n = cx.c.state_name(series, code).unwrap();
    cx.names.insert((series, code), n.clone());
    n
}

fn opts(order: &str, limit: u64) -> QueryOpts {
    let mut o = if order == "desc" { QueryOpts::desc() } else { QueryOpts::asc() };
    if limit > 0 {
        o = o.limit(limit);
    }
    o
}

fn unit(s: &str) -> Unit {
    match s {
        "second" => Unit::Second,
        "minute" => Unit::Minute,
        "hour" => Unit::Hour,
        "day" => Unit::Day,
        "week" => Unit::Week,
        "month" => Unit::Month,
        "quarter" => Unit::Quarter,
        "year" => Unit::Year,
        _ => panic!("unit {s}"),
    }
}

fn read(cx: &mut Ctx, path: &str) -> R<()> {
    let stdout = std::io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    for line in BufReader::new(std::fs::File::open(path)?).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        writeln!(out, "## {line}")?;
        let t: Vec<&str> = line.split(' ').collect();
        let Some((id, ty)) = cx.find(t[1], None) else {
            writeln!(out, "!!")?;
            continue;
        };
        let (start, end): (i64, i64) = (t[2].parse().unwrap(), t[3].parse().unwrap());
        // collected first, printed once the call is known to have succeeded (an error prints just "!!")
        let mut rows: Vec<String> = Vec::new();
        let res: Result<(), tsdb_client::Error> = (|| {
            match t[0] {
                "raw" => {
                    let mut rr: Vec<(Vec<i64>, Column)> = Vec::new();
                    cx.c.query_values(id, start..end, opts(t[4], t[5].parse().unwrap()), |ts, col| {
                        rr.push((ts.to_vec(), col.clone()));
                        ControlFlow::Continue(())
                    })?;
                    for (ts, col) in rr {
                        for (i, ts) in ts.iter().enumerate() {
                            let v = if col.is_null(i) {
                                "N".to_string()
                            } else {
                                match &col.values {
                                    Values::Float32(v) => format!("{:08x}", v[i].to_bits()),
                                    Values::Float64(v) => format!("{:016x}", v[i].to_bits()),
                                    Values::Int64(v) => v[i].to_string(),
                                    Values::Bool(v) => (v[i] as u8).to_string(),
                                    Values::State(v) => state_name(cx, id, v[i]),
                                    _ => unreachable!(),
                                }
                            };
                            rows.push(format!("{ts}\t{v}"));
                        }
                    }
                }
                "f32raw" => {
                    let p = cx.c.query_points(id, start..end, opts(t[4], t[5].parse().unwrap()))?;
                    for (ts, v) in p.iter() {
                        rows.push(format!("{ts}\t{:08x}", v.to_bits()));
                    }
                }
                "agg" => {
                    let b = cx.c.query_agg(id, start..end, t[4].parse().unwrap())?;
                    buckets_to(&mut rows, &b);
                }
                "grp" => {
                    let o = opts(t[7], t[8].parse().unwrap());
                    let g = if t[4] == "none" {
                        None
                    } else {
                        let mut g = Group::new(unit(t[4])).count(t[5].parse().unwrap());
                        if t[6] != "-" {
                            g = g.timezone(t[6]);
                        }
                        Some(g)
                    };
                    let b = cx.c.query_group(id, start..end, g.as_ref(), o)?;
                    buckets_to(&mut rows, &b);
                }
                "bnd" => {
                    let bounds: Vec<i64> = t[6].split(',').map(|x| x.parse().unwrap()).collect();
                    let b = cx.c.query_bounds(id, start..end, &bounds, opts(t[4], t[5].parse().unwrap()))?;
                    buckets_to(&mut rows, &b);
                }
                _ => panic!("unknown query kind"),
            }
            Ok(())
        })();
        let _ = ty;
        match res {
            Ok(()) => {
                for r in rows {
                    writeln!(out, "{r}")?;
                }
            }
            Err(_) => writeln!(out, "!!")?,
        }
    }
    out.flush()?;
    Ok(())
}

fn buckets_to(rows: &mut Vec<String>, b: &[Bucket]) {
    for b in b {
        rows.push(format!(
            "{}\t{}\t{:016x}\t{:016x}\t{:016x}\t{}\t{}\t{}",
            b.start,
            b.count,
            b.sum.to_bits(),
            b.min.to_bits(),
            b.max.to_bits(),
            b.isum,
            b.imin,
            b.imax
        ));
    }
}

const LOAD_ROWS: usize = 1_000_000;
const LOAD_BATCH: usize = 200;
const LOAD_FLIGHT: usize = 8;

fn load(cx: &mut Ctx, name: &str, seed: i64, level: i32) -> R<()> {
    let id = cx.c.series_create(name, ValueType::Int64, None)?;
    cx.c.set_compression(level)?;
    println!("READY");
    std::io::stdout().flush()?;
    let nb = LOAD_ROWS / LOAD_BATCH;
    for pass in 1..=2 {
        let mut first = 0;
        for b in 0..nb {
            let (mut ts, mut v) = (Vec::with_capacity(LOAD_BATCH), Vec::with_capacity(LOAD_BATCH));
            for i in 0..LOAD_BATCH {
                let r = (b * LOAD_BATCH + i) as i64;
                ts.push(1_700_000_000_000 + r * 1000);
                v.push(seed * 100_000_000 + r + if pass == 2 { 1_000_000 } else { 0 });
            }
            cx.c.insert_series_values_async(id, &ts, &v)?;
            if (b + 1) % LOAD_FLIGHT == 0 || b == nb - 1 {
                cx.c.wait()?;
                println!("ACK {pass} {first} {b}");
                std::io::stdout().flush()?;
                first = b + 1;
            }
        }
        if pass == 1 {
            // wait for the driver: the clients start pass 2 together
            println!("PASS1");
            std::io::stdout().flush()?;
            std::io::stdin().read_line(&mut String::new())?;
        }
    }
    println!("DONE");
    Ok(())
}

fn main() -> R<()> {
    let a: Vec<String> = std::env::args().collect();
    if a.len() < 4 {
        eprintln!("usage: itr write|read|load|admin <port> <arg>...");
        std::process::exit(2);
    }
    let addr = format!("127.0.0.1:{}", a[2]);
    // run.py --auth on names the user to log in as in the environment, as it does for every client program
    let c = match (std::env::var_os("ITG_USER"), std::env::var_os("ITG_PASSWORD_FILE")) {
        (Some(u), Some(pf)) => {
            let o = ConnectOptions::new().user(u.to_string_lossy()).password_file(pf).require_auth(true);
            let mut c = Client::connect_opts(addr, &o)?;
            c.set_timeout(None)?; // as Client::connect: no limit on requests
            c
        }
        _ => Client::connect(addr)?,
    };
    let mut cx = Ctx { c, series: Default::default(), names: Default::default() };
    match a[1].as_str() {
        "write" => write(&mut cx, &a[3])?,
        "read" => read(&mut cx, &a[3])?,
        "load" => load(&mut cx, &a[3], a[4].parse().unwrap(), a.get(5).map_or(Ok(1), |s| s.parse())?)?,
        "admin" => match a[3].as_str() {
            "flush" => cx.c.flush()?,
            "optimize" => cx.c.optimize()?,
            _ => std::process::exit(2),
        },
        _ => std::process::exit(2),
    }
    cx.c.wait()?;
    cx.c.close()?;
    Ok(())
}
