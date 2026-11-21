//! Shopping Sessions pipeline: exact rules from rules.md, parallel over 8 cores.
//!
//! Phases
//!  1. lookups: products, campaigns, manifest; links -> union-find -> person names
//!  2. events: parse every line in parallel, bucket by event_id hash
//!  3. per bucket: dedup (smallest ingest_ts), validate, drop late; re-bucket by person
//!  4. clicks: parse, validate, bucket by person (dedup is unnecessary: copies are identical)
//!  5. per person bucket: sessionize, credit, assists, aggregate
//!  6. write parquet

use ahash::AHashMap;
use memmap2::Mmap;
use rayon::prelude::*;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use xxhash_rust::xxh3::{xxh3_128, xxh3_64};

const DAY_MS: i64 = 86_400_000;
const GAP_MS: i64 = 1_800_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const LATE_MS: i64 = DAY_MS;
const NB: usize = 256; // buckets

// ---------------------------------------------------------------- files

struct Files {
    maps: Vec<Mmap>,
}

impl Files {
    #[inline]
    fn bytes(&self, r: u64) -> &[u8] {
        let f = (r >> 52) as usize;
        let off = ((r >> 12) & ((1u64 << 40) - 1)) as usize;
        let len = (r & 0xfff) as usize;
        &self.maps[f][off..off + len]
    }
}

#[inline]
fn mkref(file: usize, off: usize, len: usize) -> u64 {
    ((file as u64) << 52) | ((off as u64) << 12) | (len.min(0xfff) as u64)
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file())
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}

fn map_file(p: &Path) -> Option<Mmap> {
    let f = File::open(p).ok()?;
    let len = f.metadata().ok()?.len();
    if len == 0 {
        return None;
    }
    let m = unsafe { Mmap::map(&f).ok()? };
    #[cfg(unix)]
    {
        let _ = m.advise(memmap2::Advice::Sequential);
        let _ = m.advise(memmap2::Advice::WillNeed);
    }
    Some(m)
}

/// Split a file into chunks (start, end) of whole lines, skipping the header line.
fn chunks(data: &[u8], target: usize) -> Vec<(usize, usize)> {
    let start = match memchr(b'\n', data, 0) {
        Some(p) => p + 1,
        None => return vec![],
    };
    let mut out = vec![];
    let mut s = start;
    while s < data.len() {
        let mut e = (s + target).min(data.len());
        if e < data.len() {
            e = match memchr(b'\n', data, e) {
                Some(p) => p + 1,
                None => data.len(),
            };
        }
        out.push((s, e));
        s = e;
    }
    out
}

#[inline]
fn memchr(c: u8, d: &[u8], from: usize) -> Option<usize> {
    memchr::memchr(c, &d[from..]).map(|p| p + from)
}

// ---------------------------------------------------------------- csv

/// Split one line into exactly N fields per the dialect. Returns field (start,end) offsets
/// relative to the line with quotes removed, or None if malformed.
#[inline]
fn split_fields<const N: usize>(line: &[u8], out: &mut [(u32, u32); N]) -> bool {
    if line.is_empty() {
        return false;
    }
    let mut k = 0usize;
    let mut s = 0usize;
    let n = line.len();
    loop {
        // find end of this field
        let e = match memchr::memchr(b',', &line[s..]) {
            Some(p) => s + p,
            None => n,
        };
        if k >= N {
            return false;
        }
        let f = &line[s..e];
        let (a, b) = if !f.is_empty() && f[0] == b'"' {
            if f.len() < 2 || f[f.len() - 1] != b'"' {
                return false;
            }
            if f[1..f.len() - 1].contains(&b'"') {
                return false;
            }
            (s + 1, e - 1)
        } else {
            if f.contains(&b'"') {
                return false;
            }
            (s, e)
        };
        out[k] = (a as u32, b as u32);
        k += 1;
        if e == n {
            break;
        }
        s = e + 1;
    }
    k == N
}

/// One SIMD pass over [s,e): split into lines and fields together. For every well-formed line
/// with exactly N fields calls f(line_start, line, fields) with quote-stripped field offsets
/// relative to the line. Malformed lines (field count, stray quotes, empty) are skipped.
#[inline]
fn for_rows<const N: usize>(data: &[u8], s: usize, e: usize, mut f: impl FnMut(usize, &[u8], &[(u32, u32); N])) {
    let buf = &data[s..e];
    let mut fl = [(0u32, 0u32); N];
    let mut line_start = 0usize; // relative to buf
    let mut field_start = 0usize;
    let mut k = 0usize;
    let mut bad = false;
    let mut quotes: u32 = 0; // quote positions seen in the current field (count)
    let mut q_first = usize::MAX;
    let mut q_last = usize::MAX;
    let mut finish_field = |end: usize, k: &mut usize, bad: &mut bool, quotes: u32, qf: usize, ql: usize, fs: usize, ls: usize, fl: &mut [(u32, u32); N]| {
        let (a, b) = if quotes == 0 {
            (fs, end)
        } else if quotes == 2 && qf == fs && ql + 1 == end && end - fs >= 2 {
            (fs + 1, end - 1)
        } else {
            *bad = true;
            (fs, end)
        };
        if *k < N {
            fl[*k] = ((a - ls) as u32, (b - ls) as u32);
        }
        *k += 1;
    };
    for pos in memchr::memchr3_iter(b'\n', b',', b'"', buf) {
        match buf[pos] {
            b'"' => {
                quotes += 1;
                if q_first == usize::MAX {
                    q_first = pos;
                }
                q_last = pos;
            }
            b',' => {
                finish_field(pos, &mut k, &mut bad, quotes, q_first, q_last, field_start, line_start, &mut fl);
                field_start = pos + 1;
                quotes = 0;
                q_first = usize::MAX;
            }
            _ => {
                // newline
                if pos > line_start {
                    finish_field(pos, &mut k, &mut bad, quotes, q_first, q_last, field_start, line_start, &mut fl);
                    if !bad && k == N {
                        f(s + line_start, &buf[line_start..pos], &fl);
                    }
                }
                line_start = pos + 1;
                field_start = pos + 1;
                k = 0;
                bad = false;
                quotes = 0;
                q_first = usize::MAX;
            }
        }
    }
    // last line without a trailing newline
    if line_start < buf.len() {
        let end = buf.len();
        finish_field(end, &mut k, &mut bad, quotes, q_first, q_last, field_start, line_start, &mut fl);
        if !bad && k == N {
            f(s + line_start, &buf[line_start..end], &fl);
        }
    }
}

/// Iterate lines in [s,e) of data; calls f(line_start, line_bytes).
#[inline]
fn for_lines(data: &[u8], s: usize, e: usize, mut f: impl FnMut(usize, &[u8])) {
    let mut p = s;
    while p < e {
        let q = match memchr::memchr(b'\n', &data[p..e]) {
            Some(x) => p + x,
            None => e,
        };
        f(p, &data[p..q]);
        p = q + 1;
    }
}

// ---------------------------------------------------------------- time

#[inline]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[inline]
fn dig2(b: &[u8], i: usize) -> Option<i64> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a > 9 || c > 9 {
        None
    } else {
        Some((a * 10 + c) as i64)
    }
}

#[inline]
fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

/// Parse "YYYY-MM-DDTHH:MM:SS" prefix (19 bytes) into epoch ms.
#[inline]
fn parse_dt19(b: &[u8]) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let y = dig2(b, 0)? * 100 + dig2(b, 2)?;
    let mo = dig2(b, 5)?;
    let d = dig2(b, 8)?;
    let h = dig2(b, 11)?;
    let mi = dig2(b, 14)?;
    let s = dig2(b, 17)?;
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || s > 59 || d < 1 {
        return None;
    }
    let dim = match mo {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
    };
    if d > dim {
        return None;
    }
    Some(((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + s * 1000)
}

/// F1, F2 or F3 -> epoch ms. None if bad form.
#[inline]
fn parse_ts(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 0 {
        return None;
    }
    if n == 20 && b[19] == b'Z' {
        return parse_dt19(b);
    }
    if n == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':' {
        let base = parse_dt19(b)?;
        let oh = dig2(b, 20)?;
        let om = dig2(b, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 60 + om) * 60_000;
        return Some(if b[19] == b'+' { base - off } else { base + off });
    }
    // F3: digits only
    let mut v: i64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(d as i64)?;
    }
    Some(v)
}

#[inline]
fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            // keep scanning for non-digits is unnecessary: it is invalid either way
            return None;
        }
    }
    if v == 0 {
        None
    } else {
        Some(v as u32)
    }
}

// ---------------------------------------------------------------- lookups

struct Lookups {
    win_start: i64,
    win_end: i64,
    product_cat: HMap<u16>,
    unknown_cat: u16,
    cat_names: Vec<String>,
    camp: HMap<u16>,
    camp_names: Vec<String>, // index 0 = organic
    camp_channel: Vec<String>,
    link: HMap<u32>, // id hash -> component
    comp_name: Vec<u64>,      // component -> ref of the name
    link_files: Files,
}

fn read_simple_csv(path: &Path, n: usize) -> Vec<Vec<Vec<u8>>> {
    let data = std::fs::read(path).unwrap_or_default();
    let mut out = vec![];
    let mut first = true;
    for line in data.split(|&c| c == b'\n') {
        if first {
            first = false;
            continue;
        }
        if line.is_empty() {
            continue;
        }
        let mut fs: Vec<Vec<u8>> = vec![];
        let mut ok = true;
        for f in line.split(|&c| c == b',') {
            let f = if f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' { &f[1..f.len() - 1] } else { f };
            if f.contains(&b'"') {
                ok = false;
            }
            fs.push(f.to_vec());
        }
        if ok && fs.len() == n {
            out.push(fs);
        }
    }
    out
}

fn parse_manifest(path: &Path) -> (i64, i64) {
    let s = std::fs::read_to_string(path).expect("manifest");
    let get = |key: &str| -> i64 {
        let i = s.find(key).expect("key");
        let rest = &s[i + key.len()..];
        let q1 = rest.find('"').unwrap();
        let rest2 = &rest[q1 + 1..];
        let q2 = rest2.find('"').unwrap();
        let v = &rest2[..q2];
        parse_ts(v.as_bytes()).expect("manifest ts")
    };
    (get("\"window_start\""), get("\"window_end\""))
}

fn find(p: &mut [u32], mut x: u32) -> u32 {
    while p[x as usize] != x {
        let g = p[p[x as usize] as usize];
        p[x as usize] = g;
        x = g;
    }
    x
}

fn build_lookups(data: &Path) -> Lookups {
    let (win_start, win_end) = parse_manifest(&data.join("manifest.json"));

    // products / categories
    let prows = read_simple_csv(&data.join("products.csv"), 2);
    let mut cats: Vec<Vec<u8>> = prows.iter().map(|r| r[1].clone()).collect();
    cats.push(b"unknown".to_vec());
    cats.sort();
    cats.dedup();
    let cat_idx: AHashMap<Vec<u8>, u16> = cats.iter().enumerate().map(|(i, c)| (c.clone(), i as u16)).collect();
    let unknown_cat = cat_idx[&b"unknown".to_vec()];
    let mut product_cat: HMap<u16> = HMap::with_capacity_and_hasher(prows.len() * 2, IdBuild::default());
    for r in &prows {
        product_cat.insert(xxh3_64(&r[0]), cat_idx[&r[1]]);
    }
    let cat_names = cats.iter().map(|c| String::from_utf8_lossy(c).into_owned()).collect();

    // campaigns
    let crows = read_simple_csv(&data.join("campaigns.csv"), 2);
    let mut camp: HMap<u16> = HMap::with_capacity_and_hasher(crows.len() * 2, IdBuild::default());
    let mut camp_names = vec!["organic".to_string()];
    let mut camp_channel = vec!["organic".to_string()];
    for r in &crows {
        camp.insert(xxh3_64(&r[0]), camp_names.len() as u16);
        camp_names.push(String::from_utf8_lossy(&r[0]).into_owned());
        camp_channel.push(String::from_utf8_lossy(&r[1]).into_owned());
    }

    Lookups {
        win_start,
        win_end,
        product_cat,
        unknown_cat,
        cat_names,
        camp,
        camp_names,
        camp_channel,
        link: HMap::default(),
        comp_name: vec![],
        link_files: Files { maps: vec![] },
    }
}

/// Links -> union-find -> (id hash -> component, component -> ref of its name, the mapped files).
fn build_links(data: &Path) -> (HMap<u32>, Vec<u64>, Files) {
    // links
    let paths = list_csv(&data.join("links"));
    let mut maps = vec![];
    for p in &paths {
        if let Some(m) = map_file(p) {
            maps.push(m);
        }
    }
    let files = Files { maps };
    let mut jobs = vec![];
    for (fi, m) in files.maps.iter().enumerate() {
        for (s, e) in chunks(m, 8 << 20) {
            jobs.push((fi, s, e));
        }
    }
    let pairs: Vec<Vec<(u64, u64, u64, u64)>> = jobs
        .par_iter()
        .map(|&(fi, s, e)| {
            let d = &files.maps[fi][..];
            let mut out = vec![];
            for_rows::<2>(d, s, e, |ls, line, fl| {
                let (a0, a1) = (fl[0].0 as usize, fl[0].1 as usize);
                let (b0, b1) = (fl[1].0 as usize, fl[1].1 as usize);
                if a0 == a1 || b0 == b1 {
                    return;
                }
                let a = &line[a0..a1];
                let b = &line[b0..b1];
                out.push((xxh3_64(a), xxh3_64(b), mkref(fi, ls + a0, a1 - a0), mkref(fi, ls + b0, b1 - b0)));
            });
            out
        })
        .collect();
    let total: usize = pairs.iter().map(|v| v.len()).sum();
    let mut link: HMap<u32> = HMap::with_capacity_and_hasher(total + 16, IdBuild::default());
    let mut refs: Vec<u64> = Vec::with_capacity(total);
    let mut parent: Vec<u32> = Vec::with_capacity(total);
    let mut edges: Vec<(u32, u32)> = Vec::with_capacity(total);
    for v in &pairs {
        for &(ha, hb, ra, rb) in v {
            let ia = *link.entry(ha).or_insert_with(|| {
                refs.push(ra);
                parent.push(parent.len() as u32);
                (refs.len() - 1) as u32
            });
            let ib = *link.entry(hb).or_insert_with(|| {
                refs.push(rb);
                parent.push(parent.len() as u32);
                (refs.len() - 1) as u32
            });
            edges.push((ia, ib));
        }
    }
    for &(a, b) in &edges {
        let ra = find(&mut parent, a);
        let rb = find(&mut parent, b);
        if ra != rb {
            if ra < rb {
                parent[rb as usize] = ra;
            } else {
                parent[ra as usize] = rb;
            }
        }
    }
    let n = parent.len();
    let mut comp_of = vec![0u32; n];
    let mut comp_ids: AHashMap<u32, u32> = AHashMap::new();
    let mut comp_name: Vec<u64> = vec![];
    for i in 0..n {
        let r = find(&mut parent, i as u32);
        let c = *comp_ids.entry(r).or_insert_with(|| {
            comp_name.push(refs[i]);
            (comp_name.len() - 1) as u32
        });
        comp_of[i] = c;
        if files.bytes(refs[i]) < files.bytes(comp_name[c as usize]) {
            comp_name[c as usize] = refs[i];
        }
    }
    for v in link.values_mut() {
        *v = comp_of[*v as usize];
    }
    (link, comp_name, files)
}

const LINKED: u64 = 1 << 63;

#[derive(Default, Clone, Copy)]
struct IdHasher(u64);
impl std::hash::Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!()
    }
    fn write_u64(&mut self, n: u64) {
        self.0 = n;
    }
}
type IdBuild = std::hash::BuildHasherDefault<IdHasher>;
type HMap<V> = std::collections::HashMap<u64, V, IdBuild>;

struct UserRow {
    name_ref: u64,
    linked: bool,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: i32,
}

#[inline]
fn person_key(lk: &Lookups, uid: &[u8]) -> u64 {
    let h = xxh3_64(uid);
    match lk.link.get(&h) {
        Some(&c) => LINKED | c as u64,
        None => h & !LINKED,
    }
}

// ---------------------------------------------------------------- events

#[derive(Clone, Copy)]
struct Ev {
    eid: u64,    // event_id hash (64 bits)
    person: u64,
    uref: u64,   // user_id bytes (name of an unlinked person)
    eid2: u32,   // second, independent 32-bit hash of event_id: 96-bit identity
    ingest: i32, // seconds since window start
    ts: i32,     // ms since window start (valid only if ok)
    price: u32,
    cat: u16,
    kind: u8, // 0 view 1 cart 2 purchase
    ok: u8,   // passes validation (except lateness)
}

/// A surviving event, routed to its person.
#[derive(Clone, Copy)]
struct Sv {
    person: u64,
    uref: u64,
    ts: i32,
    price: u32,
    cat: u16,
    kind: u8,
}

#[derive(Clone, Copy)]
struct Ck {
    person: u64,
    ts: i64,
    cref: u64,
    camp: u16,
}

fn bucket_of(h: u64) -> usize {
    ((h.wrapping_mul(0x9E3779B97F4A7C15)) >> 56) as usize % NB
}

fn main() {
    let t0 = std::time::Instant::now();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let ncpu: usize = std::env::var("NCPU").ok().and_then(|s| s.parse().ok()).unwrap_or(8);
    rayon::ThreadPoolBuilder::new().num_threads(ncpu.max(1)).build_global().ok();

    // map event and click files early so the kernel starts reading
    let ev_paths = list_csv(&data.join("events"));
    let ck_paths = list_csv(&data.join("clicks"));
    let ev_maps: Vec<Mmap> = ev_paths.iter().filter_map(|p| map_file(p)).collect();
    let ck_maps: Vec<Mmap> = ck_paths.iter().filter_map(|p| map_file(p)).collect();
    let evf = Arc::new(Files { maps: ev_maps });
    let ckf = Arc::new(Files { maps: ck_maps });

    let exp: u32 = std::env::var("EXP").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    let mut lk = build_lookups(&data);
    eprintln!("lookups {:?}", t0.elapsed());
    let ws = lk.win_start;
    let we = lk.win_end;

    // ------------------------------------------------------------ events: parse
    let mut jobs = vec![];
    for (fi, m) in evf.maps.iter().enumerate() {
        for (s, e) in chunks(m, 16 << 20) {
            jobs.push((fi, s, e));
        }
    }

    let (links, parts): ((HMap<u32>, Vec<u64>, Files), Vec<Vec<Vec<Ev>>>) = rayon::join(|| build_links(&data), || jobs
        .par_iter()
        .map(|&(fi, s, e)| {
            let d = &evf.maps[fi][..];
            let mut b: Vec<Vec<Ev>> = (0..NB).map(|_| Vec::new()).collect();
            let mut cnt = 0u64;
            for_rows::<7>(d, s, e, |ls, line, fl| {
                if exp == 1 || exp == 2 {
                    cnt += fl[3].0 as u64;
                    return;
                }
                let f = |i: usize| &line[fl[i].0 as usize..fl[i].1 as usize];
                let eid = f(0);
                if eid.is_empty() {
                    return;
                }
                let ingest = match parse_ts(f(3)) {
                    Some(x) => x,
                    None => return, // never happens per rules
                };
                let uid = f(1);
                let mut ok = !uid.is_empty();
                let ts = parse_ts(f(2));
                let ts = match ts {
                    Some(t) if t >= ws && t < we => t,
                    _ => {
                        ok = false;
                        ws
                    }
                };
                let kind = match f(4) {
                    b"view" => 0u8,
                    b"cart" => 1,
                    b"purchase" => 2,
                    _ => {
                        ok = false;
                        255
                    }
                };
                let pid = f(5);
                if pid.is_empty() {
                    ok = false;
                }
                let mut price = 0u32;
                if kind == 2 {
                    match parse_price(f(6)) {
                        Some(p) => price = p,
                        None => ok = false,
                    }
                }
                let (person, cat) = if ok {
                    (xxh3_64(uid), *lk.product_cat.get(&xxh3_64(pid)).unwrap_or(&lk.unknown_cat))
                } else {
                    (0, 0)
                };
                if exp == 3 {
                    cnt += (ingest ^ ts) as u64 + person + cat as u64 + price as u64;
                    return;
                }
                let h128 = xxh3_128(eid);
                let h = h128 as u64;
                let h2 = (h128 >> 64) as u32;
                b[bucket_of(h)].push(Ev {
                    eid: h,
                    eid2: h2,
                    ingest: ((ingest - ws).div_euclid(1000)).clamp(i32::MIN as i64, i32::MAX as i64) as i32,
                    ts: (ts - ws) as i32,
                    person,
                    uref: mkref(fi, ls + fl[1].0 as usize, uid.len()),
                    price,
                    cat,
                    kind,
                    ok: ok as u8,
                });
            });
            if cnt == 1 { eprintln!("x"); }
            b
        })
        .collect());
    lk.link = links.0;
    lk.comp_name = links.1;
    lk.link_files = links.2;
    eprintln!("events parsed (+links) {:?} links={} comps={}", t0.elapsed(), lk.link.len(), lk.comp_name.len());
    if exp > 0 { return; }

    // ------------------------------------------------------------ clicks: parse
    let mut cjobs = vec![];
    for (fi, m) in ckf.maps.iter().enumerate() {
        for (s, e) in chunks(m, 16 << 20) {
            cjobs.push((fi, s, e));
        }
    }
    let cparts: Vec<Vec<Vec<Ck>>> = cjobs
        .par_iter()
        .map(|&(fi, s, e)| {
            let d = &ckf.maps[fi][..];
            let mut b: Vec<Vec<Ck>> = (0..NB).map(|_| Vec::new()).collect();
            for_rows::<4>(d, s, e, |ls, line, fl| {
                let f = |i: usize| &line[fl[i].0 as usize..fl[i].1 as usize];
                let cid = f(0);
                if cid.is_empty() {
                    return;
                }
                let uid = f(1);
                if uid.is_empty() {
                    return;
                }
                let ts = match parse_ts(f(3)) {
                    Some(t) if t >= ws - WEEK_MS && t < we => t,
                    _ => return,
                };
                let camp = match lk.camp.get(&xxh3_64(f(2))) {
                    Some(&c) => c,
                    None => return,
                };
                let person = person_key(&lk, uid);
                b[bucket_of(person)].push(Ck { person, ts, cref: mkref(fi, ls + fl[0].0 as usize, cid.len()), camp });
            });
            b
        })
        .collect();
    eprintln!("clicks parsed {:?}", t0.elapsed());

    // ------------------------------------------------------------ dedup + validate, rebucket by person
    // transpose [job][bucket] -> [bucket][job] so each bucket can be consumed and freed
    let mut ebk: Vec<Vec<Vec<Ev>>> = (0..NB).map(|_| Vec::new()).collect();
    for job in parts {
        for (bi, v) in job.into_iter().enumerate() {
            if !v.is_empty() {
                ebk[bi].push(v);
            }
        }
    }
    let byperson: Vec<Vec<Vec<Sv>>> = ebk
        .into_par_iter()
        .map(|chunks| {
            let n: usize = chunks.iter().map(|c| c.len()).sum();
            let mut v: Vec<Ev> = Vec::with_capacity(n);
            for c in chunks {
                v.extend_from_slice(&c);
            }
            // open-addressing table: event identity -> index of the copy with the smallest ingest_ts
            let cap = (n * 2).next_power_of_two().max(16);
            let mask = cap - 1;
            let mut table: Vec<u32> = vec![u32::MAX; cap];
            for (i, e) in v.iter().enumerate() {
                let mut slot = (e.eid as usize ^ (e.eid >> 32) as usize) & mask;
                loop {
                    let cur = table[slot];
                    if cur == u32::MAX {
                        table[slot] = i as u32;
                        break;
                    }
                    let c = &v[cur as usize];
                    if c.eid == e.eid && c.eid2 == e.eid2 {
                        if e.ingest < c.ingest {
                            table[slot] = i as u32;
                        }
                        break;
                    }
                    slot = (slot + 1) & mask;
                }
            }
            let mut out: Vec<Vec<Sv>> = (0..NB).map(|_| Vec::new()).collect();
            for &idx in &table {
                if idx == u32::MAX {
                    continue;
                }
                let e = &v[idx as usize];
                if e.ok == 0 {
                    continue;
                }
                if e.ingest as i64 * 1000 - e.ts as i64 > LATE_MS {
                    continue;
                }
                let person = match lk.link.get(&e.person) {
                    Some(&c) => LINKED | c as u64,
                    None => e.person & !LINKED,
                };
                out[bucket_of(person)].push(Sv { person, uref: e.uref, ts: e.ts, price: e.price, cat: e.cat, kind: e.kind });
            }
            out
        })
        .collect();
    eprintln!("dedup {:?}", t0.elapsed());
    let mut pbk: Vec<Vec<Vec<Sv>>> = (0..NB).map(|_| Vec::new()).collect();
    for src in byperson {
        for (bi, v) in src.into_iter().enumerate() {
            if !v.is_empty() {
                pbk[bi].push(v);
            }
        }
    }
    let mut cbk: Vec<Vec<Vec<Ck>>> = (0..NB).map(|_| Vec::new()).collect();
    for job in cparts {
        for (bi, v) in job.into_iter().enumerate() {
            if !v.is_empty() {
                cbk[bi].push(v);
            }
        }
    }
    let work: Vec<(Vec<Vec<Sv>>, Vec<Vec<Ck>>)> = pbk.into_iter().zip(cbk.into_iter()).collect();

    // ------------------------------------------------------------ sessions per person bucket
    let ncamp = lk.camp_names.len();
    let ndays = (((we - ws) + DAY_MS - 1) / DAY_MS) as usize;
    let ncat = lk.cat_names.len();

    let results: Vec<(Vec<UserRow>, Vec<[i64; 4]>)> = work
        .into_par_iter()
        .fold(|| (Vec::<UserRow>::new(), vec![[0i64; 4]; ncamp * ndays]), |(mut rows, mut camp_stats), (evs, cks)| {
            let mut ev: Vec<Sv> = Vec::with_capacity(evs.iter().map(|c| c.len()).sum());
            for c in evs {
                ev.extend_from_slice(&c);
            }
            ev.sort_unstable_by(|a, b| a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)));
            let mut ck: Vec<Ck> = Vec::with_capacity(cks.iter().map(|c| c.len()).sum());
            for c in cks {
                ck.extend_from_slice(&c);
            }
            ck.sort_unstable_by(|a, b| a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)));

            let mut catrev = vec![0i64; ncat];
            let mut touched: Vec<u16> = vec![];
            let mut cnt: AHashMap<u16, u32> = AHashMap::new();

            let mut ci = 0usize;
            let mut i = 0usize;
            while i < ev.len() {
                let person = ev[i].person;
                let mut j = i;
                while j < ev.len() && ev[j].person == person {
                    j += 1;
                }
                while ci < ck.len() && ck[ci].person < person {
                    ci += 1;
                }
                let mut cj = ci;
                while cj < ck.len() && ck[cj].person == person {
                    cj += 1;
                }
                let clicks = &ck[ci..cj];
                let events = &ev[i..j];
                let linked = person & LINKED != 0;
                let name_ref = if linked { lk.comp_name[(person & !LINKED) as usize] } else { events[0].uref };

                // sliding window over clicks for assists
                cnt.clear();
                let (mut lo, mut hi) = (0usize, 0usize);

                // current user-day accumulation
                let mut cur_day: i32 = -1;
                let (mut u_s, mut u_e, mut u_r) = (0i64, 0i64, 0i64);

                let mut k = 0usize;
                while k < events.len() {
                    // one session: [k, m)
                    let start = events[k].ts as i64 + ws;
                    let mut m = k + 1;
                    let mut last = start;
                    while m < events.len() {
                        let t = events[m].ts as i64 + ws;
                        if t - last > GAP_MS {
                            break;
                        }
                        last = t;
                        m += 1;
                    }
                    let day = ((start - ws) / DAY_MS) as i32;
                    // flush user-day when the day changes
                    if day != cur_day {
                        if cur_day >= 0 {
                            rows.push(flush(name_ref, linked, cur_day, u_s, u_e, u_r, &mut catrev, &mut touched));
                        }
                        cur_day = day;
                        u_s = 0;
                        u_e = 0;
                        u_r = 0;
                    }
                    let mut rev = 0i64;
                    let mut purch = false;
                    for e in &events[k..m] {
                        if e.kind == 2 {
                            purch = true;
                            rev += e.price as i64;
                            let c = e.cat as usize;
                            if catrev[c] == 0 {
                                touched.push(e.cat);
                            }
                            catrev[c] += e.price as i64;
                        }
                    }
                    u_s += 1;
                    u_e += (m - k) as i64;
                    u_r += rev;

                    // credit: latest click with ts <= start and start - ts <= 7d
                    while hi < clicks.len() && clicks[hi].ts <= start {
                        *cnt.entry(clicks[hi].camp).or_insert(0) += 1;
                        hi += 1;
                    }
                    while lo < hi && clicks[lo].ts < start - WEEK_MS {
                        let c = cnt.get_mut(&clicks[lo].camp).unwrap();
                        *c -= 1;
                        if *c == 0 {
                            cnt.remove(&clicks[lo].camp);
                        }
                        lo += 1;
                    }
                    let credited: usize = if hi > lo {
                        let t = clicks[hi - 1].ts;
                        let mut best = hi - 1;
                        let mut q = hi - 1;
                        while q > lo && clicks[q - 1].ts == t {
                            q -= 1;
                            if ckf.bytes(clicks[q].cref) < ckf.bytes(clicks[best].cref) {
                                best = q;
                            }
                        }
                        clicks[best].camp as usize
                    } else {
                        0
                    };
                    let d = day as usize;
                    let st = &mut camp_stats[credited * ndays + d];
                    st[0] += 1;
                    st[1] += purch as i64;
                    st[2] += rev;
                    for (&c, _) in cnt.iter() {
                        camp_stats[c as usize * ndays + d][3] += 1;
                    }
                    k = m;
                }
                if cur_day >= 0 {
                    rows.push(flush(name_ref, linked, cur_day, u_s, u_e, u_r, &mut catrev, &mut touched));
                }
                i = j;
                ci = cj;
            }
            (rows, camp_stats)
        })
        .collect();

    fn flush(name_ref: u64, linked: bool, day: i32, s: i64, e: i64, r: i64, catrev: &mut [i64], touched: &mut Vec<u16>) -> UserRow {
        let mut top: i32 = -1;
        let mut best = 0i64;
        for &c in touched.iter() {
            let v = catrev[c as usize];
            if v > best || (v == best && v > 0 && (c as i32) < top) {
                best = v;
                top = c as i32;
            }
            catrev[c as usize] = 0;
        }
        touched.clear();
        UserRow { name_ref, linked, day, sessions: s, events: e, revenue: r, top }
    }
    eprintln!("sessions {:?}", t0.elapsed());

    // ------------------------------------------------------------ write
    write_outputs(&out, &lk, &evf, ws, ndays, results);
    eprintln!("done {:?}", t0.elapsed());
}

fn write_outputs(
    out: &Path,
    lk: &Lookups,
    evf: &Files,
    ws: i64,
    ndays: usize,
    results: Vec<(Vec<UserRow>, Vec<[i64; 4]>)>,
) {
    use arrow_array::{ArrayRef, Date32Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;

    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap();
    std::fs::create_dir_all(&cd).unwrap();
    let day0 = ws.div_euclid(DAY_MS) as i32;
    let props = || WriterProperties::builder().set_compression(Compression::UNCOMPRESSED).set_dictionary_enabled(false).build();

    let uschema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));

    // campaign totals
    let ncamp = lk.camp_names.len();
    let mut tot = vec![[0i64; 4]; ncamp * ndays];
    for (_, cs) in &results {
        for (a, b) in tot.iter_mut().zip(cs.iter()) {
            for k in 0..4 {
                a[k] += b[k];
            }
        }
    }

    // user rows: write in parallel groups
    let groups: Vec<&Vec<_>> = results.iter().map(|(r, _)| r).collect();
    let ngroups = 8usize;
    let per = (groups.len() + ngroups - 1) / ngroups;
    (0..ngroups).into_par_iter().for_each(|g| {
        let slice = &groups[(g * per).min(groups.len())..((g + 1) * per).min(groups.len())];
        let n: usize = slice.iter().map(|r| r.len()).sum();
        let mut names: Vec<&str> = Vec::with_capacity(n);
        let mut days = Vec::with_capacity(n);
        let mut s = Vec::with_capacity(n);
        let mut e = Vec::with_capacity(n);
        let mut r = Vec::with_capacity(n);
        let mut top: Vec<Option<&str>> = Vec::with_capacity(n);
        for rows in slice {
            for row in rows.iter() {
                let b = if row.linked { lk.link_files.bytes(row.name_ref) } else { evf.bytes(row.name_ref) };
                names.push(std::str::from_utf8(b).unwrap_or(""));
                days.push(day0 + row.day);
                s.push(row.sessions);
                e.push(row.events);
                r.push(row.revenue);
                top.push(if row.top < 0 { None } else { Some(lk.cat_names[row.top as usize].as_str()) });
            }
        }
        let cols: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(names)),
            Arc::new(Date32Array::from(days)),
            Arc::new(Int64Array::from(s)),
            Arc::new(Int64Array::from(e)),
            Arc::new(Int64Array::from(r)),
            Arc::new(StringArray::from(top)),
        ];
        let batch = RecordBatch::try_new(uschema.clone(), cols).unwrap();
        let f = File::create(ud.join(format!("part-{g}.parquet"))).unwrap();
        let mut w = ArrowWriter::try_new(f, uschema.clone(), Some(props())).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    });

    let cschema = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));
    let (mut cid, mut ch, mut dy, mut s, mut p, mut r, mut a) = (vec![], vec![], vec![], vec![], vec![], vec![], vec![]);
    for c in 0..ncamp {
        for d in 0..ndays {
            let t = tot[c * ndays + d];
            if t[0] > 0 || t[3] > 0 {
                cid.push(lk.camp_names[c].as_str());
                ch.push(lk.camp_channel[c].as_str());
                dy.push(day0 + d as i32);
                s.push(t[0]);
                p.push(t[1]);
                r.push(t[2]);
                a.push(t[3]);
            }
        }
    }
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(cid)),
        Arc::new(StringArray::from(ch)),
        Arc::new(Date32Array::from(dy)),
        Arc::new(Int64Array::from(s)),
        Arc::new(Int64Array::from(p)),
        Arc::new(Int64Array::from(r)),
        Arc::new(Int64Array::from(a)),
    ];
    let batch = RecordBatch::try_new(cschema.clone(), cols).unwrap();
    let f = File::create(cd.join("part-0.parquet")).unwrap();
    let mut w = ArrowWriter::try_new(f, cschema.clone(), Some(props())).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

