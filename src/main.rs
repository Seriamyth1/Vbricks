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

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
use memmap2::Mmap;
use rayon::prelude::*;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;


const DAY_MS: i64 = 86_400_000;
const GAP_MS: i64 = 1_800_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const LATE_MS: i64 = DAY_MS;
const NB: usize = 512; // buckets

// ---------------------------------------------------------------- files

struct Files {
    maps: Vec<Mmap>,
}

impl Files {
    #[inline]
    fn bytes(&self, r: u64) -> &[u8] {
        let f = ((r >> 40) & 0xff) as usize;
        let off = ((r >> 8) & 0xffff_ffff) as usize;
        let len = (r & 0xff) as usize;
        let m = &self.maps[f][..];
        if len < 255 {
            &m[off..off + len]
        } else {
            // long value: it ends at the next comma, quote or newline (values never contain them)
            let end = m[off..].iter().position(|&c| c == b',' || c == b'"' || c == b'\n').map(|p| off + p).unwrap_or(m.len());
            &m[off..end]
        }
    }
}

#[inline]
fn mkref(file: usize, off: usize, len: usize) -> u64 {
    debug_assert!(file < 256 && off < (1usize << 32));
    ((file as u64) << 40) | ((off as u64) << 8) | (len.min(255) as u64)
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

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    let a = _mm256_loadu_si256(p as *const __m256i);
    let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let nl = _mm256_set1_epi8(b'\n' as i8);
    let cm = _mm256_set1_epi8(b',' as i8);
    let qt = _mm256_set1_epi8(b'"' as i8);
    let m = |v: __m256i, c: __m256i| _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, c)) as u32 as u64;
    (m(a, nl) | (m(b, nl) << 32), m(a, cm) | (m(b, cm) << 32), m(a, qt) | (m(b, qt) << 32))
}

/// AVX2 version of for_rows: classify 64 bytes at a time into newline / comma / quote bitmasks
/// and walk the set bits. Same rules and output as for_rows.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn for_rows_avx2<const N: usize>(data: &[u8], s: usize, e: usize, f: &mut impl FnMut(usize, &[u8], &[(u32, u32); N])) {
    let buf = &data[s..e];
    let n = buf.len();
    let mut fl = [(0u32, 0u32); N];
    let mut line_start = 0usize;
    let mut field_start = 0usize;
    let mut k = 0usize;
    let mut bad = false;
    let mut quotes = 0u32;
    let mut q_first = usize::MAX;
    let mut q_last = 0usize;
    let mut tail = [0u8; 64];
    let mut base = 0usize;
    while base < n {
        let (nlm, cmm, qtm) = if base + 64 <= n {
            masks64(buf.as_ptr().add(base))
        } else {
            let rem = n - base;
            tail[..rem].copy_from_slice(&buf[base..]);
            for x in tail[rem..].iter_mut() {
                *x = 0;
            }
            masks64(tail.as_ptr())
        };
        let mut m = nlm | cmm | qtm;
        while m != 0 {
            let bit = m.trailing_zeros() as usize;
            m &= m - 1;
            let pos = base + bit;
            let b1 = 1u64 << bit;
            if qtm & b1 != 0 {
                quotes += 1;
                if q_first == usize::MAX {
                    q_first = pos;
                }
                q_last = pos;
                continue;
            }
            // end of a field (comma or newline)
            if pos > line_start || cmm & b1 != 0 {
                let (a, b) = if quotes == 0 {
                    (field_start, pos)
                } else if quotes == 2 && q_first == field_start && q_last + 1 == pos && pos - field_start >= 2 {
                    (field_start + 1, pos - 1)
                } else {
                    bad = true;
                    (field_start, pos)
                };
                if k < N {
                    *fl.get_unchecked_mut(k) = ((a - line_start) as u32, (b - line_start) as u32);
                }
                k += 1;
            }
            quotes = 0;
            q_first = usize::MAX;
            field_start = pos + 1;
            if nlm & b1 != 0 {
                if pos > line_start && !bad && k == N {
                    f(s + line_start, &buf[line_start..pos], &fl);
                }
                line_start = pos + 1;
                k = 0;
                bad = false;
            }
        }
        base += 64;
    }
    if line_start < n {
        let pos = n;
        let (a, b) = if quotes == 0 {
            (field_start, pos)
        } else if quotes == 2 && q_first == field_start && q_last + 1 == pos && pos - field_start >= 2 {
            (field_start + 1, pos - 1)
        } else {
            bad = true;
            (field_start, pos)
        };
        if k < N {
            fl[k] = ((a - line_start) as u32, (b - line_start) as u32);
        }
        k += 1;
        if !bad && k == N {
            f(s + line_start, &buf[line_start..n], &fl);
        }
    }
}

/// Faster AVX2 tokenizer: walk only commas and newlines; validate quotes per line by count.
/// A line is well-formed iff it has N fields and its quote count equals 2 x (fields that start
/// and end with a quote, length >= 2) and no other field starts or ends with a quote.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,popcnt")]
unsafe fn for_rows_avx2b<const N: usize>(data: &[u8], s: usize, e: usize, f: &mut impl FnMut(usize, &[u8], &[(u32, u32); N])) {
    let buf = &data[s..e];
    let n = buf.len();
    let mut ends = [0usize; 16]; // comma positions in the current line (N <= 16)
    let mut k = 0usize; // commas seen in this line
    let mut line_start = 0usize;
    let mut qcount = 0u32;
    let mut tail = [0u8; 64];
    let mut base = 0usize;

    #[inline(always)]
    unsafe fn finish<const N: usize>(
        buf: &[u8], s: usize, line_start: usize, line_end: usize, ends: &[usize; 16], k: usize, qcount: u32,
        f: &mut impl FnMut(usize, &[u8], &[(u32, u32); N]),
    ) {
        if line_end == line_start || k + 1 != N {
            return;
        }
        let mut fl = [(0u32, 0u32); N];
        if qcount == 0 {
            // no quotes on this line: fields are exactly the spans between commas
            let mut st = 0u32;
            for i in 0..N {
                let en = if i + 1 < N { (*ends.get_unchecked(i) - line_start) as u32 } else { (line_end - line_start) as u32 };
                *fl.get_unchecked_mut(i) = (st, en);
                st = en + 1;
            }
            f(s + line_start, buf.get_unchecked(line_start..line_end), &fl);
            return;
        }
        // quoted line: a field may be wrapped in quotes as a whole; any other quote makes the line malformed.
        // Branch-free per field; the total quote count catches quotes inside fields.
        let mut expect = 0u32;
        let mut bad = false;
        let mut st = line_start;
        for i in 0..N {
            let en = if i + 1 < N { *ends.get_unchecked(i) } else { line_end };
            let len = en - st;
            let first = len > 0 && *buf.get_unchecked(st) == b'"';
            let last = len > 0 && *buf.get_unchecked(en - 1) == b'"';
            bad |= (first != last) | (first & (len < 2));
            let q = first as usize;
            expect += 2 * q as u32;
            *fl.get_unchecked_mut(i) = ((st + q - line_start) as u32, (en - q - line_start) as u32);
            st = en + 1;
        }
        if bad || qcount != expect {
            return;
        }
        f(s + line_start, buf.get_unchecked(line_start..line_end), &fl);
    }

    while base < n {
        let (nlm, cmm, qtm) = if base + 64 <= n {
            masks64(buf.as_ptr().add(base))
        } else {
            let rem = n - base;
            tail[..rem].copy_from_slice(&buf[base..]);
            for x in tail[rem..].iter_mut() {
                *x = 0;
            }
            masks64(tail.as_ptr())
        };
        let mut qt_rem = qtm;
        let mut m = nlm | cmm;
        while m != 0 {
            let bit = m.trailing_zeros();
            m &= m - 1;
            let pos = base + bit as usize;
            if (nlm >> bit) & 1 != 0 {
                let low = (1u64 << bit) - 1;
                qcount += (qt_rem & low).count_ones();
                qt_rem &= !low;
                finish::<N>(buf, s, line_start, pos, &ends, k, qcount, f);
                line_start = pos + 1;
                k = 0;
                qcount = 0;
            } else {
                if k < 16 {
                    *ends.get_unchecked_mut(k) = pos;
                }
                k += 1;
            }
        }
        qcount += qt_rem.count_ones();
        base += 64;
    }
    if line_start < n {
        finish::<N>(buf, s, line_start, n, &ends, k, qcount, f);
    }
}

/// Dispatch: AVX2 when the CPU has it, else the portable version.
#[inline]
fn rows<const N: usize>(data: &[u8], s: usize, e: usize, mut f: impl FnMut(usize, &[u8], &[(u32, u32); N])) {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") && std::env::var_os("NO_AVX2").is_none() {
            unsafe { for_rows_avx2b::<N>(data, s, e, &mut f) };
            return;
        }
    }
    for_rows::<N>(data, s, e, f);
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

/// Days since 1970-01-01 of the first of each month, and the month's length, for 1900..2199.
const TBL_Y0: i64 = 1900;
const TBL_YEARS: usize = 300;
const fn civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}
const fn build_table() -> [(i32, u8); TBL_YEARS * 12] {
    let mut t = [(0i32, 0u8); TBL_YEARS * 12];
    let mut yi = 0;
    while yi < TBL_YEARS {
        let y = TBL_Y0 + yi as i64;
        let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
        let mut m = 1;
        while m <= 12 {
            let dim = match m {
                1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                4 | 6 | 9 | 11 => 30,
                _ => if leap { 29 } else { 28 },
            };
            t[yi * 12 + (m as usize - 1)] = (civil(y, m, 1) as i32, dim);
            m += 1;
        }
        yi += 1;
    }
    t
}
static MONTHS: [(i32, u8); TBL_YEARS * 12] = build_table();
fn month_table() -> &'static [(i32, u8)] {
    &MONTHS
}

/// Load 8 bytes at b[i..i+8] (little endian). Callers guarantee i + 8 <= b.len().
#[inline(always)]
fn ld8(b: &[u8], i: usize) -> u64 {
    debug_assert!(i + 8 <= b.len());
    unsafe { u64::from_le((b.as_ptr().add(i) as *const u64).read_unaligned()) }
}
#[inline(always)]
fn ld4(b: &[u8], i: usize) -> u64 {
    debug_assert!(i + 4 <= b.len());
    unsafe { u32::from_le((b.as_ptr().add(i) as *const u32).read_unaligned()) as u64 }
}

/// SWAR check-and-convert: bytes at positions in `dmask` (0xFF per digit byte) must be ASCII digits.
/// Returns the word with each digit byte replaced by its value, or None.
#[inline(always)]
fn swar_digits(w: u64, dmask: u64) -> Option<u64> {
    let x = (w ^ 0x3030_3030_3030_3030) & dmask;
    let bad = (((x & 0x7f7f_7f7f_7f7f_7f7f) + 0x7676_7676_7676_7676) | x) & 0x8080_8080_8080_8080 & dmask;
    if bad != 0 {
        None
    } else {
        Some(x)
    }
}

/// Parse "YYYY-MM-DDTHH:MM:SS" prefix (19 bytes) into epoch ms.
#[inline]
fn parse_dt19(b: &[u8]) -> Option<i64> {
    if b.len() < 19 {
        return None;
    }
    // "YYYY-MM-" | "DDTHH:MM" | (bytes 11..19) "HH:MM:SS"
    let w0 = ld8(b, 0);
    let w1 = ld8(b, 8);
    let w2 = ld8(b, 11);
    // separators: b4='-' b7='-' | b10='T' b13=':' | b16=':'
    const S0: u64 = 0xff00_00ff_0000_0000; // bytes 4 and 7
    const E0: u64 = 0x2d00_002d_0000_0000;
    const S1: u64 = 0x0000_ff00_00ff_0000; // bytes 10 and 13 (positions 2 and 5 in w1)
    const E1: u64 = 0x0000_3a00_0054_0000;
    const S2: u64 = 0x0000_ff00_0000_0000; // byte 16 (position 5 in w2)
    const E2: u64 = 0x0000_3a00_0000_0000;
    if (w0 & S0) != E0 || (w1 & S1) != E1 || (w2 & S2) != E2 {
        return None;
    }
    let x0 = swar_digits(w0, 0x00ff_ff00_ffff_ffff)?; // YYYY MM
    let x1 = swar_digits(w1, 0xffff_00ff_ff00_ffff)?; // DD HH MM
    let x2 = swar_digits(w2, 0xffff_0000_0000_0000)?; // SS (bytes 17,18 = positions 6,7)
    let by = |x: u64, i: u32| ((x >> (8 * i)) & 0xff) as i64;
    let y = by(x0, 0) * 1000 + by(x0, 1) * 100 + by(x0, 2) * 10 + by(x0, 3);
    let mo = by(x0, 5) * 10 + by(x0, 6);
    let d = by(x1, 0) * 10 + by(x1, 1);
    let h = by(x1, 3) * 10 + by(x1, 4);
    let mi = by(x1, 6) * 10 + by(x1, 7);
    let s = by(x2, 6) * 10 + by(x2, 7);
    if mo < 1 || mo > 12 || h > 23 || mi > 59 || s > 59 || d < 1 {
        return None;
    }
    let days = if y >= TBL_Y0 && y < TBL_Y0 + TBL_YEARS as i64 {
        let (base, dim) = MONTHS[((y - TBL_Y0) * 12 + mo - 1) as usize];
        if d > dim as i64 {
            return None;
        }
        base as i64 + d - 1
    } else {
        let dim = match mo {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            _ => if is_leap(y) { 29 } else { 28 },
        };
        if d > dim {
            return None;
        }
        days_from_civil(y, mo, d)
    };
    Some(((days * 24 + h) * 60 + mi) * 60_000 + s * 1000)
}

/// Year-month prefixes ("YYYY-MM-") of the few months around the window, so the common timestamp
/// is checked with one 8-byte compare instead of decoding and validating year and month.
struct MonthPre {
    pre: [u64; 8],
    base: [i64; 8], // epoch day of the month's day 0
    dim: [u8; 8],
    n: usize,
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

impl MonthPre {
    fn new(ws: i64, we: i64) -> Self {
        let mut mp = MonthPre { pre: [u64::MAX; 8], base: [0; 8], dim: [0; 8], n: 0 };
        let mut day = ws.div_euclid(DAY_MS) - 40;
        let last = we.div_euclid(DAY_MS) + 40;
        while day <= last && mp.n < 8 {
            let (y, m, _) = civil_from_days(day);
            if (0..=9999).contains(&y) && (mp.n == 0 || mp.base[mp.n - 1] != days_from_civil(y, m, 1) - 1) {
                let txt = format!("{:04}-{:02}-", y, m);
                let dim = match m {
                    1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                    4 | 6 | 9 | 11 => 30,
                    _ => if is_leap(y) { 29 } else { 28 },
                };
                mp.pre[mp.n] = u64::from_le_bytes(txt.as_bytes().try_into().unwrap());
                mp.base[mp.n] = days_from_civil(y, m, 1) - 1;
                mp.dim[mp.n] = dim;
                mp.n += 1;
            }
            day += 1;
        }
        mp
    }

    /// Same result as parse_dt19 on b[..19] (b.len() >= 19).
    #[inline(always)]
    fn dt19(&self, b: &[u8]) -> Option<i64> {
        let w0 = ld8(b, 0);
        let mut i = 0;
        while i < self.n {
            if w0 == self.pre[i] {
                let w1 = ld8(b, 8); // D D T H H : M M
                let w2 = ld8(b, 11); // H H : M M : S S
                const S1: u64 = 0x0000_ff00_00ff_0000;
                const E1: u64 = 0x0000_3a00_0054_0000;
                const S2: u64 = 0x0000_ff00_0000_0000;
                const E2: u64 = 0x0000_3a00_0000_0000;
                if (w1 & S1) != E1 || (w2 & S2) != E2 {
                    return None;
                }
                let x1 = swar_digits(w1, 0xffff_00ff_ff00_ffff)?;
                let x2 = swar_digits(w2, 0xffff_0000_0000_0000)?;
                let p1 = x1.wrapping_mul(10).wrapping_add(x1 >> 8);
                let p2 = x2.wrapping_mul(10).wrapping_add(x2 >> 8);
                let d = (p1 & 0xff) as i64;
                let h = ((p1 >> 24) & 0xff) as i64;
                let mi = ((p1 >> 48) & 0xff) as i64;
                let s = ((p2 >> 48) & 0xff) as i64;
                if d < 1 || d > self.dim[i] as i64 || h > 23 || mi > 59 || s > 59 {
                    return None;
                }
                return Some((self.base[i] + d) * DAY_MS + h * 3_600_000 + mi * 60_000 + s * 1000);
            }
            i += 1;
        }
        parse_dt19(b)
    }

    /// ingest_ts: always valid F1 per the rules, so a known month needs no checks.
    #[inline(always)]
    fn ingest(&self, b: &[u8]) -> Option<i64> {
        if b.len() == 20 {
            let w0 = ld8(b, 0);
            let mut i = 0;
            while i < self.n {
                if w0 == self.pre[i] {
                    let x1 = (ld8(b, 8) ^ 0x3030_3030_3030_3030) & 0xffff_00ff_ff00_ffff;
                    let x2 = (ld8(b, 11) ^ 0x3030_3030_3030_3030) & 0xffff_0000_0000_0000;
                    if (x1 | x2) & 0xf0f0_f0f0_f0f0_f0f0 != 0 || b[19] != b'Z' {
                        break; // not plain digits: take the checked path
                    }
                    let p1 = x1.wrapping_mul(10).wrapping_add(x1 >> 8);
                    let p2 = x2.wrapping_mul(10).wrapping_add(x2 >> 8);
                    let d = (p1 & 0xff) as i64;
                    let h = ((p1 >> 24) & 0xff) as i64;
                    let mi = ((p1 >> 48) & 0xff) as i64;
                    let s = ((p2 >> 48) & 0xff) as i64;
                    return Some((self.base[i] + d) * DAY_MS + h * 3_600_000 + mi * 60_000 + s * 1000);
                }
                i += 1;
            }
        }
        self.ts(b)
    }

    /// parse_ts with the fast date path.
    #[inline(always)]
    fn ts(&self, b: &[u8]) -> Option<i64> {
        let n = b.len();
        if n == 20 {
            if b[19] == b'Z' {
                return self.dt19(b);
            }
        } else if n == 25 {
            if (b[19] == b'+' || b[19] == b'-') && b[22] == b':' {
                let base = self.dt19(b)?;
                let oh = dig2(b, 20)?;
                let om = dig2(b, 23)?;
                if oh > 23 || om > 59 {
                    return None;
                }
                let off = (oh * 60 + om) * 60_000;
                return Some(if b[19] == b'+' { base - off } else { base + off });
            }
        }
        parse_ts(b)
    }
}

/// ingest_ts is always valid F1 per the rules: convert without re-validating every field.
/// Falls back to the full parser if the shape is not the expected 20-byte F1.
#[inline]
fn parse_f1_trusted(b: &[u8]) -> Option<i64> {
    if b.len() != 20 || b[19] != b'Z' {
        return parse_ts(b);
    }
    let w0 = ld8(b, 0) ^ 0x3030_3030_3030_3030;
    let w1 = ld8(b, 8) ^ 0x3030_3030_3030_3030;
    let w2 = ld8(b, 11) ^ 0x3030_3030_3030_3030;
    let by = |x: u64, i: u32| ((x >> (8 * i)) & 0xff) as i64;
    let y = by(w0, 0) * 1000 + by(w0, 1) * 100 + by(w0, 2) * 10 + by(w0, 3);
    let mo = by(w0, 5) * 10 + by(w0, 6);
    let d = by(w1, 0) * 10 + by(w1, 1);
    let h = by(w1, 3) * 10 + by(w1, 4);
    let mi = by(w1, 6) * 10 + by(w1, 7);
    let s = by(w2, 6) * 10 + by(w2, 7);
    if y < TBL_Y0 || y >= TBL_Y0 + TBL_YEARS as i64 || !(1..=12).contains(&mo) {
        return parse_ts(b);
    }
    let days = MONTHS[((y - TBL_Y0) * 12 + mo - 1) as usize].0 as i64 + d - 1;
    Some(((days * 24 + h) * 60 + mi) * 60_000 + s * 1000)
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
    // F3: digits only. Fast path: 13 digits = 5 scalar + 8 via SWAR.
    if n == 13 {
        let w = ld8(b, 5);
        if let Some(x) = swar_digits(w, u64::MAX) {
            let mut hi: i64 = 0;
            let mut ok = true;
            for &c in &b[..5] {
                let d = c.wrapping_sub(b'0');
                ok &= d <= 9;
                hi = hi * 10 + d as i64;
            }
            if ok {
                // 8 digit bytes (first digit in the lowest byte) -> integer
                let x = (x * 10 + (x >> 8)) & 0x00ff_00ff_00ff_00ff;
                let x = (x * 100 + (x >> 16)) & 0x0000_ffff_0000_ffff;
                let x = (x * 10000 + (x >> 32)) & 0xffff_ffff;
                return Some(hi * 100_000_000 + x as i64);
            }
        }
        return None;
    }
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

// ---------------------------------------------------------------- hashing

#[inline(always)]
fn mum(a: u64, b: u64) -> u64 {
    let r = (a as u128) * (b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}

/// Fast hash for short IDs (wyhash-style). Exact for <= 16 bytes: two overlapping reads cover
/// every byte, the length is mixed in.
#[inline(always)]
fn fh(b: &[u8], seed: u64) -> u64 {
    const P0: u64 = 0xa076_1d64_78bd_642f;
    const P1: u64 = 0xe703_7ed1_a0b4_28db;
    const P2: u64 = 0x8ebc_6af0_9c88_c6e3;
    let n = b.len();
    let (a, c) = if n >= 8 {
        if n <= 16 {
            (ld8(b, 0), ld8(b, n - 8))
        } else {
            let mut acc = seed ^ P2;
            let mut i = 0;
            while i + 8 <= n {
                acc = mum(acc ^ ld8(b, i), P1);
                i += 8;
            }
            (acc, ld8(b, n - 8))
        }
    } else if n >= 4 {
        (ld4(b, 0), ld4(b, n - 4))
    } else if n > 0 {
        ((b[0] as u64) | ((b[n / 2] as u64) << 8) | ((b[n - 1] as u64) << 16), 0)
    } else {
        (0, 0)
    };
    mum(a ^ P0 ^ seed, c ^ P1 ^ (n as u64).wrapping_mul(P2))
}

/// 64-bit hash plus 32 more bits from the same 128-bit product (96-bit identity for event ids).
#[inline(always)]
fn fh96(b: &[u8]) -> (u64, u32) {
    const P0: u64 = 0xa076_1d64_78bd_642f;
    const P1: u64 = 0xe703_7ed1_a0b4_28db;
    const P2: u64 = 0x8ebc_6af0_9c88_c6e3;
    const SEED: u64 = 0x2d35_8dcc_aa6c_78a5;
    let n = b.len();
    if (8..=16).contains(&n) {
        let r = ((ld8(b, 0) ^ P0 ^ SEED) as u128) * ((ld8(b, n - 8) ^ P1 ^ (n as u64).wrapping_mul(P2)) as u128);
        let (lo, hi) = (r as u64, (r >> 64) as u64);
        (lo ^ hi, hi as u32)
    } else {
        (hid(b), fh(b, 0x9e37_79b9_7f4a_7c15) as u32)
    }
}

#[inline(always)]
fn hid(b: &[u8]) -> u64 {
    fh(b, 0x2d35_8dcc_aa6c_78a5)
}

// ---------------------------------------------------------------- lookups

struct Lookups {
    win_start: i64,
    win_end: i64,
    product_cat: HMap<u16>,
    product_arr: Vec<u16>, // product_arr[n] = category of "p{n}" when every product id has that exact form
    unknown_cat: u16,
    cat_names: Vec<String>,
    camp: HMap<u16>,
    camp_arr: Vec<u16>, // camp_arr[n] = index of "c_{n}" when every campaign id has that exact form
    camp_names: Vec<String>, // index 0 = organic
    camp_channel: Vec<String>,
    link: LinkTable, // id hash -> component
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
        product_cat.insert(hid(&r[0]), cat_idx[&r[1]]);
    }
    let cat_names = cats.iter().map(|c| String::from_utf8_lossy(c).into_owned()).collect();
    // "p<n>" ids with no leading zero: index an array instead of hashing
    let mut product_arr: Vec<u16> = vec![];
    let canon = |id: &[u8]| -> Option<usize> {
        if id.len() < 2 || id.len() > 8 || id[0] != b'p' || id[1] == b'0' {
            return None;
        }
        let mut v = 0usize;
        for &c in &id[1..] {
            if !c.is_ascii_digit() {
                return None;
            }
            v = v * 10 + (c - b'0') as usize;
        }
        Some(v)
    };
    if !prows.is_empty() && prows.iter().all(|r| canon(&r[0]).map(|v| v < 10_000_000).unwrap_or(false)) {
        let maxv = prows.iter().map(|r| canon(&r[0]).unwrap()).max().unwrap();
        product_arr = vec![u16::MAX; maxv + 1];
        for r in &prows {
            product_arr[canon(&r[0]).unwrap()] = cat_idx[&r[1]];
        }
    }

    // campaigns
    let crows = read_simple_csv(&data.join("campaigns.csv"), 2);
    let mut camp: HMap<u16> = HMap::with_capacity_and_hasher(crows.len() * 2, IdBuild::default());
    let mut camp_names = vec!["organic".to_string()];
    let mut camp_channel = vec!["organic".to_string()];
    let ccanon = |id: &[u8]| -> Option<usize> {
        if id.len() < 3 || id.len() > 9 || &id[..2] != b"c_" || id[2] == b'0' {
            return None;
        }
        let mut v = 0usize;
        for &c in &id[2..] {
            if !c.is_ascii_digit() {
                return None;
            }
            v = v * 10 + (c - b'0') as usize;
        }
        Some(v)
    };
    let mut camp_arr: Vec<u16> = vec![];
    if !crows.is_empty() && crows.iter().all(|r| ccanon(&r[0]).map(|v| v < 1_000_000).unwrap_or(false)) {
        let maxv = crows.iter().map(|r| ccanon(&r[0]).unwrap()).max().unwrap();
        camp_arr = vec![u16::MAX; maxv + 1];
        for (i, r) in crows.iter().enumerate() {
            camp_arr[ccanon(&r[0]).unwrap()] = (i + 1) as u16;
        }
    }
    for r in &crows {
        camp.insert(hid(&r[0]), camp_names.len() as u16);
        camp_names.push(String::from_utf8_lossy(&r[0]).into_owned());
        camp_channel.push(String::from_utf8_lossy(&r[1]).into_owned());
    }

    Lookups {
        win_start,
        win_end,
        product_cat,
        product_arr,
        unknown_cat,
        cat_names,
        camp,
        camp_arr,
        camp_names,
        camp_channel,
        link: LinkTable::new(0),
        comp_name: vec![],
        link_files: Files { maps: vec![] },
    }
}

/// Links -> union-find -> (id hash -> component, component -> ref of its name, the mapped files).
fn build_links(data: &Path) -> (LinkTable, Vec<u64>, Files) {
    // links
    let paths = list_csv(&data.join("links"));
    let mut maps = vec![];
    for p in &paths {
        if let Some(m) = map_file(p) {
            maps.push(m);
        }
    }
    assert!(maps.len() <= 256 && maps.iter().all(|m| m.len() < (1usize << 32)), "more than 256 link files or a file over 4 GB");
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
            rows::<2>(d, s, e, |ls, line, fl| {
                let (a0, a1) = (fl[0].0 as usize, fl[0].1 as usize);
                let (b0, b1) = (fl[1].0 as usize, fl[1].1 as usize);
                if a0 == a1 || b0 == b1 {
                    return;
                }
                let a = &line[a0..a1];
                let b = &line[b0..b1];
                out.push((hid(a), hid(b), mkref(fi, ls + a0, a1 - a0), mkref(fi, ls + b0, b1 - b0)));
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
    let mut lt = LinkTable::new(link.len());
    for (k, v) in link.iter() {
        lt.insert(*k, comp_of[*v as usize]);
    }
    (lt, comp_name, files)
}

const LINKED: u64 = 1 << 63;

static TM: [std::sync::atomic::AtomicU64; 8] = [const { std::sync::atomic::AtomicU64::new(0) }; 8];
#[inline(always)]
fn tm_add(i: usize, t: std::time::Instant) -> std::time::Instant {
    let now = std::time::Instant::now();
    TM[i].fetch_add((now - t).as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
    now
}

/// Flat open-addressing table: linked-ID hash -> component. 16-byte slots, linear probing,
/// so callers can prefetch the slot ahead of time and overlap the cache misses.
struct LinkTable {
    shift: u32,
    mask: usize,
    slots: Vec<[u64; 2]>,
    len: usize,
}

const LT_EMPTY: u64 = u64::MAX;

impl LinkTable {
    fn new(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        LinkTable { shift: 64 - cap.trailing_zeros(), mask: cap - 1, slots: vec![[LT_EMPTY, 0]; cap], len: 0 }
    }
    #[inline(always)]
    fn key(k: u64) -> u64 {
        if k == LT_EMPTY { LT_EMPTY - 1 } else { k }
    }
    #[inline(always)]
    fn slot(&self, k: u64) -> usize {
        (Self::key(k).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> self.shift) as usize
    }
    fn insert(&mut self, k: u64, v: u32) {
        let k = Self::key(k);
        let mut i = self.slot(k);
        loop {
            let s = &mut self.slots[i];
            if s[0] == LT_EMPTY {
                *s = [k, v as u64];
                self.len += 1;
                return;
            }
            if s[0] == k {
                s[1] = v as u64;
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn get(&self, k: u64) -> Option<u32> {
        let k = Self::key(k);
        let mut i = self.slot(k);
        loop {
            let s = unsafe { self.slots.get_unchecked(i) };
            if s[0] == k {
                return Some(s[1] as u32);
            }
            if s[0] == LT_EMPTY {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn prefetch(&self, k: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch(self.slots.as_ptr().add(self.slot(k)) as *const i8, _MM_HINT_T0);
        }
    }
    /// user-id hash -> person key
    #[inline(always)]
    fn person(&self, uidh: u64) -> u64 {
        match self.get(uidh) {
            Some(c) => LINKED | c as u64,
            None => uidh & !LINKED,
        }
    }
}

/// How far ahead to prefetch link-table slots.
const PF: usize = 16;

/// Index of a campaign id in campaigns.csv (1-based; 0 is organic), or None if unknown.
#[inline]
fn campaign_index(lk: &Lookups, cid: &[u8]) -> Option<u16> {
    if !lk.camp_arr.is_empty() {
        let n = cid.len();
        if n >= 3 && n <= 9 && cid[0] == b'c' && cid[1] == b'_' && cid[2] != b'0' {
            let mut v = 0usize;
            let mut ok = true;
            for &c in &cid[2..] {
                let d = c.wrapping_sub(b'0');
                ok &= d <= 9;
                v = v * 10 + d as usize;
            }
            if ok {
                return match lk.camp_arr.get(v) {
                    Some(&c) if c != u16::MAX => Some(c),
                    _ => None,
                };
            }
        }
        return None;
    }
    lk.camp.get(&hid(cid)).copied()
}

#[inline]
fn product_category(lk: &Lookups, pid: &[u8]) -> u16 {
    if !lk.product_arr.is_empty() {
        // exact "p<n>" form (no leading zero, digits only): direct index; anything else is unknown
        let n = pid.len();
        if n >= 2 && n <= 8 && pid[0] == b'p' && pid[1] != b'0' {
            let mut v = 0usize;
            let mut ok = true;
            for &c in &pid[1..] {
                let d = c.wrapping_sub(b'0');
                ok &= d <= 9;
                v = v * 10 + d as usize;
            }
            if ok {
                return match lk.product_arr.get(v) {
                    Some(&c) if c != u16::MAX => c,
                    _ => lk.unknown_cat,
                };
            }
        }
        return lk.unknown_cat;
    }
    *lk.product_cat.get(&hid(pid)).unwrap_or(&lk.unknown_cat)
}

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

/// Multiply-mix hasher for u64 keys that may be small sequential numbers (linked person ids).
#[derive(Default, Clone, Copy)]
struct MixHasher(u64);
impl std::hash::Hasher for MixHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!()
    }
    fn write_u64(&mut self, n: u64) {
        let r = (n as u128) * 0x9E37_79B9_7F4A_7C15u128;
        self.0 = (r as u64) ^ ((r >> 64) as u64);
    }
}
type MixBuild = std::hash::BuildHasherDefault<MixHasher>;

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
    lk.link.person(hid(uid))
}

// ---------------------------------------------------------------- events

#[derive(Clone, Copy)]
struct Ev {
    eid: u64,      // event_id hash (64 bits)
    person: u64,   // user_id hash (resolved to a person after links are ready)
    uref_cat: u64, // user_id ref (48 bits) | category (16 bits)
    eid2: u32,     // second, independent 32-bit hash of event_id: 96-bit identity
    ingest: i32,   // seconds since window start
    ts: i32,       // ms since window start (valid only if ok)
    pk: u32,       // price (24 bits) | kind << 24 (2 bits) | ok << 26
}

impl Ev {
    #[inline(always)]
    fn ok(&self) -> bool {
        (self.pk >> 26) & 1 == 1
    }
}

const _: () = assert!(std::mem::size_of::<Ev>() == 40);

/// A surviving event, routed to its person (24 bytes).
#[derive(Clone, Copy)]
struct Sv {
    person: u64,
    uref_cat: u64, // user_id ref (48 bits) | category (16 bits)
    ts: i32,
    pk: u32, // price (24 bits) | kind << 24
}

impl Sv {
    #[inline(always)]
    fn kind(&self) -> u8 {
        ((self.pk >> 24) & 3) as u8
    }
    #[inline(always)]
    fn price(&self) -> u32 {
        self.pk & 0xff_ffff
    }
    #[inline(always)]
    fn cat(&self) -> u16 {
        (self.uref_cat >> 48) as u16
    }
    #[inline(always)]
    fn uref(&self) -> u64 {
        self.uref_cat & 0xffff_ffff_ffff
    }
}

const _: () = assert!(std::mem::size_of::<Sv>() == 24);

#[derive(Clone, Copy)]
struct Ck {
    person: u64,
    ts: i64,
    cref: u64,
    camp: u16,
}

const NB1: usize = 64; // event-id buckets for the dedup shuffle
fn ebucket_of(h: u64) -> usize {
    ((h.wrapping_mul(0x9E3779B97F4A7C15)) >> (64 - NB1.trailing_zeros())) as usize
}

fn bucket_of(h: u64) -> usize {
    ((h.wrapping_mul(0x9E3779B97F4A7C15)) >> 55) as usize % NB
}

fn main() {
    let _ = month_table();
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
    for ms in [&ev_maps, &ck_maps] {
        assert!(ms.len() <= 256 && ms.iter().all(|m| m.len() < (1usize << 32)), "more than 256 files or a file over 4 GB");
    }
    let evf = Arc::new(Files { maps: ev_maps });
    let ckf = Arc::new(Files { maps: ck_maps });

    let exp: u32 = std::env::var("EXP").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    let mut lk = build_lookups(&data);
    eprintln!("lookups {:?}", t0.elapsed());
    let ws = lk.win_start;
    let we = lk.win_end;
    let mpre = MonthPre::new(ws, we);
    let mpre = &mpre;

    // ------------------------------------------------------------ events: parse
    let mut jobs = vec![];
    for (fi, m) in evf.maps.iter().enumerate() {
        for (s, e) in chunks(m, 16 << 20) {
            jobs.push((fi, s, e));
        }
    }

    // shared "seen" / "seen twice" bitmaps over event-id hashes: only ids seen twice need dedup
    let ev_bytes: usize = evf.maps.iter().map(|m| m.len()).sum();
    let bbits: usize = ((ev_bytes / 70).max(1024) * 16).next_power_of_two();
    let bmask = bbits - 1;
    let seen: Vec<std::sync::atomic::AtomicU64> = (0..bbits / 64).map(|_| std::sync::atomic::AtomicU64::new(0)).collect();
    let dupb: Vec<std::sync::atomic::AtomicU64> = (0..bbits / 64).map(|_| std::sync::atomic::AtomicU64::new(0)).collect();
    let mark = |bi: usize| {
        let w = bi >> 6;
        let bit = 1u64 << (bi & 63);
        unsafe {
            if seen.get_unchecked(w).fetch_or(bit, std::sync::atomic::Ordering::Relaxed) & bit != 0 {
                dupb.get_unchecked(w).fetch_or(bit, std::sync::atomic::Ordering::Relaxed);
            }
        }
    };
    let (links, parts): ((LinkTable, Vec<u64>, Files), Vec<Vec<Ev>>) = rayon::join(|| build_links(&data), || jobs
        .par_iter()
        .map(|&(fi, s, e)| {
            let d = &evf.maps[fi][..];
            let mut b: Vec<Ev> = Vec::with_capacity((e - s) / 70 + 64);
            let mut ring = [0usize; 16];
            let mut rn = 0usize;
            rows::<7>(d, s, e, |ls, line, fl| {
                debug_assert!(fl.iter().all(|&(a, b)| a <= b && b as usize <= line.len()));
                let f = |i: usize| unsafe { line.get_unchecked(fl[i].0 as usize..fl[i].1 as usize) };
                let eid = f(0);
                if eid.is_empty() {
                    return;
                }
                let ingest = match mpre.ingest(f(3)) {
                    Some(x) => x,
                    None => return, // never happens per rules
                };
                let uid = f(1);
                let mut ok = !uid.is_empty();
                let ts = mpre.ts(f(2));
                let ts = match ts {
                    Some(t) if t >= ws && t < we => t,
                    _ => {
                        ok = false;
                        ws
                    }
                };
                let kf = f(4);
                let kind = match kf.len() {
                    4 => {
                        let w = u32::from_le_bytes([kf[0], kf[1], kf[2], kf[3]]);
                        if w == u32::from_le_bytes(*b"view") {
                            0u8
                        } else if w == u32::from_le_bytes(*b"cart") {
                            1
                        } else {
                            ok = false;
                            255
                        }
                    }
                    8 if kf == b"purchase" => 2,
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
                    (hid(uid), if kind == 2 { product_category(&lk, pid) } else { 0 })
                } else {
                    (0, 0)
                };
                let (h, h2) = fh96(eid);
                let bi = (h as usize) & bmask;
                #[cfg(target_arch = "x86_64")]
                unsafe {
                    use std::arch::x86_64::{_mm_prefetch, _MM_HINT_ET0};
                    _mm_prefetch(seen.as_ptr().add(bi >> 6) as *const i8, _MM_HINT_ET0);
                }
                if rn >= 16 {
                    mark(ring[rn & 15]);
                }
                ring[rn & 15] = bi;
                rn += 1;
                b.push(Ev {
                    eid: h,
                    eid2: h2,
                    ingest: ((ingest - ws).div_euclid(1000)).clamp(i32::MIN as i64, i32::MAX as i64) as i32,
                    ts: (ts - ws) as i32,
                    person,
                    uref_cat: mkref(fi, ls + fl[1].0 as usize, uid.len()) | ((cat as u64) << 48),
                    pk: price | ((kind as u32 & 3) << 24) | ((ok as u32) << 26),
                });
            });
            for k in rn.saturating_sub(16)..rn {
                mark(ring[k & 15]);
            }
            b
        })
        .collect());
    lk.link = links.0;
    lk.comp_name = links.1;
    lk.link_files = links.2;
    eprintln!("events parsed (+links) {:?} links={} comps={}", t0.elapsed(), lk.link.len, lk.comp_name.len());
    if exp > 0 && exp < 10 { return; }

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
            let cap = (e - s) / 40 / NB * 5 / 4 + 16;
            let mut b: Vec<Vec<Ck>> = (0..NB).map(|_| Vec::with_capacity(cap)).collect();
            let mut tmp: Vec<Ck> = Vec::with_capacity((e - s) / 40 + 16);
            rows::<4>(d, s, e, |ls, line, fl| {
                let f = |i: usize| &line[fl[i].0 as usize..fl[i].1 as usize];
                let cid = f(0);
                if cid.is_empty() {
                    return;
                }
                let uid = f(1);
                if uid.is_empty() {
                    return;
                }
                let ts = match mpre.ts(f(3)) {
                    Some(t) if t >= ws - WEEK_MS && t < we => t,
                    _ => return,
                };
                let camp = match campaign_index(&lk, f(2)) {
                    Some(c) => c,
                    None => return,
                };
                tmp.push(Ck { person: hid(uid), ts, cref: mkref(fi, ls + fl[0].0 as usize, cid.len()), camp });
            });
            // resolve persons in a batch, prefetching link-table slots ahead
            let n = tmp.len();
            for i in 0..n {
                if i + PF < n {
                    lk.link.prefetch(tmp[i + PF].person);
                }
                let mut c = tmp[i];
                c.person = lk.link.person(c.person);
                b[bucket_of(c.person)].push(c);
            }
            b
        })
        .collect();
    eprintln!("clicks parsed {:?}", t0.elapsed());

    // ------------------------------------------------------------ route events to persons; collect dedup candidates
    // Every row whose id hash was seen twice is a candidate (real copies plus a few hash collisions).
    // Candidates are resolved by event id; surviving rows go straight to their person bucket.
    const NOLOC: u64 = u64::MAX;
    #[derive(Clone, Copy)]
    struct Cand {
        eid: u64,
        loc: u64, // person bucket << 40 | job << 20 | position, or NOLOC for a row that is dropped anyway
        eid2: u32,
        ingest: i32,
    }
    let dupv: Vec<u64> = dupb.into_iter().map(|a| a.into_inner()).collect();
    drop(seen);
    assert!(parts.len() < (1 << 10), "too many parse jobs");
    let routed: Vec<(Vec<Vec<Sv>>, Vec<Vec<Cand>>)> = parts
        .into_par_iter()
        .enumerate()
        .map(|(ji, v)| {
            let n = v.len();
            let ocap = n / NB * 5 / 4 + 16;
            let mut out: Vec<Vec<Sv>> = (0..NB).map(|_| Vec::with_capacity(ocap)).collect();
            let mut cands: Vec<Vec<Cand>> = (0..NB1).map(|_| Vec::new()).collect();
            for i in 0..n {
                if i + PF < n {
                    let e2 = unsafe { v.get_unchecked(i + PF) };
                    lk.link.prefetch(e2.person);
                    #[cfg(target_arch = "x86_64")]
                    unsafe {
                        use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
                        _mm_prefetch(dupv.as_ptr().add(((e2.eid as usize) & bmask) >> 6) as *const i8, _MM_HINT_T0);
                    }
                }
                let e = unsafe { v.get_unchecked(i) };
                let bi = (e.eid as usize) & bmask;
                let isdup = unsafe { *dupv.get_unchecked(bi >> 6) } >> (bi & 63) & 1 == 1;
                let good = e.ok() && e.ingest as i64 * 1000 - e.ts as i64 <= LATE_MS;
                let mut loc = NOLOC;
                if good {
                    let person = lk.link.person(e.person);
                    let pb = bucket_of(person);
                    let o = unsafe { out.get_unchecked_mut(pb) };
                    loc = ((pb as u64) << 40) | ((ji as u64) << 20) | o.len() as u64;
                    o.push(Sv { person, uref_cat: e.uref_cat, ts: e.ts, pk: e.pk & 0x03ff_ffff });
                }
                if isdup {
                    cands[ebucket_of(e.eid)].push(Cand { eid: e.eid, loc, eid2: e.eid2, ingest: e.ingest });
                }
            }
            debug_assert!(out.iter().all(|o| o.len() < (1 << 20)));
            (out, cands)
        })
        .collect();
    let mut pbk: Vec<Vec<Vec<Sv>>> = (0..NB).map(|_| Vec::new()).collect();
    let mut cbk2: Vec<Vec<Vec<Cand>>> = (0..NB1).map(|_| Vec::new()).collect();
    for (out, cands) in routed {
        for (bi, v) in out.into_iter().enumerate() {
            pbk[bi].push(v); // keep empty chunks: chunk index == job index
        }
        for (bi, v) in cands.into_iter().enumerate() {
            if !v.is_empty() {
                cbk2[bi].push(v);
            }
        }
    }
    // resolve candidates: per event id keep the copy with the smallest ingest; every other copy loses
    let mut losers: Vec<u64> = cbk2
        .into_par_iter()
        .flat_map_iter(|chunks| {
            let mut all: Vec<Cand> = Vec::with_capacity(chunks.iter().map(|c| c.len()).sum());
            for c in chunks {
                all.extend_from_slice(&c);
            }
            all.sort_unstable_by_key(|c| (c.eid, c.eid2, c.ingest));
            let mut lost = vec![];
            let mut i = 0;
            while i < all.len() {
                let mut j = i + 1;
                while j < all.len() && all[j].eid == all[i].eid && all[j].eid2 == all[i].eid2 {
                    if all[j].loc != NOLOC {
                        lost.push(all[j].loc);
                    }
                    j += 1;
                }
                i = j;
            }
            lost.into_iter()
        })
        .collect();
    losers.par_sort_unstable();
    eprintln!("dedup {:?} losers={}", t0.elapsed(), losers.len());
    let losers = &losers;
    let mut cbk: Vec<Vec<Vec<Ck>>> = (0..NB).map(|_| Vec::new()).collect();
    for job in cparts {
        for (bi, v) in job.into_iter().enumerate() {
            if !v.is_empty() {
                cbk[bi].push(v);
            }
        }
    }
    let mut work: Vec<(Vec<Vec<Sv>>, Vec<Vec<Ck>>, usize)> = pbk.into_iter().zip(cbk.into_iter()).enumerate().map(|(i, (e, c))| (e, c, i)).collect();
    // biggest buckets first: whales and shared-device people must not be the last job left
    work.sort_unstable_by_key(|(e, c, _)| std::cmp::Reverse(e.iter().map(|x| x.len()).sum::<usize>() + c.iter().map(|x| x.len()).sum::<usize>()));
    if std::env::var_os("BUCKET_STATS").is_some() {
        let mut sz: Vec<usize> = work.iter().map(|(e, _, _)| e.iter().map(|c| c.len()).sum::<usize>()).collect();
        let tot: usize = sz.iter().sum();
        sz.sort_unstable();
        eprintln!("buckets: total {} mean {} max {} top5 {:?}", tot, tot / NB, sz[NB - 1], &sz[NB - 5..]);
    }

    // ------------------------------------------------------------ sessions per person bucket
    let ncamp = lk.camp_names.len();
    let ndays = (((we - ws) + DAY_MS - 1) / DAY_MS) as usize;
    let ncat = lk.cat_names.len();

    // one accumulator per worker thread (not per bucket): each thread adds into its own table
    let nthreads = rayon::current_num_threads().max(1);
    let accs: Vec<std::sync::Mutex<(Vec<UserRow>, Vec<[i64; 4]>)>> =
        (0..nthreads).map(|_| std::sync::Mutex::new((Vec::new(), vec![[0i64; 4]; ncamp * ndays]))).collect();
    work.into_par_iter().with_max_len(1).for_each(|(mut evs, cks, pb)| {
            // drop the copies that lost dedup
            let lo = losers.partition_point(|&l| (l >> 40) < pb as u64);
            let hi = losers.partition_point(|&l| (l >> 40) <= pb as u64);
            if lo < hi {
                let mut last = usize::MAX;
                for &l in &losers[lo..hi] {
                    let j = ((l >> 20) & 0xfffff) as usize;
                    let p = (l & 0xfffff) as usize;
                    evs[j][p].pk |= 1 << 30;
                    last = j;
                    let _ = last;
                }
                let mut k = lo;
                while k < hi {
                    let j = ((losers[k] >> 20) & 0xfffff) as usize;
                    evs[j].retain(|x| x.pk & (1 << 30) == 0);
                    while k < hi && ((losers[k] >> 20) & 0xfffff) as usize == j {
                        k += 1;
                    }
                }
            }
            let ti = rayon::current_thread_index().unwrap_or(0) % nthreads;
            let mut guard = accs[ti].lock().unwrap();
            let (rows, camp_stats) = &mut *guard;
            // Group each person's events with a counting sort keyed by a small per-bucket person
            // number, then order each person's few events by time. Entry = ts | is-purchase | row ref.
            let mut tt = std::time::Instant::now();
            let nev: usize = evs.iter().map(|c| c.len()).sum();
            let mut pmap: std::collections::HashMap<u64, u32, MixBuild> =
                std::collections::HashMap::with_capacity_and_hasher(nev / 8 + 64, MixBuild::default());
            let mut pkeys: Vec<u64> = Vec::with_capacity(nev / 8 + 64);
            let mut pcount: Vec<u32> = Vec::with_capacity(nev / 8 + 64);
            let mut rowpid: Vec<u32> = Vec::with_capacity(nev);
            for (c, chunk) in evs.iter().enumerate() {
                assert!(c < (1 << 10) && chunk.len() < (1 << 21));
                for x in chunk.iter() {
                    let pid = *pmap.entry(x.person).or_insert_with(|| {
                        pkeys.push(x.person);
                        pcount.push(0);
                        (pkeys.len() - 1) as u32
                    });
                    pcount[pid as usize] += 1;
                    rowpid.push(pid);
                }
            }
            let np = pkeys.len();
            let mut poff: Vec<u32> = Vec::with_capacity(np + 1);
            let mut acc = 0u32;
            for &c in &pcount {
                poff.push(acc);
                acc += c;
            }
            poff.push(acc);
            let mut cur: Vec<u32> = poff[..np].to_vec();
            let mut arr: Vec<u64> = vec![0u64; nev];
            let mut ri = 0usize;
            for (c, chunk) in evs.iter().enumerate() {
                for (p, x) in chunk.iter().enumerate() {
                    let pid = rowpid[ri] as usize;
                    ri += 1;
                    let at = cur[pid] as usize;
                    cur[pid] += 1;
                    arr[at] = ((x.ts as u32 as u64) << 32) | (((x.kind() == 2) as u64) << 31) | ((c as u64) << 21) | p as u64;
                }
            }
            drop(rowpid);
            for pid in 0..np {
                let g = &mut arr[poff[pid] as usize..poff[pid + 1] as usize];
                if g.len() <= 24 {
                    // insertion sort: persons have a few dozen events on average
                    for x in 1..g.len() {
                        let v = g[x];
                        let mut y = x;
                        while y > 0 && g[y - 1] > v {
                            g[y] = g[y - 1];
                            y -= 1;
                        }
                        g[y] = v;
                    }
                } else {
                    g.sort_unstable();
                }
            }
            let row = |k: u64| -> &Sv {
                let r = k as u32 & 0x7fff_ffff;
                unsafe { evs.get_unchecked((r >> 21) as usize).get_unchecked((r & 0x1f_ffff) as usize) }
            };
            let kts = |k: u64| ((k >> 32) as u32) as i32;
            let kpurch = |k: u64| (k >> 31) & 1 == 1;
            tt = tm_add(4, tt);
            // clicks: same person numbering; clicks of people with no events are irrelevant
            let mut ccount: Vec<u32> = vec![0; np];
            let mut cpid: Vec<u32> = Vec::with_capacity(cks.iter().map(|c| c.len()).sum());
            for chunk in cks.iter() {
                for x in chunk.iter() {
                    let pid = match pmap.get(&x.person) {
                        Some(&p) => p,
                        None => u32::MAX,
                    };
                    if pid != u32::MAX {
                        ccount[pid as usize] += 1;
                    }
                    cpid.push(pid);
                }
            }
            let mut coff: Vec<u32> = Vec::with_capacity(np + 1);
            let mut acc = 0u32;
            for &c in &ccount {
                coff.push(acc);
                acc += c;
            }
            coff.push(acc);
            let mut ccur: Vec<u32> = coff[..np].to_vec();
            let mut ck: Vec<Ck> = vec![Ck { person: 0, ts: 0, cref: 0, camp: 0 }; acc as usize];
            let mut qi = 0usize;
            for chunk in cks.iter() {
                for x in chunk.iter() {
                    let pid = cpid[qi];
                    qi += 1;
                    if pid != u32::MAX {
                        let at = ccur[pid as usize] as usize;
                        ccur[pid as usize] += 1;
                        ck[at] = *x;
                    }
                }
            }
            for pid in 0..np {
                let g = &mut ck[coff[pid] as usize..coff[pid + 1] as usize];
                if g.len() > 1 {
                    g.sort_unstable_by_key(|c| c.ts);
                }
            }

            let mut catrev = vec![0i64; ncat];
            let mut touched: Vec<u16> = vec![];
            // campaigns with a click in the current 7-day window: counts + list of active ids
            let mut cnt: Vec<u32> = vec![0; ncamp];
            let mut active: Vec<u16> = Vec::with_capacity(64);
            let mut apos: Vec<u32> = vec![0; ncamp];

            tt = tm_add(5, tt);
            for pid in 0..np {
                let person = pkeys[pid];
                let clicks = &ck[coff[pid] as usize..coff[pid + 1] as usize];
                let events = &arr[poff[pid] as usize..poff[pid + 1] as usize];
                let linked = person & LINKED != 0;
                let name_ref = if linked { lk.comp_name[(person & !LINKED) as usize] } else { row(events[0]).uref() };

                // sliding window over clicks for assists
                for &c in &active {
                    cnt[c as usize] = 0;
                }
                active.clear();
                let (mut lo, mut hi) = (0usize, 0usize);

                // current user-day accumulation
                let mut cur_day: i32 = -1;
                let (mut u_s, mut u_e, mut u_r) = (0i64, 0i64, 0i64);

                let mut k = 0usize;
                while k < events.len() {
                    // one session: [k, m)
                    let start = kts(events[k]) as i64 + ws;
                    let mut m = k + 1;
                    let mut last = start;
                    while m < events.len() {
                        let t = kts(events[m]) as i64 + ws;
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
                    for &kk in &events[k..m] {
                        if kpurch(kk) {
                            let e = row(kk);
                            purch = true;
                            rev += e.price() as i64;
                            let c = e.cat() as usize;
                            if catrev[c] == 0 {
                                touched.push(e.cat());
                            }
                            catrev[c] += e.price() as i64;
                        }
                    }
                    u_s += 1;
                    u_e += (m - k) as i64;
                    u_r += rev;

                    // credit: latest click with ts <= start and start - ts <= 7d
                    while hi < clicks.len() && clicks[hi].ts <= start {
                        let c = clicks[hi].camp as usize;
                        if cnt[c] == 0 {
                            apos[c] = active.len() as u32;
                            active.push(c as u16);
                        }
                        cnt[c] += 1;
                        hi += 1;
                    }
                    while lo < hi && clicks[lo].ts < start - WEEK_MS {
                        let c = clicks[lo].camp as usize;
                        cnt[c] -= 1;
                        if cnt[c] == 0 {
                            // swap-remove from the active list
                            let p = apos[c] as usize;
                            let last = *active.last().unwrap();
                            active[p] = last;
                            apos[last as usize] = p as u32;
                            active.pop();
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
                    for &c in &active {
                        camp_stats[c as usize * ndays + d][3] += 1;
                    }
                    k = m;
                }
                if cur_day >= 0 {
                    rows.push(flush(name_ref, linked, cur_day, u_s, u_e, u_r, &mut catrev, &mut touched));
                }
            }
            tm_add(6, tt);
        });
    let results: Vec<(Vec<UserRow>, Vec<[i64; 4]>)> = accs.into_iter().map(|m| m.into_inner().unwrap()).collect();

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
    if std::env::var_os("TIMING").is_some() {
        let names = ["dd.concat", "dd.table", "dd.filter", "dd.resolve+scatter", "ss.keys+sort", "ss.clicks", "ss.loop", "-"];
        for (i, nm) in names.iter().enumerate() {
            eprintln!("  {} {:.3}s", nm, TM[i].load(std::sync::atomic::Ordering::Relaxed) as f64 / 1e9);
        }
    }
    // the OS reclaims everything at exit: skip freeing gigabytes of buffers one by one
    use std::io::Write;
    let _ = std::io::stderr().flush();
    std::process::exit(0);
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
    let props = || {
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
            .build()
    };

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
                names.push(unsafe { std::str::from_utf8_unchecked(b) });
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


#[cfg(test)]
mod tests {
    use super::*;

    fn reference(b: &[u8]) -> Option<i64> {
        if b.len() < 19 || b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
            return None;
        }
        let d2 = |i: usize| -> Option<i64> {
            let a = b[i].wrapping_sub(b'0');
            let c = b[i + 1].wrapping_sub(b'0');
            if a > 9 || c > 9 { None } else { Some((a * 10 + c) as i64) }
        };
        let y = d2(0)? * 100 + d2(2)?;
        let mo = d2(5)?;
        let d = d2(8)?;
        let h = d2(11)?;
        let mi = d2(14)?;
        let s = d2(17)?;
        if !(1..=12).contains(&mo) || h > 23 || mi > 59 || s > 59 || d < 1 {
            return None;
        }
        let dim = match mo { 1 | 3 | 5 | 7 | 8 | 10 | 12 => 31, 4 | 6 | 9 | 11 => 30, _ => if is_leap(y) { 29 } else { 28 } };
        if d > dim {
            return None;
        }
        Some(((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + s * 1000)
    }

    #[test]
    fn dt19_matches_reference() {
        let mut x: u64 = 0x1234_5678_9abc_def1;
        let mut rnd = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let alphabet = b"0123456789-T:Z +/aA\x00\xff9";
        let mut checked = 0u64;
        for _ in 0..4_000_000u64 {
            let y = 1890 + rnd() % 330;
            let mo = rnd() % 14;
            let d = rnd() % 33;
            let h = rnd() % 26;
            let mi = rnd() % 62;
            let s = rnd() % 62;
            let mut v = format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, mo, d, h, mi, s).into_bytes();
            // corrupt some bytes sometimes
            let k = rnd() % 4;
            for _ in 0..k {
                let p = (rnd() % 19) as usize;
                v[p] = alphabet[(rnd() % alphabet.len() as u64) as usize];
            }
            assert_eq!(parse_dt19(&v), reference(&v), "input {:?}", String::from_utf8_lossy(&v));
            checked += 1;
        }
        // a few fixed cases
        for c in ["2028-02-29T00:00:00Z", "2027-02-29T00:00:00Z", "2026-13-01T00:00:00Z", "2026-10-03T24:00:00Z", "2026-10-03T23:59:60Z", "2000-02-29T12:00:00Z", "1900-02-29T12:00:00Z"] {
            assert_eq!(parse_dt19(c.as_bytes()), reference(c.as_bytes()), "{c}");
        }
        assert!(checked > 0);
    }

    #[test]
    fn month_prefix_matches_parse_ts() {
        let mut x: u64 = 0x5555_1234_abcd_0007;
        let mut rnd = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let alphabet = b"0123456789-T:Z +/aA\x00\xff9";
        // windows including a leap February and a year end
        for (ws, we) in [("2028-02-20T00:00:00Z", "2028-03-05T00:00:00Z"), ("2026-12-25T00:00:00Z", "2027-01-08T00:00:00Z"), ("2026-09-06T00:00:00Z", "2026-09-20T00:00:00Z")] {
            let ws = parse_ts(ws.as_bytes()).unwrap();
            let we = parse_ts(we.as_bytes()).unwrap();
            let mp = MonthPre::new(ws, we);
            assert!(mp.n >= 3);
            for _ in 0..1_500_000u64 {
                let t = ws + (rnd() % ((we - ws) as u64 + 8 * DAY_MS as u64)) as i64 - 4 * DAY_MS;
                let (y, mo, d) = civil_from_days(t.div_euclid(DAY_MS));
                let sod = t.rem_euclid(DAY_MS) / 1000;
                let (mut h, mut mi, mut se) = (sod / 3600, sod / 60 % 60, sod % 60);
                let (mut mo, mut d) = (mo, d);
                match rnd() % 8 {
                    0 => d = (rnd() % 33) as i64,
                    1 => h = (rnd() % 26) as i64,
                    2 => mi = (rnd() % 62) as i64,
                    3 => se = (rnd() % 62) as i64,
                    4 => mo = (rnd() % 14) as i64,
                    _ => {}
                }
                let mut v = match rnd() % 3 {
                    0 => format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, mo, d, h, mi, se),
                    1 => format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}{}{:02}:{:02}", y, mo, d, h, mi, se, if rnd() % 2 == 0 { '+' } else { '-' }, rnd() % 25, rnd() % 61),
                    _ => format!("{}", t),
                }
                .into_bytes();
                if rnd() % 3 == 0 {
                    let p = (rnd() % v.len() as u64) as usize;
                    v[p] = alphabet[(rnd() % alphabet.len() as u64) as usize];
                }
                assert_eq!(mp.ts(&v), parse_ts(&v), "input {:?}", String::from_utf8_lossy(&v));
                if v.len() == 20 && parse_ts(&v).is_some() {
                    assert_eq!(mp.ingest(&v), parse_ts(&v), "ingest {:?}", String::from_utf8_lossy(&v));
                }
            }
        }
    }

    #[test]
    fn tokenizer_matches_reference() {
        let mut x: u64 = 0xdead_beef_1234_5678;
        let mut rnd = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let pieces: [&[u8]; 14] = [b"a", b"bc", b"\"q\"", b"\"\"", b"\"", b",", b",", b",", b"\n", b"x\"y", b"\r", b" ", b"123", b"\"z"];
        for round in 0..3000 {
            let mut buf = Vec::new();
            let len = 1 + (rnd() % 400) as usize;
            while buf.len() < len {
                if rnd() % 5 == 0 {
                    // a well-formed 7-field line
                    for i in 0..7 {
                        if i > 0 { buf.push(b','); }
                        match rnd() % 4 { 0 => buf.extend_from_slice(b"\"v\""), 1 => {}, 2 => buf.extend_from_slice(b"abc"), _ => buf.extend_from_slice(b"\"\"") }
                    }
                    buf.push(b'\n');
                } else {
                    buf.extend_from_slice(pieces[(rnd() % pieces.len() as u64) as usize]);
                }
            }
            for &cut in &[buf.len(), buf.len().saturating_sub(1)] {
                let mut a = vec![];
                for_rows::<7>(&buf, 0, cut, |ls, line, fl| a.push((ls, line.to_vec(), *fl)));
                let mut b = vec![];
                unsafe { for_rows_avx2b::<7>(&buf, 0, cut, &mut |ls, line: &[u8], fl: &[(u32, u32); 7]| b.push((ls, line.to_vec(), *fl))) };
                assert_eq!(a, b, "round {round}: {:?}", String::from_utf8_lossy(&buf[..cut]));
                let mut a2 = vec![];
                for_rows::<2>(&buf, 0, cut, |ls, line, fl| a2.push((ls, line.to_vec(), *fl)));
                let mut b2 = vec![];
                unsafe { for_rows_avx2b::<2>(&buf, 0, cut, &mut |ls, line: &[u8], fl: &[(u32, u32); 2]| b2.push((ls, line.to_vec(), *fl))) };
                assert_eq!(a2, b2, "round {round} N=2");
            }
        }
    }

    #[test]
    fn hash_distinguishes_short_ids() {
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        for i in 0..2_000_000u64 {
            let s = format!("u{}", i);
            assert!(seen.insert(hid(s.as_bytes())), "collision at {s}");
        }
        assert_ne!(hid(b""), hid(b"\0"));
        assert_ne!(hid(b"ab"), hid(b"ba"));
        assert_ne!(hid(b"abc"), hid(b"acb"));
    }
}

#[cfg(test)]
mod tests_f3 {
    use super::*;
    #[test]
    fn f3_matches_scalar() {
        let mut x: u64 = 0x9999_1234_abcd_0001;
        for _ in 0..2_000_000 {
            x ^= x << 13; x ^= x >> 7; x ^= x << 17;
            let mut s = format!("{:013}", x % 10_000_000_000_000);
            if x % 7 == 0 { let p = (x % 13) as usize; let mut v = s.into_bytes(); v[p] = b"a/:-. "[(x % 6) as usize]; s = String::from_utf8(v).unwrap(); }
            let b = s.as_bytes();
            let mut v: Option<i64> = Some(0);
            for &c in b { let d = c.wrapping_sub(b'0'); v = if d > 9 { None } else { v.and_then(|v| v.checked_mul(10)).and_then(|v| v.checked_add(d as i64)) }; if v.is_none() { break; } }
            assert_eq!(parse_ts(b), v, "{s}");
        }
    }
}

#[cfg(test)]
mod tests_ref {
    use super::*;
    #[test]
    fn long_values_roundtrip() {
        let dir = std::env::temp_dir().join("sessions_ref_test");
        std::fs::create_dir_all(&dir).unwrap();
        let long: String = std::iter::repeat('u').take(300).collect();
        let text = format!("h\nab,{long},\"{long}\"\nxyz,short\n");
        let p = dir.join("t.csv");
        std::fs::write(&p, &text).unwrap();
        let m = map_file(&p).unwrap();
        let files = Files { maps: vec![m] };
        let off = text.find(&long).unwrap();
        assert_eq!(files.bytes(mkref(0, off, long.len())), long.as_bytes());
        let off2 = text.rfind(&long).unwrap();
        assert_eq!(files.bytes(mkref(0, off2, long.len())), long.as_bytes());
        let off3 = text.find("short").unwrap();
        assert_eq!(files.bytes(mkref(0, off3, 5)), b"short");
    }
}
