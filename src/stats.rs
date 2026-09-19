//! Event collection, matching and the numbers behind every table.
//!
//! Block level follows the Go reference tool: the first source to deliver a block wins, every
//! other source's delay behind the winner is a sample, and a block counts once every source
//! (or `--min-sources`) delivered it. Transaction level keys on the transaction hash.

use crate::event::{Event, Payload};
use serde::Serialize;
use std::collections::HashMap;
use std::time::{Duration, Instant};

pub fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

/// Signed difference `a - b` in milliseconds.
pub fn signed_ms(a: Instant, b: Instant) -> f64 {
    if a >= b { ms(a - b) } else { -ms(b - a) }
}

/// Nearest-rank percentile on sorted samples (the rule the Go reference tool uses).
pub fn nearest_rank(sorted: &[f64], permille: usize) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let n = sorted.len();
    let rank = (permille * n).div_ceil(1000).clamp(1, n);
    sorted[rank - 1]
}

pub fn median(sorted: &[f64]) -> f64 {
    let n = sorted.len();
    if n == 0 {
        0.0
    } else if n % 2 == 1 {
        sorted[n / 2]
    } else {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
    }
}

fn sorted(mut v: Vec<f64>) -> Vec<f64> {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    v
}

fn pct(part: usize, whole: usize) -> f64 {
    if whole == 0 { 0.0 } else { part as f64 * 100.0 / whole as f64 }
}

#[derive(Serialize, Clone, Default, Debug)]
pub struct Percentiles {
    pub n: usize,
    pub p10: f64,
    pub p50: f64,
    pub p75: f64,
    pub p90: f64,
    pub p95: f64,
    pub p99: f64,
    pub p999: f64,
    pub max: f64,
}

impl Percentiles {
    pub fn of(samples: Vec<f64>) -> Percentiles {
        let s = sorted(samples);
        Percentiles {
            n: s.len(),
            p10: nearest_rank(&s, 100),
            p50: nearest_rank(&s, 500),
            p75: nearest_rank(&s, 750),
            p90: nearest_rank(&s, 900),
            p95: nearest_rank(&s, 950),
            p99: nearest_rank(&s, 990),
            p999: nearest_rank(&s, 999),
            max: s.last().copied().unwrap_or(0.0),
        }
    }
}

#[derive(Serialize, Clone, Debug)]
pub struct SourceInfo {
    pub name: String,
    /// `nitro`, `eira` or `wsjson`.
    pub kind: String,
    pub target: String,
    pub level: String,
}

pub struct BlockRec {
    pub seq: Option<u64>,
    pub hash: Option<String>,
    pub tx_count: Option<u32>,
    pub arrivals: Vec<Option<Instant>>,
    pub first: Instant,
    pub first_src: usize,
    pub seen: usize,
    /// Arrival of the delivery that made the block complete.
    pub completed_at: Option<Instant>,
}

pub struct TxRec {
    pub arrivals: Vec<Option<Instant>>,
    pub first: Instant,
    pub seen: usize,
    pub block_key: Option<String>,
    pub block_from_nitro: bool,
    pub tx_count: Option<u32>,
    pub synthetic_swap: bool,
}

pub struct Collector {
    pub sources: Vec<SourceInfo>,
    pub nitro_present: bool,
    /// Index of the reference source for pairwise tables (the Eira source when present).
    pub reference: usize,
    pub min_sources: usize,
    pub warmup: Duration,
    pub start: Instant,
    pub blocks: HashMap<String, BlockRec>,
    pub txs: HashMap<[u8; 32], TxRec>,
    pub connected: Vec<bool>,
    pub ever_connected: Vec<bool>,
    pub reconnects: Vec<u32>,
    pub details: Vec<String>,
    pub tx_events: Vec<u64>,
    pub block_events: Vec<u64>,
    /// WebSocket bytes received per source (raw feeds only).
    pub wire_bytes: Vec<u64>,
    /// Signed-transaction bytes delivered per source (as reported by the source).
    pub raw_tx_bytes: Vec<u64>,
    /// Intervals during which every source was connected.
    pub all_up: Vec<(Instant, Option<Instant>)>,
}

/// What `on_event` reports back to the caller for printing.
pub enum Notice {
    Connected { source: usize, detail: String, reconnect: bool },
    Disconnected { source: usize, reason: String },
    /// A block just completed: one line in the Go tool's per-block format.
    BlockLine(String),
}

impl Collector {
    pub fn new(sources: Vec<SourceInfo>, reference: usize, min_sources: usize, warmup: Duration) -> Collector {
        let n = sources.len();
        Collector {
            nitro_present: sources.iter().any(|s| s.kind == "nitro"),
            sources,
            reference,
            min_sources: min_sources.clamp(1, n.max(1)),
            warmup,
            start: Instant::now(),
            blocks: HashMap::new(),
            txs: HashMap::new(),
            connected: vec![false; n],
            ever_connected: vec![false; n],
            reconnects: vec![0; n],
            details: vec![String::new(); n],
            tx_events: vec![0; n],
            block_events: vec![0; n],
            wire_bytes: vec![0; n],
            raw_tx_bytes: vec![0; n],
            all_up: Vec::new(),
        }
    }

    pub fn on_event(&mut self, ev: Event) -> Option<Notice> {
        let s = ev.source;
        let n = self.sources.len();
        match ev.payload {
            Payload::Connected { detail } => {
                if self.connected[s] {
                    self.details[s] = detail;
                    return None;
                }
                let reconnect = self.ever_connected[s];
                if reconnect {
                    self.reconnects[s] += 1;
                }
                self.connected[s] = true;
                self.ever_connected[s] = true;
                self.details[s] = detail.clone();
                if self.connected.iter().all(|c| *c) && self.all_up.last().is_none_or(|(_, end)| end.is_some()) {
                    self.all_up.push((ev.t, None));
                }
                Some(Notice::Connected { source: s, detail, reconnect })
            }
            Payload::Disconnected { reason } => {
                if !self.connected[s] {
                    return None;
                }
                self.connected[s] = false;
                if let Some(last) = self.all_up.last_mut()
                    && last.1.is_none()
                {
                    last.1 = Some(ev.t);
                }
                Some(Notice::Disconnected { source: s, reason })
            }
            Payload::Block { key, seq, hash, tx_count, wire_len } => {
                self.block_events[s] += 1;
                self.wire_bytes[s] += wire_len;
                let rec = self.blocks.entry(key.clone()).or_insert_with(|| BlockRec {
                    seq,
                    hash: None,
                    tx_count: None,
                    arrivals: vec![None; n],
                    first: ev.t,
                    first_src: s,
                    seen: 0,
                    completed_at: None,
                });
                if rec.seq.is_none() {
                    rec.seq = seq;
                }
                if rec.hash.is_none() {
                    rec.hash = hash;
                }
                if rec.tx_count.is_none() {
                    rec.tx_count = tx_count;
                }
                if rec.arrivals[s].is_some() {
                    return None; // duplicate delivery (backlog replay after a reconnect)
                }
                rec.arrivals[s] = Some(ev.t);
                rec.seen += 1;
                if ev.t < rec.first {
                    rec.first = ev.t;
                    rec.first_src = s;
                }
                if rec.seen == self.min_sources {
                    rec.completed_at = Some(rec.arrivals.iter().flatten().copied().max().unwrap_or(ev.t));
                    let mut cols = vec!["[block]".to_string(), rec.seq.map_or("-".into(), |q| q.to_string()), rec.hash.clone().unwrap_or(key)];
                    for a in &rec.arrivals {
                        cols.push(a.map_or("NA".into(), |a| format!("{:.2}ms", ms(a - rec.first))));
                    }
                    cols.push(self.sources[rec.first_src].name.clone());
                    return Some(Notice::BlockLine(cols.join("\t")));
                }
                None
            }
            Payload::Tx { hash, block_key, tx_count, synthetic_swap, raw_len } => {
                self.tx_events[s] += 1;
                self.raw_tx_bytes[s] += raw_len as u64;
                let from_nitro = self.sources[s].kind == "nitro";
                let rec = self.txs.entry(hash).or_insert_with(|| TxRec {
                    arrivals: vec![None; n],
                    first: ev.t,
                    seen: 0,
                    block_key: None,
                    block_from_nitro: false,
                    tx_count: None,
                    synthetic_swap: false,
                });
                if block_key.is_some() && (rec.block_key.is_none() || (from_nitro && !rec.block_from_nitro)) {
                    rec.block_key = block_key;
                    rec.block_from_nitro = from_nitro;
                }
                if from_nitro && tx_count.is_some() {
                    rec.tx_count = tx_count;
                }
                rec.synthetic_swap |= synthetic_swap;
                if rec.arrivals[s].is_some() {
                    return None;
                }
                rec.arrivals[s] = Some(ev.t);
                rec.seen += 1;
                if ev.t < rec.first {
                    rec.first = ev.t;
                }
                None
            }
        }
    }

    /// Is `t` inside a stretch where every source was connected, past the warm-up?
    pub fn in_window(&self, t: Instant) -> bool {
        self.all_up.iter().any(|(a, b)| t >= *a + self.warmup && b.is_none_or(|b| t < b))
    }

    pub fn window_seconds(&self, now: Instant) -> f64 {
        self.all_up
            .iter()
            .map(|(a, b)| {
                let end = b.unwrap_or(now);
                let begin = *a + self.warmup;
                if end > begin { (end - begin).as_secs_f64() } else { 0.0 }
            })
            .sum::<f64>()
            .max(0.0)
    }

    /// Block-level summary over completed blocks whose completion falls in `(since, until]`
    /// (`None` = the whole run), restricted to the all-connected window.
    pub fn block_summary(&self, label: &str, since: Option<Instant>, until: Instant) -> BlockSummary {
        let n = self.sources.len();
        let mut wins = vec![0usize; n];
        let mut samples: Vec<Vec<f64>> = vec![Vec::new(); n];
        let mut completed = 0usize;
        for rec in self.blocks.values() {
            let Some(done) = rec.completed_at else { continue };
            if done > until || since.is_some_and(|s| done <= s) || !self.in_window(rec.first) {
                continue;
            }
            completed += 1;
            wins[rec.first_src] += 1;
            for (i, a) in rec.arrivals.iter().enumerate() {
                if let Some(a) = a {
                    samples[i].push(ms(*a - rec.first));
                }
            }
        }
        let per_source = (0..n)
            .map(|i| BlockSourceStats {
                name: self.sources[i].name.clone(),
                wins: wins[i],
                win_pct: pct(wins[i], completed),
                delay: Percentiles::of(std::mem::take(&mut samples[i])),
            })
            .collect();
        BlockSummary { label: label.to_string(), completed, per_source }
    }

    pub fn tx_summary(&self) -> TxSummary {
        let n = self.sources.len();
        let mut total = 0usize;
        let mut matched = 0usize;
        let mut seen = vec![0usize; n];
        let mut first = vec![0usize; n];
        let mut first_1ms = vec![0usize; n];
        let mut lags: Vec<Vec<f64>> = vec![Vec::new(); n];
        let mut swaps = 0usize;
        for rec in self.txs.values() {
            if !self.in_window(rec.first) {
                continue;
            }
            total += 1;
            for (i, a) in rec.arrivals.iter().enumerate() {
                if a.is_some() {
                    seen[i] += 1;
                }
            }
            if rec.seen < self.min_sources {
                continue;
            }
            matched += 1;
            swaps += rec.synthetic_swap as usize;
            for (i, a) in rec.arrivals.iter().enumerate() {
                let Some(a) = a else { continue };
                lags[i].push(ms(*a - rec.first));
                if *a == rec.first {
                    first[i] += 1;
                }
                let others = rec.arrivals.iter().enumerate().filter(|(j, o)| *j != i && o.is_some()).map(|(_, o)| o.unwrap()).min();
                if let Some(o) = others
                    && *a + Duration::from_millis(1) <= o
                {
                    first_1ms[i] += 1;
                }
            }
        }
        let per_source = (0..n)
            .map(|i| {
                let s = sorted(std::mem::take(&mut lags[i]));
                TxSourceStats {
                    name: self.sources[i].name.clone(),
                    seen: seen[i],
                    coverage_pct: pct(seen[i], total),
                    first_pct: pct(first[i], matched),
                    first_by_1ms_pct: pct(first_1ms[i], matched),
                    lag_p50: nearest_rank(&s, 500),
                    lag_p90: nearest_rank(&s, 900),
                    lag_p95: nearest_rank(&s, 950),
                    lag_p99: nearest_rank(&s, 990),
                    lag_max: s.last().copied().unwrap_or(0.0),
                }
            })
            .collect();
        TxSummary { total, matched, synthetic_swaps: swaps, per_source }
    }

    /// Reference source against `other`, over transactions both delivered.
    pub fn pair_summary(&self, other: usize, interval: Duration) -> PairSummary {
        let r = self.reference;
        // Per-block transaction counts from the reference source, used when no Nitro source
        // carried the count.
        let mut ref_counts: HashMap<&str, u32> = HashMap::new();
        if !self.nitro_present {
            for rec in self.txs.values() {
                if rec.arrivals[r].is_some()
                    && let Some(k) = rec.block_key.as_deref()
                {
                    *ref_counts.entry(k).or_default() += 1;
                }
            }
        }
        struct Row<'a> {
            diff: f64,
            first: Instant,
            block: Option<&'a str>,
            size: Option<u32>,
        }
        let mut rows: Vec<Row> = Vec::new();
        for rec in self.txs.values() {
            let (Some(tr), Some(to)) = (rec.arrivals[r], rec.arrivals[other]) else { continue };
            if !self.in_window(rec.first) {
                continue;
            }
            let size = if self.nitro_present {
                rec.tx_count.filter(|_| rec.block_from_nitro)
            } else {
                rec.block_key.as_deref().and_then(|k| ref_counts.get(k).copied())
            };
            rows.push(Row { diff: signed_ms(to, tr), first: rec.first, block: rec.block_key.as_deref(), size });
        }
        let n = rows.len();
        let diffs = sorted(rows.iter().map(|x| x.diff).collect());
        let count = |f: &dyn Fn(f64) -> bool| rows.iter().filter(|x| f(x.diff)).count();

        // per interval (from the run start)
        let mut by_interval: HashMap<u64, Vec<f64>> = HashMap::new();
        for x in &rows {
            let idx = (x.first.saturating_duration_since(self.start).as_secs_f64() / interval.as_secs_f64()) as u64;
            by_interval.entry(idx).or_default().push(x.diff);
        }
        let mut intervals: Vec<IntervalStats> = by_interval
            .into_iter()
            .map(|(idx, v)| {
                let s = sorted(v);
                IntervalStats { index: idx as usize + 1, start_s: idx as f64 * interval.as_secs_f64(), n: s.len(), median_ms: median(&s), ref_first_pct: pct(s.iter().filter(|d| **d > 0.0).count(), s.len()) }
            })
            .collect();
        intervals.sort_by_key(|i| i.index);

        // block-size buckets
        const BUCKETS: [(&str, u32, u32); 4] = [("1-5", 1, 5), ("6-20", 6, 20), ("21-50", 21, 50), (">50", 51, u32::MAX)];
        let mut bucket_rows: Vec<Vec<&Row>> = vec![Vec::new(); BUCKETS.len()];
        for x in &rows {
            if let Some(sz) = x.size
                && let Some(b) = BUCKETS.iter().position(|(_, lo, hi)| sz >= *lo && sz <= *hi)
            {
                bucket_rows[b].push(x);
            }
        }
        let sized_txs: usize = bucket_rows.iter().map(|b| b.len()).sum();
        let blocks_per_bucket: Vec<usize> = bucket_rows
            .iter()
            .map(|b| {
                let mut keys: Vec<&str> = b.iter().filter_map(|x| x.block).collect();
                keys.sort_unstable();
                keys.dedup();
                keys.len()
            })
            .collect();
        let sized_blocks: usize = blocks_per_bucket.iter().sum();
        let buckets = BUCKETS
            .iter()
            .enumerate()
            .map(|(i, (label, _, _))| {
                let s = sorted(bucket_rows[i].iter().map(|x| x.diff).collect());
                BucketStats {
                    label: format!("{label} txs"),
                    blocks: blocks_per_bucket[i],
                    blocks_pct: pct(blocks_per_bucket[i], sized_blocks),
                    txs: s.len(),
                    txs_pct: pct(s.len(), sized_txs),
                    median_ms: median(&s),
                    p10_ms: nearest_rank(&s, 100),
                    p90_ms: nearest_rank(&s, 900),
                    ref_first_pct: pct(s.iter().filter(|d| **d > 0.0).count(), s.len()),
                }
            })
            .collect();

        PairSummary {
            reference: self.sources[r].name.clone(),
            other: self.sources[other].name.clone(),
            n,
            median_ms: median(&diffs),
            p10_ms: nearest_rank(&diffs, 100),
            p90_ms: nearest_rank(&diffs, 900),
            ref_first_pct: pct(count(&|d| d > 0.0), n),
            within_1ms_pct: pct(count(&|d| d.abs() <= 1.0), n),
            ref_earlier_5ms_pct: pct(count(&|d| d > 5.0), n),
            other_earlier_5ms_pct: pct(count(&|d| d < -5.0), n),
            intervals,
            size_source: if self.nitro_present { "nitro frame".into() } else { format!("{} per-block count", self.sources[r].name) },
            buckets,
        }
    }
}

#[derive(Serialize, Clone, Debug)]
pub struct BlockSourceStats {
    pub name: String,
    pub wins: usize,
    pub win_pct: f64,
    pub delay: Percentiles,
}

#[derive(Serialize, Clone, Debug)]
pub struct BlockSummary {
    pub label: String,
    pub completed: usize,
    pub per_source: Vec<BlockSourceStats>,
}

#[derive(Serialize, Clone, Debug)]
pub struct TxSourceStats {
    pub name: String,
    pub seen: usize,
    pub coverage_pct: f64,
    pub first_pct: f64,
    pub first_by_1ms_pct: f64,
    pub lag_p50: f64,
    pub lag_p90: f64,
    pub lag_p95: f64,
    pub lag_p99: f64,
    pub lag_max: f64,
}

#[derive(Serialize, Clone, Debug)]
pub struct TxSummary {
    /// Distinct transactions seen by any source inside the window.
    pub total: usize,
    /// Seen by every source (or `--min-sources`).
    pub matched: usize,
    pub synthetic_swaps: usize,
    pub per_source: Vec<TxSourceStats>,
}

#[derive(Serialize, Clone, Debug)]
pub struct IntervalStats {
    pub index: usize,
    pub start_s: f64,
    pub n: usize,
    pub median_ms: f64,
    pub ref_first_pct: f64,
}

#[derive(Serialize, Clone, Debug)]
pub struct BucketStats {
    pub label: String,
    pub blocks: usize,
    pub blocks_pct: f64,
    pub txs: usize,
    pub txs_pct: f64,
    /// Percentiles of (other - reference) inside the bucket; positive = reference earlier.
    pub median_ms: f64,
    pub p10_ms: f64,
    pub p90_ms: f64,
    pub ref_first_pct: f64,
}

#[derive(Serialize, Clone, Debug)]
pub struct PairSummary {
    pub reference: String,
    pub other: String,
    pub n: usize,
    /// Median of (other - reference); positive = reference earlier.
    pub median_ms: f64,
    pub p10_ms: f64,
    pub p90_ms: f64,
    pub ref_first_pct: f64,
    pub within_1ms_pct: f64,
    pub ref_earlier_5ms_pct: f64,
    pub other_earlier_5ms_pct: f64,
    pub intervals: Vec<IntervalStats>,
    pub size_source: String,
    pub buckets: Vec<BucketStats>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_rank_matches_the_reference_rule() {
        let s: Vec<f64> = (1..=10).map(|x| x as f64).collect();
        assert_eq!(nearest_rank(&s, 500), 5.0);
        assert_eq!(nearest_rank(&s, 900), 9.0);
        assert_eq!(nearest_rank(&s, 999), 10.0);
        assert_eq!(nearest_rank(&s, 100), 1.0);
        assert_eq!(nearest_rank(&[], 500), 0.0);
        assert_eq!(median(&s), 5.5);
    }

    fn collector() -> Collector {
        let src = |name: &str, kind: &str| SourceInfo { name: name.into(), kind: kind.into(), target: String::new(), level: String::new() };
        let mut c = Collector::new(vec![src("official", "nitro"), src("eira", "eira")], 1, 2, Duration::ZERO);
        let t0 = c.start;
        c.on_event(Event { source: 0, t: t0, payload: Payload::Connected { detail: String::new() } });
        c.on_event(Event { source: 1, t: t0, payload: Payload::Connected { detail: String::new() } });
        c
    }

    #[test]
    fn blocks_and_txs_are_matched_across_sources() {
        let mut c = collector();
        let t0 = c.start + Duration::from_millis(10);
        let key = "seq:7".to_string();
        let h = [1u8; 32];
        // eira delivers the tx 3 ms before the nitro frame arrives
        c.on_event(Event { source: 1, t: t0, payload: Payload::Block { key: key.clone(), seq: Some(7), hash: None, tx_count: None, wire_len: 0 } });
        c.on_event(Event { source: 1, t: t0, payload: Payload::Tx { hash: h, block_key: Some(key.clone()), tx_count: None, synthetic_swap: true, raw_len: 100 } });
        let t1 = t0 + Duration::from_millis(3);
        let line = c.on_event(Event { source: 0, t: t1, payload: Payload::Block { key: key.clone(), seq: Some(7), hash: Some("0xabc".into()), tx_count: Some(3), wire_len: 512 } });
        assert!(matches!(line, Some(Notice::BlockLine(l)) if l.contains("0xabc") && l.ends_with("eira")));
        c.on_event(Event { source: 0, t: t1, payload: Payload::Tx { hash: h, block_key: Some(key.clone()), tx_count: Some(3), synthetic_swap: false, raw_len: 100 } });

        let now = t1 + Duration::from_secs(1);
        let b = c.block_summary("run", None, now);
        assert_eq!(b.completed, 1);
        assert_eq!(b.per_source[1].wins, 1);
        assert!((b.per_source[0].delay.p50 - 3.0).abs() < 1e-6);

        let t = c.tx_summary();
        assert_eq!((t.total, t.matched, t.synthetic_swaps), (1, 1, 1));
        assert_eq!(t.per_source[1].first_pct, 100.0);
        assert_eq!(t.per_source[1].first_by_1ms_pct, 100.0);
        assert!((t.per_source[0].lag_p50 - 3.0).abs() < 1e-6);

        let p = c.pair_summary(0, Duration::from_secs(300));
        assert_eq!(p.n, 1);
        assert!((p.median_ms - 3.0).abs() < 1e-6);
        assert_eq!(p.ref_first_pct, 100.0);
        assert_eq!(p.buckets[0].txs, 1); // 3 txs in the block -> bucket 1-5
        assert_eq!(p.buckets[0].blocks, 1);
        assert_eq!(p.intervals.len(), 1);
    }

    #[test]
    fn window_excludes_events_while_a_source_is_down() {
        let mut c = collector();
        let t = c.start + Duration::from_secs(1);
        assert!(c.in_window(t));
        c.on_event(Event { source: 0, t, payload: Payload::Disconnected { reason: "x".into() } });
        assert!(!c.in_window(t + Duration::from_secs(1)));
        let back = t + Duration::from_secs(2);
        let n = c.on_event(Event { source: 0, t: back, payload: Payload::Connected { detail: String::new() } });
        assert!(matches!(n, Some(Notice::Connected { reconnect: true, .. })));
        assert_eq!(c.reconnects[0], 1);
        assert!(c.in_window(back + Duration::from_secs(1)));
        assert!((c.window_seconds(back + Duration::from_secs(1)) - 2.0).abs() < 1e-6);
    }
}
