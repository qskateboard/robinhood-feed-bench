//! The final report: plain-text tables sized for a chat screenshot, and the same data as JSON.

use crate::stats::{BlockSummary, PairSummary, SourceInfo, TxSummary};
use serde::Serialize;
use std::fmt::Write;
use std::time::{SystemTime, UNIX_EPOCH};

pub const FAIRNESS_NOTE: &str = "Timestamps are taken at message arrival, before decoding, for every source. \
Eira's already-decoded events are therefore compared on equal terms with raw feeds whose consumer still has to decode them.";

#[derive(Serialize, Clone, Debug)]
pub struct SourceReport {
    #[serde(flatten)]
    pub info: SourceInfo,
    pub detail: String,
    pub reconnects: u32,
    pub block_events: u64,
    pub tx_events: u64,
    pub wire_bytes: u64,
    pub raw_tx_bytes: u64,
}

#[derive(Serialize, Clone, Debug)]
pub struct Header {
    pub tool: String,
    pub version: String,
    pub chain_id: u64,
    pub start_utc: String,
    pub end_utc: String,
    pub duration_s: f64,
    pub hostname: String,
    pub stamp: String,
    pub warmup_s: f64,
    pub min_sources: usize,
    pub summary_interval_s: u64,
    pub interrupted: bool,
    pub sources: Vec<SourceReport>,
    pub window_s: f64,
    pub matched_blocks: usize,
    pub matched_txs: usize,
    pub fairness_note: String,
}

#[derive(Serialize, Clone, Debug)]
pub struct Report {
    pub header: Header,
    pub block: BlockSummary,
    pub block_intervals: Vec<BlockSummary>,
    pub tx: TxSummary,
    pub pairs: Vec<PairSummary>,
}

pub fn utc_string(t: SystemTime) -> String {
    let secs = t.duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0) as i64;
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    // Howard Hinnant's civil-from-days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02} UTC", sod / 3600, (sod / 60) % 60, sod % 60)
}

/// Aligned plain-text table: first column left-aligned, the rest right-aligned.
pub struct Table {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl Table {
    pub fn new(headers: &[&str]) -> Table {
        Table { headers: headers.iter().map(|s| s.to_string()).collect(), rows: Vec::new() }
    }
    pub fn row(&mut self, cells: Vec<String>) {
        self.rows.push(cells);
    }
    pub fn render(&self) -> String {
        let cols = self.headers.len();
        let mut width = vec![0usize; cols];
        for (i, h) in self.headers.iter().enumerate() {
            width[i] = width[i].max(h.chars().count());
        }
        for r in &self.rows {
            for (i, c) in r.iter().enumerate().take(cols) {
                width[i] = width[i].max(c.chars().count());
            }
        }
        let line = |cells: &[String]| -> String {
            let mut s = String::new();
            for (i, w) in width.iter().enumerate() {
                let c = cells.get(i).map(String::as_str).unwrap_or("");
                if i == 0 {
                    s.push_str(&format!("{c:<w$}"));
                } else {
                    s.push_str(&format!("  {c:>w$}"));
                }
            }
            s.trim_end().to_string()
        };
        let mut out = line(&self.headers);
        out.push('\n');
        let total: usize = width.iter().sum::<usize>() + 2 * (cols - 1);
        out.push_str(&"-".repeat(total));
        out.push('\n');
        for r in &self.rows {
            out.push_str(&line(r));
            out.push('\n');
        }
        out
    }
}

fn f2(x: f64) -> String {
    format!("{x:.2}")
}
fn signed2(x: f64) -> String {
    format!("{x:+.2}")
}
fn pc(x: f64) -> String {
    format!("{x:.1}%")
}

pub fn render_block_summary(b: &BlockSummary) -> String {
    let mut t = Table::new(&["source", "wins", "win%", "p10", "p50", "p75", "p90", "p95", "p99", "p99.9", "max", "blocks"]);
    for s in &b.per_source {
        let d = &s.delay;
        t.row(vec![s.name.clone(), s.wins.to_string(), pc(s.win_pct), f2(d.p10), f2(d.p50), f2(d.p75), f2(d.p90), f2(d.p95), f2(d.p99), f2(d.p999), f2(d.max), d.n.to_string()]);
    }
    format!("{} completed blocks: {}\n{}", b.label, b.completed, t.render())
}

pub fn render(r: &Report) -> String {
    let h = &r.header;
    let mut o = String::new();
    let _ = writeln!(o, "{} {}  Robinhood Chain (chain id {})", h.tool, h.version, h.chain_id);
    let _ = writeln!(o, "start    {}", h.start_utc);
    let _ = writeln!(o, "end      {}   duration {:.1} s{}", h.end_utc, h.duration_s, if h.interrupted { "   (interrupted)" } else { "" });
    let _ = writeln!(o, "host     {}", h.hostname);
    let _ = writeln!(o, "stamp    {}   warmup {:.0} s   min-sources {}", h.stamp, h.warmup_s, h.min_sources);
    let _ = writeln!(o, "sources");
    for s in &h.sources {
        let level = if s.info.level.is_empty() { String::new() } else { format!("   level {}", s.info.level) };
        let _ = writeln!(o, "  {:<10} {:<7} {}{}", s.info.name, s.info.kind, s.info.target, level);
        let bytes = if s.wire_bytes > 0 { format!("   rx {:.1} MB", s.wire_bytes as f64 / 1e6) } else { String::new() };
        let _ = writeln!(o, "  {:<10} {}   reconnects {}   blocks {}   txs {} ({:.1} MB raw){}", "", s.detail, s.reconnects, s.block_events, s.tx_events, s.raw_tx_bytes as f64 / 1e6, bytes);
    }
    let _ = writeln!(o, "window   {:.1} s with every source connected   matched blocks {}   matched txs {}", h.window_s, h.matched_blocks, h.matched_txs);
    let _ = writeln!(o, "note     {}", h.fairness_note);
    o.push('\n');

    let _ = writeln!(o, "A. Block level: first source to deliver a block wins; delay behind the winner in ms");
    o.push_str(&render_block_summary(&r.block));
    o.push('\n');

    let _ = writeln!(o, "B. Transaction level: key = tx hash; lag behind the earliest source in ms");
    let _ = writeln!(o, "distinct txs {}   seen by all {}   flagged swaps {}", r.tx.total, r.tx.matched, r.tx.synthetic_swaps);
    let mut t = Table::new(&["source", "seen", "coverage", "first", "first>=1ms", "p50", "p90", "p95", "p99", "max"]);
    for s in &r.tx.per_source {
        t.row(vec![s.name.clone(), s.seen.to_string(), pc(s.coverage_pct), pc(s.first_pct), pc(s.first_by_1ms_pct), f2(s.lag_p50), f2(s.lag_p90), f2(s.lag_p95), f2(s.lag_p99), f2(s.lag_max)]);
    }
    o.push_str(&t.render());
    o.push('\n');

    for (i, p) in r.pairs.iter().enumerate() {
        let _ = writeln!(o, "C{}. {} vs {}: {} txs seen by both; positive = {} earlier", i + 1, p.reference, p.other, p.n, p.reference);
        let mut t = Table::new(&["median", "p10", "p90", &format!("{} first", p.reference), "within 1ms", &format!("{} >5ms earlier", p.reference), &format!("{} >5ms earlier", p.other)]);
        t.row(vec![signed2(p.median_ms), signed2(p.p10_ms), signed2(p.p90_ms), pc(p.ref_first_pct), pc(p.within_1ms_pct), pc(p.ref_earlier_5ms_pct), pc(p.other_earlier_5ms_pct)]);
        o.push_str(&t.render());
        if p.intervals.len() > 1 {
            let mut t = Table::new(&["interval", "from", "txs", "median", &format!("{} first", p.reference)]);
            for iv in &p.intervals {
                t.row(vec![format!("#{}", iv.index), format!("{:.0}s", iv.start_s), iv.n.to_string(), signed2(iv.median_ms), pc(iv.ref_first_pct)]);
            }
            o.push_str(&t.render());
        }
        let _ = writeln!(o, "by block size (tx count from {}):", p.size_source);
        let mut t = Table::new(&["block size", "blocks", "blocks%", "txs", "txs%", &format!("{} earlier by", p.reference), "p10", "p90", &format!("{} first", p.reference)]);
        for b in &p.buckets {
            t.row(vec![b.label.clone(), b.blocks.to_string(), pc(b.blocks_pct), b.txs.to_string(), pc(b.txs_pct), format!("{} ms", signed2(b.median_ms)), signed2(b.p10_ms), signed2(b.p90_ms), pc(b.ref_first_pct)]);
        }
        o.push_str(&t.render());
        o.push('\n');
    }
    let _ = write!(o, "percentiles: nearest rank; ms with two decimals; win% and first% over matched blocks/txs");
    o
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utc_formatting() {
        assert_eq!(utc_string(UNIX_EPOCH), "1970-01-01 00:00:00 UTC");
        assert_eq!(utc_string(UNIX_EPOCH + std::time::Duration::from_secs(1_789_819_200)), "2026-09-19 12:00:00 UTC");
    }

    #[test]
    fn table_alignment() {
        let mut t = Table::new(&["source", "p50"]);
        t.row(vec!["a".into(), "1.00".into()]);
        t.row(vec!["longer".into(), "12.50".into()]);
        let s = t.render();
        assert_eq!(s, "source    p50\n-------------\na        1.00\nlonger  12.50\n");
    }
}
