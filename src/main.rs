//! feedbench: latency benchmark for Robinhood Chain (Arbitrum Nitro, chain id 4663) block and
//! transaction feeds. Every source runs as its own task and stamps events on arrival; one
//! collector matches them by block and by transaction hash and prints the tables.

mod eira;
mod event;
mod inflate;
mod l2;
mod nitro;
mod report;
mod stats;
mod ws;
mod wsjson;

use anyhow::{Context, Result, bail};
use clap::Parser;
use event::{Event, Stamp};
use report::{Header, Report, SourceReport};
use stats::{Collector, Notice, SourceInfo};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::mpsc;

const CHAIN_ID: u64 = 4663;

#[derive(Parser, Debug)]
#[command(name = "feedbench", version, about = "Latency benchmark for Robinhood Chain (Arbitrum Nitro, chain id 4663) block and transaction feeds")]
struct Args {
    /// Nitro-format WebSocket feed, repeatable: NAME=wss://host/path
    #[arg(long = "source", value_name = "NAME=URL")]
    sources: Vec<String>,

    /// Eira Pulse gRPC endpoint, e.g. pulse.eiranodes.dev:443
    #[arg(long, value_name = "HOST:PORT")]
    eira: Option<String>,

    /// Commitment level for the Eira subscription
    #[arg(long, value_enum, default_value = "received")]
    eira_level: eira::Level,

    /// Generic JSON WebSocket feed, repeatable: NAME=wss://url,path=a.b[].hash[,header=K:V][,subscribe=TEXT]
    #[arg(long = "wsjson", value_name = "SPEC")]
    wsjson: Vec<String>,

    /// Run length in seconds (Ctrl-C prints what was collected so far)
    #[arg(long, default_value_t = 300)]
    seconds: u64,

    /// A block or transaction is matched once this many sources delivered it (default: all)
    #[arg(long, value_name = "N")]
    min_sources: Option<usize>,

    /// Seconds to skip after every source is connected (backlog replay, warm caches)
    #[arg(long, default_value_t = 5.0, value_name = "SECONDS")]
    warmup: f64,

    /// Rolling block-level summary interval in seconds
    #[arg(long, default_value_t = 300, value_name = "SECONDS")]
    summary_interval: u64,

    /// Which instant raw-feed events are stamped with
    #[arg(long, value_enum, default_value = "arrival")]
    stamp: Stamp,

    /// Compression offered to Nitro feeds
    #[arg(long, value_enum, default_value = "auto")]
    compression: ws::Offer,

    /// Write the full report as JSON to this file
    #[arg(long, value_name = "PATH")]
    json: Option<PathBuf>,

    /// Print one line per completed block (the Go tool's per-block output)
    #[arg(long)]
    print_blocks: bool,

    /// Label printed as "host" in the report instead of the machine's hostname
    #[arg(long, value_name = "LABEL")]
    host: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut infos: Vec<SourceInfo> = Vec::new();
    let mut nitro_sources = Vec::new();
    for s in &args.sources {
        let src = nitro::NitroSource::parse(s)?;
        infos.push(SourceInfo { name: src.name.clone(), kind: "nitro".into(), target: src.url.clone(), level: String::new() });
        nitro_sources.push(src);
    }
    let eira_index = args.eira.as_ref().map(|addr| {
        infos.push(SourceInfo { name: "eira".into(), kind: "eira".into(), target: addr.clone(), level: args.eira_level.name().into() });
        infos.len() - 1
    });
    let mut wsjson_sources = Vec::new();
    for s in &args.wsjson {
        let src = wsjson::WsJsonSource::parse(s)?;
        infos.push(SourceInfo { name: src.name.clone(), kind: "wsjson".into(), target: src.url.clone(), level: String::new() });
        wsjson_sources.push(src);
    }
    if infos.is_empty() {
        bail!("no sources: pass --source NAME=wss://..., --eira HOST:PORT and/or --wsjson SPEC");
    }
    let mut names: Vec<&str> = infos.iter().map(|i| i.name.as_str()).collect();
    names.sort_unstable();
    if names.windows(2).any(|w| w[0] == w[1]) {
        bail!("source names must be unique");
    }
    let n = infos.len();
    let min_sources = args.min_sources.unwrap_or(n).clamp(1, n);
    let warmup = Duration::from_secs_f64(args.warmup.max(0.0));
    let interval = Duration::from_secs(args.summary_interval.max(1));

    let (tx, mut rx) = mpsc::channel::<Event>(1 << 16);
    let mut tasks = Vec::new();
    let mut index = 0usize;
    for src in nitro_sources {
        tasks.push(tokio::spawn(nitro::run(index, src, args.compression, args.stamp, tx.clone())));
        index += 1;
    }
    if let Some(addr) = args.eira.clone() {
        tasks.push(tokio::spawn(eira::run(index, addr, args.eira_level, tx.clone())));
        index += 1;
    }
    for src in wsjson_sources {
        tasks.push(tokio::spawn(wsjson::run(index, src, args.stamp, tx.clone())));
        index += 1;
    }
    drop(tx);

    let start_wall = SystemTime::now();
    let mut col = Collector::new(infos, eira_index.unwrap_or(0), min_sources, warmup);
    let start = col.start;
    let deadline = start + Duration::from_secs(args.seconds);

    println!("feedbench {}  chain id {}  {} source(s)  {} s  stamp={:?}", env!("CARGO_PKG_VERSION"), CHAIN_ID, n, args.seconds, args.stamp);
    for s in &col.sources {
        println!("  {:<10} {:<7} {}{}", s.name, s.kind, s.target, if s.level.is_empty() { String::new() } else { format!("  level {}", s.level) });
    }
    if args.print_blocks {
        let mut cols = vec!["[block]".to_string(), "sequence".into(), "block".into()];
        cols.extend(col.sources.iter().map(|s| s.name.clone()));
        cols.push("winner".into());
        println!("{}", cols.join("\t"));
    }

    let mut ticker = tokio::time::interval_at(tokio::time::Instant::from_std(start + interval), interval);
    let mut last_summary = start;
    let mut block_intervals = Vec::new();
    let mut interrupted = false;
    loop {
        tokio::select! {
            ev = rx.recv() => {
                let Some(ev) = ev else { break };
                match col.on_event(ev) {
                    Some(Notice::Connected { source, detail, reconnect }) => {
                        println!("+{:7.1}s [{}] {} {}", ms(start) / 1e3, col.sources[source].name, if reconnect { "reconnected" } else { "connected" }, detail);
                    }
                    Some(Notice::Disconnected { source, reason }) => {
                        println!("+{:7.1}s [{}] disconnected: {}", ms(start) / 1e3, col.sources[source].name, reason);
                    }
                    Some(Notice::BlockLine(line)) => {
                        if args.print_blocks {
                            println!("{line}");
                        }
                    }
                    None => {}
                }
            }
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => break,
            _ = tokio::signal::ctrl_c() => {
                println!("interrupted, printing what was collected");
                interrupted = true;
                break;
            }
            _ = ticker.tick() => {
                let now = Instant::now();
                let label = format!("+{:.0}s..+{:.0}s", (last_summary - start).as_secs_f64(), (now - start).as_secs_f64());
                let s = col.block_summary(&label, Some(last_summary), now);
                println!();
                println!("rolling block summary {}", report::render_block_summary(&s));
                block_intervals.push(s);
                last_summary = now;
            }
        }
    }
    for t in &tasks {
        t.abort();
    }
    let end = Instant::now();
    let end_wall = SystemTime::now();

    let block = col.block_summary("whole run", None, end);
    let txs = col.tx_summary();
    let pairs: Vec<_> = (0..n).filter(|&i| i != col.reference).map(|i| col.pair_summary(i, interval)).collect();
    let header = Header {
        tool: "feedbench".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        chain_id: CHAIN_ID,
        start_utc: report::utc_string(start_wall),
        end_utc: report::utc_string(end_wall),
        duration_s: (end - start).as_secs_f64(),
        hostname: args.host.clone().unwrap_or_else(|| hostname::get().map(|h| h.to_string_lossy().into_owned()).unwrap_or_else(|_| "unknown".into())),
        stamp: match args.stamp {
            Stamp::Arrival => "arrival (message in memory, before inflating and parsing)".into(),
            Stamp::Decoded => "decoded (after inflating, before parsing; the Go tool's instant)".into(),
        },
        warmup_s: warmup.as_secs_f64(),
        min_sources,
        summary_interval_s: interval.as_secs(),
        interrupted,
        sources: col
            .sources
            .iter()
            .enumerate()
            .map(|(i, s)| SourceReport {
                info: s.clone(),
                detail: col.details[i].clone(),
                reconnects: col.reconnects[i],
                block_events: col.block_events[i],
                tx_events: col.tx_events[i],
                wire_bytes: col.wire_bytes[i],
                raw_tx_bytes: col.raw_tx_bytes[i],
            })
            .collect(),
        window_s: col.window_seconds(end),
        matched_blocks: block.completed,
        matched_txs: txs.matched,
        fairness_note: report::FAIRNESS_NOTE.into(),
    };
    let rep = Report { header, block, block_intervals, tx: txs, pairs };
    println!();
    println!("{}", report::render(&rep));
    if let Some(path) = &args.json {
        let f = std::fs::File::create(path).with_context(|| format!("create {}", path.display()))?;
        serde_json::to_writer_pretty(f, &rep)?;
        println!("json written to {}", path.display());
    }
    Ok(())
}

fn ms(since: Instant) -> f64 {
    since.elapsed().as_secs_f64() * 1e3
}
