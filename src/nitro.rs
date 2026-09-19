//! Source adapter for any Nitro-format broadcast feed (`{"version":1,"messages":[...]}`).
//! One block event per feed message and one transaction event per signed transaction in its
//! `l2Msg`, all stamped with the arrival instant of the WebSocket message that carried them.

use crate::event::{Event, Payload, Stamp};
use crate::l2;
use crate::ws::{self, Offer};
use anyhow::{Context, Result};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::Deserialize;
use std::borrow::Cow;
use std::time::Duration;
use tokio::sync::mpsc::Sender;

pub const MAX_MESSAGE: usize = 15 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct NitroSource {
    pub name: String,
    pub url: String,
}

impl NitroSource {
    /// `name=wss://host/path`
    pub fn parse(spec: &str) -> Result<NitroSource> {
        let (name, url) = spec.split_once('=').context("expected name=url")?;
        if name.is_empty() || !url.contains("://") {
            anyhow::bail!("expected name=ws(s)://url, got {spec:?}");
        }
        Ok(NitroSource { name: name.to_string(), url: url.to_string() })
    }
}

#[derive(Deserialize)]
struct Frame<'a> {
    #[serde(borrow, default)]
    messages: Vec<FeedMessage<'a>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FeedMessage<'a> {
    sequence_number: u64,
    #[serde(default)]
    block_hash: Option<String>,
    #[serde(borrow)]
    message: Outer<'a>,
}

#[derive(Deserialize)]
struct Outer<'a> {
    #[serde(borrow)]
    message: Inner<'a>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Inner<'a> {
    #[serde(borrow, default)]
    l2_msg: Option<Cow<'a, str>>,
}

/// Blocks and transactions found in one feed frame.
pub struct Parsed {
    pub blocks: Vec<ParsedBlock>,
}

pub struct ParsedBlock {
    pub seq: u64,
    pub hash: Option<String>,
    pub txs: Vec<([u8; 32], u32)>,
}

pub fn parse_frame(data: &[u8]) -> Result<Parsed> {
    let frame: Frame = serde_json::from_slice(data)?;
    let mut blocks = Vec::with_capacity(frame.messages.len());
    for m in frame.messages {
        let mut txs = Vec::new();
        if let Some(b64) = m.message.message.l2_msg.as_deref() {
            let bin = STANDARD.decode(b64).context("l2Msg is not base64")?;
            for raw in l2::signed_txs(&bin) {
                txs.push((l2::keccak256(raw), raw.len() as u32));
            }
        }
        blocks.push(ParsedBlock { seq: m.sequence_number, hash: m.block_hash.filter(|h| !h.trim().is_empty()), txs });
    }
    Ok(Parsed { blocks })
}

pub async fn run(index: usize, src: NitroSource, offer: Offer, stamp: Stamp, out: Sender<Event>) {
    let mut next_seq: Option<u64> = None;
    let mut backoff = Duration::from_secs(1);
    loop {
        let mut headers = vec![("Arbitrum-Feed-Client-Version".to_string(), "2".to_string())];
        // No cursor on the first connection: the server chooses where to start. Some servers
        // drop a connection that asks for sequence 0 explicitly, so the header is omitted.
        if let Some(n) = next_seq {
            headers.push(("Arbitrum-Requested-Sequence-Number".to_string(), n.to_string()));
        }
        let cfg = ws::Config {
            url: src.url.clone(),
            headers,
            offer,
            max_message: MAX_MESSAGE,
            ping_every: Duration::from_secs(20),
            read_timeout: Duration::from_secs(60),
        };
        let mut client = match ws::Client::connect(&cfg).await {
            Ok(c) => c,
            Err(e) => {
                let wait = ws::retry_delay(&e, backoff);
                eprintln!("[{}] connect failed: {e:#}; next attempt in {} s", src.name, wait.as_secs());
                tokio::time::sleep(wait).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
                continue;
            }
        };
        backoff = Duration::from_secs(1);
        let mut detail = format!("compression={}", client.handshake.compression);
        if let Some(v) = client.handshake.header("arbitrum-feed-server-version") {
            detail.push_str(&format!(" server-version={v}"));
        }
        if let Some(v) = client.handshake.header("arbitrum-chain-id") {
            detail.push_str(&format!(" chain-id={v}"));
        }
        if out.send(Event { source: index, t: std::time::Instant::now(), payload: Payload::Connected { detail } }).await.is_err() {
            return;
        }
        let reason = loop {
            let msg = match client.next_message().await {
                Ok(m) => m,
                Err(e) => break format!("{e:#}"),
            };
            let t = match stamp {
                Stamp::Arrival => msg.arrival,
                Stamp::Decoded => msg.decoded,
            };
            let parsed = match parse_frame(&msg.data) {
                Ok(p) => p,
                Err(_) => continue, // keep-alive or confirmation frames carry no block
            };
            for b in parsed.blocks {
                if b.seq != u64::MAX {
                    next_seq = Some(next_seq.map_or(b.seq + 1, |n| n.max(b.seq + 1)));
                }
                let key = format!("seq:{}", b.seq);
                let n = b.txs.len() as u32;
                let block = Event { source: index, t, payload: Payload::Block { key: key.clone(), seq: Some(b.seq), hash: b.hash, tx_count: Some(n), wire_len: msg.wire_len as u64 } };
                if out.send(block).await.is_err() {
                    return;
                }
                for (hash, raw_len) in b.txs {
                    let ev = Event {
                        source: index,
                        t,
                        payload: Payload::Tx { hash, block_key: Some(key.clone()), tx_count: Some(n), synthetic_swap: false, raw_len },
                    };
                    if out.send(ev).await.is_err() {
                        return;
                    }
                }
            }
        };
        if out.send(Event { source: index, t: std::time::Instant::now(), payload: Payload::Disconnected { reason } }).await.is_err() {
            return;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_parsing_extracts_blocks_and_hashes() {
        // l2Msg: batch with one signed tx (0x02 || rlp) and one heartbeat
        let typed = [0x02u8, 0xc3, 0x01, 0x02, 0x03];
        let mut l2 = vec![3u8];
        let sub = [&[4u8][..], &typed].concat();
        l2.extend((sub.len() as u64).to_be_bytes());
        l2.extend(&sub);
        l2.extend(1u64.to_be_bytes());
        l2.push(6);
        let json = format!(
            r#"{{"version":1,"messages":[{{"sequenceNumber":77,"blockHash":"0xabc","message":{{"message":{{"header":{{"kind":3,"blockNumber":5}},"l2Msg":"{}"}},"delayedMessagesRead":1}}}}]}}"#,
            STANDARD.encode(&l2)
        );
        let p = parse_frame(json.as_bytes()).unwrap();
        assert_eq!(p.blocks.len(), 1);
        assert_eq!(p.blocks[0].seq, 77);
        assert_eq!(p.blocks[0].hash.as_deref(), Some("0xabc"));
        assert_eq!(p.blocks[0].txs, vec![(l2::keccak256(&typed), typed.len() as u32)]);
        // confirmation-only frames carry no messages
        let p = parse_frame(br#"{"version":1,"confirmedSequenceNumberMessage":{"sequenceNumber":5}}"#).unwrap();
        assert!(p.blocks.is_empty());
        assert!(NitroSource::parse("official=wss://feed.example.com").is_ok());
        assert!(NitroSource::parse("official").is_err());
    }
}
