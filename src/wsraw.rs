//! Generic binary WebSocket source: every binary frame carries one or more signed transactions
//! back to back (typed envelope `type || rlp(list)` or a legacy RLP list), each of which is hashed
//! with keccak256. A text frame is treated as JSON and every string that is a 32-byte hash or a
//! hex raw transaction becomes an event, so an unknown vendor format still yields something.
//! The first frame's leading bytes are printed once so the wire format can be seen.

use crate::event::{Event, Payload, Stamp};
use crate::l2::keccak256;
use crate::ws::{self, Offer};
use anyhow::{Result, bail};
use std::time::Duration;
use tokio::sync::mpsc::Sender;

#[derive(Debug, Clone)]
pub struct WsRawSource {
    pub name: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub subscribe: Option<String>,
}

impl WsRawSource {
    /// `name=wss://url[,header=K:V...][,subscribe=<text, must be last>]`
    pub fn parse(spec: &str) -> Result<WsRawSource> {
        let mut src = WsRawSource { name: String::new(), url: String::new(), headers: Vec::new(), subscribe: None };
        let mut rest = spec;
        while !rest.is_empty() {
            let Some((key, after)) = rest.split_once('=') else { bail!("expected key=value in {rest:?}") };
            let key = key.trim();
            if key == "subscribe" {
                src.subscribe = Some(after.to_string());
                break;
            }
            let (value, next) = match after.find(',') {
                Some(i) => (&after[..i], &after[i + 1..]),
                None => (after, ""),
            };
            match key {
                "name" => src.name = value.to_string(),
                "url" => src.url = value.to_string(),
                "header" => {
                    let Some((k, v)) = value.split_once(':') else { bail!("header must be K:V, got {value:?}") };
                    src.headers.push((k.trim().to_string(), v.trim().to_string()));
                }
                other if value.contains("://") && src.name.is_empty() => {
                    src.name = other.to_string();
                    src.url = value.to_string();
                }
                other => bail!("unknown key {other:?} in wsraw spec"),
            }
            rest = next;
        }
        if src.name.is_empty() || src.url.is_empty() {
            bail!("wsraw spec needs name=wss://url");
        }
        Ok(src)
    }
}

/// Length of the RLP item at the start of `b`, header included.
fn rlp_item_len(b: &[u8]) -> Option<usize> {
    let b0 = *b.first()? as usize;
    Some(match b0 {
        0x00..=0x7f => 1,
        0x80..=0xb7 => 1 + (b0 - 0x80),
        0xb8..=0xbf => {
            let ll = b0 - 0xb7;
            1 + ll + be(b.get(1..1 + ll)?)?
        }
        0xc0..=0xf7 => 1 + (b0 - 0xc0),
        _ => {
            let ll = b0 - 0xf7;
            1 + ll + be(b.get(1..1 + ll)?)?
        }
    })
}

fn be(b: &[u8]) -> Option<usize> {
    if b.is_empty() || b.len() > 8 {
        return None;
    }
    Some(b.iter().fold(0usize, |acc, &x| (acc << 8) | x as usize))
}

/// Length of one signed transaction at the start of `b`: `type || rlp(list)` for typed
/// transactions (type < 0x80), a bare RLP list for legacy ones. None when `b` cannot start a tx.
pub fn tx_len(b: &[u8]) -> Option<usize> {
    let b0 = *b.first()?;
    if b0 < 0x80 {
        let body = b.get(1..)?;
        if *body.first()? < 0xc0 {
            return None;
        }
        Some(1 + rlp_item_len(body)?)
    } else if b0 >= 0xc0 {
        rlp_item_len(b)
    } else {
        None
    }
}

/// Split a frame into consecutive signed transactions. Tolerates a 4- or 8-byte big-endian length
/// prefix in front of each transaction. Returns nothing if the frame is not a run of transactions.
pub fn split_txs(frame: &[u8]) -> Vec<&[u8]> {
    for prefix in [0usize, 4, 8] {
        let mut out = Vec::new();
        let mut pos = 0;
        let mut ok = true;
        while pos < frame.len() {
            let Some(rest) = frame.get(pos + prefix..) else { ok = false; break };
            let Some(n) = tx_len(rest) else { ok = false; break };
            if prefix > 0 && be(&frame[pos..pos + prefix]) != Some(n) {
                ok = false;
                break;
            }
            let Some(tx) = rest.get(..n) else { ok = false; break };
            if n < 8 {
                ok = false;
                break;
            }
            out.push(tx);
            pos += prefix + n;
        }
        if ok && !out.is_empty() {
            return out;
        }
    }
    Vec::new()
}

fn json_keys(v: &serde_json::Value, out: &mut Vec<([u8; 32], u32)>) {
    match v {
        serde_json::Value::String(_) => {
            if let Some(k) = crate::wsjson::key_from_value(v) {
                out.push(k);
            }
        }
        serde_json::Value::Array(a) => a.iter().for_each(|x| json_keys(x, out)),
        serde_json::Value::Object(o) => o.values().for_each(|x| json_keys(x, out)),
        _ => {}
    }
}

pub async fn run(index: usize, src: WsRawSource, stamp: Stamp, out: Sender<Event>) {
    let mut backoff = Duration::from_secs(1);
    let mut shown = false;
    loop {
        let cfg = ws::Config {
            url: src.url.clone(),
            headers: src.headers.clone(),
            offer: Offer::Standard,
            max_message: crate::nitro::MAX_MESSAGE,
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
        if let Some(s) = &src.subscribe
            && let Err(e) = client.send_text(s).await
        {
            eprintln!("[{}] subscribe failed: {e:#}", src.name);
            continue;
        }
        let detail = format!("compression={} format=binary-txs", client.handshake.compression);
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
            if !shown {
                shown = true;
                let head: String = msg.data.iter().take(24).map(|b| format!("{b:02x}")).collect();
                eprintln!("[{}] first frame: {} bytes, head {head}", src.name, msg.data.len());
            }
            let mut keys: Vec<([u8; 32], u32)> = Vec::new();
            for tx in split_txs(&msg.data) {
                keys.push((keccak256(tx), tx.len() as u32));
            }
            if keys.is_empty()
                && let Ok(v) = serde_json::from_slice::<serde_json::Value>(&msg.data)
            {
                json_keys(&v, &mut keys);
            }
            for (hash, raw_len) in keys {
                let ev = Event { source: index, t, payload: Payload::Tx { hash, block_key: None, tx_count: None, synthetic_swap: false, raw_len } };
                if out.send(ev).await.is_err() {
                    return;
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
    fn splits_typed_and_legacy_transactions_with_and_without_length_prefix() {
        // typed: 0x02 || rlp list of 9 single-byte items; legacy: rlp list of 9 items
        let typed: Vec<u8> = [vec![0x02, 0xc9], vec![1; 9]].concat();
        let legacy: Vec<u8> = [vec![0xc9], vec![2; 9]].concat();
        let frame = [typed.clone(), legacy.clone()].concat();
        assert_eq!(split_txs(&frame), vec![typed.as_slice(), legacy.as_slice()]);
        let mut prefixed = Vec::new();
        for tx in [&typed, &legacy] {
            prefixed.extend((tx.len() as u32).to_be_bytes());
            prefixed.extend(tx);
        }
        assert_eq!(split_txs(&prefixed), vec![typed.as_slice(), legacy.as_slice()]);
        // a long-form list header (0xf8 + 1 length byte)
        let long: Vec<u8> = [vec![0x02, 0xf8, 0x40], vec![3; 0x40]].concat();
        assert_eq!(split_txs(&long), vec![long.as_slice()]);
        assert!(split_txs(b"{\"not\":\"txs\"}").is_empty());
        assert!(split_txs(&[0x02, 0xc9, 1, 2]).is_empty(), "truncated");
    }

    #[test]
    fn spec_parsing() {
        let s = WsRawSource::parse("new=wss://h/transactions?format=binary,header=Authorization:Bearer x").unwrap();
        assert_eq!((s.name.as_str(), s.url.as_str()), ("new", "wss://h/transactions?format=binary"));
        assert_eq!(s.headers, vec![("Authorization".to_string(), "Bearer x".to_string())]);
        assert!(WsRawSource::parse("header=a:b").is_err());
    }
}
