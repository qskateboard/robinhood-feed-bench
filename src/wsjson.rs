//! Generic JSON WebSocket source for third-party transaction feeds whose wire format is not
//! built in: a dotted JSON path selects the transaction hash (or the raw signed transaction,
//! which is hashed) in every message.

use crate::event::{Event, Payload, Stamp};
use crate::l2::keccak256;
use crate::ws::{self, Offer};
use anyhow::{Result, bail};
use serde_json::Value;
use std::time::Duration;
use tokio::sync::mpsc::Sender;

#[derive(Debug, Clone)]
pub struct WsJsonSource {
    pub name: String,
    pub url: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub subscribe: Option<String>,
}

impl WsJsonSource {
    /// `name=wss://url,path=a.b[].c[,header=K:V...][,subscribe=<text, must be last>]`
    pub fn parse(spec: &str) -> Result<WsJsonSource> {
        let mut src = WsJsonSource { name: String::new(), url: String::new(), path: String::new(), headers: Vec::new(), subscribe: None };
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
                "path" => src.path = value.to_string(),
                "header" => {
                    let Some((k, v)) = value.split_once(':') else { bail!("header must be K:V, got {value:?}") };
                    src.headers.push((k.trim().to_string(), v.trim().to_string()));
                }
                other if other.contains("://") || other.is_empty() => bail!("unexpected token {other:?} in wsjson spec"),
                other => {
                    // `name=wss://...` shorthand: an unknown key with a URL value names the source.
                    if value.contains("://") && src.name.is_empty() {
                        src.name = other.to_string();
                        src.url = value.to_string();
                    } else {
                        bail!("unknown key {other:?} in wsjson spec");
                    }
                }
            }
            rest = next;
        }
        if src.name.is_empty() || src.url.is_empty() {
            bail!("wsjson spec needs name=wss://url");
        }
        if src.path.is_empty() {
            bail!("wsjson spec needs path=<dotted JSON path to the tx hash or raw tx>");
        }
        Ok(src)
    }
}

/// Values at a dotted path. `a.b` descends into objects, `a[]` fans out over an array, `a[2]`
/// picks one element, and a leading `[]` applies to the message itself.
pub fn extract<'a>(v: &'a Value, path: &str) -> Vec<&'a Value> {
    let mut cur: Vec<&'a Value> = vec![v];
    for seg in path.split('.') {
        if seg.is_empty() {
            continue;
        }
        let (name, brackets) = match seg.find('[') {
            Some(i) => (&seg[..i], &seg[i..]),
            None => (seg, ""),
        };
        if !name.is_empty() {
            cur = cur.into_iter().filter_map(|x| x.get(name)).collect();
        }
        for b in brackets.split(']').filter(|b| !b.is_empty()) {
            let inner = b.trim_start_matches('[');
            if inner.is_empty() {
                cur = cur.into_iter().filter_map(|x| x.as_array()).flatten().collect();
            } else if let Ok(i) = inner.parse::<usize>() {
                cur = cur.into_iter().filter_map(|x| x.get(i)).collect();
            } else {
                return Vec::new();
            }
        }
    }
    cur
}

/// A transaction key from a JSON value: a 32-byte hex hash, or a hex raw signed transaction
/// (hashed with keccak256). Returns the hash and the raw length (0 when a hash was given).
pub fn key_from_value(v: &Value) -> Option<([u8; 32], u32)> {
    let s = v.as_str()?.trim();
    let hexs = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")).unwrap_or(s);
    if hexs.is_empty() || hexs.len() % 2 != 0 || !hexs.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let bytes = hex::decode(hexs).ok()?;
    if bytes.len() == 32 {
        let mut h = [0u8; 32];
        h.copy_from_slice(&bytes);
        Some((h, 0))
    } else {
        Some((keccak256(&bytes), bytes.len() as u32))
    }
}

pub async fn run(index: usize, src: WsJsonSource, stamp: Stamp, out: Sender<Event>) {
    let mut backoff = Duration::from_secs(1);
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
        let detail = format!("compression={} path={}", client.handshake.compression, src.path);
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
            let Ok(v) = serde_json::from_slice::<Value>(&msg.data) else { continue };
            for val in extract(&v, &src.path) {
                if let Some((hash, raw_len)) = key_from_value(val) {
                    let ev = Event { source: index, t, payload: Payload::Tx { hash, block_key: None, tx_count: None, synthetic_swap: false, raw_len } };
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
    use serde_json::json;

    #[test]
    fn json_path_extractor() {
        let h = "0x".to_string() + &"ab".repeat(32);
        let v = json!({"params": {"result": {"hash": h}}});
        let got = extract(&v, "params.result.hash");
        assert_eq!(got.len(), 1);
        assert_eq!(key_from_value(got[0]).unwrap().0, [0xab; 32]);

        let v = json!({"data": [{"raw": "0x02c3010203"}, {"raw": "0x02c3010204"}, {"other": 1}]});
        let got = extract(&v, "data[].raw");
        assert_eq!(got.len(), 2);
        let (hash, raw_len) = key_from_value(got[0]).unwrap();
        assert_eq!(hash, keccak256(&[0x02, 0xc3, 0x01, 0x02, 0x03]));
        assert_eq!(raw_len, 5);
        assert_eq!(extract(&v, "data[1].raw"), vec![&json!("0x02c3010204")]);

        let v = json!([["0x01", "0x02"], ["0x03"]]);
        assert_eq!(extract(&v, "[][]").len(), 3);
        assert!(extract(&v, "nope.x").is_empty());
        assert!(key_from_value(&json!("not hex")).is_none());
        assert!(key_from_value(&json!(12)).is_none());
    }

    #[test]
    fn spec_parsing() {
        let s = WsJsonSource::parse("other=wss://feed.example.com/v1,path=params.result.hash,header=Authorization:Bearer x,subscribe={\"op\":\"sub\",\"ch\":[\"tx\"]}").unwrap();
        assert_eq!(s.name, "other");
        assert_eq!(s.url, "wss://feed.example.com/v1");
        assert_eq!(s.path, "params.result.hash");
        assert_eq!(s.headers, vec![("Authorization".to_string(), "Bearer x".to_string())]);
        assert_eq!(s.subscribe.as_deref(), Some("{\"op\":\"sub\",\"ch\":[\"tx\"]}"));
        let s = WsJsonSource::parse("name=n,url=ws://h:1/,path=[].hash").unwrap();
        assert_eq!((s.name.as_str(), s.url.as_str(), s.path.as_str()), ("n", "ws://h:1/", "[].hash"));
        assert!(WsJsonSource::parse("x=wss://h").is_err());
        assert!(WsJsonSource::parse("path=a").is_err());
    }
}
