//! A small RFC 6455 WebSocket client (TLS through rustls with the system root store) that
//! stamps every message when its last byte reached this process, before any inflating or
//! parsing. Supports fragmented messages, ping/pong, close frames, `permessage-deflate` and
//! the Nitro feed's `Arbitrum-permessage-deflate` (see `inflate.rs`).

use crate::inflate::{Inflater, Mode};
use anyhow::{Context, Result, anyhow, bail};
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::{Buf, BytesMut};
use rand::RngCore;
use sha1::{Digest, Sha1};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_rustls::{TlsConnector, client::TlsStream, rustls::pki_types::ServerName};

const OP_CONT: u8 = 0x0;
const OP_TEXT: u8 = 0x1;
const OP_BINARY: u8 = 0x2;
const OP_CLOSE: u8 = 0x8;
const OP_PING: u8 = 0x9;
const OP_PONG: u8 = 0xA;
const WS_GUID: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

const NITRO_EXTENSION: &str = "Arbitrum-permessage-deflate";
const STANDARD_EXTENSION: &str = "permessage-deflate";
const EXTENSION_PARAMS: &str = "server_no_context_takeover; client_no_context_takeover";

/// Which compression extensions to offer in the handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Offer {
    /// Offer the Nitro extension first and the standard one as a fallback; use whichever the server picks.
    Auto,
    /// Offer only `Arbitrum-permessage-deflate` (what the Go reference tool does).
    Nitro,
    /// Offer only RFC 7692 `permessage-deflate`.
    Standard,
    /// Offer no compression.
    None,
}

impl Offer {
    fn header_value(self) -> Option<String> {
        let nitro = format!("{NITRO_EXTENSION}; {EXTENSION_PARAMS}");
        let standard = format!("{STANDARD_EXTENSION}; {EXTENSION_PARAMS}");
        match self {
            Offer::Auto => Some(format!("{nitro}, {standard}")),
            Offer::Nitro => Some(nitro),
            Offer::Standard => Some(standard),
            Offer::None => None,
        }
    }
}

pub struct Config {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub offer: Offer,
    /// Limit on both the wire size and the inflated size of one message.
    pub max_message: usize,
    pub ping_every: Duration,
    pub read_timeout: Duration,
}

pub struct Message {
    pub data: Vec<u8>,
    /// When the last byte of the message reached this process (before inflating).
    pub arrival: Instant,
    /// After inflating, before any parsing.
    pub decoded: Instant,
    pub wire_len: usize,
}

/// The server answered the upgrade request with something other than 101.
#[derive(Debug, Clone)]
pub struct Rejected {
    pub status: u16,
    pub reason: String,
    /// `Retry-After` in seconds, when the server sent one.
    pub retry_after: Option<u64>,
}

impl std::fmt::Display for Rejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "handshake rejected: {}", self.reason)?;
        if let Some(s) = self.retry_after {
            write!(f, " (retry-after {s}s)")?;
        }
        Ok(())
    }
}

impl std::error::Error for Rejected {}

/// Seconds to wait before dialing again after a failed connect: the server's `Retry-After`,
/// at least 45 s after a 429 (a rate limiter usually keeps the slot busy for a while), else the
/// caller's exponential backoff.
pub fn retry_delay(err: &anyhow::Error, backoff: Duration) -> Duration {
    match err.downcast_ref::<Rejected>() {
        Some(r) if r.status == 429 => Duration::from_secs(r.retry_after.unwrap_or(45).max(45)),
        Some(r) => Duration::from_secs(r.retry_after.unwrap_or(backoff.as_secs())),
        None => backoff,
    }
}

/// What the handshake response told us.
#[derive(Debug, Clone, Default)]
pub struct Handshake {
    pub compression: String,
    pub server_headers: Vec<(String, String)>,
}

impl Handshake {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.server_headers.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str())
    }
}

enum Stream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for Stream {
    fn poll_read(self: Pin<&mut Self>, cx: &mut TaskContext<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut TaskContext<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_flush(cx),
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Stream::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

struct Url {
    tls: bool,
    host: String,
    port: u16,
    path: String,
}

fn parse_url(s: &str) -> Result<Url> {
    let (scheme, rest) = s.split_once("://").ok_or_else(|| anyhow!("url {s:?} has no scheme"))?;
    let tls = match scheme.to_ascii_lowercase().as_str() {
        "wss" | "https" => true,
        "ws" | "http" => false,
        other => bail!("unsupported scheme {other:?}"),
    };
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) if !h.contains(']') || h.ends_with(']') => (h, p.parse::<u16>().context("bad port")?),
        _ => (authority, if tls { 443 } else { 80 }),
    };
    if host.is_empty() {
        bail!("url {s:?} has no host");
    }
    Ok(Url { tls, host: host.trim_matches(|c| c == '[' || c == ']').to_string(), port, path: path.to_string() })
}

fn tls_connector() -> Result<TlsConnector> {
    let mut roots = rustls::RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    for cert in native.certs {
        let _ = roots.add(cert);
    }
    if roots.is_empty() {
        bail!("no system root certificates found ({} errors)", native.errors.len());
    }
    let cfg = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(TlsConnector::from(Arc::new(cfg)))
}

struct OpenMessage {
    compressed: bool,
    acc: Vec<u8>,
    wire_len: usize,
}

pub struct Client {
    stream: Stream,
    buf: BytesMut,
    inflater: Option<Inflater>,
    open: Option<OpenMessage>,
    /// Instant of the read that last added bytes to `buf`.
    last_read: Instant,
    max: usize,
    ping_every: Duration,
    next_ping: Instant,
    read_timeout: Duration,
    pub handshake: Handshake,
}

impl Client {
    pub async fn connect(cfg: &Config) -> Result<Client> {
        let url = parse_url(&cfg.url)?;
        let addr = format!("{}:{}", url.host, url.port);
        let tcp = timeout(Duration::from_secs(10), TcpStream::connect(&addr)).await.map_err(|_| anyhow!("tcp connect timeout"))??;
        tcp.set_nodelay(true)?;
        let mut stream = if url.tls {
            let name = ServerName::try_from(url.host.clone()).context("invalid TLS server name")?;
            let tls = timeout(Duration::from_secs(10), tls_connector()?.connect(name, tcp)).await.map_err(|_| anyhow!("tls handshake timeout"))??;
            Stream::Tls(Box::new(tls))
        } else {
            Stream::Plain(tcp)
        };
        let key = {
            let mut k = [0u8; 16];
            rand::thread_rng().fill_bytes(&mut k);
            STANDARD.encode(k)
        };
        let host_header = if (url.tls && url.port == 443) || (!url.tls && url.port == 80) { url.host.clone() } else { addr.clone() };
        let mut req = format!(
            "GET {} HTTP/1.1\r\nHost: {host_header}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n",
            url.path
        );
        if let Some(offer) = cfg.offer.header_value() {
            req.push_str(&format!("Sec-WebSocket-Extensions: {offer}\r\n"));
        }
        for (k, v) in &cfg.headers {
            req.push_str(&format!("{k}: {v}\r\n"));
        }
        req.push_str(&format!("User-Agent: feedbench/{}\r\n\r\n", env!("CARGO_PKG_VERSION")));
        stream.write_all(req.as_bytes()).await?;

        let mut buf = BytesMut::with_capacity(256 * 1024);
        let head_end = loop {
            if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break i;
            }
            if buf.len() > 64 * 1024 {
                bail!("handshake response head too large");
            }
            let n = timeout(Duration::from_secs(15), stream.read_buf(&mut buf)).await.map_err(|_| anyhow!("handshake read timeout"))??;
            if n == 0 {
                bail!("connection closed during handshake");
            }
        };
        let head = std::str::from_utf8(&buf[..head_end]).context("handshake head is not UTF-8")?.to_string();
        buf.advance(head_end + 4);
        let mut lines = head.split("\r\n");
        let status_line = lines.next().unwrap_or("");
        let status: u16 = status_line.split_whitespace().nth(1).and_then(|s| s.parse().ok()).unwrap_or(0);
        let mut headers = Vec::new();
        for l in lines {
            if let Some((k, v)) = l.split_once(':') {
                headers.push((k.trim().to_string(), v.trim().to_string()));
            }
        }
        if status != 101 {
            let retry_after = headers.iter().find(|(k, _)| k.eq_ignore_ascii_case("retry-after")).and_then(|(_, v)| v.trim().parse::<u64>().ok());
            return Err(Rejected { status, reason: status_line.to_string(), retry_after }.into());
        }
        let expected = {
            let mut h = Sha1::new();
            h.update(key.as_bytes());
            h.update(WS_GUID);
            STANDARD.encode(h.finalize())
        };
        let accept = headers.iter().find(|(k, _)| k.eq_ignore_ascii_case("sec-websocket-accept")).map(|(_, v)| v.as_str());
        if accept != Some(expected.as_str()) {
            bail!("bad Sec-WebSocket-Accept");
        }
        let ext = headers.iter().find(|(k, _)| k.eq_ignore_ascii_case("sec-websocket-extensions")).map(|(_, v)| v.clone());
        let mode = match &ext {
            Some(h) => negotiated_mode(h, cfg.offer)?,
            None => None,
        };
        let inflater = match mode {
            Some(m) => Some(Inflater::new(m, cfg.max_message)?),
            None => None,
        };
        let now = Instant::now();
        Ok(Client {
            stream,
            buf,
            inflater,
            open: None,
            last_read: now,
            max: cfg.max_message,
            ping_every: cfg.ping_every,
            next_ping: now + cfg.ping_every,
            read_timeout: cfg.read_timeout,
            handshake: Handshake { compression: mode.map(|m| m.name().to_string()).unwrap_or_else(|| "none".into()), server_headers: headers },
        })
    }

    pub async fn send_text(&mut self, text: &str) -> Result<()> {
        self.write(&client_frame(text.as_bytes(), OP_TEXT)).await
    }

    async fn write(&mut self, frame: &[u8]) -> Result<()> {
        timeout(Duration::from_secs(5), self.stream.write_all(frame)).await.map_err(|_| anyhow!("write timeout"))??;
        Ok(())
    }

    /// Next data message. Control frames are answered inline; a close frame or a broken
    /// connection ends the session with an error.
    pub async fn next_message(&mut self) -> Result<Message> {
        loop {
            while let Some(frame) = parse_frame(&mut self.buf, self.max)? {
                let arrival = self.last_read;
                match frame.opcode {
                    OP_TEXT | OP_BINARY | OP_CONT => {
                        if let Some(m) = self.data_frame(frame, arrival)? {
                            return Ok(m);
                        }
                    }
                    OP_PING => self.write(&client_frame(&frame.payload, OP_PONG)).await?,
                    OP_PONG => {}
                    OP_CLOSE => {
                        let p = &frame.payload;
                        let code = if p.len() >= 2 { Some(u16::from_be_bytes([p[0], p[1]])) } else { None };
                        let reason = String::from_utf8_lossy(&p[p.len().min(2)..]).to_string();
                        let _ = self.write(&client_frame(&p[..p.len().min(2)], OP_CLOSE)).await;
                        bail!("closed by server: code {} {reason}", code.map(|c| c.to_string()).unwrap_or_else(|| "-".into()));
                    }
                    other => bail!("unknown opcode {other}"),
                }
            }
            if self.buf.capacity() - self.buf.len() < 64 * 1024 {
                self.buf.reserve(256 * 1024);
            }
            let ping_due = tokio::select! {
                r = timeout(self.read_timeout, self.stream.read_buf(&mut self.buf)) => {
                    let n = r.map_err(|_| anyhow!("no data for {:?}", self.read_timeout))??;
                    if n == 0 {
                        bail!("connection closed");
                    }
                    self.last_read = Instant::now();
                    false
                }
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(self.next_ping)) => true,
            };
            if ping_due {
                self.next_ping = Instant::now() + self.ping_every;
                self.write(&client_frame(b"", OP_PING)).await?;
            }
        }
    }

    fn data_frame(&mut self, f: Frame, arrival: Instant) -> Result<Option<Message>> {
        match f.opcode {
            OP_TEXT | OP_BINARY => {
                if self.open.is_some() {
                    bail!("data frame inside a fragmented message");
                }
                if f.rsv1 && self.inflater.is_none() {
                    bail!("compressed frame but no compression was negotiated");
                }
                if f.fin {
                    return self.finish(f.rsv1, f.payload, f.wire_len, arrival);
                }
                self.open = Some(OpenMessage { compressed: f.rsv1, acc: f.payload, wire_len: f.wire_len });
                Ok(None)
            }
            _ => {
                let Some(mut o) = self.open.take() else { bail!("continuation frame without a message in progress") };
                o.acc.extend_from_slice(&f.payload);
                o.wire_len += f.wire_len;
                if o.acc.len() > self.max {
                    bail!("message exceeds {} bytes", self.max);
                }
                if f.fin {
                    self.finish(o.compressed, o.acc, o.wire_len, arrival)
                } else {
                    self.open = Some(o);
                    Ok(None)
                }
            }
        }
    }

    fn finish(&mut self, compressed: bool, payload: Vec<u8>, wire_len: usize, arrival: Instant) -> Result<Option<Message>> {
        let data = if compressed {
            let inf = self.inflater.as_mut().ok_or_else(|| anyhow!("compressed frame but no inflater"))?;
            inf.inflate_message(&payload)?
        } else {
            payload
        };
        Ok(Some(Message { data, arrival, decoded: Instant::now(), wire_len }))
    }
}

/// Interpret the server's `Sec-WebSocket-Extensions` reply.
fn negotiated_mode(header: &str, offer: Offer) -> Result<Option<Mode>> {
    for ext in header.split(',') {
        let mut parts = ext.split(';').map(str::trim);
        let Some(name) = parts.next() else { continue };
        let nitro = name.eq_ignore_ascii_case(NITRO_EXTENSION);
        let standard = name.eq_ignore_ascii_case(STANDARD_EXTENSION);
        if !nitro && !standard {
            bail!("server selected an extension that was not offered: {name:?}");
        }
        let offered = match offer {
            Offer::Auto => true,
            Offer::Nitro => nitro,
            Offer::Standard => standard,
            Offer::None => false,
        };
        if !offered {
            bail!("server selected {name:?}, which was not offered");
        }
        let mut server_no_context_takeover = false;
        for p in parts.filter(|p| !p.is_empty()) {
            let key = p.split('=').next().unwrap_or("").trim().to_ascii_lowercase();
            match key.as_str() {
                "server_no_context_takeover" => server_no_context_takeover = true,
                "client_no_context_takeover" | "server_max_window_bits" | "client_max_window_bits" => {}
                other => bail!("unsupported extension parameter {other:?}"),
            }
        }
        return Ok(Some(if nitro { Mode::Nitro } else { Mode::Standard { server_no_context_takeover } }));
    }
    Ok(None)
}

struct Frame {
    fin: bool,
    rsv1: bool,
    opcode: u8,
    payload: Vec<u8>,
    wire_len: usize,
}

/// Take one complete frame off the front of `buf`, if it is all there.
fn parse_frame(buf: &mut BytesMut, max: usize) -> Result<Option<Frame>> {
    if buf.len() < 2 {
        return Ok(None);
    }
    let b0 = buf[0];
    let b1 = buf[1];
    if b0 & 0x30 != 0 {
        bail!("reserved bits RSV2/RSV3 set");
    }
    let masked = b1 & 0x80 != 0;
    let mut len = (b1 & 0x7f) as usize;
    let mut hl = 2usize;
    if len == 126 {
        if buf.len() < 4 {
            return Ok(None);
        }
        len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
        hl = 4;
    } else if len == 127 {
        if buf.len() < 10 {
            return Ok(None);
        }
        let mut b = [0u8; 8];
        b.copy_from_slice(&buf[2..10]);
        len = u64::from_be_bytes(b) as usize;
        hl = 10;
    }
    if masked {
        hl += 4;
    }
    if len > max {
        bail!("frame of {len} bytes exceeds the {max} byte limit");
    }
    if buf.len() < hl + len {
        return Ok(None);
    }
    let mask = if masked { Some([buf[hl - 4], buf[hl - 3], buf[hl - 2], buf[hl - 1]]) } else { None };
    buf.advance(hl);
    let mut payload = buf.split_to(len).to_vec();
    if let Some(m) = mask {
        for (i, b) in payload.iter_mut().enumerate() {
            *b ^= m[i & 3];
        }
    }
    Ok(Some(Frame { fin: b0 & 0x80 != 0, rsv1: b0 & 0x40 != 0, opcode: b0 & 0x0f, payload, wire_len: hl + len }))
}

/// A masked client-to-server frame.
fn client_frame(payload: &[u8], opcode: u8) -> Vec<u8> {
    let n = payload.len();
    let mut mask = [0u8; 4];
    rand::thread_rng().fill_bytes(&mut mask);
    let mut out = Vec::with_capacity(n + 14);
    out.push(0x80 | opcode);
    if n < 126 {
        out.push(0x80 | n as u8);
    } else if n < 65536 {
        out.push(0x80 | 126);
        out.extend_from_slice(&(n as u16).to_be_bytes());
    } else {
        out.push(0x80 | 127);
        out.extend_from_slice(&(n as u64).to_be_bytes());
    }
    out.extend_from_slice(&mask);
    out.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i & 3]));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_parsing() {
        let u = parse_url("wss://feed.example.com").unwrap();
        assert!(u.tls && u.port == 443 && u.path == "/" && u.host == "feed.example.com");
        let u = parse_url("ws://127.0.0.1:9642/ws/abc?x=1").unwrap();
        assert!(!u.tls && u.port == 9642 && u.path == "/ws/abc?x=1");
        assert!(parse_url("ftp://x").is_err());
    }

    #[test]
    fn frames_round_trip_through_the_parser() {
        let payload = vec![7u8; 70_000];
        let mut buf = BytesMut::from(&client_frame(&payload, OP_BINARY)[..]);
        let f = parse_frame(&mut buf, 1 << 20).unwrap().unwrap();
        assert!(f.fin && !f.rsv1 && f.opcode == OP_BINARY && f.payload == payload);
        assert!(buf.is_empty());
        // Incomplete frame: nothing is consumed.
        let mut short = BytesMut::from(&client_frame(b"hello", OP_TEXT)[..8]);
        assert!(parse_frame(&mut short, 1 << 20).unwrap().is_none());
        assert_eq!(short.len(), 8);
    }

    #[test]
    fn extension_negotiation() {
        let m = negotiated_mode("Arbitrum-permessage-deflate; server_no_context_takeover; client_no_context_takeover", Offer::Auto).unwrap();
        assert_eq!(m, Some(Mode::Nitro));
        let m = negotiated_mode("permessage-deflate; server_max_window_bits=15", Offer::Auto).unwrap();
        assert_eq!(m, Some(Mode::Standard { server_no_context_takeover: false }));
        assert!(negotiated_mode("permessage-deflate", Offer::Nitro).is_err());
        assert!(negotiated_mode("x-webkit-deflate-frame", Offer::Auto).is_err());
    }
}
