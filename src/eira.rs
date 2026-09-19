//! Source adapter for the Eira Pulse gRPC stream (`proto/pulse.proto`): a bidirectional
//! `Subscribe` with one `replace` request sent immediately and a ping every 15 s. Every update
//! is stamped inside the codec, when its bytes are complete and before protobuf decoding.

use crate::event::{Event, Payload};
use anyhow::{Result, anyhow, bail};
use prost::Message;
use std::collections::HashSet;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{self, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tonic::client::Grpc;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::codegen::http::uri::PathAndQuery;
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::{Request, Status};

pub mod pb {
    tonic::include_proto!("robin.pulse.v1");
}

use pb::subscribe_request::Action;
use pb::subscribe_update::Update;

/// Synthetic log marker on RECEIVED events: a predicted swap.
const SYNTHETIC_SWAP: u32 = 0xFFFF_FFFE;
const SUBSCRIBE_PATH: &str = "/robin.pulse.v1.Pulse/Subscribe";

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Level {
    Received,
    Processed,
    Confirmed,
}

impl Level {
    pub fn commitment(self) -> pb::Commitment {
        match self {
            Level::Received => pb::Commitment::Received,
            Level::Processed => pb::Commitment::Processed,
            Level::Confirmed => pb::Commitment::Confirmed,
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Level::Received => "RECEIVED",
            Level::Processed => "PROCESSED",
            Level::Confirmed => "CONFIRMED",
        }
    }
}

/// An update together with the instant its bytes were complete, before decoding.
pub struct Stamped {
    pub t: Instant,
    pub update: pb::SubscribeUpdate,
}

#[derive(Default, Clone)]
pub struct StampCodec;

pub struct RequestEncoder;
pub struct StampDecoder;

impl Codec for StampCodec {
    type Encode = pb::SubscribeRequest;
    type Decode = Stamped;
    type Encoder = RequestEncoder;
    type Decoder = StampDecoder;
    fn encoder(&mut self) -> RequestEncoder {
        RequestEncoder
    }
    fn decoder(&mut self) -> StampDecoder {
        StampDecoder
    }
}

impl Encoder for RequestEncoder {
    type Item = pb::SubscribeRequest;
    type Error = Status;
    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        item.encode(dst).map_err(|e| Status::internal(e.to_string()))
    }
}

impl Decoder for StampDecoder {
    type Item = Stamped;
    type Error = Status;
    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Stamped>, Status> {
        let t = Instant::now();
        let update = pb::SubscribeUpdate::decode(src).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Some(Stamped { t, update }))
    }
}

pub async fn run(index: usize, addr: String, level: Level, out: Sender<Event>) {
    let mut backoff = Duration::from_secs(1);
    loop {
        match session(index, &addr, level, &out).await {
            Ok(()) => return,
            Err(e) => {
                eprintln!("[eira] {e}; next attempt in {} s", backoff.as_secs());
                if out.send(Event { source: index, t: Instant::now(), payload: Payload::Disconnected { reason: format!("{e}") } }).await.is_err() {
                    return;
                }
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

/// One subscription; returns Ok only when the collector went away.
async fn session(index: usize, addr: &str, level: Level, out: &Sender<Event>) -> Result<()> {
    let endpoint = Channel::from_shared(format!("https://{addr}"))?
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .tcp_nodelay(true)
        .connect_timeout(Duration::from_secs(10));
    let channel = endpoint.connect().await?;
    let mut grpc = Grpc::new(channel).max_decoding_message_size(64 << 20);
    grpc.ready().await?;

    let (req_tx, req_rx) = mpsc::channel::<pb::SubscribeRequest>(8);
    // The server closes a stream that has not sent a replace within a few seconds: queue it
    // before the call so it leaves with the first request frame.
    let replace = pb::SubscribeRequest {
        request_id: 1,
        action: Some(Action::Replace(pb::Subscription {
            transactions: [("all".to_string(), pb::TransactionFilter::default())].into(),
            commitment: level.commitment() as i32,
        })),
    };
    req_tx.send(replace).await.map_err(|_| anyhow!("request channel closed"))?;
    let response = grpc.streaming(Request::new(ReceiverStream::new(req_rx)), PathAndQuery::from_static(SUBSCRIBE_PATH), StampCodec).await?;
    let mut stream = response.into_inner();
    let pinger = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(15));
        tick.tick().await;
        let mut n = 0u64;
        loop {
            tick.tick().await;
            n += 1;
            if req_tx.send(pb::SubscribeRequest { request_id: 0, action: Some(Action::Ping(n)) }).await.is_err() {
                break;
            }
        }
    });

    let result = async {
        let mut announced = false;
        let mut seen_blocks: HashSet<String> = HashSet::new();
        let wanted = level.commitment() as i32;
        loop {
            let Some(s) = stream.message().await? else { bail!("stream ended") };
            match s.update.update {
                Some(Update::Ack(ack)) => {
                    if !announced {
                        announced = true;
                        let detail = format!("level={} filters={}", level.name(), ack.filter_count);
                        if out.send(Event { source: index, t: s.t, payload: Payload::Connected { detail } }).await.is_err() {
                            return Ok(());
                        }
                    }
                }
                Some(Update::SourceStatus(st)) => {
                    if st.commitment == wanted && announced {
                        let payload = if st.connected {
                            Payload::Connected { detail: format!("level={} upstream reconnected (epoch {})", level.name(), st.source_epoch) }
                        } else {
                            Payload::Disconnected { reason: format!("upstream source for {} disconnected", level.name()) }
                        };
                        if out.send(Event { source: index, t: s.t, payload }).await.is_err() {
                            return Ok(());
                        }
                    }
                }
                Some(Update::Transaction(t)) => {
                    if t.transaction_hash.len() != 32 || t.commitment != wanted {
                        continue;
                    }
                    let mut hash = [0u8; 32];
                    hash.copy_from_slice(&t.transaction_hash);
                    let block_key = if t.feed_sequence > 0 { format!("seq:{}", t.feed_sequence) } else { format!("num:{}", t.block_number) };
                    if seen_blocks.len() > 100_000 {
                        seen_blocks.clear();
                    }
                    if seen_blocks.insert(block_key.clone()) {
                        let seq = (t.feed_sequence > 0).then_some(t.feed_sequence);
                        let block = Event { source: index, t: s.t, payload: Payload::Block { key: block_key.clone(), seq, hash: None, tx_count: None, wire_len: 0 } };
                        if out.send(block).await.is_err() {
                            return Ok(());
                        }
                    }
                    let synthetic_swap = t.logs.iter().any(|l| l.transaction_log_index == SYNTHETIC_SWAP);
                    let ev = Event {
                        source: index,
                        t: s.t,
                        payload: Payload::Tx { hash, block_key: Some(block_key), tx_count: None, synthetic_swap, raw_len: t.raw_tx.len() as u32 },
                    };
                    if out.send(ev).await.is_err() {
                        return Ok(());
                    }
                }
                _ => {}
            }
        }
    }
    .await;
    pinger.abort();
    result
}
