//! Events every source adapter sends into the single collector channel.

use std::time::Instant;

#[derive(Debug, Clone)]
pub enum Payload {
    /// One block (one feed message) became visible on this source.
    Block {
        /// Matching key shared by all sources: `seq:<feed sequence number>` when the source
        /// reports one, otherwise `num:<block number>`.
        key: String,
        seq: Option<u64>,
        hash: Option<String>,
        /// Signed transactions carried by the feed message (Nitro sources only).
        tx_count: Option<u32>,
        /// Bytes of the WebSocket message that carried it (raw feeds only).
        wire_len: u64,
    },
    /// One signed transaction became visible on this source.
    Tx {
        hash: [u8; 32],
        /// Block key (see `Block::key`) when the source reports which block carried it.
        block_key: Option<String>,
        /// Transactions in the feed message that carried it (Nitro sources only).
        tx_count: Option<u32>,
        /// The source flagged the transaction as a predicted swap.
        synthetic_swap: bool,
        raw_len: u32,
    },
    /// The source is delivering data (a reconnect after the first one counts as a reconnect).
    Connected { detail: String },
    Disconnected { reason: String },
}

#[derive(Debug, Clone)]
pub struct Event {
    pub source: usize,
    /// Monotonic arrival stamp, taken when the message reached this process and before decoding.
    pub t: Instant,
    pub payload: Payload,
}

/// Which instant a raw-feed source stamps its events with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Stamp {
    /// When the complete WebSocket message is in memory, before inflating and parsing it.
    Arrival,
    /// After inflating, before JSON parsing (the instant the Go reference tool stamps).
    Decoded,
}
