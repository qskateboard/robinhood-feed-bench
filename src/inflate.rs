//! Per-message deflate for WebSocket feeds: the standard `permessage-deflate` extension
//! (RFC 7692) and the Nitro broadcast feed's `Arbitrum-permessage-deflate`, which uses the same
//! framing but compresses every message independently against a preset static dictionary.

use anyhow::{Result, anyhow, bail};
use flate2::{Decompress, FlushDecompress, Status};

/// Static dictionary of Arbitrum Nitro's `wsbroadcastserver` (see `third_party/nitro/README.md`).
pub const NITRO_DICTIONARY: &[u8] = include_bytes!("../third_party/nitro/dictionary.bin");

/// The empty stored block that ends a permessage-deflate message and that the sender strips
/// (RFC 7692 section 7.2.1).
pub const TAIL: [u8; 4] = [0x00, 0x00, 0xff, 0xff];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// `Arbitrum-permessage-deflate`: fresh context and the static dictionary for every message.
    Nitro,
    /// RFC 7692 `permessage-deflate`.
    Standard { server_no_context_takeover: bool },
}

impl Mode {
    pub fn name(&self) -> &'static str {
        match self {
            Mode::Nitro => "Arbitrum-permessage-deflate",
            Mode::Standard { .. } => "permessage-deflate",
        }
    }
}

pub struct Inflater {
    de: Decompress,
    mode: Mode,
    max: usize,
    /// The previous message ended the DEFLATE stream, so the next one needs a fresh context.
    ended: bool,
}

impl Inflater {
    pub fn new(mode: Mode, max: usize) -> Result<Inflater> {
        let mut inf = Inflater { de: Decompress::new(false), mode, max, ended: false };
        inf.reset()?;
        Ok(inf)
    }

    fn reset(&mut self) -> Result<()> {
        self.de.reset(false);
        if self.mode == Mode::Nitro {
            self.de.set_dictionary(NITRO_DICTIONARY).map_err(|e| anyhow!("set dictionary: {e}"))?;
        }
        self.ended = false;
        Ok(())
    }

    /// Inflate one complete message (all fragments concatenated, tail stripped).
    pub fn inflate_message(&mut self, z: &[u8]) -> Result<Vec<u8>> {
        let fresh = match self.mode {
            Mode::Nitro => true,
            Mode::Standard { server_no_context_takeover } => server_no_context_takeover,
        };
        if fresh || self.ended {
            self.reset()?;
        }
        let mut out = Vec::with_capacity(z.len().saturating_mul(4).max(4096));
        if !self.run(z, &mut out)? {
            self.run(&TAIL, &mut out)?;
        }
        Ok(out)
    }

    /// Push `input` through the inflater; returns true when the stream reached its final block.
    fn run(&mut self, mut input: &[u8], out: &mut Vec<u8>) -> Result<bool> {
        loop {
            if out.capacity() - out.len() < 4096 {
                out.reserve(32 * 1024);
            }
            let before_in = self.de.total_in();
            let before_out = self.de.total_out();
            let status = self.de.decompress_vec(input, out, FlushDecompress::None).map_err(|e| anyhow!("inflate: {e}"))?;
            let consumed = (self.de.total_in() - before_in) as usize;
            let produced = (self.de.total_out() - before_out) as usize;
            input = &input[consumed..];
            if out.len() > self.max {
                bail!("inflated message exceeds {} bytes", self.max);
            }
            if status == Status::StreamEnd {
                self.ended = true;
                return Ok(true);
            }
            if input.is_empty() && produced == 0 {
                return Ok(false);
            }
            if consumed == 0 && produced == 0 {
                bail!("inflate made no progress");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{Compress, Compression, FlushCompress};

    /// Compress `plain` the way a permessage-deflate sender does: one sync-flushed block, tail
    /// removed, optionally against the Nitro dictionary.
    fn deflate_message(plain: &[u8], dictionary: Option<&[u8]>) -> Vec<u8> {
        let mut c = Compress::new(Compression::best(), false);
        if let Some(d) = dictionary {
            c.set_dictionary(d).unwrap();
        }
        let mut out = Vec::with_capacity(plain.len() + 64);
        c.compress_vec(plain, &mut out, FlushCompress::Sync).unwrap();
        assert!(out.ends_with(&TAIL), "sync flush must end with the empty stored block");
        out.truncate(out.len() - TAIL.len());
        out
    }

    const SAMPLE: &[u8] = br#"{"version":1,"messages":[{"sequenceNumber":4242,"message":{"message":{"header":{"kind":3,"sender":"0xa4b000000000000000000073657175656e636572","blockNumber":23456789,"timestamp":1760000000,"requestId":null,"baseFeeL1":null},"l2Msg":"AwAAAAAAAAAB"},"delayedMessagesRead":17},"signature":null}]}"#;

    #[test]
    fn nitro_dictionary_is_the_vendored_blob() {
        assert_eq!(NITRO_DICTIONARY.len(), 20023);
        assert!(NITRO_DICTIONARY.starts_with(b"ADDUAtcQ"));
        assert!(NITRO_DICTIONARY.ends_with(b"\"signature\":null}]}\n"));
    }

    #[test]
    fn nitro_round_trip_with_dictionary() {
        let z = deflate_message(SAMPLE, Some(NITRO_DICTIONARY));
        let mut inf = Inflater::new(Mode::Nitro, 1 << 20).unwrap();
        assert_eq!(inf.inflate_message(&z).unwrap(), SAMPLE);
        // A second message on the same inflater must start from the dictionary again.
        let z2 = deflate_message(b"second message", Some(NITRO_DICTIONARY));
        assert_eq!(inf.inflate_message(&z2).unwrap(), b"second message");
        assert_eq!(inf.inflate_message(&z).unwrap(), SAMPLE);
    }

    #[test]
    fn dictionary_actually_matters() {
        let z = deflate_message(SAMPLE, Some(NITRO_DICTIONARY));
        let plain = deflate_message(SAMPLE, None);
        assert!(z.len() < plain.len(), "the dictionary should shrink a feed-shaped message");
        let mut inf = Inflater::new(Mode::Standard { server_no_context_takeover: true }, 1 << 20).unwrap();
        // Without the dictionary the back-references point before the start of the stream.
        assert!(inf.inflate_message(&z).map(|v| v != SAMPLE).unwrap_or(true));
    }

    #[test]
    fn standard_round_trip_with_and_without_context_takeover() {
        let mut inf = Inflater::new(Mode::Standard { server_no_context_takeover: true }, 1 << 20).unwrap();
        for _ in 0..3 {
            assert_eq!(inf.inflate_message(&deflate_message(SAMPLE, None)).unwrap(), SAMPLE);
        }
        // Context takeover: the sender keeps its window across messages, the receiver must too.
        let mut c = Compress::new(Compression::default(), false);
        let mut msgs = Vec::new();
        for i in 0..3 {
            let plain = format!("{} #{i}", std::str::from_utf8(SAMPLE).unwrap());
            let mut out = Vec::with_capacity(plain.len() + 64);
            c.compress_vec(plain.as_bytes(), &mut out, FlushCompress::Sync).unwrap();
            assert!(out.ends_with(&TAIL));
            out.truncate(out.len() - TAIL.len());
            msgs.push((plain, out));
        }
        let mut inf = Inflater::new(Mode::Standard { server_no_context_takeover: false }, 1 << 20).unwrap();
        for (plain, z) in &msgs {
            assert_eq!(inf.inflate_message(z).unwrap(), plain.as_bytes());
        }
    }
}
