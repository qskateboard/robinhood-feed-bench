//! Nitro L2 message layout: the `l2Msg` of a feed message is one L2 message whose first byte is
//! its kind. Kind 3 is a batch of `[u64 big-endian length][sub-message]` entries, kind 4 is a
//! signed transaction (the bytes `eth_sendRawTransaction` takes: a typed envelope or a legacy RLP
//! list). Transaction hash = keccak256 of those bytes.

use sha3::{Digest, Keccak256};

pub const KIND_BATCH: u8 = 3;
pub const KIND_SIGNED_TX: u8 = 4;

pub fn keccak256(b: &[u8]) -> [u8; 32] {
    let mut h = Keccak256::new();
    h.update(b);
    h.finalize().into()
}

/// Every signed transaction carried by an L2 message, in order. Nested batches are walked;
/// other kinds are skipped. Truncated or oversized entries end the walk.
pub fn signed_txs(l2: &[u8]) -> Vec<&[u8]> {
    let mut out = Vec::new();
    walk(l2, &mut out, 0);
    out
}

fn walk<'a>(l2: &'a [u8], out: &mut Vec<&'a [u8]>, depth: u8) {
    let Some(&kind) = l2.first() else { return };
    match kind {
        KIND_SIGNED_TX => {
            if l2.len() > 1 {
                out.push(&l2[1..]);
            }
        }
        KIND_BATCH => {
            if depth > 4 {
                return;
            }
            let mut pos = 1usize;
            while pos + 8 <= l2.len() {
                let n = u64::from_be_bytes(l2[pos..pos + 8].try_into().expect("8 bytes")) as usize;
                pos += 8;
                if n > 16 << 20 || pos + n > l2.len() {
                    return;
                }
                walk(&l2[pos..pos + n], out, depth + 1);
                pos += n;
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rlp_bytes(b: &[u8]) -> Vec<u8> {
        if b.len() == 1 && b[0] < 0x80 {
            return b.to_vec();
        }
        let mut v = vec![0x80 + b.len() as u8];
        v.extend_from_slice(b);
        v
    }

    fn rlp_list(items: &[Vec<u8>]) -> Vec<u8> {
        let payload: Vec<u8> = items.iter().flat_map(|i| rlp_bytes(i)).collect();
        let mut v = vec![0xc0 + payload.len() as u8];
        v.extend(payload);
        v
    }

    fn entry(sub: &[u8]) -> Vec<u8> {
        let mut v = (sub.len() as u64).to_be_bytes().to_vec();
        v.extend_from_slice(sub);
        v
    }

    #[test]
    fn keccak_vector() {
        assert_eq!(hex::encode(keccak256(b"")), "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470");
    }

    #[test]
    fn batch_parser_finds_signed_txs_and_skips_the_rest() {
        // legacy tx: [nonce, gasPrice, gas, to, value, data, v, r, s]
        let legacy = rlp_list(&[vec![1], vec![0x3b, 0x9a, 0xca, 0x00], vec![0x52, 0x08], vec![0xaa; 20], vec![0x0a], vec![], vec![0x25], vec![0x11; 32], vec![0x22; 32]]);
        // EIP-1559 tx: 0x02 || rlp([...])
        let mut typed = vec![0x02];
        typed.extend(rlp_list(&[vec![0x12, 0x37], vec![2], vec![1], vec![2], vec![0x52, 0x08], vec![0xbb; 20], vec![], vec![0xde, 0xad], vec![], vec![1], vec![0x33; 32], vec![0x44; 32]]));
        let mut frame = vec![KIND_BATCH];
        frame.extend(entry(&[&[KIND_SIGNED_TX][..], &legacy].concat()));
        frame.extend(entry(&[0x06])); // heartbeat: not a transaction
        frame.extend(entry(&[&[KIND_SIGNED_TX][..], &typed].concat()));
        // a nested batch with one more signed tx
        let mut nested = vec![KIND_BATCH];
        nested.extend(entry(&[&[KIND_SIGNED_TX][..], &legacy].concat()));
        frame.extend(entry(&nested));

        let txs = signed_txs(&frame);
        assert_eq!(txs.len(), 3);
        assert_eq!(txs[0], &legacy[..]);
        assert_eq!(txs[1], &typed[..]);
        assert_eq!(txs[2], &legacy[..]);
        assert_eq!(keccak256(txs[1]), keccak256(&typed));

        // single signed tx at top level
        let single = [&[KIND_SIGNED_TX][..], &typed].concat();
        assert_eq!(signed_txs(&single), vec![&typed[..]]);
        // truncated length prefix ends the walk without panicking
        let mut truncated = frame.clone();
        truncated.truncate(frame.len() - 5);
        assert_eq!(signed_txs(&truncated).len(), 2);
        assert!(signed_txs(&[]).is_empty());
        assert!(signed_txs(&[0x00, 1, 2, 3]).is_empty());
    }
}
