# feedbench

Latency benchmark for Robinhood Chain (Arbitrum Nitro, chain id 4663, ~100 ms blocks) block and
transaction feeds. It connects to several sources at once from one machine, stamps every message
on arrival, matches the same block and the same transaction across sources, and prints who was
first and by how much.

Sources:

- `--source NAME=wss://...` — any Nitro-format broadcast feed (the official
  `wss://feed.mainnet.chain.robinhood.com`, third-party feeders in the same format). Repeatable.
- `--eira HOST:PORT` — the Eira Pulse gRPC stream (`proto/pulse.proto`).
- `--wsjson SPEC` — any JSON WebSocket feed that carries a tx hash or a raw signed tx at a known
  JSON path. Repeatable.
- `--wsraw SPEC` — any binary WebSocket feed whose frames carry signed transactions back to back
  (typed envelope or legacy RLP, optionally length-prefixed); text frames fall back to scanning
  JSON for hashes or raw transactions. `NAME=wss://url[,header=K:V]`. Repeatable.

The block-level table reproduces the methodology of BlockRazor's
[robinhood-feed-speed](https://github.com/HYPERLIQUIDATED/robinhood-feed-speed) so its numbers are
comparable. The transaction-level tables are what a consumer of a transaction feed needs.

## Install

Prebuilt binaries for Linux (x86_64, aarch64) and macOS (Apple Silicon, Intel) are attached to
every [release](../../releases) as `feedbench-<version>-<target>.tar.gz` with a SHA-256 sum next
to it. Unpack and run `./feedbench --help`. macOS may quarantine an unsigned download; clear it
with `xattr -d com.apple.quarantine feedbench`.

### Verifying a binary

Every archive is built by `.github/workflows/release.yml` on GitHub-hosted runners, and GitHub
signs a [build provenance attestation](https://docs.github.com/en/actions/security-for-github-actions/using-artifact-attestations)
for it: a statement, recorded in the Sigstore transparency log, that a file with this hash was
produced by that workflow from that commit. The repository owner cannot forge or alter it, so a
replaced asset fails the check:

```
gh attestation verify feedbench-0.1.0-x86_64-unknown-linux-gnu.tar.gz --owner qskateboard
```

If you would rather not trust any binary, build from source (below): the toolchain is pinned in
`rust-toolchain.toml` and `scripts/build-release.sh` strips local paths, so a Linux build of the
same commit is byte-identical to the released one and the SHA-256 sums can be compared directly.

## Build

Rust 1.88 or newer. `protoc` is vendored (`protoc-bin-vendored`), nothing else is needed. The Pulse
schema comes from [pulse-proto](https://github.com/qskateboard/pulse-proto) as a git submodule at
`proto/`, so clone with `--recurse-submodules` (or run `git submodule update --init`).

```
cargo build --release
./target/release/feedbench --help
```

`scripts/build-release.sh` builds the same binary with `--remap-path-prefix` for the checkout,
the Cargo registry and the toolchain, then checks with `strings` that no local path is left in
it. The release workflow uses it; use it too before sharing a binary you built yourself, because
a plain `cargo build` embeds the absolute paths of your home directory in panic messages.

## Run

Official feed against Eira, five minutes:

```
feedbench --source official=wss://feed.mainnet.chain.robinhood.com \
          --eira pulse.eiranodes.dev:443
```

Official feed, Eira and a BlockRazor feeder (put your token in the URL):

```
feedbench --source official=wss://feed.mainnet.chain.robinhood.com \
          --source blockrazor=wss://us.robinhood-feeder.blockrazor.io/ws/{token} \
          --eira pulse.eiranodes.dev:443 --seconds 600 --json run.json
```

A generic JSON WebSocket feed. The shape below is an **illustration of the option syntax only**;
it is not the format of any particular vendor. Look up the vendor's message layout and point
`path` at the field that holds the tx hash (`0x` + 64 hex) or the raw signed transaction (hashed
with keccak256):

```
feedbench --eira pulse.eiranodes.dev:443 \
          --wsjson 'other=wss://example.invalid/stream,path=params.result.hash,header=Authorization:Bearer TOKEN,subscribe={"method":"subscribe","params":["newRawTransactions"]}'
```

`path` syntax: `a.b.c` descends into objects, `a[]` fans out over every element of an array,
`a[2]` picks one element, a leading `[]` applies to the message itself (`[].hash`).
`subscribe=` must be the last option because its value runs to the end of the string.

Other options:

| option | default | meaning |
|---|---|---|
| `--seconds N` | 300 | run length; Ctrl-C stops early and prints what was collected |
| `--eira-level received\|processed\|confirmed` | received | Eira commitment level |
| `--eira http://HOST:PORT` | | plaintext gRPC instead of TLS, for a Pulse on the same host |
| `--min-sources N` | all | a block/tx counts as matched once N sources delivered it |
| `--warmup S` | 5 | seconds skipped after every source is connected (backlog replay) |
| `--summary-interval S` | 300 | rolling block-level summary period |
| `--stamp arrival\|decoded` | arrival | see "Timestamps" |
| `--compression auto\|nitro\|standard\|none` | auto | extension offered to Nitro feeds |
| `--json PATH` | | write everything (header, both levels, pairs, intervals, buckets) as JSON |
| `--print-blocks` | off | one line per completed block, the Go tool's `[block]` format |
| `--host LABEL` | hostname | what the report's `host` line shows, for screenshots that should not name the machine |

## Nitro feed details

The client sends `Arbitrum-Feed-Client-Version: 2`, omits `Arbitrum-Requested-Sequence-Number`
on the first connection (the server picks the starting point) and reconnects with
`last seen + 1`. It offers `Arbitrum-permessage-deflate` — Nitro's per-message deflate with a
static dictionary and a fresh context per message — and, with `--compression auto`, plain
`permessage-deflate` as a fallback; the negotiated mode is printed on connect. Reconnects are
counted per source and printed in the header.

Each feed message is one block. The tool emits a block event keyed by the feed sequence number
(`seq:N`; on a Nitro feed a sequence number identifies exactly one block, and Eira reports the
same number as `feed_sequence`, so the two can be matched; the Go tool keys by `blockHash`, which
is one-to-one with the sequence number) and one transaction event per signed transaction in the
`l2Msg` (`kind 3` batch of `[u64 length][sub-message]`, sub-message `kind 4` = signed tx; `kind 4`
at the top level = one signed tx). The tx hash is keccak256 of the signed bytes.

For Eira, the block event is the first transaction of that block, so in the block-level table
Eira's "block arrival" means "first transaction of the block delivered"; a block with no
transactions never appears from Eira.

## Timestamps

Every source is stamped when its message is complete in this process's memory and before any
decoding: for WebSocket feeds when the last byte of the frame has been read (before inflating and
JSON parsing), for Eira inside the gRPC codec before protobuf decoding. This is the fairness rule
of the tool: Eira's events are already decoded transactions, the raw feeds still have to be
inflated, parsed, base64-decoded and hashed by the consumer, and none of that work is charged to
either side.

The Go reference tool stamps after inflating and before JSON parsing. `--stamp decoded` uses that
instant for the WebSocket sources so the block-level table matches its rule exactly; the
difference is the inflate time, well under a millisecond for a typical frame.

All stamps come from one monotonic clock on one machine. The tool makes no claim about
cross-machine clocks.

## Reading the tables

The header gives UTC start/end, duration, hostname, each source with its negotiated compression
or level, reconnect counts, event counts, and how long every source was connected at the same
time. Only blocks and transactions first seen inside that window (after the warm-up) are counted.

**A. Block level** (comparable with the Go tool). A block is completed when every source (or
`--min-sources`) delivered it. Per source: `wins` and `win%` (how often it was first), the delay
behind the winner at p10/p50/p75/p90/p95/p99/p99.9/max in ms (nearest-rank percentiles, as in the
Go tool; the winner's own delay is 0), and the number of completed blocks it took part in. A
rolling summary of the same shape is printed every `--summary-interval` seconds, then the whole
run at the end.

**B. Transaction level.** Key = tx hash. `seen` and `coverage` count distinct transactions inside
the window that the source delivered; `first` is the share of matched transactions where the
source was earliest; `first>=1ms` requires a lead of at least 1 ms over every other source; the
lag columns are the delay behind the earliest source in ms. `flagged swaps` counts transactions
that Eira marked as a predicted swap.

**C. Pairs.** Eira (or the first source when there is no Eira) against every other source, over
transactions both delivered. `median`, `p10`, `p90` are the difference `other - reference` in ms,
so positive means the reference was earlier. Then: share where the reference was first, share
within ±1 ms (a tie for practical purposes), and shares where either side led by more than 5 ms.
When the run is longer than one summary interval, the median per interval follows, so a drift in
the relation is visible. Finally the block-size breakdown.

### Why block size matters

A Nitro feed delivers a block as one message, so every transaction of a 60-tx block reaches the consumer
at the same instant, after the whole message has been inflated and parsed. A per-transaction stream is not
bound to that boundary, and its lead therefore depends on how many transactions the block has; a single
median hides that. The buckets (1–5, 6–20, 21–50, >50 txs per block) show the share of blocks and of
transactions in each bucket, the median, p10 and p90 of the lead and the first% inside the bucket. The
transaction count comes from the Nitro message that carried the block when a Nitro source is present,
otherwise from the reference source's own per-block count.

## What the tool does not do

- It does not compare clocks across machines. Every number is a difference between two arrivals
  on one host, taken from one monotonic clock. Where you run it (region, provider, network path)
  changes the numbers; the header records the hostname so a screenshot says where it was taken.
- It does not treat `PROCESSED` as a raw feed. `--eira-level processed` delivers executed
  transactions (with real logs), which is a different product from a raw transaction feed; the
  two are not like for like and should not be read against each other, exactly as
  [eiranodes.dev/docs/benchmarks](https://eiranodes.dev/docs/benchmarks) says. Compare raw with
  raw (`RECEIVED` against Nitro feeds and raw-tx feeds) and executed with executed.
- It does not verify feed signatures, check sequence gaps, or persist a cursor across runs.
- It does not know third-party wire formats. `--wsjson` extracts a hash or raw tx from a JSON
  path you supply; the example above is not a description of any vendor's protocol.
- Startup and reconnect catch-up: a Nitro server may replay a backlog after connect, and a
  transaction stream has no replay, so the first seconds are skipped (`--warmup`) and only
  stretches where every source was connected are counted.

## Licenses

The code is MIT (`LICENSE`). `third_party/nitro/dictionary.bin` is the static compression
dictionary of Arbitrum Nitro's `wsbroadcastserver`, copied unchanged from
[OffchainLabs/nitro at a6181559](https://github.com/OffchainLabs/nitro/blob/a618155919315241665356fe60f3cd00d66d5e46/wsbroadcastserver/dictionary.go);
it is Copyright 2023-2026 Offchain Labs, Inc. and licensed under the Business Source License 1.1
(`third_party/nitro/LICENSE.md`), not under this crate's MIT license. A Nitro feed compressed with
`Arbitrum-permessage-deflate` cannot be inflated without it.

## Running it more than once on a host

Both sides limit connections per address: the official feed answers `429 Too Many Requests`
when an address already holds its allowed connections (the tool then waits `Retry-After` or
45 s before dialing again), and an Eira plan allows one `RECEIVED` stream at a time (`resource
exhausted: one RECEIVED stream per plan`). Run one benchmark per host and stop other consumers
of the same feeds on that host first, otherwise a source stays disconnected and the tables
stay empty.

`EXAMPLE_OUTPUT.md` holds three captured runs (host names replaced, numbers as printed),
including one where a source could not connect.

## Releasing

Push a tag `vX.Y.Z`: `.github/workflows/release.yml` builds the four targets with
`scripts/build-release.sh`, packages each with `README.md`, `LICENSE` and `third_party/`, and
attaches the archives and their SHA-256 sums to a GitHub release. `ci.yml` runs the tests on
Linux and macOS for every push.
