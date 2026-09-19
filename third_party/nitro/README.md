# Nitro static compression dictionary

`dictionary.bin` holds, byte for byte, the static dictionary that Arbitrum Nitro's
`wsbroadcastserver` uses for the `Arbitrum-permessage-deflate` WebSocket extension.

- Source: https://github.com/OffchainLabs/nitro/blob/a618155919315241665356fe60f3cd00d66d5e46/wsbroadcastserver/dictionary.go
  (the body of the Go raw string returned by `GetStaticCompressorDictionary`, 20023 bytes,
  sha256 `d96272e5e58bd299c3466bc2c519e210afd192d897a4456c4c88322f8d563fa9`).
- Copyright 2023-2026, Offchain Labs, Inc.
- License: Business Source License 1.1, see `LICENSE.md` in this directory. The dictionary is
  not covered by the MIT license of the rest of this crate.

The dictionary must not be modified: a feed compressed with it can only be inflated with the
identical bytes.
