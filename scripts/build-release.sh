#!/usr/bin/env sh
# Release build that leaves no local paths in the binary.
#
# `strip = true` in Cargo.toml drops the symbol table, but panic locations and a few assertion
# messages keep the absolute paths of the source files they came from: the crate registry under
# $CARGO_HOME, the toolchain under $RUSTUP_HOME and this checkout. Those paths carry the user
# and machine name. `--remap-path-prefix` replaces them at compile time; the flags below cover
# every directory rustc can embed. Build with this script (or with the same RUSTFLAGS) before
# handing a binary to anyone.
#
#   scripts/build-release.sh [--target TRIPLE]
set -eu
cd "$(dirname "$0")/.."
SRC=$(pwd)
CARGO_DIR=${CARGO_HOME:-$HOME/.cargo}
RUSTUP_DIR=${RUSTUP_HOME:-$HOME/.rustup}
# rustc uses the last matching prefix, so the broad $HOME rule goes first and the specific ones after it
export RUSTFLAGS="${RUSTFLAGS:-} \
 --remap-path-prefix=$HOME=/home \
 --remap-path-prefix=$SRC=/feedbench \
 --remap-path-prefix=$CARGO_DIR=/cargo \
 --remap-path-prefix=$RUSTUP_DIR=/rustup"
cargo build --release --locked "$@"
BIN=target/release/feedbench
prev=""
for a in "$@"; do case $prev in --target) BIN=target/$a/release/feedbench;; esac; prev=$a; done
if command -v strings >/dev/null 2>&1; then
  if strings "$BIN" | grep -qE "^$HOME|$HOME/|$SRC"; then
    echo "local paths remain in $BIN:" >&2
    strings "$BIN" | grep -E "$HOME|$SRC" | head >&2
    exit 1
  fi
fi
echo "built $BIN (no local paths)"
