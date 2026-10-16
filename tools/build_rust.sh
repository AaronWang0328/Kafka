#!/usr/bin/env bash
set -euo pipefail
task_root="$(cd "$(dirname "$0")/.." && pwd)"
export CARGO_HOME="$task_root/.runtime/cargo"
export RUSTUP_HOME="$task_root/.runtime/rustup"
export PKG_CONFIG_PATH="$task_root/.runtime/native-libs/pkgconfig:/home/aaron/anaconda3/envs/kafka/lib/pkgconfig"
export SQLITE3_LIB_DIR="/home/aaron/anaconda3/envs/kafka/lib"
export SQLITE3_INCLUDE_DIR="/home/aaron/anaconda3/envs/kafka/include"
export RUSTFLAGS="-C link-arg=-Wl,-rpath,/home/aaron/anaconda3/envs/kafka/lib -C link-arg=-Wl,-rpath,/home/aaron/anaconda3/envs/kafka/lib/python3.11/site-packages/confluent_kafka.libs"
export CARGO_TARGET_DIR="$task_root/.runtime/rust-target"
"$CARGO_HOME/bin/cargo" "${AIS_CARGO_COMMAND:-build}" --release --manifest-path "$task_root/src/rust/Cargo.toml" "$@"
