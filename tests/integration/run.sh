#!/bin/sh
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
docker compose -f "$repo/compose.yaml" up -d --wait
cleanup() { docker compose -f "$repo/compose.yaml" down -v; }
trap cleanup EXIT INT TERM
RUSTCLAMP_TEST_DATABASE_URL=postgres://rustclamp_test:rustclamp_test@127.0.0.1:55432/rustclamp_test \
  cargo test --manifest-path "$repo/Cargo.toml" --test postgres -- --ignored
users_manifest="$repo/../rustclamp/examples/06-users/Cargo.toml"
cargo test --manifest-path "$users_manifest" --features postgres --test postgres -- --ignored
