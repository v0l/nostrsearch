#!/usr/bin/env bash
# Build the operator console and drop the single-file bundle where the server
# crate includes it at compile time.
#
# The output is committed, so a plain `cargo build` never needs bun installed.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
out="$root/crates/nostrsearch-server/assets/dashboard.html"

cd "$root/dashboard"
bun install --frozen-lockfile
bun run build

mkdir -p "$(dirname "$out")"
cp dist/index.html "$out"

printf 'wrote %s (%s)\n' "$out" "$(du -h "$out" | cut -f1)"
