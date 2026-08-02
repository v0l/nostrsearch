#!/usr/bin/env bash
# Build the operator console.
#
# The output lands at dashboard/dist/index.html, which the server crate pulls
# straight in with include_str! — so this has to run at least once before
# `cargo build` on a fresh clone, and again after any change under dashboard/.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
out="$root/dashboard/dist/index.html"

cd "$root/dashboard"
bun install --frozen-lockfile
bun run build

printf 'wrote %s (%s)\n' "$out" "$(du -h "$out" | cut -f1)"
