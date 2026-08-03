# syntax=docker/dockerfile:1

# ── Operator console ──────────────────────────────────────────────────────────
# The bundle is a build artifact, never committed, so the image builds it from
# source and hands it to the Rust stage, which compiles it into the binary.
FROM oven/bun:1 AS dashboard
WORKDIR /dash
COPY dashboard/package.json dashboard/bun.lock ./
RUN bun install --frozen-lockfile
COPY dashboard ./
RUN bun run build

# ── Rust dependency cache ─────────────────────────────────────────────────────
# Pre-compile dependencies in isolation; this layer is only invalidated when
# Cargo.toml / Cargo.lock change, not on every source edit.
FROM rust:1-bookworm AS rust-deps
WORKDIR /src
# git is required to fetch the nostr-archive-cursor git dependency.
# clang/libclang are required to build librocksdb-sys (archive id index).
RUN apt-get update && \
    apt-get install -y --no-install-recommends git pkg-config libssl-dev \
        clang libclang-dev llvm-dev && \
    rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY crates/nostrsearch-archive/Cargo.toml    crates/nostrsearch-archive/Cargo.toml
COPY crates/nostrsearch-core/Cargo.toml       crates/nostrsearch-core/Cargo.toml
COPY crates/nostrsearch-indexer/Cargo.toml    crates/nostrsearch-indexer/Cargo.toml
COPY crates/nostrsearch-server/Cargo.toml     crates/nostrsearch-server/Cargo.toml
COPY crates/nostrsearch-stats/Cargo.toml      crates/nostrsearch-stats/Cargo.toml
# Stub out the crates so cargo builds only dependencies.
RUN mkdir -p crates/nostrsearch-archive/src \
             crates/nostrsearch-core/src \
             crates/nostrsearch-indexer/src/bin \
             crates/nostrsearch-server/src/bin \
             crates/nostrsearch-stats/src && \
    echo "" > crates/nostrsearch-archive/src/lib.rs && \
    echo "" > crates/nostrsearch-core/src/lib.rs && \
    echo "" > crates/nostrsearch-indexer/src/lib.rs && \
    echo "fn main() {}" > crates/nostrsearch-indexer/src/bin/ingest.rs && \
    echo "fn main() {}" > crates/nostrsearch-indexer/src/bin/stats.rs && \
    echo "fn main() {}" > crates/nostrsearch-indexer/src/bin/archive.rs && \
    echo "fn main() {}" > crates/nostrsearch-indexer/src/bin/scrape.rs && \
    echo "" > crates/nostrsearch-server/src/lib.rs && \
    echo "fn main() {}" > crates/nostrsearch-server/src/main.rs && \
    echo "" > crates/nostrsearch-stats/src/lib.rs && \
    cargo build --release && \
    rm -f target/release/ingest \
          target/release/stats \
          target/release/archive \
          target/release/scrape \
          target/release/nostrsearch-server \
          target/release/deps/nostrsearch_core-* \
          target/release/deps/nostrsearch_indexer-* \
          target/release/deps/nostrsearch_server-* \
          target/release/deps/nostrsearch_stats-* \
          target/release/deps/libnostrsearch_*

# ── Application build ─────────────────────────────────────────────────────────
FROM rust-deps AS rust-build
COPY crates ./crates
# include_str! target for crates/nostrsearch-server/src/dashboard.rs.
COPY --from=dashboard /dash/dist/index.html dashboard/dist/index.html
# include_str! target for crates/nostrsearch-archive/src/theme.rs: the archive
# listing and the ingest status page serve the console's own stylesheet. The
# bundle above is a single inlined HTML file with no separate CSS asset, so the
# source is what they point at.
COPY dashboard/src/styles.css dashboard/src/styles.css
# Touch entry points so Cargo rebuilds the app crates, not the world.
RUN touch crates/nostrsearch-archive/src/lib.rs \
          crates/nostrsearch-core/src/lib.rs \
          crates/nostrsearch-indexer/src/lib.rs \
          crates/nostrsearch-server/src/lib.rs \
          crates/nostrsearch-stats/src/lib.rs && \
    cargo build --release && \
    mkdir -p /app/bin && \
    cp target/release/ingest /app/bin/ingest && \
    cp target/release/stats /app/bin/stats && \
    cp target/release/archive /app/bin/archive && \
    cp target/release/scrape /app/bin/scrape && \
    cp target/release/nostrsearch-server /app/bin/nostrsearch-server

# ── Runtime image ─────────────────────────────────────────────────────────────
FROM debian:trixie-slim
LABEL org.opencontainers.image.source="https://github.com/v0l/nostrsearch"
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.authors="Kieran"
WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates libssl3 && \
    rm -rf /var/lib/apt/lists/* && \
    useradd -r -u 10001 -m nostrsearch

COPY --from=rust-build /app/bin ./bin

# All state lives under the mounted volume; the defaults must be writable by
# the non-root user, so point them at /data rather than the CWD-relative
# defaults the binaries use outside a container.
ENV INDEX_ROOT=/data/index \
    STATE_DIR=/data/stats \
    WOT_OUT=/data/wot.bin \
    BIND=0.0.0.0:8080 \
    RUST_LOG=nostrsearch=info,tower_http=info
VOLUME ["/data"]
EXPOSE 8080

USER nostrsearch
ENTRYPOINT ["./bin/nostrsearch-server"]
