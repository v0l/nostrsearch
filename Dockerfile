# syntax=docker/dockerfile:1

# ── Rust dependency cache ─────────────────────────────────────────────────────
# Pre-compile dependencies in isolation; this layer is only invalidated when
# Cargo.toml / Cargo.lock change, not on every source edit.
FROM rust:1-bookworm AS rust-deps
WORKDIR /src
# git is required to fetch the nostr-archive-cursor git dependency.
RUN apt-get update && \
    apt-get install -y --no-install-recommends git pkg-config libssl-dev && \
    rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY crates/nostrsearch-core/Cargo.toml       crates/nostrsearch-core/Cargo.toml
COPY crates/nostrsearch-indexer/Cargo.toml    crates/nostrsearch-indexer/Cargo.toml
COPY crates/nostrsearch-server/Cargo.toml     crates/nostrsearch-server/Cargo.toml
# Stub out the crates so cargo builds only dependencies.
RUN mkdir -p crates/nostrsearch-core/src \
             crates/nostrsearch-indexer/src/bin \
             crates/nostrsearch-server/src/bin && \
    echo "" > crates/nostrsearch-core/src/lib.rs && \
    echo "" > crates/nostrsearch-indexer/src/lib.rs && \
    echo "fn main() {}" > crates/nostrsearch-indexer/src/bin/ingest.rs && \
    echo "" > crates/nostrsearch-server/src/lib.rs && \
    echo "fn main() {}" > crates/nostrsearch-server/src/main.rs && \
    cargo build --release && \
    rm -f target/release/ingest \
          target/release/nostrsearch-server \
          target/release/deps/nostrsearch_core-* \
          target/release/deps/nostrsearch_indexer-* \
          target/release/deps/nostrsearch_server-* \
          target/release/deps/libnostrsearch_*

# ── Application build ─────────────────────────────────────────────────────────
FROM rust-deps AS rust-build
COPY crates ./crates
# Touch entry points so Cargo rebuilds the app crates, not the world.
RUN touch crates/nostrsearch-core/src/lib.rs \
          crates/nostrsearch-indexer/src/lib.rs \
          crates/nostrsearch-server/src/lib.rs && \
    cargo build --release && \
    mkdir -p /app/bin && \
    cp target/release/ingest /app/bin/ingest && \
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

# Index data lives here (mount a volume).
ENV INDEX_ROOT=/data/index \
    BIND=0.0.0.0:8080 \
    RUST_LOG=nostrsearch=info,tower_http=info
VOLUME ["/data"]
EXPOSE 8080

USER nostrsearch
ENTRYPOINT ["./bin/nostrsearch-server"]
