//! nostrsearch-server binary.

use nostrsearch_core::scoring::ScoreWeights;
use nostrsearch_server::{AppState, ArchiveState, ShardRegistry};
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "nostrsearch=info,tower_http=info".into()),
        )
        .init();

    let index_root = std::env::var("INDEX_ROOT").unwrap_or_else(|_| "./data/index".into());
    let bind = std::env::var("BIND").unwrap_or_else(|_| "0.0.0.0:8080".into());

    let registry = ShardRegistry::open(&index_root, ScoreWeights::default())?;
    let stats_docs = {
        let mut r = ShardRegistry::open(&index_root, ScoreWeights::default())?;
        r.stats().total_docs
    };
    tracing::info!(index_root = %index_root, docs = stats_docs, "opened index root");

    let state = Arc::new(AppState {
        registry: Mutex::new(registry),
    });

    // Optional archive serving (nostrhole's role): publishes the raw
    // .jsonl.zst corpus + listing under /archive from the same data dir the
    // unified ingest writes.
    // Archive serving (nostrhole's HTTP role). Index-free by default so it can
    // run alongside an ingest process that holds the exclusive RocksDB lock.
    // The relay needs to persist events, so it requires the indexed open —
    // enable it only on the process that owns the archive.
    let want_relay = matches!(
        std::env::var("ENABLE_RELAY").ok().as_deref(),
        Some("1") | Some("true")
    );

    let archive = match std::env::var("ARCHIVE_DIR") {
        Ok(dir) if !dir.is_empty() => {
            let a = if want_relay {
                ArchiveState::open_with_index(&dir)?
            } else {
                ArchiveState::open(&dir)?
            };
            tracing::info!(
                dir = %dir,
                indexed = a.db.is_some(),
                "serving archive at /archive"
            );
            Some(a)
        }
        _ => None,
    };

    // Optional nostr relay: accepts inbound writes at `/` and archives them
    // into the same corpus. Requires ARCHIVE_DIR + the index lock.
    let relay = match (&archive, want_relay) {
        (Some(a), true) => match &a.db {
            Some(db) => {
                let kinds = std::env::var("RELAY_KINDS").ok().map(|s| {
                    s.split(',')
                        .filter_map(|k| k.trim().parse::<u16>().ok())
                        .collect::<Vec<_>>()
                });
                tracing::info!("nostr relay enabled at /");
                Some(nostrsearch_server::RelayState::new((**db).clone(), kinds))
            }
            None => None,
        },
        (None, true) => {
            tracing::warn!("ENABLE_RELAY set but ARCHIVE_DIR missing; relay disabled");
            None
        }
        _ => None,
    };

    let app = nostrsearch_server::http::router_full(state, archive, relay);
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(bind = %bind, "nostrsearch-server listening");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
}
