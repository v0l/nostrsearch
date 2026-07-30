//! nostrsearch node — one process, one shared index.
//!
//! Runs the search API, the archive HTTP server, the nostr archive relay, and
//! the live firehose together over a single Tantivy index, a single archive
//! database, and a single stats/WoT engine.
//!
//! Environment:
//!   INDEX_ROOT     Tantivy shard root                (default ./data/index)
//!   BIND           listen address                    (default 0.0.0.0:8080)
//!   ARCHIVE_DIR    corpus dir; enables /archive and archiving
//!   ENABLE_RELAY   1/true — accept writes at /       (needs ARCHIVE_DIR)
//!   RELAY_KINDS    comma-separated kind whitelist for the relay
//!   RELAYS         comma-separated upstream relays — enables the firehose
//!   STATE_DIR      stats/analysis state              (default ./data/stats)
//!   WOT_OUT        WoT snapshot path                 (default ./data/wot.bin)
//!   WOT_REFRESH_EVERY  events between WoT rebuilds   (default 100000)

use nostrsearch_core::scoring::ScoreWeights;
use nostrsearch_indexer::{PipelineConfig, ShardWriterConfig};
use nostrsearch_server::{AppState, ArchiveState, ShardRegistry};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

fn env_flag(k: &str) -> bool {
    matches!(
        std::env::var(k).ok().as_deref(),
        Some("1") | Some("true") | Some("yes")
    )
}

fn env_path(k: &str, default: &str) -> PathBuf {
    PathBuf::from(std::env::var(k).unwrap_or_else(|_| default.into()))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider().install_default().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "nostrsearch=info,tower_http=info".into()),
        )
        .init();

    let index_root = env_path("INDEX_ROOT", "./data/index");
    let bind = std::env::var("BIND").unwrap_or_else(|_| "0.0.0.0:8080".into());
    let archive_dir = std::env::var("ARCHIVE_DIR").ok().filter(|s| !s.is_empty());
    let relays: Vec<String> = std::env::var("RELAYS")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let want_relay = env_flag("ENABLE_RELAY");
    let want_firehose = !relays.is_empty();
    // Any write role means this process owns the archive index + Tantivy writer.
    let is_writer = want_relay || want_firehose;

    // ── Search side (reader; auto-reloads on the writer's commits) ──────────
    let registry = ShardRegistry::open(&index_root, ScoreWeights::default())?;
    let docs = {
        let mut r = ShardRegistry::open(&index_root, ScoreWeights::default())?;
        r.stats().total_docs
    };
    tracing::info!(index_root = %index_root.display(), docs, "opened index root");
    let state = Arc::new(AppState {
        registry: Mutex::new(registry),
    });

    // ── Archive: one handle for this process (relay + firehose share it) ────
    // Writers need the RocksDB id index; a read-only node serves files without
    // taking the lock so it can run beside a separate writer.
    let archive = match &archive_dir {
        Some(dir) if is_writer => Some(ArchiveState::open_with_index(dir)?),
        Some(dir) => Some(ArchiveState::open(dir)?),
        None => None,
    };
    if let Some(a) = &archive {
        tracing::info!(dir = %a.dir.display(), indexed = a.db.is_some(), "serving archive at /archive");
    }

    // ── Writer task: single owner of the Pipeline (index + stats + WoT) ─────
    let mut writer_handle = None;
    let sink = if is_writer {
        let cfg = PipelineConfig {
            index_root: index_root.clone(),
            shard: ShardWriterConfig::default(),
            state_dir: Some(env_path("STATE_DIR", "./data/stats")),
            wot_refresh_every: std::env::var("WOT_REFRESH_EVERY")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100_000),
            wot_out: Some(env_path("WOT_OUT", "./data/wot.bin")),
        };
        let (sink, handle) = nostrsearch_server::spawn_writer(
            cfg,
            10_000,
            std::time::Duration::from_secs(30),
        )?;
        writer_handle = Some(handle);
        Some(sink)
    } else {
        None
    };

    // ── Relay: archives AND indexes inbound writes ──────────────────────────
    let relay = match (&archive, &sink, want_relay) {
        (Some(a), Some(sink), true) => match &a.db {
            Some(db) => {
                let kinds = std::env::var("RELAY_KINDS").ok().map(|s| {
                    s.split(',')
                        .filter_map(|k| k.trim().parse::<u16>().ok())
                        .collect::<Vec<_>>()
                });
                let node_db = nostrsearch_server::NodeDb::new((**db).clone(), sink.clone());
                tracing::info!("nostr relay enabled at / (archives + indexes)");
                Some(nostrsearch_server::RelayState::new(node_db, kinds))
            }
            None => None,
        },
        (_, _, true) => {
            tracing::warn!("ENABLE_RELAY set but ARCHIVE_DIR missing; relay disabled");
            None
        }
        _ => None,
    };

    // ── Firehose: shares the same archive handle and writer task ────────────
    if want_firehose {
        if let Some(sink) = &sink {
            let db = archive.as_ref().and_then(|a| a.db.as_ref()).map(|d| (**d).clone());
            tracing::info!(
                relays = relays.len(),
                archiving = db.is_some(),
                "starting firehose"
            );
            nostrsearch_server::spawn_firehose(relays.clone(), db, sink.clone());
        }
    }

    let app = nostrsearch_server::http::router_full(state, archive, relay);
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(bind = %bind, "nostrsearch node listening");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;

    // Flush the index before exiting, otherwise everything since the last
    // commit interval is lost from the search index on every deploy.
    if let Some(h) = writer_handle {
        tracing::info!("flushing writer before exit");
        h.shutdown().await;
    }
    tracing::info!("nostrsearch node stopped");
    Ok(())
}

/// Resolve on SIGTERM (container stop) or Ctrl-C.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("received SIGINT"),
        _ = terminate => tracing::info!("received SIGTERM"),
    }
}
