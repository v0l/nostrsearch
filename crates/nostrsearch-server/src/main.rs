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
use std::sync::{Arc, Mutex};

// The environment contract is defined once in nostrsearch_indexer::env so the
// server node, `ingest` and `stats` all agree on the same variables.
use nostrsearch_indexer::env;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "nostrsearch=info,tower_http=info".into()),
        )
        .init();

    // Serving `/archive` downloads and relay websockets spends descriptors on
    // sockets, on top of RocksDB and Tantivy. Containers commonly default to a
    // 1024 soft limit, which EMFILEs under real traffic; raise it to the hard
    // limit before binding anything.
    let (soft, hard) = nostrsearch_indexer::mem::raise_nofile();
    tracing::info!(
        nofile_soft = soft,
        nofile_hard = hard,
        "file descriptor limit"
    );
    if soft < 8192 {
        tracing::warn!(
            nofile_soft = soft,
            "low descriptor limit; raise the container's hard limit (LimitNOFILE / --ulimit nofile)"
        );
    }

    let index_root = env::index_root();

    // This node writes to the index (firehose, relay, scraper), so it has the
    // same exposure `ingest` does: dirty pages are charged to the cgroup and
    // cannot be reclaimed until they reach disk, so a writer that dirties
    // faster than writeback retires gets OOM-killed while its own heap is
    // modest. `ingest` has always forced periodic writeback; the server never
    // did, despite having become a writer.
    {
        let sync_root = index_root.clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_secs(10));
                nostrsearch_indexer::mem::syncfs(&sync_root);
            }
        });
    }
    if let Some(limit) = nostrsearch_indexer::mem::cgroup_limit_mb() {
        tracing::info!(limit_mb = limit, "cgroup memory limit");
    }
    let bind = env::bind().unwrap_or_else(|| "0.0.0.0:8080".into());
    let archive_dir = env::archive_dir();
    let relays = env::relays();
    let want_relay = env::flag("ENABLE_RELAY");
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
        // Pick up shards written by anything other than this process (an
        // external relay backup, a restored dump) so their events are
        // fetchable by id. Incremental: unchanged shards cost one stat each.
        a.spawn_index_new_shards();
    }

    // ── Writer task: single owner of the Pipeline (index + stats + WoT) ─────
    // Reports are published from the writer into this store; a read-only node
    // leaves it empty (and `/reports` reports generated_at = 0).
    let reports = nostrsearch_server::reports::ReportStore::new();
    let mut writer_handle = None;
    let mut writer_ctl = None;
    // One handle for the dedupe set, shared by the writer, the scraper and the
    // admin ingest. RocksDB takes an exclusive per-process lock, so this
    // cannot be opened twice.
    //
    // The writer needs it to *record* what it indexes: an id store that drifts
    // ahead of the index makes every later ingest skip the events it should
    // add, which is how a large span of this corpus went missing.
    let dedupe_path = index_root.join(".dedupe");
    let shared_dedupe: Option<std::sync::Arc<nostrsearch_indexer::id_store::IdStore>> = if is_writer
    {
        match nostrsearch_indexer::id_store::IdStore::open(&dedupe_path) {
            Ok(s) => Some(std::sync::Arc::new(s)),
            Err(e) => {
                tracing::warn!(error = %e, "dedupe store unavailable");
                None
            }
        }
    } else {
        None
    };

    let mut writer_pipeline = None;
    let sink = if is_writer {
        // Writer heap is charged per *open* shard, so these two multiply.
        // Holding a whole corpus open is only affordable with a small
        // per-shard heap -- 400 shards at the 64 MB default would be 25.6 GB.
        let mut shard = ShardWriterConfig {
            max_open_shards: env::max_open_shards(),
            heap_bytes: env::shard_heap_mb() * 1_000_000,
            ..Default::default()
        };
        // Keep the writer arena to half the cgroup budget, as ingest does. The
        // server has to fit the Tantivy readers, the stats engine and the page
        // cache alongside it, so a writer heap sized purely from the shard
        // count is how a config change becomes an OOM.
        if let Some(limit_mb) = nostrsearch_indexer::mem::cgroup_limit_mb() {
            let budget = (limit_mb as usize * 1_000_000) / 2;
            if let Some(was) = shard.fit_to_budget(budget) {
                tracing::warn!(
                    requested_heap_mb = was / 1_000_000,
                    using_heap_mb = shard.heap_bytes / 1_000_000,
                    max_open_shards = shard.max_open_shards,
                    cgroup_limit_mb = limit_mb,
                    "writer heap would exceed half the cgroup limit; reduced per-shard heap"
                );
            }
        }
        tracing::info!(
            max_open_shards = shard.max_open_shards,
            heap_mb = shard.heap_bytes / 1_000_000,
            total_writer_heap_gb = format!("{:.1}", shard.total_heap_bytes() as f64 / 1e9),
            "writer heap budget"
        );
        let cfg = PipelineConfig {
            index_root: index_root.clone(),
            shard,
            state_dir: Some(env::state_dir()),
            wot_refresh_every: env::wot_refresh_every(),
            min_refresh_interval: env::min_refresh_interval(),
            persist_interval: env::persist_interval(),
            wot_out: Some(env::wot_out()),
        };
        let (sink, handle, ctl, pipeline) = nostrsearch_server::node::spawn_writer_with_reports(
            cfg,
            10_000,
            std::time::Duration::from_secs(30),
            Some(reports.clone()),
            shared_dedupe.clone(),
        )?;
        writer_handle = Some(handle);
        writer_ctl = Some(ctl);
        writer_pipeline = Some(pipeline);
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
            let db = archive
                .as_ref()
                .and_then(|a| a.db.as_ref())
                .map(|d| (**d).clone());
            tracing::info!(
                relays = relays.len(),
                archiving = db.is_some(),
                "starting firehose"
            );
            nostrsearch_server::spawn_firehose(relays.clone(), db, sink.clone());
        }
    }

    // ── Scraper: continuous full-network gap-filler ─────────────────────────
    // Walks day-by-day backwards across relays discovered from kind-10002,
    // negentropy where supported, feeding the same archive + writer funnel as
    // the firehose. On by default whenever this node is the writer and has an
    // archive; SCRAPE=0 disables.
    let want_scrape = std::env::var("SCRAPE").map(|v| v != "0").unwrap_or(true);
    // Shared with the HTTP layer so /sync can report progress from the same
    // RocksDB handle (it takes an exclusive per-process lock).
    let mut scrape_state = None;
    if want_scrape {
        match (&archive, &sink) {
            (Some(a), Some(sink)) => match &a.db {
                Some(db) => {
                    let opts = nostrsearch_server::scraper::ScraperOptions::from_env();
                    tracing::info!(
                        max_relays = opts.max_relays,
                        concurrency = opts.concurrency,
                        "starting network scraper"
                    );
                    match nostrsearch_server::scraper::spawn_scraper(
                        opts,
                        (**db).clone(),
                        sink.clone(),
                        shared_dedupe.clone(),
                        writer_ctl.clone(),
                    ) {
                        Ok(st) => scrape_state = Some(st),
                        Err(e) => tracing::warn!(error = %e, "scraper failed to start"),
                    }
                }
                None => tracing::info!("scraper disabled: archive has no event index"),
            },
            _ => tracing::info!("scraper disabled: needs writer role + archive"),
        }
    }

    // Admin routes only exist when ADMIN_PUBKEYS names at least one valid key
    // and this node owns the pipeline; otherwise they are never mounted.
    let admin = match (
        writer_ctl,
        nostrsearch_server::admin::AdminConfig::from_env(),
    ) {
        (Some(ctl), Some(cfg)) => {
            let mut st = nostrsearch_server::admin::AdminState::new(cfg, ctl, scrape_state.clone());
            // The archive ingest runs the same engine as the CLI, against
            // this node's pipeline. It needs the shared dedupe handle:
            // RocksDB takes an exclusive lock, so it cannot be reopened
            // alongside the scraper's.
            if let (Some(dir), Some(pipeline)) = (archive_dir.clone(), writer_pipeline.clone()) {
                st = st.with_replay(nostrsearch_server::admin::IngestCtx {
                    pipeline,
                    dir,
                    dedupe: shared_dedupe.clone(),
                    state: Default::default(),
                });
            }
            Some(st)
        }
        (None, Some(_)) => {
            tracing::warn!("ADMIN_PUBKEYS set but this node is not the writer; admin disabled");
            None
        }
        _ => None,
    };

    let has_admin = admin.is_some();
    let app = nostrsearch_server::http::router_full_node(
        state,
        archive,
        relay,
        Some(reports),
        scrape_state,
        admin,
    );
    tracing::info!(
        admin = has_admin,
        "operator console at / (admin panels need a key from ADMIN_PUBKEYS)"
    );
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
