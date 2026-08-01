//! Continuous full-network gap-filler running inside the unified node.
//!
//! Reuses the scrape engine from the indexer crate: day-by-day backwards from
//! yesterday across relays discovered from kind-10002 lists, negentropy first,
//! adaptive since/until fallback. Runs in a loop — each pass skips finished
//! (relay, day) pairs, so steady state is "scrape yesterday on every relay,
//! then idle", with periodic re-discovery picking up new relays.
//!
//! Events flow through the same funnel as the relay + firehose: the archive DB
//! dedupes on save, and only genuinely new events reach the index/stats
//! writer. The `.dedupe` store (populated by dump ingest) is consulted as an
//! additional read-only gate so dump-ingested events — present in the index
//! but not the archive — are not re-indexed.

use nostr_archive_cursor::DefaultJsonFilesDatabase;
use nostr_sdk::prelude::*;
use nostrsearch_indexer::id_store::IdStore;
use nostrsearch_indexer::scrape::{
    discover_relays, RelayInfo, ScrapeConfig, ScrapeState, Sink,
};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::node::{to_core, EventSink};

/// Sink into the unified node: archive DB is the arbiter of novelty, the
/// writer task feeds index + stats.
struct NodeSink {
    db: DefaultJsonFilesDatabase,
    sink: EventSink,
    /// Ids indexed by dump ingest (read-only; may be absent on fresh deploys).
    dedupe: Option<IdStore>,
    seen: AtomicU64,
    new: AtomicU64,
}

impl NodeSink {
    fn in_dedupe(&self, id: &[u8; 32]) -> bool {
        self.dedupe.as_ref().map(|s| s.contains(id)).unwrap_or(false)
    }
}

impl Sink for NodeSink {
    async fn missing(&self, ids: Vec<[u8; 32]>) -> Vec<[u8; 32]> {
        let mut out = Vec::new();
        for id in ids {
            if self.in_dedupe(&id) {
                continue;
            }
            let eid = EventId::from_byte_array(id);
            match self.db.check_id(&eid).await {
                Ok(DatabaseEventStatus::NotExistent) => out.push(id),
                _ => {}
            }
        }
        out
    }

    async fn process(&self, events: Vec<Event>) -> u64 {
        self.seen.fetch_add(events.len() as u64, Ordering::Relaxed);
        let mut new = 0u64;
        for ev in events {
            // Dump-ingested events are already in the index (but not the
            // archive); archiving them again would double-index.
            if self.in_dedupe(&ev.id.to_bytes()) {
                continue;
            }
            match self.db.save_event(&ev).await {
                Ok(SaveEventStatus::Success) => {
                    self.sink.submit(to_core(&ev));
                    new += 1;
                }
                Ok(_) => {}
                Err(e) => tracing::debug!(error = %e, "archive save failed"),
            }
        }
        self.new.fetch_add(new, Ordering::Relaxed);
        new
    }
}

/// Configuration read from the environment (`SCRAPE_*`).
pub struct ScraperOptions {
    pub index_root: PathBuf,
    pub state_dir: PathBuf,
    pub min_date: u64,
    pub max_relays: usize,
    pub min_sources: u32,
    pub concurrency: usize,
    pub floor_secs: u64,
    /// Consecutive empty days before a relay's data horizon is recorded.
    pub birthday_days: u32,
    /// Idle time between passes once caught up.
    pub pass_interval: std::time::Duration,
    /// How often to re-run kind-10002 relay discovery.
    pub rediscover_interval: std::time::Duration,
}

impl ScraperOptions {
    pub fn from_env() -> Self {
        use nostrsearch_indexer::env;
        let u = |k: &str, d: u64| {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(d)
        };
        Self {
            index_root: env::index_root(),
            state_dir: env::state_dir(),
            min_date: std::env::var("SCRAPE_MIN_DATE")
                .ok()
                .and_then(|v| nostrsearch_indexer::scrape::parse_date(&v))
                .unwrap_or_else(|| {
                    nostrsearch_indexer::scrape::parse_date("2022-01-01").unwrap()
                }),
            max_relays: u("SCRAPE_MAX_RELAYS", 200) as usize,
            min_sources: u("SCRAPE_MIN_SOURCES", 3) as u32,
            concurrency: u("SCRAPE_CONCURRENCY", 8) as usize,
            floor_secs: u("SCRAPE_FLOOR_MINS", 10) * 60,
            birthday_days: u("SCRAPE_BIRTHDAY_DAYS", 14) as u32,
            pass_interval: std::time::Duration::from_secs(u("SCRAPE_PASS_INTERVAL_SECS", 1800)),
            rediscover_interval: std::time::Duration::from_secs(u(
                "SCRAPE_REDISCOVER_SECS",
                86_400,
            )),
        }
    }
}

/// Spawn the continuous scraper. Runs for the life of the process.
pub fn spawn_scraper(
    opts: ScraperOptions,
    db: DefaultJsonFilesDatabase,
    sink: EventSink,
) -> anyhow::Result<()> {
    let state = Arc::new(ScrapeState::open(&opts.state_dir.join("scrape"))?);
    let dedupe_path = opts.index_root.join(".dedupe");
    let dedupe = if dedupe_path.exists() {
        Some(IdStore::open(&dedupe_path)?)
    } else {
        None
    };
    let node_sink = Arc::new(NodeSink {
        db,
        sink,
        dedupe,
        seen: AtomicU64::new(0),
        new: AtomicU64::new(0),
    });

    tokio::spawn(async move {
        let mut last_discovery = std::time::Instant::now() - opts.rediscover_interval;
        loop {
            // (Re-)discover targets from kind-10002 lists in the index.
            if last_discovery.elapsed() >= opts.rediscover_interval
                || state.relays().is_empty()
            {
                let root = opts.index_root.clone();
                match tokio::task::spawn_blocking(move || discover_relays(&root)).await {
                    Ok(Ok(found)) => {
                        let existing: std::collections::HashMap<String, RelayInfo> =
                            state.relays().into_iter().collect();
                        let mut kept = 0;
                        for (url, sources) in found
                            .iter()
                            .filter(|(_, n)| *n >= opts.min_sources)
                            .take(opts.max_relays)
                        {
                            let mut info = existing.get(url).cloned().unwrap_or_default();
                            info.sources = *sources;
                            state.put_relay(url, &info);
                            kept += 1;
                        }
                        tracing::info!(found = found.len(), kept, "scraper: relay discovery");
                        last_discovery = std::time::Instant::now();
                    }
                    other => {
                        tracing::warn!(?other, "scraper: relay discovery failed");
                    }
                }
            }

            if !state.relays().is_empty() {
                let cfg = ScrapeConfig {
                    min_date: opts.min_date,
                    floor_secs: opts.floor_secs,
                    concurrency: opts.concurrency,
                    empty_days_limit: opts.birthday_days,
                };
                nostrsearch_indexer::scrape::run_pass(state.clone(), node_sink.clone(), cfg)
                    .await;
                tracing::info!(
                    seen = node_sink.seen.load(Ordering::Relaxed),
                    new = node_sink.new.load(Ordering::Relaxed),
                    "scraper: pass complete (cumulative)"
                );
            }
            tokio::time::sleep(opts.pass_interval).await;
        }
    });
    Ok(())
}
