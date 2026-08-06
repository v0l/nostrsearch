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
use nostrsearch_indexer::scrape::{RelayInfo, ScrapeConfig, ScrapeState, Sink, discover_relays};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::node::{EventSink, to_core};

/// Sink into the unified node: archive DB is the arbiter of novelty, the
/// writer task feeds index + stats.
struct NodeSink {
    db: DefaultJsonFilesDatabase,
    sink: EventSink,
    /// Ids indexed by dump ingest (read-only; may be absent on fresh deploys).
    dedupe: Option<Arc<IdStore>>,
    seen: AtomicU64,
    new: AtomicU64,
}

impl NodeSink {
    fn in_dedupe(&self, id: &[u8; 32]) -> bool {
        self.dedupe
            .as_ref()
            .map(|s| s.contains(id))
            .unwrap_or(false)
    }
}

impl Sink for NodeSink {
    async fn local_items(&self, since: u64, until: u64) -> Vec<(EventId, Timestamp)> {
        // The archive's time index knows exactly which events we hold for the
        // window. While its backfill is still running this returns empty,
        // which degrades to plain id enumeration — correct either way.
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.list_ids(since, until))
            .await
            .unwrap_or_default()
    }

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
                    self.sink.send(to_core(&ev)).await;
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
    /// Cap on how many discovered relays are ever targeted. `0` means no cap.
    ///
    /// This is not a rate limit: it permanently discards everything past the
    /// cut, so a capped scraper never sees those relays' history at all. The
    /// real throughput bounds are `concurrency` and the birthday logic that
    /// retires a relay after enough empty days -- both of which apply however
    /// many relays are known.
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
                .unwrap_or_else(|| nostrsearch_indexer::scrape::parse_date("2022-01-01").unwrap()),
            // Unlimited by default: the network has thousands of relays and
            // discarding all but the most-advertised 200 silently caps coverage.
            max_relays: u("SCRAPE_MAX_RELAYS", 0) as usize,
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
///
/// Returns the shared [`ScrapeState`] so the HTTP layer can report progress
/// from the same handle. RocksDB takes an exclusive per-process lock, so this
/// must be shared rather than reopened.
pub fn spawn_scraper(
    opts: ScraperOptions,
    db: DefaultJsonFilesDatabase,
    sink: EventSink,
    dedupe: Option<Arc<IdStore>>,
    // Writer handle, for reading relay targets out of the `relays` report.
    // Without one the scraper falls back to scanning the index.
    ctl: Option<crate::node::WriterCtl>,
) -> anyhow::Result<Arc<ScrapeState>> {
    let state = Arc::new(ScrapeState::open(&opts.state_dir.join("scrape"))?);
    let state_out = state.clone();
    let node_sink = Arc::new(NodeSink {
        db,
        sink,
        dedupe,
        seen: AtomicU64::new(0),
        new: AtomicU64::new(0),
    });

    tokio::spawn(async move {
        loop {
            // Discovery is due on wall-clock time, read from the state
            // database rather than an in-process timer.
            //
            // An `Instant` cannot survive the restart a deploy causes, so the
            // old timer made the most expensive thing the scraper does run in
            // full on every boot, however recently it had last finished.
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let due = state
                .last_discovery()
                .is_none_or(|t| now.saturating_sub(t) >= opts.rediscover_interval.as_secs());

            if due || state.relays().is_empty() {
                // Prefer the `relays` report: the indexer folds kind-10002
                // events as they stream past, so the answer is already
                // computed. Scanning the index for it means opening every
                // shard and fetching a stored document per hit, which is
                // minutes of solid disk read.
                let from_report = match &ctl {
                    Some(c) => c.relay_targets().await,
                    None => Vec::new(),
                };

                let found = if from_report.is_empty() {
                    // Nothing has folded a relay list yet -- a node whose
                    // reports predate this report, or one that has never seen
                    // one. Pay for the scan this once; the report takes over
                    // as soon as it has data.
                    tracing::info!(
                        "scraper: relays report empty, falling back to an index scan (slow)"
                    );
                    let root = opts.index_root.clone();
                    match tokio::task::spawn_blocking(move || discover_relays(&root)).await {
                        Ok(Ok(v)) => Ok(v.into_iter().map(|(u, n)| (u, n as u64)).collect()),
                        other => {
                            tracing::warn!(?other, "scraper: relay discovery failed");
                            Err(())
                        }
                    }
                } else {
                    tracing::info!(relays = from_report.len(), "scraper: targets from report");
                    Ok(from_report)
                };

                match found {
                    Ok(found) => {
                        let existing: std::collections::HashMap<String, RelayInfo> =
                            state.relays().into_iter().collect();
                        let mut kept = 0;
                        for (url, sources) in found
                            .iter()
                            .filter(|(_, n)| *n >= opts.min_sources as u64)
                            .take(if opts.max_relays == 0 {
                                usize::MAX
                            } else {
                                opts.max_relays
                            })
                        {
                            let mut info = existing.get(url).cloned().unwrap_or_default();
                            info.sources = *sources as u32;
                            state.put_relay(url, &info);
                            kept += 1;
                        }
                        tracing::info!(found = found.len(), kept, "scraper: relay discovery");
                        state.set_last_discovery(now);
                    }
                    Err(()) => {}
                }
            }

            if !state.relays().is_empty() {
                let cfg = ScrapeConfig {
                    min_date: opts.min_date,
                    floor_secs: opts.floor_secs,
                    concurrency: opts.concurrency,
                    empty_days_limit: opts.birthday_days,
                };
                nostrsearch_indexer::scrape::run_pass(state.clone(), node_sink.clone(), cfg).await;
                tracing::info!(
                    seen = node_sink.seen.load(Ordering::Relaxed),
                    new = node_sink.new.load(Ordering::Relaxed),
                    "scraper: pass complete (cumulative)"
                );
            }
            tokio::time::sleep(opts.pass_interval).await;
        }
    });
    Ok(state_out)
}
