//! Serving for the analysis reports (activity, active users, client tags,
//! trending, …) computed by `nostrsearch-stats`.
//!
//! The [`Pipeline`](nostrsearch_indexer::pipeline::Pipeline) is owned
//! exclusively by the single writer task, so the HTTP layer cannot reach into
//! it. Instead the writer *publishes* a snapshot into this [`ReportStore`] on
//! its commit tick, and requests are served from that copy. Readers therefore
//! never contend with the ingest hot path — at the cost of reports being at
//! most one publish interval stale, which `generated_at` makes explicit.

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{
        IntoResponse, Sse,
        sse::{Event, KeepAlive},
    },
    routing::get,
};
use futures::stream::Stream;
use nostrsearch_stats::{ReportDelta, merge_patch};
use serde::Serialize;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;

/// Delta frames buffered per subscriber before it is considered too slow and
/// dropped (it can re-sync with a full `GET /reports/{name}`).
const DELTA_BUFFER: usize = 256;

/// A published set of report snapshots.
#[derive(Debug, Default, Clone, Serialize)]
pub struct Reports {
    /// Unix seconds when the writer published this set (0 = never).
    pub generated_at: u64,
    /// Report name -> JSON snapshot.
    pub data: BTreeMap<String, serde_json::Value>,
}

/// Shared, cheaply-cloneable handle to the latest [`Reports`].
///
/// Holds the last full snapshot *and* a broadcast channel of incremental
/// updates. A dashboard does one `GET /reports/{name}` to seed its state, then
/// subscribes to `GET /reports/stream` and merge-patches each frame — so the
/// numbers move without ever refetching a whole report.
#[derive(Debug, Clone)]
pub struct ReportStore {
    inner: Arc<RwLock<Reports>>,
    tx: broadcast::Sender<ReportDelta>,
}

impl Default for ReportStore {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Reports::default())),
            tx: broadcast::channel(DELTA_BUFFER).0,
        }
    }
}

impl ReportStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply and fan out incremental changes drained from the pipeline.
    ///
    /// The held snapshot is patched too, so a client that connects between
    /// full publishes still gets current data from `GET /reports/{name}`.
    pub fn apply_deltas(&self, generated_at: u64, deltas: Vec<ReportDelta>) {
        if deltas.is_empty() {
            return;
        }
        if let Ok(mut w) = self.inner.write() {
            for d in &deltas {
                let slot = w
                    .data
                    .entry(d.name.clone())
                    .or_insert(serde_json::Value::Null);
                merge_patch(slot, &d.patch);
            }
            w.generated_at = generated_at;
        }
        for d in deltas {
            // Err just means nobody is subscribed.
            let _ = self.tx.send(d);
        }
    }

    /// Subscribe to the incremental update stream.
    pub fn subscribe(&self) -> broadcast::Receiver<ReportDelta> {
        self.tx.subscribe()
    }

    /// Replace the published snapshot. Called by the writer task.
    pub fn publish<I, S>(&self, generated_at: u64, snapshots: I)
    where
        I: IntoIterator<Item = (S, serde_json::Value)>,
        S: Into<String>,
    {
        let data = snapshots.into_iter().map(|(k, v)| (k.into(), v)).collect();
        if let Ok(mut w) = self.inner.write() {
            *w = Reports { generated_at, data };
        }
    }

    /// Snapshot of everything currently published.
    pub fn get(&self) -> Reports {
        self.inner.read().map(|r| r.clone()).unwrap_or_default()
    }

    /// One report by name.
    pub fn report(&self, name: &str) -> Option<serde_json::Value> {
        self.inner.read().ok()?.data.get(name).cloned()
    }

    /// Names currently available.
    pub fn names(&self) -> Vec<String> {
        self.inner
            .read()
            .map(|r| r.data.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Publication time and the available names, without touching the payloads.
    ///
    /// The index endpoint needs exactly this much and nothing more; reading it
    /// through [`get`](Self::get) clones every report to answer a question
    /// about their keys.
    pub fn summary(&self) -> (u64, Vec<String>) {
        self.inner
            .read()
            .map(|r| (r.generated_at, r.data.keys().cloned().collect()))
            .unwrap_or_default()
    }
}

#[derive(Serialize)]
struct ReportIndex {
    generated_at: u64,
    reports: Vec<String>,
}

/// Scrape/sync progress, served at `/sync`.
///
/// Reads the live [`ScrapeState`] the scraper task owns — RocksDB takes an
/// exclusive per-process lock, so the handle is shared rather than reopened.
pub fn sync_router(state: std::sync::Arc<nostrsearch_indexer::scrape::ScrapeState>) -> Router {
    Router::new().route("/", get(sync_status)).with_state(state)
}

/// How much of the scrape horizon is left.
///
/// "Relay-days" is the unit of scrape work: one relay, one day. The total is
/// the relays currently in scope times the days between the configured floor
/// and yesterday, so it moves when the cut widens -- which is the honest
/// number, since a narrower cut genuinely has less to do.
#[derive(Serialize)]
struct Horizon {
    /// Relays currently in scope (after the usage-weight cut).
    relays: usize,
    /// Relays discovered in total, in or out of scope.
    relays_discovered: usize,
    /// Days between the floor and yesterday.
    days: u64,
    /// relays x days: the whole job at the current cut.
    relay_days_total: u64,
    /// Completed, counting only relays currently in scope.
    relay_days_done: u64,
    /// Still to do.
    relay_days_remaining: u64,
    /// 0-100.
    percent_complete: u32,
    /// The cut in force, and its bounds.
    usage_percentile: u32,
    oldest_day: String,
}

#[derive(Serialize)]
struct SyncStatus {
    /// Relays discovered from kind-10002 lists, and how they are behaving.
    relays: SyncRelays,
    /// Day-by-day backfill coverage.
    scrape: nostrsearch_indexer::scrape::ScrapeProgress,
    /// How much scraping is left.
    horizon: Horizon,
}

/// Page controls for the relay list.
///
/// The list used to be a hard-truncated top 50 with no way to see past it,
/// which on a node that discovers thousands of relays meant most of them were
/// simply invisible.
#[derive(serde::Deserialize)]
struct RelayPage {
    #[serde(default)]
    offset: usize,
    limit: Option<usize>,
}

impl RelayPage {
    /// Page size, defaulted and bounded. The cap is about response size, not
    /// policy: a caller that wants everything pages through it.
    fn limit(&self) -> usize {
        self.limit.unwrap_or(50).clamp(1, 500)
    }
}

#[derive(Serialize)]
struct SyncRelays {
    total: usize,
    /// Relays that accepted a negentropy reconciliation.
    negentropy: usize,
    /// Probed and refused negentropy (windowed REQ fallback).
    no_negentropy: usize,
    /// Not yet probed.
    unprobed: usize,
    /// Currently failing (consecutive day-level failures).
    failing: usize,
    /// Where this page starts, and how many were returned.
    offset: usize,
    limit: usize,
    /// One page of relays, most-advertised first.
    top: Vec<SyncRelay>,
}

#[derive(Serialize)]
struct SyncRelay {
    url: String,
    sources: u32,
    negentropy: Option<bool>,
    cap: u32,
    fails: u32,
    last_ok: u64,
    birthday: Option<u64>,
    /// Days scraped from this relay, and what they yielded.
    days: u64,
    events_seen: u64,
    events_new: u64,
    /// Set while the relay is being left alone after repeated failures.
    #[serde(skip_serializing_if = "Option::is_none")]
    dead_until: Option<u64>,
    /// Inside the current usage-weight cut, and therefore actually scraped.
    ///
    /// The list shows every relay ever discovered, which is thousands; only a
    /// few dozen are in scope. Without this the table reads as if all of them
    /// are being worked on, and a relay sitting at zero looks broken rather
    /// than simply out of scope.
    in_scope: bool,
}

async fn sync_status(
    State(state): State<std::sync::Arc<nostrsearch_indexer::scrape::ScrapeState>>,
    axum::extract::Query(page): axum::extract::Query<RelayPage>,
) -> impl IntoResponse {
    let (offset, limit) = (page.offset, page.limit());
    // RocksDB scans are blocking work; keep them off the async worker.
    let out = tokio::task::spawn_blocking(move || {
        let relays = state.relays();
        // One scan, reused: `progress` already walks every recorded day, and
        // accumulates per-relay totals while it is there.
        let progress = state.progress(25);
        // Same cut the scraper applies, so the figure describes the work it
        // will actually do rather than a hypothetical full sweep.
        let env_u = |k: &str, d: u64| {
            std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
        };
        let min_date = std::env::var("SCRAPE_MIN_DATE")
            .ok()
            .and_then(|v| nostrsearch_indexer::scrape::parse_date(&v))
            .unwrap_or_else(|| {
                nostrsearch_indexer::scrape::parse_date("2022-01-01").unwrap_or(0)
            });
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let today = now - (now % 86_400);
        let days = today.saturating_sub(min_date) / 86_400;

        let floor_p = env_u("SCRAPE_USAGE_PERCENTILE_MIN", 2) as u32;
        let ceil_p = env_u("SCRAPE_USAGE_PERCENTILE", 80) as u32;
        // Same floor the scraper applies: the kept set never shrinks below
        // the worker count, so this page reports the scope the pass will
        // actually cover.
        let min_keep = env_u("SCRAPE_CONCURRENCY", 50) as usize;
        let kept_at_floor = nostrsearch_indexer::scrape::top_by_usage_weight(
            relays.clone(),
            floor_p,
            min_keep,
        );
        // Coverage counts only relays the floor cut keeps -- the raw total
        // includes every relay ever scraped, junk included, which inflates
        // coverage and snaps the cut to the ceiling.
        let done_in_scope: u64 = kept_at_floor
            .iter()
            .map(|(u, _)| progress.by_relay.get(u).map(|t| t.days).unwrap_or(0))
            .sum();
        let cut = nostrsearch_indexer::scrape::adaptive_percentile(
            done_in_scope,
            (kept_at_floor.len() as u64).saturating_mul(days.max(1)),
            floor_p,
            ceil_p,
        );
        let kept =
            nostrsearch_indexer::scrape::top_by_usage_weight(relays.clone(), cut, min_keep);
        let in_scope: std::collections::HashSet<String> =
            kept.iter().map(|(u, _)| u.clone()).collect();

        let mut top: Vec<SyncRelay> = relays
            .iter()
            .map(|(url, i)| {
                let t = progress.by_relay.get(url).copied().unwrap_or_default();
                SyncRelay {
                    url: url.clone(),
                    sources: i.sources,
                    negentropy: i.negentropy,
                    cap: i.cap,
                    fails: i.fails,
                    last_ok: i.last_ok,
                    birthday: i.birthday,
                    days: t.days,
                    events_seen: t.seen,
                    events_new: t.new,
                    dead_until: i.dead_until,
                    in_scope: in_scope.contains(url),
                }
            })
            .collect();
        // Most-advertised relays first: the ones that matter for coverage.
        // Sort before slicing so paging is stable across requests.
        top.sort_by(|a, b| b.sources.cmp(&a.sources).then_with(|| a.url.cmp(&b.url)));
        let top: Vec<SyncRelay> = top.into_iter().skip(offset).take(limit).collect();

        // Count only the days done for relays still in scope: days scraped
        // from a relay the cut has since dropped are not progress toward the
        // job that remains.
        let done: u64 = kept
            .iter()
            .map(|(url, _)| progress.by_relay.get(url).map(|t| t.days).unwrap_or(0))
            .sum();
        let total = (kept.len() as u64).saturating_mul(days);
        let horizon = Horizon {
            relays: kept.len(),
            relays_discovered: relays.len(),
            days,
            relay_days_total: total,
            relay_days_done: done.min(total),
            relay_days_remaining: total.saturating_sub(done),
            percent_complete: if total == 0 {
                100
            } else {
                ((done.min(total) * 100) / total) as u32
            },
            usage_percentile: cut,
            oldest_day: chrono::DateTime::from_timestamp(min_date as i64, 0)
                .map(|d| d.format("%Y-%m-%d").to_string())
                .unwrap_or_default(),
        };

        SyncStatus {
            horizon,
            relays: SyncRelays {
                total: relays.len(),
                negentropy: relays
                    .iter()
                    .filter(|(_, i)| i.negentropy == Some(true))
                    .count(),
                no_negentropy: relays
                    .iter()
                    .filter(|(_, i)| i.negentropy == Some(false))
                    .count(),
                unprobed: relays
                    .iter()
                    .filter(|(_, i)| i.negentropy.is_none())
                    .count(),
                failing: relays.iter().filter(|(_, i)| i.fails > 0).count(),
                offset,
                limit,
                top,
            },
            scrape: progress,
        }
    })
    .await;

    match out {
        Ok(s) => Json(s).into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "sync status unavailable"})),
        )
            .into_response(),
    }
}

/// `/reports` (index), `/reports/stream` (live deltas), `/reports/{name}`.
pub fn router(store: ReportStore) -> Router {
    Router::new()
        .route("/", get(index))
        // Registered before `/{name}` so it is not captured as a report name.
        .route("/stream", get(stream))
        .route("/{name}", get(one))
        .with_state(store)
}

/// `GET /reports/stream` — server-sent events, one JSON [`ReportDelta`] per
/// frame. Clients seed from a full report then merge each patch.
async fn stream(
    State(store): State<ReportStore>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = store.subscribe();
    let s = async_stream::stream! {
        let mut rx = rx;
        loop {
            match rx.recv().await {
                Ok(delta) => {
                    if let Ok(ev) = Event::default().event("delta").json_data(&delta) {
                        yield Ok(ev);
                    }
                }
                // Slow consumer: tell it to re-sync rather than silently
                // handing it a gapped stream.
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    let ev = Event::default()
                        .event("lagged")
                        .data(n.to_string());
                    yield Ok(ev);
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };
    Sse::new(s).keep_alive(KeepAlive::default())
}

async fn index(State(store): State<ReportStore>) -> impl IntoResponse {
    // Names and a timestamp only -- deliberately not `get()`, which deep-clones
    // every report to build the reply and then discards all of it.
    //
    // The payloads are `serde_json::Value` trees with a node per day bucket per
    // kind; on the production corpus that is ~10 MB of nested maps, and cloning
    // it took 22 seconds to return 114 bytes of names. The console calls this
    // endpoint first, so every page load paid for it.
    let (generated_at, reports) = store.summary();
    Json(ReportIndex {
        generated_at,
        reports,
    })
}

async fn one(State(store): State<ReportStore>, Path(name): Path<String>) -> impl IntoResponse {
    match store.report(&name) {
        Some(v) => Json(v).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": "unknown report",
                "available": store.names(),
            })),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_then_read_back() {
        let s = ReportStore::new();
        assert_eq!(s.get().generated_at, 0);
        assert!(s.report("activity").is_none());

        s.publish(
            1_700_000_000,
            vec![("activity", serde_json::json!({"days": 1}))],
        );

        assert_eq!(s.get().generated_at, 1_700_000_000);
        assert_eq!(s.report("activity").unwrap()["days"], 1);
        assert_eq!(s.names(), vec!["activity".to_string()]);

        // publishing replaces wholesale, so removed analyses disappear
        s.publish(1_700_000_100, vec![("client_tags", serde_json::json!({}))]);
        assert!(s.report("activity").is_none());
        assert_eq!(s.names(), vec!["client_tags".to_string()]);
    }

    #[test]
    fn deltas_patch_the_held_snapshot() {
        let s = ReportStore::new();
        s.publish(
            1_700_000_000,
            vec![("client_tags", serde_json::json!({"snort": {"sum": 1}}))],
        );

        s.apply_deltas(
            1_700_000_005,
            vec![ReportDelta {
                name: "client_tags".into(),
                patch: serde_json::json!({"damus": {"sum": 4}}),
            }],
        );

        // A client polling the plain endpoint between publishes sees the
        // incremental update too, not a stale snapshot.
        let r = s.report("client_tags").unwrap();
        assert_eq!(r["snort"]["sum"], 1, "untouched key preserved");
        assert_eq!(r["damus"]["sum"], 4, "delta applied");
        assert_eq!(s.get().generated_at, 1_700_000_005);
    }

    #[tokio::test]
    async fn subscribers_receive_delta_frames() {
        let s = ReportStore::new();
        let mut rx = s.subscribe();

        s.apply_deltas(
            1_700_000_000,
            vec![ReportDelta {
                name: "activity".into(),
                patch: serde_json::json!({"1700000000": {"zap_count": 2}}),
            }],
        );

        let got = rx.try_recv().expect("subscriber got the frame");
        assert_eq!(got.name, "activity");
        assert_eq!(got.patch["1700000000"]["zap_count"], 2);

        // No-op drains produce no traffic at all.
        s.apply_deltas(1_700_000_001, vec![]);
        assert!(rx.try_recv().is_err());
    }
}

#[cfg(test)]
mod relay_page_tests {
    use super::*;

    fn page(offset: usize, limit: Option<usize>) -> RelayPage {
        RelayPage { offset, limit }
    }

    /// The relay list was a hard top-50 with nothing behind it, so a node that
    /// discovers thousands of relays showed 50 and hid the rest. Paging has to
    /// default to something small, honour a caller's size, and refuse a size
    /// that would serialise the whole set into one response.
    #[test]
    fn page_size_is_defaulted_and_bounded() {
        assert_eq!(page(0, None).limit(), 50, "default stays small");
        assert_eq!(page(0, Some(200)).limit(), 200, "caller's size is honoured");
        assert_eq!(page(0, Some(0)).limit(), 1, "zero is not a page");
        assert_eq!(page(0, Some(10_000)).limit(), 500, "capped for response size");
    }

    /// Paging is only coherent if the order is total. Sorting by advertiser
    /// count alone leaves ties in arbitrary order, so a relay could appear on
    /// two pages or none as the map rehashes between requests.
    #[test]
    fn ordering_breaks_ties_so_paging_is_stable() {
        let mut v = vec![("b.example", 5u32), ("a.example", 5), ("c.example", 9)];
        v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
        assert_eq!(
            v.iter().map(|(u, _)| *u).collect::<Vec<_>>(),
            vec!["c.example", "a.example", "b.example"],
            "equal advertiser counts must fall back to a stable key"
        );
    }
}

#[cfg(test)]
mod index_cost_tests {
    use super::*;

    /// The index endpoint must not touch report payloads.
    ///
    /// It answers a question about keys, but was reading the store through
    /// `get()`, which deep-clones every report. On the production corpus the
    /// payloads are ~10 MB of nested `serde_json::Value` maps -- a node per day
    /// bucket per kind -- and cloning them took 22 seconds to produce 114 bytes
    /// of names. The console fetches this first, so it stalled every page load.
    ///
    /// Guards the cost by shape rather than by clock: build a store whose
    /// payloads are far larger than the reply, and assert `summary()` is
    /// unaffected by their size.
    #[test]
    fn index_summary_does_not_read_payloads() {
        let store = ReportStore::new();

        // A payload much larger than the index reply will ever be.
        let big: serde_json::Value = serde_json::Value::Object(
            (0..50_000u64)
                .map(|i| (i.to_string(), serde_json::json!({"n": i, "kinds": {"1": i}})))
                .collect(),
        );
        store.publish(
            1_700_000_000,
            vec![("activity", big.clone()), ("active_users", big)],
        );

        let (generated_at, names) = store.summary();
        assert_eq!(generated_at, 1_700_000_000);
        assert_eq!(names, vec!["active_users", "activity"]);

        // The whole point: summary() is cheap enough to call repeatedly, which
        // it cannot be if it clones the payloads.
        let t = std::time::Instant::now();
        for _ in 0..200 {
            let _ = store.summary();
        }
        let per_call = t.elapsed() / 200;
        assert!(
            per_call < std::time::Duration::from_millis(2),
            "summary() cost {per_call:?} per call; it is cloning payloads"
        );
    }
}
