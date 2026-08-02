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

#[derive(Serialize)]
struct SyncStatus {
    /// Relays discovered from kind-10002 lists, and how they are behaving.
    relays: SyncRelays,
    /// Day-by-day backfill coverage.
    scrape: nostrsearch_indexer::scrape::ScrapeProgress,
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
}

async fn sync_status(
    State(state): State<std::sync::Arc<nostrsearch_indexer::scrape::ScrapeState>>,
) -> impl IntoResponse {
    // RocksDB scans are blocking work; keep them off the async worker.
    let out = tokio::task::spawn_blocking(move || {
        let relays = state.relays();
        let mut top: Vec<SyncRelay> = relays
            .iter()
            .map(|(url, i)| SyncRelay {
                url: url.clone(),
                sources: i.sources,
                negentropy: i.negentropy,
                cap: i.cap,
                fails: i.fails,
                last_ok: i.last_ok,
                birthday: i.birthday,
            })
            .collect();
        // Most-advertised relays first: the ones that matter for coverage.
        top.sort_by(|a, b| b.sources.cmp(&a.sources));
        top.truncate(50);

        SyncStatus {
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
                top,
            },
            scrape: state.progress(25),
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
    let r = store.get();
    Json(ReportIndex {
        generated_at: r.generated_at,
        reports: r.data.keys().cloned().collect(),
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
