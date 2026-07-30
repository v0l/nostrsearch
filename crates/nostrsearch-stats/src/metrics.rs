//! Pipeline observability: per-analysis + pipeline-wide counters and
//! throughput, serialized to JSON for the web dashboard.
//!
//! Counters are updated inline by the registry (it already visits every event),
//! so metrics cost is a few integer increments per event. Throughput uses a
//! light EWMA so the dashboard can show a live events/sec without a time series.

use serde::{Deserialize, Serialize};
use std::time::Instant;

/// Exponentially-weighted moving average of events/second.
#[derive(Debug, Clone)]
pub struct RateMeter {
    ewma: f64,
    alpha: f64,
    last: Option<Instant>,
    last_count: u64,
}

impl Default for RateMeter {
    fn default() -> Self {
        Self {
            ewma: 0.0,
            alpha: 0.2,
            last: None,
            last_count: 0,
        }
    }
}

impl RateMeter {
    /// Record cumulative `total` events observed so far; updates the rate.
    pub fn tick(&mut self, total: u64) {
        let now = Instant::now();
        if let Some(prev) = self.last {
            let dt = now.duration_since(prev).as_secs_f64();
            if dt > 0.0 {
                let inst = (total - self.last_count) as f64 / dt;
                self.ewma = self.alpha * inst + (1.0 - self.alpha) * self.ewma;
            }
        }
        self.last = Some(now);
        self.last_count = total;
    }

    pub fn per_sec(&self) -> f64 {
        self.ewma
    }
}

/// Cumulative counters for one analysis. Persisted inside [`Progress`] so they
/// survive restarts (throughput/consumed totals are lifetime, not per-run).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Counters {
    /// Events routed to this analysis (passed kind + watermark checks).
    pub observed: u64,
    /// Events actually folded (passed the analysis's internal filter too).
    pub consumed: u64,
    /// Events skipped by a publisher filter inside the analysis.
    pub filtered: u64,
    /// How many times the expensive `refresh()` ran.
    pub refresh_count: u64,
    /// Duration of the last refresh, microseconds.
    pub last_refresh_micros: u64,
}

/// Serializable per-analysis metrics for the dashboard.
#[derive(Debug, Clone, Serialize)]
pub struct AnalysisMetrics {
    pub name: &'static str,
    pub epoch: u32,
    pub watermark: u64,
    pub events_total: u64,
    pub observed: u64,
    pub consumed: u64,
    pub filtered: u64,
    pub backfilled: bool,
    pub refresh_count: u64,
    pub last_refresh_wall: u64,
    pub last_refresh_micros: u64,
    /// Seconds until the next scheduled refresh (None = incremental/no schedule).
    pub next_refresh_in: Option<u64>,
}

/// Pipeline phase, for the dashboard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// At least one analysis is still doing its initial full backfill.
    Backfilling,
    /// All analyses caught up; folding live events.
    Live,
}

/// Serializable pipeline-wide metrics for the dashboard.
#[derive(Debug, Clone, Serialize)]
pub struct PipelineMetrics {
    /// Wall-clock (unix secs) this snapshot was produced.
    pub at: u64,
    pub phase: Phase,
    /// Total events pushed through the pipeline.
    pub total_events: u64,
    /// Live throughput (EWMA events/sec).
    pub events_per_sec: f64,
    /// Distinct pubkeys currently tracked in the materialized world.
    pub world_pubkeys: usize,
    pub analyses: Vec<AnalysisMetrics>,
}

/// A realtime metrics frame emitted to observers (dashboard WS/SSE clients).
///
/// On connect the server replays the latest [`Snapshot`](MetricsEvent::Snapshot)
/// so a new client sees full pipeline state immediately, then forwards
/// subsequent frames as they are emitted.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum MetricsEvent {
    /// Full pipeline state — emitted once at startup and to each new subscriber.
    Snapshot(PipelineMetrics),
    /// Periodic realtime frame (throughput + counters).
    Tick(PipelineMetrics),
    /// An expensive analysis finished a scheduled recompute.
    Refreshed {
        name: &'static str,
        wall: u64,
        micros: u64,
    },
    /// An analysis completed its initial backfill and is now live.
    BackfillComplete { name: &'static str },
}

/// Transport-agnostic sink for realtime metrics. The `nostrsearch-server`
/// implements this over a broadcast channel to WebSocket/SSE clients; the stats
/// crate stays free of any async/transport dependency.
pub trait MetricsObserver: Send + Sync {
    fn emit(&self, ev: &MetricsEvent);
}

/// No-op observer (default when none is wired).
pub struct NullObserver;
impl MetricsObserver for NullObserver {
    fn emit(&self, _ev: &MetricsEvent) {}
}

/// Simple observer that keeps the latest snapshot + a bounded ring of recent
/// frames — handy for a pull endpoint or tests without a real transport.
#[derive(Default)]
pub struct BufferObserver {
    inner: std::sync::Mutex<BufferInner>,
    cap: usize,
}

#[derive(Default)]
struct BufferInner {
    latest: Option<MetricsEvent>,
    recent: std::collections::VecDeque<MetricsEvent>,
}

impl BufferObserver {
    pub fn new(cap: usize) -> Self {
        Self {
            inner: std::sync::Mutex::new(BufferInner::default()),
            cap: cap.max(1),
        }
    }
    /// The most recent full snapshot/tick, if any.
    pub fn latest(&self) -> Option<MetricsEvent> {
        self.inner.lock().unwrap().latest.clone()
    }
    pub fn recent(&self) -> Vec<MetricsEvent> {
        self.inner.lock().unwrap().recent.iter().cloned().collect()
    }
}

impl MetricsObserver for BufferObserver {
    fn emit(&self, ev: &MetricsEvent) {
        let mut g = self.inner.lock().unwrap();
        if matches!(ev, MetricsEvent::Snapshot(_) | MetricsEvent::Tick(_)) {
            g.latest = Some(ev.clone());
        }
        g.recent.push_back(ev.clone());
        let cap = self.cap;
        while g.recent.len() > cap {
            g.recent.pop_front();
        }
    }
}
