//! nostrsearch-stats
//!
//! Pluggable, parallel-friendly, resumable analysis framework over the Nostr
//! event corpus. Generalizes nostr-dashboard's `StatObject` into a map-reduce
//! [`Analysis`] trait so new collectors (trending, per-kind breakdowns, zap
//! flows, …) drop in without touching the pipeline, run in parallel across
//! time-shards, resume incrementally, and depend on one another.
//!
//! Scale-oriented properties:
//! - **Compact keys** — pubkeys/ids are [`Hash32`](types::Hash32) (`[u8; 32]`),
//!   not 64-char hex strings.
//! - **Binary checkpoints** — state persists as bincode, never a giant JSON
//!   value tree.
//! - **Additive + resumable** — each analysis has its own watermark; a new one
//!   backfills alone, existing ones tail live.
//! - **Per-analysis refresh interval** — cheap analyses fold every event;
//!   expensive ones (pagerank) recompute on a schedule via [`Analysis::refresh`].
//! - **Dependency stages** — producers materialize a [`World`]; consumers read
//!   it (and can filter publishers by follower/WoT thresholds).
//! - **Observability** — the registry emits per-analysis + pipeline
//!   [`metrics`] (throughput, consumed/filtered, refresh timing) as JSON.

pub mod analyses;
pub mod ctx;
pub mod delta;
pub mod graph;
pub mod hll;
pub mod metrics;
pub mod progress;
pub mod registry;
pub mod run;
pub mod store;
pub mod types;
pub mod wot;

pub use ctx::{AnalysisCtx, PubkeyStat, PublisherFilter, World};
pub use delta::{ReportDelta, merge_patch};
pub use graph::{GraphStore, SharedGraph};
pub use metrics::{
    AnalysisMetrics, BufferObserver, MetricsEvent, MetricsObserver, NullObserver, Phase,
    PipelineMetrics,
};
pub use progress::{Progress, Rebuild};
pub use registry::{AnalysisStatus, DynAnalysis, RebuildPlan, Registry};
pub use run::backfill_in_memory;
pub use store::StatStore;
pub use types::{EventId, Hash32, Pubkey};
pub use wot::{SharedWot, WotIndex};

use crate::ctx::World as WorldTy;

/// Shared resources handed to [`Analysis::attach`].
pub struct AttachCtx {
    /// The on-disk follow graph, shared by every analysis that needs it so
    /// there is exactly one copy.
    pub graph: crate::graph::SharedGraph,
}
use nostrsearch_core::event::NostrEvent;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::time::Duration;

/// A pluggable analysis over the event stream.
///
/// Implementors keep partial state that can be produced independently over a
/// slice of the corpus (one time-shard, one archive file, one thread) and then
/// [`merge`](Analysis::merge)d — this is what makes the framework parallel over
/// nostrsearch's shard-per-writer layout.
pub trait Analysis: Send + Sync {
    /// Typed, serializable result served by the dashboard API.
    type Output: Serialize + DeserializeOwned + Send;

    /// Stable identifier — used as the file/endpoint name.
    fn name(&self) -> &'static str;

    /// Discard state this analysis keeps *outside* itself.
    ///
    /// Called on reset, while the handles are still live, before the struct is
    /// replaced with its default. Without it an analysis backed by external
    /// storage cannot be reset at all: clearing the struct leaves the store
    /// fully populated, and `follow_graph` then rejects every replayed contact
    /// list as not-newer-than what it already holds, folding nothing. Its
    /// report stayed empty through a reset and a full archive rebuild, with
    /// `observed=1102, consumed=0` the only sign anything was wrong.
    fn on_reset(&mut self) {}

    /// Attach shared external storage. Called by the registry after `load`
    /// and before any `observe`, so analyses whose state is too large for RAM
    /// (the follow graph) can keep it on disk instead of serializing it.
    fn attach(&mut self, _ctx: &AttachCtx) -> anyhow::Result<()> {
        Ok(())
    }

    /// Recompute epoch. Bump when the analysis logic changes enough that
    /// persisted state is invalid — the runner discards saved state and
    /// re-scrapes the corpus for this analysis only.
    fn epoch(&self) -> u32 {
        0
    }

    /// Names of analyses this one depends on. The runner topologically orders
    /// analyses into stages: every dependency runs (and
    /// [`contribute`](Analysis::contribute)s to the [`World`]) in an earlier
    /// stage. Cycles are rejected.
    fn deps(&self) -> &'static [&'static str] {
        &[]
    }

    /// How often the expensive [`refresh`](Analysis::refresh) should run.
    ///
    /// `None` (default) = the analysis is incremental; its `contribute` already
    /// reflects the latest state, so no scheduled recompute is needed. `Some(d)`
    /// = only recompute at most once per `d` of wall-clock time. Pagerank uses
    /// e.g. `Some(24h)` so a full recompute doesn't run on every follow change.
    fn refresh_interval(&self) -> Option<Duration> {
        None
    }

    /// Expensive periodic recompute of derived/cached results from accumulated
    /// raw state (e.g. run pagerank over the follow graph). Called by the runner
    /// only when [`refresh_interval`](Analysis::refresh_interval) has elapsed.
    /// Default: no-op (incremental analyses need nothing here).
    fn refresh(&mut self) {}

    /// Publish this analysis's results into the shared [`World`] so downstream
    /// analyses can read them. Called after this analysis's stage/refresh.
    /// Producers override; leaf consumers leave it a no-op.
    fn contribute(&self, _world: &mut WorldTy) {}

    /// Kinds this analysis cares about. `None` = all kinds. Used to skip
    /// feeding irrelevant events — a large saving at ~900M+ events.
    fn kinds(&self) -> Option<&[u16]> {
        None
    }

    /// Fold one event into partial state. `ctx` exposes the parsed author/id and
    /// the materialized world so analyses stay pure folds (no I/O).
    ///
    /// Returns `true` if the event was actually folded, or `false` if the
    /// analysis skipped it internally (e.g. a [`PublisherFilter`] rejected the
    /// author). The registry uses this for `consumed` vs `filtered` metrics.
    fn observe(&mut self, ev: &NostrEvent, ctx: &AnalysisCtx) -> bool;

    /// Map-reduce merge: fold another partial (from a different shard/thread)
    /// into `self`. Must be associative and commutative.
    fn merge(&mut self, other: Self)
    where
        Self: Sized;

    /// Snapshot the current typed result for serving.
    fn snapshot(&self) -> Self::Output;

    /// Drain the changes accumulated since the previous call, as a JSON merge
    /// patch over this analysis's [`snapshot`](Analysis::snapshot) shape.
    ///
    /// This is what lets a dashboard show numbers moving in realtime instead
    /// of re-fetching a whole report: the pipeline drains deltas on a short
    /// cadence and streams them to subscribers, who
    /// [`merge_patch`](crate::merge_patch) them into the snapshot they hold.
    ///
    /// Each impl chooses its own granularity — the activity report emits only
    /// the day buckets it touched, the client report only the clients that
    /// published. Returning `None` (the default) means "no incremental view";
    /// consumers then just poll [`snapshot`](Analysis::snapshot).
    ///
    /// Implementors must reset their dirty set here: draining twice with no
    /// intervening [`observe`](Analysis::observe) returns `None` the second
    /// time. See [`delta`](crate::delta) for the full contract.
    fn drain_delta(&mut self) -> Option<serde_json::Value> {
        None
    }

    /// Does this analysis want to see `ev`? (honours [`kinds`](Analysis::kinds).)
    fn wants(&self, ev: &NostrEvent) -> bool {
        match self.kinds() {
            None => true,
            Some(ks) => ks.contains(&ev.kind),
        }
    }
}
