//! Per-analysis resumable progress.
//!
//! Every analysis tracks its own watermark independently, so the framework is
//! **additive**: registering a brand-new analysis (watermark 0) triggers a full
//! backfill scan of the corpus *for that analysis only*, while already-computed
//! analyses resume from their saved watermark and just tail live events.

use crate::metrics::Counters;
use crate::types::EventId;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// How far a single analysis has consumed the corpus.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Progress {
    /// Epoch the persisted state was computed under. If the analysis's current
    /// [`epoch`](crate::Analysis::epoch) differs, saved state is discarded and
    /// the analysis is re-scraped from scratch.
    pub epoch: u32,
    /// Highest `created_at` (unix secs) fully consumed.
    pub watermark: u64,
    /// Event ids observed at exactly `watermark` — dedupes the backfill→live
    /// boundary so events on the boundary second are counted once.
    pub boundary: HashSet<EventId>,
    /// Total events folded so far.
    pub events: u64,
    /// Whether the initial full backfill scan has completed.
    pub backfilled: bool,
    /// Wall-clock (unix secs) of the last expensive `refresh()`; drives the
    /// per-analysis refresh interval.
    pub last_refresh_wall: u64,
    /// Cumulative observability counters, persisted so totals survive restarts.
    #[serde(default)]
    pub counters: Counters,
}

impl Progress {
    pub fn fresh(epoch: u32) -> Self {
        Self {
            epoch,
            watermark: 0,
            boundary: HashSet::new(),
            events: 0,
            backfilled: false,
            last_refresh_wall: 0,
            counters: Counters::default(),
        }
    }

    /// Advance the watermark, ignoring timestamps beyond `max_ts`.
    ///
    /// Nostr `created_at` is publisher-supplied and unvalidated. A single event
    /// dated in the year 55913 (the corpus contains them) would otherwise park
    /// the watermark there, after which [`should_consume`](Self::should_consume)
    /// rejects **every** real event forever — the analysis silently stops
    /// counting and never recovers. The event is still folded and counted; it
    /// just does not get to define "how far we have consumed".
    pub fn advance_bounded(&mut self, created_at: u64, id: EventId, max_ts: u64) {
        if created_at > max_ts {
            self.events += 1;
            return;
        }
        self.advance(created_at, id);
    }

    /// Pull a watermark that is implausibly far ahead back to `max_ts`.
    ///
    /// Repairs state already poisoned by the above before it was fixed:
    /// without this, a node restarts with the bad watermark still persisted and
    /// stays stuck until wall-clock time catches up with it. Returns whether a
    /// repair was needed.
    pub fn clamp_watermark(&mut self, max_ts: u64) -> bool {
        if self.watermark > max_ts {
            self.watermark = max_ts;
            self.boundary.clear();
            return true;
        }
        false
    }

    /// Should this event be folded? Enforces monotonic, count-once semantics
    /// given an ascending event stream.
    pub fn should_consume(&self, created_at: u64, id: &EventId) -> bool {
        if created_at < self.watermark {
            return false;
        }
        if created_at == self.watermark && self.boundary.contains(id) {
            return false;
        }
        true
    }

    /// Record that `(created_at, id)` was consumed, advancing the watermark.
    pub fn advance(&mut self, created_at: u64, id: EventId) {
        if created_at > self.watermark {
            self.watermark = created_at;
            self.boundary.clear();
        }
        if created_at == self.watermark {
            self.boundary.insert(id);
        }
        self.events += 1;
    }
}
