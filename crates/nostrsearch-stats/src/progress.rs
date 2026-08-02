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

/// How far below the watermark an event may arrive and still be folded.
///
/// Covers ordinary relay jitter and the gap scraper handing back events from
/// earlier in the day. Anything older than this is presumed already consumed on
/// a previous pass.
pub const LIVE_LAG_SECS: u64 = 6 * 3600;

/// Cap on the count-once id set, which now spans [`LIVE_LAG_SECS`] rather than
/// a single second.
const MAX_BOUNDARY: usize = 200_000;

/// Where one analysis has reached in a rebuild over the archive.
///
/// Per-analysis, because analyses are reset independently and a newly
/// registered one needs a full pass while everything else needs none. A single
/// shared position cannot express that, and worse, clearing it on one analysis
/// destroys the resume point of a rebuild running for another.
///
/// The position is the id of the last event folded, not a byte offset. An id is
/// self-validating: if a dump is rewritten, re-sorted or appended to, a stale
/// offset still points somewhere plausible and silently skips or repeats a span
/// of events, corrupting every counter with nothing to show for it. A missing
/// id is detectable. Resuming costs a linear re-read either way, since a plain
/// zstd frame cannot be opened part-way through.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Rebuild {
    /// Dumps this analysis has folded in full.
    pub completed: Vec<String>,
    /// Dump in progress, if any.
    pub file: String,
    /// Id of the last event folded from `file`.
    pub last_id: String,
}

impl Rebuild {
    /// Whether `file` still needs folding into this analysis.
    pub fn needs(&self, file: &str) -> bool {
        !self.completed.iter().any(|f| f == file)
    }

    /// Record a folded event.
    pub fn advance(&mut self, file: &str, id: &str) {
        if self.file != file {
            self.file.clear();
            self.file.push_str(file);
        }
        self.last_id.clear();
        self.last_id.push_str(id);
    }

    /// Mark `file` fully folded.
    pub fn finish(&mut self, file: &str) {
        if !self.completed.iter().any(|f| f == file) {
            self.completed.push(file.to_string());
        }
        if self.file == file {
            self.file.clear();
            self.last_id.clear();
        }
    }
}

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
    /// Position in an archive rebuild. Defaulted so state written before this
    /// existed still loads.
    #[serde(default)]
    pub rebuild: Rebuild,
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
            rebuild: Rebuild::default(),
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
        // Out-of-order delivery is normal, not exceptional. A relay can hand us
        // a thirty-second-old event straight after a fresh one, and the gap
        // scraper deliberately walks *backwards* through history. A strict
        // `created_at < watermark` test rejects both: the watermark ratchets to
        // the highest timestamp ever seen and only successive record-highs get
        // through. In production that meant 70 events counted against an index
        // taking hundreds of thousands a day.
        //
        // Allow anything within the lag window and rely on the id set for
        // count-once, which is what actually guarantees correctness here.
        if created_at + LIVE_LAG_SECS < self.watermark {
            return false;
        }
        if self.boundary.contains(id) {
            return false;
        }
        true
    }

    /// Record that `(created_at, id)` was consumed, advancing the watermark.
    pub fn advance(&mut self, created_at: u64, id: EventId) {
        if created_at > self.watermark {
            self.watermark = created_at;
        }
        // The id set now spans the whole lag window rather than a single
        // second, so it needs its own bound. Clearing wholesale is crude but
        // safe: the archive database and the dedupe store both reject repeats
        // before an event ever reaches an analysis, so this set is a second
        // line of defence rather than the only one.
        if self.boundary.len() >= MAX_BOUNDARY {
            self.boundary.clear();
        }
        self.boundary.insert(id);
        self.events += 1;
    }
}
