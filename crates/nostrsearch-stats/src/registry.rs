//! Object-safe erasure ([`DynAnalysis`]) + a [`Registry`] that tracks per-
//! analysis [`Progress`], dependency **stages**, [`World`] materialization,
//! refresh scheduling, and realtime [`metrics`](crate::metrics) emission.

use crate::metrics::{
    AnalysisMetrics, MetricsEvent, MetricsObserver, NullObserver, Phase, PipelineMetrics, RateMeter,
};
use crate::progress::Progress;
use crate::store::StatStore;
use crate::types::Hash32;
use crate::{Analysis, AnalysisCtx, World};
use anyhow::Result;
use nostrsearch_core::event::NostrEvent;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How far ahead of the reference clock a `created_at` may be and still be
/// allowed to advance an analysis watermark. Publisher clocks drift; the year
/// 55913 is not drift.
pub const FUTURE_SKEW_SECS: u64 = 300;

/// Object-safe view over any [`Analysis`].
pub trait DynAnalysis: Send + Sync {
    fn name(&self) -> &'static str;
    fn epoch(&self) -> u32;
    fn deps(&self) -> &'static [&'static str];
    fn refresh_interval(&self) -> Option<Duration>;
    fn attach(&mut self, ctx: &crate::AttachCtx) -> Result<()>;
    fn wants(&self, ev: &NostrEvent) -> bool;
    /// Kinds this analysis consumes, or `None` for "all of them".
    fn kinds_dyn(&self) -> Option<Vec<u16>>;
    fn health_dyn(&self) -> Option<String>;
    fn observe(&mut self, ev: &NostrEvent, ctx: &AnalysisCtx) -> bool;
    fn refresh(&mut self);
    fn contribute(&self, world: &mut World);
    fn merge_dyn(&mut self, other: Box<dyn DynAnalysis>) -> Result<(), Box<dyn DynAnalysis>>;
    fn snapshot_json(&self) -> serde_json::Value;
    fn drain_delta_json(&mut self) -> Option<serde_json::Value>;
    fn reset_to_default(&mut self);
    fn checkpoint_bin(&self) -> Result<Vec<u8>>;
    fn restore_bin(&mut self, bytes: &[u8]) -> Result<()>;
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

impl<A> DynAnalysis for A
where
    A: Analysis + Clone + Default + Serialize + DeserializeOwned + 'static,
{
    fn name(&self) -> &'static str {
        Analysis::name(self)
    }
    fn epoch(&self) -> u32 {
        Analysis::epoch(self)
    }
    fn deps(&self) -> &'static [&'static str] {
        Analysis::deps(self)
    }
    fn refresh_interval(&self) -> Option<Duration> {
        Analysis::refresh_interval(self)
    }
    fn attach(&mut self, ctx: &crate::AttachCtx) -> Result<()> {
        Analysis::attach(self, ctx)
    }
    fn wants(&self, ev: &NostrEvent) -> bool {
        Analysis::wants(self, ev)
    }
    fn kinds_dyn(&self) -> Option<Vec<u16>> {
        Analysis::kinds(self).map(|k| k.to_vec())
    }
    fn health_dyn(&self) -> Option<String> {
        Analysis::health(self)
    }
    fn observe(&mut self, ev: &NostrEvent, ctx: &AnalysisCtx) -> bool {
        Analysis::observe(self, ev, ctx)
    }
    fn refresh(&mut self) {
        Analysis::refresh(self)
    }
    fn contribute(&self, world: &mut World) {
        Analysis::contribute(self, world)
    }
    fn merge_dyn(&mut self, other: Box<dyn DynAnalysis>) -> Result<(), Box<dyn DynAnalysis>> {
        match other.into_any().downcast::<A>() {
            Ok(c) => {
                Analysis::merge(self, *c);
                Ok(())
            }
            Err(_) => Err(Box::new(A::default())),
        }
    }
    fn snapshot_json(&self) -> serde_json::Value {
        serde_json::to_value(Analysis::snapshot(self)).unwrap_or(serde_json::Value::Null)
    }
    fn drain_delta_json(&mut self) -> Option<serde_json::Value> {
        Analysis::drain_delta(self)
    }
    fn reset_to_default(&mut self) {
        // Clear external storage first, while this value still holds the
        // handles to it. Replacing self drops them.
        Analysis::on_reset(self);
        *self = A::default();
    }
    fn checkpoint_bin(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }
    fn restore_bin(&mut self, bytes: &[u8]) -> Result<()> {
        *self = bincode::deserialize(bytes)?;
        Ok(())
    }
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

/// Per-analysis progress as served by the status endpoint.
#[derive(Debug, Clone, Serialize)]
pub struct AnalysisStatus {
    pub name: String,
    pub epoch: u32,
    /// False = still deriving from the corpus; its report is incomplete.
    pub backfilled: bool,
    /// Highest `created_at` consumed.
    pub watermark: u64,
    pub events: u64,
    pub observed: u64,
    pub consumed: u64,
    pub filtered: u64,
    pub deps: &'static [&'static str],
    /// Set when the analysis could not derive a real answer. A report whose
    /// producer is unhealthy is not evidence of anything.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unhealthy: Option<String>,
}

/// One registered analysis plus its resumable progress (which carries the
/// persisted observability counters).
/// Which policy [`Registry::fold`] applies. The gate *sequence* is fixed;
/// what a mode changes is which gates arm.
#[derive(Clone, Copy)]
enum FoldMode {
    /// Ordered realtime tail: the watermark gate applies to every analysis.
    Live,
    /// Unordered backfill over the archive: analyses still deriving fold
    /// everything, finished ones stay on the watermark rule.
    Backfill,
}

pub struct Entry {
    pub analysis: Box<dyn DynAnalysis>,
    pub progress: Progress,
}

impl Entry {
    pub fn name(&self) -> &'static str {
        self.analysis.name()
    }
    pub fn needs_backfill(&self) -> bool {
        !self.progress.backfilled
    }
}

/// A set of registered analyses that consume the same event stream.
#[derive(Default)]
pub struct Registry {
    entries: Vec<Entry>,
    total_events: u64,
    rate: RateMeter,
    observer: Option<Arc<dyn MetricsObserver>>,
    /// The shared follow-graph handle, kept so re-attaching does not reopen it.
    ///
    /// RocksDB holds an exclusive lock per path. Opening a second handle while
    /// this process already has one fails, so a re-attach after a reset --
    /// which is exactly when an analysis has lost its store -- would error out
    /// and leave that analysis attached to nothing.
    graph: Option<Arc<crate::graph::GraphStore>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Wire a realtime metrics sink (dashboard WS/SSE bridge).
    pub fn set_observer(&mut self, obs: Arc<dyn MetricsObserver>) -> &mut Self {
        self.observer = Some(obs);
        self
    }

    pub fn register<A>(&mut self, analysis: A) -> &mut Self
    where
        A: Analysis + Clone + Default + Serialize + DeserializeOwned + 'static,
    {
        let epoch = analysis.epoch();
        self.entries.push(Entry {
            analysis: Box::new(analysis),
            progress: Progress::fresh(epoch),
        });
        self
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }
    pub fn total_events(&self) -> u64 {
        self.total_events
    }
    pub fn needs_backfill(&self) -> bool {
        self.entries.iter().any(Entry::needs_backfill)
    }
    pub fn phase(&self) -> Phase {
        if self.needs_backfill() {
            Phase::Backfilling
        } else {
            Phase::Live
        }
    }

    /// Lowest watermark among analyses still needing backfill (scan start).
    pub fn backfill_from(&self) -> u64 {
        self.entries
            .iter()
            .filter(|e| e.needs_backfill())
            .map(|e| e.progress.watermark)
            .min()
            .unwrap_or(0)
    }

    // --- persistence (binary) ---

    pub fn load(&mut self, store: &StatStore) -> Result<()> {
        // Rebuild position moved into each analysis's own progress; drop the
        // single shared file an older build may have left behind.
        store.remove_legacy_rebuild_checkpoint();
        for e in &mut self.entries {
            let name = e.analysis.name();
            let cur_epoch = e.analysis.epoch();
            match store.load(name)? {
                Some((state, progress)) if progress.epoch == cur_epoch => {
                    // A checkpoint that will not deserialize must not stop the
                    // node from starting. It happens whenever an analysis's
                    // serialized shape changes without an epoch bump, and
                    // propagating it here takes down the whole process at
                    // startup — in Kubernetes, a CrashLoopBackOff that is
                    // indistinguishable from a slow boot. Re-derive that one
                    // analysis from the corpus instead.
                    match e.analysis.restore_bin(&state) {
                        Ok(()) => {
                            e.progress = progress;
                            // Repair a watermark poisoned by a future-dated
                            // event before `advance_bounded` existed. Left
                            // alone, the analysis consumes nothing until
                            // wall-clock time reaches that timestamp.
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_secs())
                                .unwrap_or(0);
                            let max_ts = now.saturating_add(FUTURE_SKEW_SECS);
                            if e.progress.clamp_watermark(max_ts) {
                                tracing::warn!(
                                    analysis = name,
                                    max_ts,
                                    "watermark was ahead of now; clamped so the analysis \
                                     resumes consuming events"
                                );
                            }
                        }
                        Err(err) => {
                            tracing::warn!(
                                analysis = name,
                                epoch = cur_epoch,
                                error = %err,
                                "unreadable analysis checkpoint; discarding it and re-running \
                                 this analysis's backfill"
                            );
                            e.progress = Progress::fresh(cur_epoch);
                        }
                    }
                }
                _ => e.progress = Progress::fresh(cur_epoch),
            }
        }
        // External storage must be attached before any observe.
        self.attach_all(store.dir())?;
        Ok(())
    }

    /// Open the shared on-disk graph and hand it to every analysis.
    ///
    /// Analyses that need the adjacency keep it in RocksDB rather than holding
    /// billions of edges in RAM, and they share one store so the graph is not
    /// duplicated per analysis. [`load`](Registry::load) calls this; call it
    /// directly when running without a [`StatStore`].
    pub fn attach_all(&mut self, dir: &std::path::Path) -> Result<()> {
        // Reuse the handle if this registry has already opened one. Reopening
        // races the lock held by the analyses that were *not* reset and still
        // hold their Arc.
        let graph = match self.graph.clone() {
            Some(g) => g,
            None => {
                let g = std::sync::Arc::new(crate::graph::GraphStore::open(dir.join("graph"))?);
                self.graph = Some(g.clone());
                g
            }
        };
        let ctx = crate::AttachCtx { graph };
        for e in &mut self.entries {
            e.analysis.attach(&ctx)?;
        }
        Ok(())
    }

    pub fn persist(&self, store: &StatStore) -> Result<()> {
        for e in &self.entries {
            store.save(
                e.analysis.name(),
                &e.analysis.checkpoint_bin()?,
                &e.progress,
            )?;
        }
        Ok(())
    }

    // --- dependency staging ---

    /// Topologically group analyses into dependency stages. Errors on cycles /
    /// unknown deps.
    pub fn stages(&self) -> Result<Vec<Vec<usize>>> {
        let name_to_idx: HashMap<&str, usize> = self
            .entries
            .iter()
            .enumerate()
            .map(|(i, e)| (e.analysis.name(), i))
            .collect();

        let mut stage_of: Vec<Option<usize>> = vec![None; self.entries.len()];
        let mut resolved: HashSet<usize> = HashSet::new();

        for _ in 0..self.entries.len() {
            let mut progressed = false;
            for (i, e) in self.entries.iter().enumerate() {
                if stage_of[i].is_some() {
                    continue;
                }
                let mut max_dep = -1i64;
                let mut ready = true;
                for dep in e.analysis.deps() {
                    let di = *name_to_idx.get(dep).ok_or_else(|| {
                        anyhow::anyhow!(
                            "analysis '{}' depends on unknown '{}'",
                            e.analysis.name(),
                            dep
                        )
                    })?;
                    match stage_of[di] {
                        Some(s) => max_dep = max_dep.max(s as i64),
                        None => {
                            ready = false;
                            break;
                        }
                    }
                }
                if ready {
                    stage_of[i] = Some((max_dep + 1) as usize);
                    resolved.insert(i);
                    progressed = true;
                }
            }
            if !progressed {
                break;
            }
        }

        if resolved.len() != self.entries.len() {
            anyhow::bail!("dependency cycle detected among analyses");
        }

        let max_stage = stage_of.iter().flatten().copied().max().unwrap_or(0);
        let mut stages = vec![Vec::new(); max_stage + 1];
        for (i, s) in stage_of.iter().enumerate() {
            stages[s.unwrap()].push(i);
        }
        Ok(stages)
    }

    // --- folding ---

    /// Feed one event to every entry in `stage` that wants it and hasn't
    /// consumed it (watermark rule). Events must arrive in ascending
    /// `created_at` order.
    pub fn observe_stage(&mut self, stage: &[usize], ev: &NostrEvent, now: u64, world: &World) {
        let indices: Vec<usize> = stage.to_vec();
        self.fold(ev, now, world, FoldMode::Live, &indices);
    }

    /// Feed one event to *all* entries (realtime tail).
    pub fn observe(&mut self, ev: &NostrEvent, now: u64, world: &World) {
        let all: Vec<usize> = (0..self.entries.len()).collect();
        self.fold(ev, now, world, FoldMode::Live, &all);
    }

    /// Feed one event during an unordered backfill over the archive.
    ///
    /// Feeds **every** analysis, so on its own it is correct only for
    /// independent producers. Analyses that depend on another's completed
    /// `World` must be driven one stage per pass with
    /// [`observe_backfill_stage`](Registry::observe_backfill_stage).
    pub fn observe_backfill(&mut self, ev: &NostrEvent, now: u64, world: &World) {
        let all: Vec<usize> = (0..self.entries.len()).collect();
        self.fold(ev, now, world, FoldMode::Backfill, &all);
    }

    /// Unordered-backfill fold restricted to the entries in `stage`.
    ///
    /// The caller replays the corpus once per stage, so that by the time stage
    /// *n* folds, every stage below it has finished and `contribute`d to
    /// `world`. Without this, a consumer like the daily activity report reads
    /// an empty `World` on a cold backfill and silently records every author
    /// as untrusted.
    pub fn observe_backfill_stage(
        &mut self,
        stage: &[usize],
        ev: &NostrEvent,
        now: u64,
        world: &World,
    ) {
        let indices: Vec<usize> = stage.to_vec();
        self.fold(ev, now, world, FoldMode::Backfill, &indices);
    }

    /// The single fold path. Every public observe_* entry point is a thin
    /// wrapper over this.
    ///
    /// It used to be several near-copies, each re-implementing the gate
    /// sequence slightly differently, and every difference was a bug: one copy
    /// pinned watermarks another then honoured; one was written without the
    /// staging rule and recorded every event untrusted. What varies between
    /// modes is *policy*, named in [`FoldMode`]; the sequence itself --
    /// watermark gate, fold, advance -- exists once, so a change to it changes
    /// every mode or none.
    fn fold(
        &mut self,
        ev: &NostrEvent,
        now: u64,
        world: &World,
        mode: FoldMode,
        indices: &[usize],
    ) {
        let (author, id) = match (Hash32::from_hex(&ev.pubkey), Hash32::from_hex(&ev.id)) {
            (Some(a), Some(i)) => (a, i),
            _ => return, // malformed key; drop
        };

        let mut touched = false;
        for &i in indices {
            let e = &mut self.entries[i];

            // The watermark gate. Live consumption is ordered, so it always
            // applies. An analysis still backfilling reads *unordered*
            // history and must fold everything -- the watermark rule would
            // drop earlier events the moment a later one bumped the mark.
            // Once backfilled it rejoins the ordered rule, or replaying newly
            // published dumps would index events and never update stats
            // again.
            let initial = e.needs_backfill();
            let ordered = matches!(mode, FoldMode::Live) || !initial;
            if ordered && !e.progress.should_consume(ev.created_at, &id) {
                continue;
            }

            // The fold itself: identical in every mode.
            if e.analysis.wants(ev) {
                e.progress.counters.observed += 1;
                let ctx = AnalysisCtx::new(now, author, id, world);
                if e.analysis.observe(ev, &ctx) {
                    e.progress.counters.consumed += 1;
                } else {
                    e.progress.counters.filtered += 1;
                }
            }

            // Ordered consumption keeps the count-once boundary set;
            // unordered tracks the high-water mark without it.
            if ordered {
                e.progress
                    .advance_bounded(ev.created_at, id, now.saturating_add(FUTURE_SKEW_SECS));
            } else {
                if ev.created_at > e.progress.watermark {
                    e.progress.watermark = ev.created_at;
                }
                e.progress.events += 1;
            }
            touched = true;
        }
        if touched {
            self.total_events += 1;
        }
    }

    /// Refresh + materialize every stage in dependency order into `world`.
    pub fn materialize_all(&mut self, now_wall: u64, world: &mut World) -> Result<()> {
        for stage in self.stages()? {
            self.materialize_stage(&stage, now_wall, world);
        }
        Ok(())
    }

    /// Mark every analysis's initial backfill complete.
    pub fn mark_all_backfilled(&mut self) -> Result<()> {
        for stage in self.stages()? {
            self.mark_backfilled(&stage);
        }
        Ok(())
    }

    /// Run due refreshes for `stage` and materialize its producers into `world`.
    /// `now_wall` is real wall-clock unix secs (drives refresh intervals).
    /// Position of the analysis called `name`.
    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.entries.iter().position(|e| e.analysis.name() == name)
    }

    /// Union of the kinds every analysis in `stage` consumes.
    ///
    /// `None` means at least one of them takes everything, so nothing can be
    /// filtered out. `Some(ks)` means a replay for this stage only has to look
    /// at those kinds -- rebuilding pagerank walks the whole corpus to fold
    /// kind 3, and `wants()` only saves the fold, which is the cheap half.
    pub fn stage_kinds(&self, stage: &[usize]) -> Option<Vec<u16>> {
        let mut all = std::collections::BTreeSet::new();
        for &i in stage {
            let ks = self.entries[i].analysis.kinds_dyn()?;
            all.extend(ks);
        }
        Some(all.into_iter().collect())
    }

    /// Refresh and materialize `names` now, ignoring their schedules.
    ///
    /// A derived analysis reset by an operator has to rebuild immediately.
    /// Pagerank refreshes daily, so without this a re-derive cleared the ranks
    /// and left them empty until the next scheduled run -- up to 24 hours of
    /// looking like the operation silently failed.
    ///
    /// Returns the number of analyses refreshed.
    pub fn refresh_now(&mut self, names: &[&str], now_wall: u64, world: &mut World) -> usize {
        let idx: Vec<usize> = names.iter().filter_map(|n| self.index_of(n)).collect();
        for &i in &idx {
            // Force the schedule gate in materialize_stage to fire.
            self.entries[i].progress.last_refresh_wall = 0;
        }
        self.materialize_stage(&idx, now_wall, world);
        idx.len()
    }

    pub fn materialize_stage(&mut self, stage: &[usize], now_wall: u64, world: &mut World) {
        for &i in stage {
            if let Some(d) = self.entries[i].analysis.refresh_interval() {
                let last = self.entries[i].progress.last_refresh_wall;
                if now_wall.saturating_sub(last) >= d.as_secs() {
                    let t = Instant::now();
                    self.entries[i].analysis.refresh();
                    let micros = t.elapsed().as_micros() as u64;
                    self.entries[i].progress.last_refresh_wall = now_wall;
                    self.entries[i].progress.counters.refresh_count += 1;
                    self.entries[i].progress.counters.last_refresh_micros = micros;
                    let name = self.entries[i].analysis.name();
                    self.emit(&MetricsEvent::Refreshed {
                        name,
                        wall: now_wall,
                        micros,
                    });
                }
            }
            self.entries[i].analysis.contribute(world);
        }
    }

    /// Per-analysis progress, for the status endpoint.
    pub fn status(&self) -> Vec<AnalysisStatus> {
        self.entries
            .iter()
            .map(|e| AnalysisStatus {
                name: e.analysis.name().to_string(),
                epoch: e.analysis.epoch(),
                backfilled: e.progress.backfilled,
                watermark: e.progress.watermark,
                events: e.progress.events,
                observed: e.progress.counters.observed,
                consumed: e.progress.counters.consumed,
                filtered: e.progress.counters.filtered,
                deps: e.analysis.deps(),
                unhealthy: e.analysis.health_dyn(),
            })
            .collect()
    }

    /// Discard an analysis's accumulated state and progress so it re-derives
    /// from scratch, along with everything that depends on it.
    ///
    /// Returns every name that was reset, or `None` if no analysis has that
    /// name.
    ///
    /// The cascade is not a convenience. A dependent analysis reads its
    /// dependency's output as it folds -- `activity` and `active_users` label
    /// each event trusted or untrusted from the world `follow_graph` built --
    /// so its stored numbers are a function of both. Resetting `follow_graph`
    /// alone leaves those reports holding counts derived from a graph that no
    /// longer exists, and no amount of re-ingesting fixes them, because they
    /// are already-folded totals rather than something recomputed on read.
    ///
    /// Clearing `backfilled` is the part that makes the rebuild possible: an
    /// analysis in that state folds every event it is handed regardless of
    /// watermark, so out-of-order history (the scraper walking backwards, or
    /// an archive rebuild) is picked up instead of rejected as "already past".
    pub fn reset(&mut self, name: &str) -> Option<Vec<&'static str>> {
        if !self.entries.iter().any(|e| e.analysis.name() == name) {
            return None;
        }

        // Transitive closure over reverse dependencies.
        let mut doomed: Vec<&'static str> = Vec::new();
        let mut queue = vec![name.to_string()];
        while let Some(cur) = queue.pop() {
            for e in &self.entries {
                let n = e.analysis.name();
                if n == cur && !doomed.contains(&n) {
                    doomed.push(n);
                }
                if e.analysis.deps().contains(&cur.as_str()) && !doomed.contains(&n) {
                    doomed.push(n);
                    queue.push(n.to_string());
                }
            }
        }

        for e in self.entries.iter_mut() {
            if doomed.contains(&e.analysis.name()) {
                e.analysis.reset_to_default();
                e.progress = Progress::fresh(e.analysis.epoch());
            }
        }
        doomed.sort_unstable();
        Some(doomed)
    }
    /// Reset every analysis, external storage included.
    ///
    /// Unlike resetting one analysis there is no dependency question to
    /// answer -- everything goes -- which makes this the honest way to rebuild
    /// reports whose numbers are suspect.
    pub fn reset_all(&mut self) -> Vec<&'static str> {
        let mut names = Vec::with_capacity(self.entries.len());
        for e in self.entries.iter_mut() {
            e.analysis.reset_to_default();
            e.progress = Progress::fresh(e.analysis.epoch());
            names.push(e.analysis.name());
        }
        self.total_events = 0;
        names.sort_unstable();
        names
    }

    /// Names of analyses that have not completed a backfill over the corpus.
    ///
    /// Purely a report: `backfilled` is only ever set by something that
    /// actually replayed the corpus (the staged ingest passes, or a completed
    /// admin replay). An earlier version inferred it from "has consumed at
    /// least one event", which marked an analysis complete after two live
    /// events against a 470M-document index -- and being marked complete is
    /// what puts it on the watermark path, where the gap scraper's historical
    /// events are then rejected as "already consumed". The two bugs together
    /// left the reports permanently empty.
    pub fn outstanding_backfills(&self) -> Vec<&'static str> {
        self.entries
            .iter()
            .filter(|e| !e.progress.backfilled)
            .map(|e| e.analysis.name())
            .collect()
    }

    pub fn mark_backfilled(&mut self, stage: &[usize]) {
        for &i in stage {
            if !self.entries[i].progress.backfilled {
                self.entries[i].progress.backfilled = true;
                let name = self.entries[i].analysis.name();
                self.emit(&MetricsEvent::BackfillComplete { name });
            }
        }
    }

    /// Drain every analysis's pending partial changes.
    ///
    /// Returns only the analyses that both support deltas and actually changed,
    /// so an idle pipeline produces an empty vec (and therefore no dashboard
    /// traffic). Destructive: see [`crate::delta`] for the contract.
    pub fn drain_deltas(&mut self) -> Vec<crate::delta::ReportDelta> {
        self.entries
            .iter_mut()
            .filter_map(|e| {
                let name = e.analysis.name();
                e.analysis
                    .drain_delta_json()
                    .map(|patch| crate::delta::ReportDelta {
                        name: name.to_string(),
                        patch,
                    })
            })
            .collect()
    }

    pub fn snapshots(&self) -> Vec<(&'static str, serde_json::Value)> {
        self.entries
            .iter()
            .map(|e| (e.analysis.name(), e.analysis.snapshot_json()))
            .collect()
    }

    // --- observability ---

    fn now_wall() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    /// Build a full pipeline metrics snapshot.
    pub fn metrics(&self, world_pubkeys: usize) -> PipelineMetrics {
        let at = Self::now_wall();
        let analyses = self
            .entries
            .iter()
            .map(|e| {
                let next_refresh_in = e.analysis.refresh_interval().map(|d| {
                    let elapsed = at.saturating_sub(e.progress.last_refresh_wall);
                    d.as_secs().saturating_sub(elapsed)
                });
                let c = &e.progress.counters;
                AnalysisMetrics {
                    name: e.analysis.name(),
                    epoch: e.progress.epoch,
                    watermark: e.progress.watermark,
                    events_total: e.progress.events,
                    observed: c.observed,
                    consumed: c.consumed,
                    filtered: c.filtered,
                    backfilled: e.progress.backfilled,
                    refresh_count: c.refresh_count,
                    last_refresh_wall: e.progress.last_refresh_wall,
                    last_refresh_micros: c.last_refresh_micros,
                    next_refresh_in,
                }
            })
            .collect();
        PipelineMetrics {
            at,
            phase: self.phase(),
            total_events: self.total_events,
            events_per_sec: self.rate.per_sec(),
            world_pubkeys,
            analyses,
        }
    }

    fn emit(&self, ev: &MetricsEvent) {
        if let Some(obs) = &self.observer {
            obs.emit(ev);
        }
    }

    /// Emit the initial full snapshot (call once at startup / per new client).
    pub fn emit_snapshot(&self, world_pubkeys: usize) {
        self.emit(&MetricsEvent::Snapshot(self.metrics(world_pubkeys)));
    }

    /// Update throughput and emit a realtime tick frame. Call on a cadence
    /// (e.g. every second, or every N events).
    pub fn emit_tick(&mut self, world_pubkeys: usize) {
        let total = self.total_events;
        self.rate.tick(total);
        let m = self.metrics(world_pubkeys);
        self.emit(&MetricsEvent::Tick(m));
    }
}

impl Default for Box<dyn MetricsObserver> {
    fn default() -> Self {
        Box::new(NullObserver)
    }
}
