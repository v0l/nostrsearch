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
}

/// One registered analysis plus its resumable progress (which carries the
/// persisted observability counters).
/// What a rebuild reader may do with a dump, given where the analyses are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebuildPlan {
    /// Every stage has folded every dump: the run is over. The reader stops
    /// iterating instead of burning a full pass discovering it file by file.
    Finished,
    /// Every analysis in the current stage has already folded this file.
    SkipFile,
    /// All of them are at the same point, so the reader can fast-forward past
    /// this id without parsing.
    ResumeAfter(String),
    /// They disagree (or all start fresh), so every event must be handed over
    /// and each analysis skips on its own.
    FoldAll,
}

/// A rebuild run in progress: the dump list and each entry's dependency stage.
///
/// Transient -- re-sent by the reader at the start of every run -- because the
/// registry cannot answer "is this stage finished" without knowing what the
/// full set of dumps is, and only the reader knows that.
///
/// The run is a small state machine, and these are its only transitions:
///
/// ```text
///   set_rebuild_files ──▶ RUNNING ──▶ begin_rebuild(file)      (per file)
///                            │        finish_rebuild_file(file) (per file)
///                            │        ... repeated once per dependency stage
///                            ├──▶ finish_rebuild_run   completed: clear, keep folds
///                            └──▶ finish_rebuild_run   cancelled: same, via abort
///   (process death)     ──▶ positions persist ──▶ next start resumes RUNNING
/// ```
///
/// Folding through `FoldMode::Rebuild` outside a run is refused, not
/// tolerated: without the run there is no stage arithmetic, and unstaged
/// folding records dependents against a world that does not exist yet.
struct RebuildRun {
    files: Vec<String>,
    entry_stage: Vec<usize>,
    stage_count: usize,
}

/// Which policy [`Registry::fold`] applies. The gate *sequence* is fixed;
/// what each mode changes is which gates arm.
#[derive(Clone, Copy)]
enum FoldMode<'a> {
    /// Ordered realtime tail: the watermark gate applies to every analysis.
    Live,
    /// Unordered backfill: analyses still deriving fold everything, finished
    /// ones stay on the watermark rule.
    Backfill,
    /// A rebuild run: backfill rules plus dependency-stage gating and
    /// per-analysis, per-file resume positions.
    Rebuild { file: &'a str },
}

pub struct Entry {
    pub analysis: Box<dyn DynAnalysis>,
    pub progress: Progress,
    /// Fast-forwarding to this analysis's own resume point in the current
    /// rebuild. Transient: derived from `progress.rebuild` when a rebuild
    /// starts, never persisted.
    pub resuming: bool,
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
    /// The rebuild run in progress, if any. See [`RebuildRun`].
    rebuild_run: Option<RebuildRun>,
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
            resuming: false,
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
        let graph = std::sync::Arc::new(crate::graph::GraphStore::open(dir.join("graph"))?);
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

    /// The single fold path. Every public observe_* entry point is a thin
    /// wrapper over this.
    ///
    /// It used to be four near-copies -- live, backfill, staged backfill,
    /// rebuild -- each re-implementing the gate sequence slightly differently,
    /// and every difference was a bug: the rebuild copy was written without
    /// the stage gate and recorded every event untrusted; the backfill copy
    /// pinned watermarks the live copy then honoured. What varies between
    /// modes is *policy*, expressed in [`FoldMode`]; the sequence itself --
    /// stage gate, resume gate, watermark gate, fold, advance -- exists once,
    /// so a change to it changes every mode or none.
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

        // Rebuild pass context, resolved once per event.
        let (rebuild_file, current_stage, entry_stage) = match mode {
            FoldMode::Rebuild { file } => {
                // No declared run means no stage arithmetic, and folding
                // without it is the single-pass bug: dependents recorded
                // against a world that does not exist yet, permanently.
                // Refusing loudly beats corrupting quietly -- this is only
                // reachable through a caller that skipped set_rebuild_files.
                let Some(run) = self.rebuild_run.as_ref() else {
                    static WARNED: std::sync::Once = std::sync::Once::new();
                    WARNED.call_once(|| {
                        tracing::error!(
                            "rebuild event dropped: no run declared \
                             (set_rebuild_files was not called)"
                        );
                    });
                    return;
                };
                (Some(file), self.rebuild_stage(), run.entry_stage.clone())
            }
            _ => (None, None, Vec::new()),
        };

        let mut touched = false;
        for &i in indices {
            let e = &mut self.entries[i];

            // Gate 1 (rebuild only): dependency stage. Dependents wait for
            // their own pass -- they label events using the world their
            // dependency builds, and folding them early records everything
            // against a world that does not exist yet.
            if let Some(s) = current_stage
                && entry_stage.get(i).copied().unwrap_or(0) != s
            {
                continue;
            }

            // Gate 2 (rebuild only): this analysis's own resume point. The
            // matching event is the last one it folded, so it is consumed
            // here and folding restarts with the next.
            if rebuild_file.is_some() && e.resuming {
                if e.progress.rebuild.last_id == ev.id {
                    e.resuming = false;
                }
                continue;
            }

            // Gate 3: the watermark. Live consumption is ordered, so it
            // always applies. An analysis mid-backfill or mid-rebuild reads
            // *unordered* history and must fold everything -- the watermark
            // rule would drop earlier events the moment a later one bumped
            // the mark. Once backfilled it rejoins the ordered rule, or
            // replaying newly published dumps would never update it again.
            let initial = e.needs_backfill();
            let ordered = matches!(mode, FoldMode::Live) || !initial;
            if ordered && !e.progress.should_consume(ev.created_at, &id) {
                continue;
            }

            // Gate 4 (rebuild only): a dump this analysis already folded.
            if let Some(f) = rebuild_file
                && !e.progress.rebuild.needs(f)
            {
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

            // Advance: ordered consumption keeps the count-once boundary set;
            // unordered tracks the high-water mark without it, plus the
            // per-file rebuild position when there is one.
            if ordered {
                e.progress
                    .advance_bounded(ev.created_at, id, now.saturating_add(FUTURE_SKEW_SECS));
            } else {
                if ev.created_at > e.progress.watermark {
                    e.progress.watermark = ev.created_at;
                }
                e.progress.events += 1;
                if let Some(f) = rebuild_file {
                    e.progress.rebuild.advance(f, &ev.id);
                }
            }
            touched = true;
        }
        if touched {
            self.total_events += 1;
        }
    }

    /// Feed one event to *all* entries (realtime tail).
    pub fn observe(&mut self, ev: &NostrEvent, now: u64, world: &World) {
        let all: Vec<usize> = (0..self.entries.len()).collect();
        self.observe_stage(&all, ev, now, world);
    }

    /// Feed one event during an **unordered, from-scratch** backfill — e.g.
    /// streaming the archive, which is not sorted by `created_at`. Unlike
    /// [`observe`](Registry::observe) it does not apply the watermark dedup gate
    /// (the source, `NostrCursor`, already dedupes by id) and only feeds
    /// analyses still needing backfill. The watermark tracks the max
    /// `created_at` seen so the live tail can resume after it.
    ///
    /// Feeds **every** analysis, so it is correct on its own only for
    /// independent / stage-0 producers. Analyses that depend on another
    /// analysis's completed `World` must be driven one stage per pass with
    /// [`observe_backfill_stage`](Registry::observe_backfill_stage).
    pub fn observe_backfill(&mut self, ev: &NostrEvent, now: u64, world: &World) {
        let all: Vec<usize> = (0..self.entries.len()).collect();
        self.observe_backfill_stage(&all, ev, now, world);
    }

    /// Start (or resume) a rebuild run over `files`.
    ///
    /// Must be called before `begin_rebuild`: stage arithmetic needs the full
    /// dump list, and only the reader knows it.
    pub fn set_rebuild_files(&mut self, files: Vec<String>) -> Result<()> {
        let stages = self.stages()?;
        let mut entry_stage = vec![0usize; self.entries.len()];
        for (s, idxs) in stages.iter().enumerate() {
            for &i in idxs {
                entry_stage[i] = s;
            }
        }
        self.rebuild_run = Some(RebuildRun {
            files,
            entry_stage,
            stage_count: stages.len(),
        });
        Ok(())
    }

    /// The dependency stage the rebuild is currently folding, or `None` when
    /// every stage has folded every dump.
    ///
    /// The earliest stage with outstanding work wins. Dependents must not fold
    /// alongside their dependencies: they label events using the world their
    /// dependency builds, so folding them in the same pass records everything
    /// against a world that does not exist yet -- on a cold graph, every event
    /// permanently untrusted.
    pub fn rebuild_stage(&self) -> Option<usize> {
        let run = self.rebuild_run.as_ref()?;
        (0..run.stage_count).find(|&s| {
            self.entries.iter().enumerate().any(|(i, e)| {
                run.entry_stage[i] == s
                    && e.needs_backfill()
                    && run.files.iter().any(|f| e.progress.rebuild.needs(f))
            })
        })
    }

    /// Begin a rebuild pass over `file`, arming each participating analysis's
    /// resume point.
    ///
    /// Returns what the *reader* may safely do: skip the file, fast-forward
    /// past a shared id, or hand over every event so each analysis skips on
    /// its own. Fast-forwarding is only offered when every participant agrees
    /// on the position, since a reader-level skip skips for all of them.
    pub fn begin_rebuild(&mut self, file: &str) -> RebuildPlan {
        let Some(stage) = self.rebuild_stage() else {
            return RebuildPlan::Finished;
        };

        let mut positions: Vec<Option<String>> = Vec::new();
        let run_stage: Vec<usize> = self
            .rebuild_run
            .as_ref()
            .map(|r| r.entry_stage.clone())
            .unwrap_or_default();

        for (i, e) in self.entries.iter_mut().enumerate() {
            let participating = run_stage.get(i).copied().unwrap_or(0) == stage
                && e.needs_backfill()
                && e.progress.rebuild.needs(file);
            if !participating {
                e.resuming = false;
                continue;
            }
            let at = (e.progress.rebuild.file == file && !e.progress.rebuild.last_id.is_empty())
                .then(|| e.progress.rebuild.last_id.clone());
            e.resuming = at.is_some();
            positions.push(at);
        }

        let Some(first) = positions.first() else {
            return RebuildPlan::SkipFile;
        };
        match first {
            Some(id) if positions.iter().all(|p| p == first) => {
                RebuildPlan::ResumeAfter(id.clone())
            }
            _ => RebuildPlan::FoldAll,
        }
    }

    /// Mark `file` fully folded for every analysis folding it *in this pass*.
    ///
    /// Only the current stage: marking dependents too would record files as
    /// folded into analyses that never saw them.
    pub fn finish_rebuild_file(&mut self, file: &str) {
        let stage = self.rebuild_stage();
        let run_stage: Vec<usize> = self
            .rebuild_run
            .as_ref()
            .map(|r| r.entry_stage.clone())
            .unwrap_or_default();
        for (i, e) in self.entries.iter_mut().enumerate() {
            let in_stage = match stage {
                Some(s) => run_stage.get(i).copied().unwrap_or(0) == s,
                None => false,
            };
            if in_stage && e.needs_backfill() {
                e.progress.rebuild.finish(file);
            }
            e.resuming = false;
        }
    }

    /// End the rebuild run and clear every position, so `rebuilding()` goes
    /// quiet and the next run starts from the top.
    ///
    /// `backfilled` is deliberately left false: flipping it would put the
    /// analyses on the watermark path, where the gap scraper's historical
    /// events -- days or months older than the watermark -- are rejected. The
    /// analyses keep folding everything, which is the live behaviour that
    /// works.
    pub fn finish_rebuild_run(&mut self) {
        self.rebuild_run = None;
        for e in self.entries.iter_mut() {
            e.progress.rebuild = crate::progress::Rebuild::default();
            e.resuming = false;
        }
    }

    /// Analyses with an archive rebuild still in progress.
    pub fn rebuilding(&self) -> Vec<&'static str> {
        self.entries
            .iter()
            .filter(|e| {
                e.needs_backfill()
                    && (!e.progress.rebuild.file.is_empty()
                        || !e.progress.rebuild.completed.is_empty())
            })
            .map(|e| e.analysis.name())
            .collect()
    }

    /// Fold a replayed event during a rebuild, honouring each analysis's own
    /// resume point.
    ///
    /// An analysis still fast-forwarding folds nothing until it sees the last
    /// event it recorded; everything after that is folded and its position
    /// advanced. Positions are per-analysis because resets are: one analysis
    /// may be a third of the way through the archive while another has never
    /// started.
    pub fn observe_rebuild(&mut self, ev: &NostrEvent, now: u64, world: &World, file: &str) {
        let all: Vec<usize> = (0..self.entries.len()).collect();
        self.fold(ev, now, world, FoldMode::Rebuild { file }, &all);
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
    /// Returns the names cleared. Unlike resetting one analysis there is no
    /// dependency question to answer -- everything goes -- which makes this the
    /// honest way to rebuild reports whose numbers are suspect, rather than
    /// resetting them one at a time and reasoning about what each one's stored
    /// totals were derived from.
    pub fn reset_all(&mut self) -> Vec<&'static str> {
        let mut names = Vec::with_capacity(self.entries.len());
        for e in self.entries.iter_mut() {
            e.analysis.reset_to_default();
            e.progress = Progress::fresh(e.analysis.epoch());
            e.resuming = false;
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
