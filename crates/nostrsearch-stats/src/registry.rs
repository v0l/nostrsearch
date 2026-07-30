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

/// One registered analysis plus its resumable progress (which carries the
/// persisted observability counters).
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
        for e in &mut self.entries {
            let name = e.analysis.name();
            let cur_epoch = e.analysis.epoch();
            match store.load(name)? {
                Some((state, progress)) if progress.epoch == cur_epoch => {
                    e.analysis.restore_bin(&state)?;
                    e.progress = progress;
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
            store.save(e.analysis.name(), &e.analysis.checkpoint_bin()?, &e.progress)?;
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
        let (author, id) = match (Hash32::from_hex(&ev.pubkey), Hash32::from_hex(&ev.id)) {
            (Some(a), Some(i)) => (a, i),
            _ => return, // malformed key; drop
        };
        let mut touched = false;
        for &i in stage {
            let e = &mut self.entries[i];
            if !e.progress.should_consume(ev.created_at, &id) {
                continue;
            }
            if e.analysis.wants(ev) {
                e.progress.counters.observed += 1;
                let ctx = AnalysisCtx::new(now, author, id, world);
                if e.analysis.observe(ev, &ctx) {
                    e.progress.counters.consumed += 1;
                } else {
                    e.progress.counters.filtered += 1;
                }
            }
            e.progress.advance(ev.created_at, id);
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
    /// NOTE: single-pass, so it correctly serves only **independent / stage-0**
    /// producers (e.g. the WoT producers). Analyses that depend on another
    /// analysis's completed `World` need the staged multipass runner.
    pub fn observe_backfill(&mut self, ev: &NostrEvent, now: u64, world: &World) {
        let (author, id) = match (Hash32::from_hex(&ev.pubkey), Hash32::from_hex(&ev.id)) {
            (Some(a), Some(i)) => (a, i),
            _ => return,
        };
        let mut touched = false;
        for e in &mut self.entries {
            // An analysis still doing its initial scan folds everything: the
            // archive is unordered, so the watermark rule would drop earlier
            // events as soon as a later one bumped the mark.
            //
            // An analysis that already finished falls back to the watermark
            // rule rather than being skipped — otherwise re-running a backfill
            // over newly published dumps would index the events but silently
            // never update stats/WoT again.
            let initial = e.needs_backfill();
            if !initial && !e.progress.should_consume(ev.created_at, &id) {
                continue;
            }
            if e.analysis.wants(ev) {
                e.progress.counters.observed += 1;
                let ctx = AnalysisCtx::new(now, author, id, world);
                if e.analysis.observe(ev, &ctx) {
                    e.progress.counters.consumed += 1;
                } else {
                    e.progress.counters.filtered += 1;
                }
            }
            if initial {
                // Track the high-water mark without the ordered bookkeeping.
                if ev.created_at > e.progress.watermark {
                    e.progress.watermark = ev.created_at;
                }
                e.progress.events += 1;
            } else {
                e.progress.advance(ev.created_at, id);
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

    pub fn mark_backfilled(&mut self, stage: &[usize]) {
        for &i in stage {
            if !self.entries[i].progress.backfilled {
                self.entries[i].progress.backfilled = true;
                let name = self.entries[i].analysis.name();
                self.emit(&MetricsEvent::BackfillComplete { name });
            }
        }
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
