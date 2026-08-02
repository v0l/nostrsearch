//! Unified ingestion pipeline: one event stream → **both** the Tantivy index
//! and the stats / web-of-trust engine.
//!
//! Static JSONL archives and the live relay firehose are just two *sources*
//! that call [`Pipeline::process`]; the pipeline fans each event out to:
//!
//! 1. the stats [`Registry`] (follow-graph / pagerank / trending / …), and
//! 2. the time-sharded Tantivy [`ShardManager`], writing the current WoT tier.
//!
//! WoT is bootstrapped from the same stream: every `wot_refresh_every` events
//! the pipeline re-materializes the [`World`], rebuilds a [`WotIndex`], and
//! **hot-swaps** it into the shared lookup ([`SharedWot`]) so subsequently
//! indexed documents pick up fresh trust without a restart.

use crate::shard_writer::{ShardManager, ShardWriterConfig};
use anyhow::Result;
use nostrsearch_core::event::NostrEvent;
use nostrsearch_stats::analyses::{ActiveUsers, Activity, Clients, FollowGraph, Pagerank, Relays};
use nostrsearch_stats::{Registry, SharedWot, StatStore, World, WotIndex};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Configuration for the unified pipeline.
pub struct PipelineConfig {
    pub index_root: PathBuf,
    pub shard: ShardWriterConfig,
    /// Persist analysis state here (resumable). `None` = in-memory only.
    pub state_dir: Option<PathBuf>,
    /// Re-materialize WoT and hot-swap the lookup every N processed events.
    pub wot_refresh_every: u64,
    /// Wall-clock floor between refreshes. At high ingest rates the event
    /// counter alone fires far too often — materializing the world and
    /// rebuilding the index every second or two produces an identical snapshot
    /// while stalling the writer.
    pub min_refresh_interval: Duration,
    /// How often analysis state is written to disk. Persisting serializes the
    /// whole follow/pagerank graph, which is orders of magnitude more expensive
    /// than materializing WoT, so it gets its own much slower cadence (state is
    /// always flushed by [`Pipeline::finish`]).
    pub persist_interval: Duration,
    /// Also write the WoT snapshot here on each refresh (for a separate ingest).
    pub wot_out: Option<PathBuf>,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            index_root: PathBuf::from("./data/index"),
            shard: ShardWriterConfig::default(),
            state_dir: None,
            wot_refresh_every: 1_000_000,
            min_refresh_interval: Duration::from_secs(60),
            persist_interval: Duration::from_secs(300),
            wot_out: None,
        }
    }
}

/// One pipeline instance = one index + one stats engine + one shared WoT.
pub struct Pipeline {
    /// Shared so a bulk ingest can index without holding the pipeline lock.
    manager: Arc<ShardManager>,
    registry: Registry,
    world: World,
    wot: SharedWot,
    store: Option<StatStore>,
    cfg: PipelineConfig,
    live: bool,
    /// Dependency stages, computed once at construction. A streaming backfill
    /// must replay the corpus once per stage (see [`Pipeline::advance_pass`]).
    stages: Vec<Vec<usize>>,
    /// Which stage the current backfill pass is folding.
    pass: usize,
    since_refresh: u64,
    last_refresh: Instant,
    last_persist: Instant,

    refreshed_once: bool,
    /// Cumulative time spent folding events into analyses, in nanoseconds.
    stats_ns: u64,
    /// Cumulative time spent handing events to Tantivy (including synchronous
    /// commits), in nanoseconds.
    index_ns: u64,
}

impl Pipeline {
    /// Build a pipeline with the default WoT producers (follow-graph + pagerank)
    /// registered. Resumes analysis state from `state_dir` if present.
    pub fn new(cfg: PipelineConfig) -> Result<Self> {
        let wot = SharedWot::empty();
        let manager = Arc::new(
            ShardManager::new(&cfg.index_root, cfg.shard.clone()).with_wot_lookup(wot.lookup()),
        );

        let mut registry = Registry::new();
        registry
            .register(FollowGraph::default())
            .register(Pagerank::default());

        // Dashboard reports (ported from nostr-dashboard). Activity,
        // `Clients` is independent (stage 0). `Activity` and `ActiveUsers`
        // read follower/WoT data for their trusted/untrusted split, so they
        // depend on `follow_graph` and fold in a later pass.
        registry
            .register(Activity::default())
            .register(ActiveUsers::default())
            .register(Clients::default())
            // Feeds the scraper its relay targets, replacing a full-index scan
            // that ran on every boot.
            .register(Relays::default());

        let store = match &cfg.state_dir {
            Some(dir) => {
                let s = StatStore::new(dir)?;
                registry.load(&s)?;
                Some(s)
            }
            None => None,
        };

        let stages = registry.stages()?;

        let mut me = Self {
            manager,
            registry,
            world: World::new(),
            wot,
            store,
            cfg,
            live: false,
            stages,
            pass: 0,
            since_refresh: 0,
            // Allow the first refresh immediately.
            last_refresh: Instant::now() - Duration::from_secs(86_400),
            last_persist: Instant::now(),

            refreshed_once: false,
            stats_ns: 0,
            index_ns: 0,
        };

        // Warm start: if analysis state was restored, materialize it now so the
        // WoT lookup is populated from event #1. Without this, everything
        // indexed before the first scheduled refresh would be written with
        // tier 0 even though we already know the trust graph.
        if me.registry.total_events() > 0
            || me.registry.entries().iter().any(|e| !e.needs_backfill())
        {
            me.refresh_wot();
            tracing::info!("warm start: WoT materialized from restored analysis state");
        }

        Ok(me)
    }

    /// Cumulative (stats, index) time in seconds spent in the hot path, so the
    /// progress line can show where ingest wall time actually goes.
    pub fn stage_secs(&self) -> (f64, f64) {
        (self.stats_ns as f64 / 1e9, self.index_ns as f64 / 1e9)
    }

    /// Access the shared WoT handle (e.g. to share the same lookup elsewhere).
    pub fn shared_wot(&self) -> SharedWot {
        self.wot.clone()
    }

    /// Current snapshot of every registered analysis, as `(name, json)`.
    /// Used to publish reports to the HTTP layer.
    pub fn reports(&self) -> Vec<(&'static str, serde_json::Value)> {
        self.registry.snapshots()
    }

    /// Per-analysis progress (watermark, backfill state, counters).
    pub fn analyses_status(&self) -> Vec<nostrsearch_stats::AnalysisStatus> {
        self.registry.status()
    }
    /// Analyses with a rebuild still in progress, so one can be resumed at    /// Reset every analysis, external storage included, and persist
    /// immediately so a restart cannot resurrect the old state.
    pub fn reset_all_analyses(&mut self) -> Vec<&'static str> {
        let names = self.registry.reset_all();
        // The world is derived from analyses that now hold nothing, so leaving
        // it in place would keep labelling events with a web of trust that has
        // been discarded.
        self.world = Default::default();
        if let Some(store) = &self.store
            && let Err(e) = self.registry.persist(store)
        {
            tracing::warn!(error = %e, "persisting reset failed");
        }
        tracing::info!(reset = ?names, "all analyses reset");
        names
    }

    /// Relay targets from the `relays` report, most advertised first.
    ///
    /// Empty until something has folded a relay list, which tells the scraper
    /// to fall back to scanning the index that one time.
    pub fn relay_targets(&self) -> Vec<(String, u64)> {
        self.registry
            .snapshots()
            .into_iter()
            .find(|(n, _)| *n == "relays")
            .and_then(|(_, v)| {
                serde_json::from_value::<
                    std::collections::HashMap<String, nostrsearch_stats::analyses::RelayStats>,
                >(v)
                .ok()
            })
            .map(|m| {
                let mut out: Vec<(String, u64)> =
                    m.into_iter().map(|(u, s)| (u, s.advertisers)).collect();
                out.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
                out
            })
            .unwrap_or_default()
    }

    /// Discard one analysis's state, and every analysis that depends on it, so
    /// they re-derive from the corpus. Persists immediately so a restart cannot
    /// resurrect the old state.
    ///
    /// Returns the names that were reset, or `None` if the name is unknown.
    pub fn reset_analysis(&mut self, name: &str) -> Option<Vec<&'static str>> {
        let reset = self.registry.reset(name)?;
        if let Some(store) = &self.store
            && let Err(e) = self.registry.persist(store)
        {
            tracing::warn!(error = %e, analysis = name, "persisting reset failed");
        }
        tracing::info!(
            requested = name,
            reset = ?reset,
            "analyses reset; they will re-derive from the corpus"
        );
        Some(reset)
    }

    /// Drain each analysis's partial changes since the last call, for streaming
    /// to a live dashboard. Empty when nothing moved. See
    /// [`nostrsearch_stats::delta`] for the merge-patch contract.
    pub fn drain_report_deltas(&mut self) -> Vec<nostrsearch_stats::ReportDelta> {
        self.registry.drain_deltas()
    }

    fn now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }
    /// Fold one event into the analyses only, without indexing it.
    ///
    /// Paired with [`index_only`](Pipeline::index_only) so a bulk ingest can
    /// hold the pipeline lock for the cheap half and index outside it. Folding
    /// mutates per-analysis state and has to be serial; indexing does not, and
    /// holding one lock across both pinned the entire ingest to a single core
    /// while every reader thread queued behind it.
    pub fn fold_only(&mut self, ev: &NostrEvent) {
        let now = Self::now();
        let t0 = Instant::now();
        if self.live {
            self.registry.observe_backfill(ev, now, &self.world);
        } else {
            let stage = std::mem::take(&mut self.stages[self.pass]);
            self.registry
                .observe_backfill_stage(&stage, ev, now, &self.world);
            self.stages[self.pass] = stage;
        }
        self.stats_ns += t0.elapsed().as_nanos() as u64;
        self.since_refresh += 1;
        if self.live && self.since_refresh >= self.cfg.wot_refresh_every {
            self.maybe_refresh_wot();
        }
    }

    /// Whether this pass writes to the index.
    ///
    /// Only pass 0 does; later passes re-read the archive purely to feed the
    /// analyses that depend on earlier ones.
    pub fn indexes_this_pass(&self) -> bool {
        self.live || self.pass == 0
    }

    /// A handle for indexing without the pipeline lock.
    pub fn indexer(&self) -> Arc<ShardManager> {
        self.manager.clone()
    }

    pub fn process(&mut self, ev: &NostrEvent) {
        let now = Self::now();
        let t0 = Instant::now();
        if self.live {
            // Live tail: the world is already materialized, so one pass feeds
            // every stage.
            //
            // `observe_backfill` rather than `observe` because the two differ
            // only for analyses still marked un-backfilled, which fold every
            // event regardless of watermark. That is what lets a reset analysis
            // rebuild from out-of-order history -- the scraper walks the
            // network backwards day by day, so its events arrive older than
            // the watermark and `observe` would reject every one of them.
            // Analyses that are already backfilled behave identically either
            // way, keeping count-once semantics.
            self.registry.observe_backfill(ev, now, &self.world);
        } else {
            // Backfill: fold only the stage this pass is responsible for, so
            // consumers never read a half-built world.
            let stage = std::mem::take(&mut self.stages[self.pass]);
            self.registry
                .observe_backfill_stage(&stage, ev, now, &self.world);
            self.stages[self.pass] = stage;
        }
        let t1 = Instant::now();
        // Index exactly once. Later backfill passes replay the same events
        // purely to feed dependent analyses.
        if (self.live || self.pass == 0)
            && let Err(e) = self.manager.index_event(ev)
        {
            tracing::warn!(error = %e, "index_event failed");
        }
        self.stats_ns += (t1 - t0).as_nanos() as u64;
        self.index_ns += t1.elapsed().as_nanos() as u64;
        // Only the live tail refreshes periodically. During a backfill the
        // graph is still being assembled, so a mid-run refresh is both
        // expensive (it re-serializes the whole graph) and wrong: documents
        // would be written with whatever partial tier happened to exist when
        // they were indexed, making scoring depend on ingest order. Backfill
        // instead uses the tier loaded at startup for the whole run and
        // materializes once at the end (`go_live` / `finish`).
        self.since_refresh += 1;
        if self.live && self.since_refresh >= self.cfg.wot_refresh_every {
            self.maybe_refresh_wot();
        }
    }

    /// Re-materialize the world, rebuild + hot-swap the WoT index, persist
    /// analysis state, and (optionally) write the WoT snapshot to disk.
    /// Refresh only if the wall-clock floor has elapsed. Called from the hot
    /// path, where the event counter alone would otherwise fire constantly.
    fn maybe_refresh_wot(&mut self) {
        if self.last_refresh.elapsed() < self.cfg.min_refresh_interval {
            // Reset the counter so we don't re-test on every subsequent event.
            self.since_refresh = 0;
            return;
        }
        self.refresh_wot();
    }

    /// Re-materialize the world, rebuild + hot-swap the WoT index, and (on its
    /// own slower cadence) persist analysis state.
    pub fn refresh_wot(&mut self) {
        self.refresh_inner(false);
    }

    fn refresh_inner(&mut self, force_persist: bool) {
        // Nothing folded since the last refresh means the world would be
        // identical; skip the work. (`go_live` refreshes at the end of a
        // backfill, so `finish` right after would otherwise repeat it.)
        if self.refreshed_once && self.since_refresh == 0 {
            return;
        }
        self.refreshed_once = true;
        self.since_refresh = 0;
        self.last_refresh = Instant::now();

        let t0 = Instant::now();
        let now_wall = Self::now();
        if let Err(e) = self.registry.materialize_all(now_wall, &mut self.world) {
            tracing::warn!(error = %e, "stats materialize failed");
            return;
        }
        let materialize_ms = t0.elapsed().as_millis();

        let idx = WotIndex::from_world(&self.world);
        let entries = idx.len();
        if let Some(path) = &self.cfg.wot_out {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).ok();
            }
            if let Err(e) = idx.save(path) {
                tracing::warn!(error = %e, "WoT snapshot save failed");
            }
        }
        self.wot.replace(idx);
        self.registry.emit_tick(self.world.len());

        // Persisting serializes the entire follow/pagerank graph — far more
        // expensive than the above — so it runs on its own cadence.
        let mut persist_ms = 0;
        if let Some(store) = &self.store
            && (force_persist || self.last_persist.elapsed() >= self.cfg.persist_interval)
        {
            {
                let t1 = Instant::now();
                // Rebuild position lives inside each analysis's Progress, so
                // it is written by exactly this call. It cannot drift from the
                // state it describes: ahead of it a resume skips events,
                // behind it they are folded twice and every counter inflates.
                if let Err(e) = self.registry.persist(store) {
                    tracing::warn!(error = %e, "stats persist failed");
                }
                persist_ms = t1.elapsed().as_millis();
                self.last_persist = Instant::now();
            }
        }

        tracing::info!(
            wot_entries = entries,
            world = self.world.len(),
            materialize_ms,
            persist_ms,
            "WoT refreshed"
        );
    }

    /// Number of times a streaming backfill must replay the corpus — one pass
    /// per dependency stage. With the default analysis set this is 2: the WoT
    /// producers and client stats fold in pass 0, then the reports that read
    /// follower/WoT data fold in pass 1.
    pub fn backfill_passes(&self) -> usize {
        self.stages.len()
    }

    /// Which stage the current backfill pass is folding (0-based).
    pub fn current_pass(&self) -> usize {
        self.pass
    }

    /// Whether the events indexed so far still need replaying for a later
    /// stage. `false` once every stage has folded.
    pub fn needs_another_pass(&self) -> bool {
        self.pass + 1 < self.stages.len()
    }

    /// Finish the current backfill pass: materialize the stage that just
    /// completed into the [`World`] (so the next stage's consumers can read
    /// its follower counts / WoT tiers), mark it backfilled, and advance.
    ///
    /// Returns `true` if another pass over the corpus is required.
    pub fn advance_pass(&mut self) -> bool {
        let stage = self.stages[self.pass].clone();
        let now_wall = Self::now();
        self.registry
            .materialize_stage(&stage, now_wall, &mut self.world);
        self.registry.mark_backfilled(&stage);

        // Publish the freshly materialized trust data to the index writer too,
        // so a later pass (and the live tail) score with real tiers.
        self.wot.replace(WotIndex::from_world(&self.world));

        if !self.needs_another_pass() {
            return false;
        }
        self.pass += 1;
        tracing::info!(
            pass = self.pass,
            passes = self.stages.len(),
            world = self.world.len(),
            "backfill advancing to next dependency stage"
        );
        true
    }

    /// Switch from backfill to live mode: finalize backfill, refresh WoT once,
    /// and start applying the watermark-gated live fold.
    pub fn go_live(&mut self) {
        // Only analyses that actually consumed events are complete. A live-only
        // node (the server) never replays the corpus, so claiming otherwise
        // leaves a newly added report looking like "barely any activity"
        // instead of "not computed yet".
        let outstanding = self.registry.outstanding_backfills();
        if !outstanding.is_empty() {
            tracing::warn!(
                analyses = ?outstanding,
                "these analyses have not backfilled the corpus; they will fold every event \
                 they are handed (including the gap scraper's history) until a full replay \
                 marks them complete. Run POST /admin/ingest, or `ingest --input-dir`"
            );
        }
        self.refresh_inner(true);
        self.live = true;
        tracing::info!("pipeline switched to live mode");
    }

    /// Commit all open shards.
    pub fn commit(&mut self) -> Result<()> {
        self.manager.commit_all()?;
        Ok(())
    }

    /// Final flush: commit shards + refresh/persist stats.
    pub fn finish(&mut self) -> Result<()> {
        self.refresh_inner(true);
        self.commit()?;
        Ok(())
    }

    pub fn total_events(&self) -> u64 {
        self.registry.total_events()
    }
}
