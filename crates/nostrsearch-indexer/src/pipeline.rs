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
use nostrsearch_stats::analyses::{FollowGraph, Pagerank};
use nostrsearch_stats::{Registry, SharedWot, StatStore, World, WotIndex};
use std::path::PathBuf;

/// Configuration for the unified pipeline.
pub struct PipelineConfig {
    pub index_root: PathBuf,
    pub shard: ShardWriterConfig,
    /// Persist analysis state here (resumable). `None` = in-memory only.
    pub state_dir: Option<PathBuf>,
    /// Re-materialize WoT and hot-swap the lookup every N processed events.
    pub wot_refresh_every: u64,
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
            wot_out: None,
        }
    }
}

/// One pipeline instance = one index + one stats engine + one shared WoT.
pub struct Pipeline {
    manager: ShardManager,
    registry: Registry,
    world: World,
    wot: SharedWot,
    store: Option<StatStore>,
    cfg: PipelineConfig,
    live: bool,
    since_refresh: u64,
}

impl Pipeline {
    /// Build a pipeline with the default WoT producers (follow-graph + pagerank)
    /// registered. Resumes analysis state from `state_dir` if present.
    pub fn new(cfg: PipelineConfig) -> Result<Self> {
        let wot = SharedWot::empty();
        let manager =
            ShardManager::new(&cfg.index_root, cfg.shard.clone()).with_wot_lookup(wot.lookup());

        let mut registry = Registry::new();
        registry
            .register(FollowGraph::default())
            .register(Pagerank::default());

        let store = match &cfg.state_dir {
            Some(dir) => {
                let s = StatStore::new(dir)?;
                registry.load(&s)?;
                Some(s)
            }
            None => None,
        };

        let mut me = Self {
            manager,
            registry,
            world: World::new(),
            wot,
            store,
            cfg,
            live: false,
            since_refresh: 0,
        };

        // Warm start: if analysis state was restored, materialize it now so the
        // WoT lookup is populated from event #1. Without this, everything
        // indexed before the first scheduled refresh would be written with
        // tier 0 even though we already know the trust graph.
        if me.registry.total_events() > 0 || me.registry.entries().iter().any(|e| !e.needs_backfill())
        {
            me.refresh_wot();
            tracing::info!("warm start: WoT materialized from restored analysis state");
        }

        Ok(me)
    }

    /// Access the shared WoT handle (e.g. to share the same lookup elsewhere).
    pub fn shared_wot(&self) -> SharedWot {
        self.wot.clone()
    }

    fn now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    /// Process one event: fold into stats, then index it with the current WoT
    /// tier. Triggers a WoT refresh every `wot_refresh_every` events.
    pub fn process(&mut self, ev: &NostrEvent) {
        let now = Self::now();
        if self.live {
            self.registry.observe(ev, now, &self.world);
        } else {
            self.registry.observe_backfill(ev, now, &self.world);
        }
        if let Err(e) = self.manager.index_event(ev) {
            tracing::warn!(error = %e, "index_event failed");
        }
        self.since_refresh += 1;
        if self.since_refresh >= self.cfg.wot_refresh_every {
            self.refresh_wot();
        }
    }

    /// Re-materialize the world, rebuild + hot-swap the WoT index, persist
    /// analysis state, and (optionally) write the WoT snapshot to disk.
    pub fn refresh_wot(&mut self) {
        self.since_refresh = 0;
        let now_wall = Self::now();
        if let Err(e) = self.registry.materialize_all(now_wall, &mut self.world) {
            tracing::warn!(error = %e, "stats materialize failed");
            return;
        }
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
        if let Some(store) = &self.store {
            if let Err(e) = self.registry.persist(store) {
                tracing::warn!(error = %e, "stats persist failed");
            }
        }
        tracing::info!(wot_entries = entries, world = self.world.len(), "WoT refreshed");
    }

    /// Switch from backfill to live mode: finalize backfill, refresh WoT once,
    /// and start applying the watermark-gated live fold.
    pub fn go_live(&mut self) {
        if let Err(e) = self.registry.mark_all_backfilled() {
            tracing::warn!(error = %e, "mark_all_backfilled failed");
        }
        self.refresh_wot();
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
        self.refresh_wot();
        self.commit()?;
        Ok(())
    }

    pub fn total_events(&self) -> u64 {
        self.registry.total_events()
    }
}
