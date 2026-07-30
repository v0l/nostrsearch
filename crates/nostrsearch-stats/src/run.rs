//! Staged backfill driver.
//!
//! Runs one ascending pass **per dependency stage**: stage 0 producers fold and
//! materialize into the [`World`], then stage 1 consumers fold with a `ctx`
//! that already sees stage 0's output, and so on. Emits an initial pipeline
//! snapshot and periodic realtime ticks so a dashboard sees live throughput.
//!
//! A corpus-scale runner streams each stage's pass from Tantivy / the archive
//! instead of an in-memory slice; the staging / `World` / metrics protocol is
//! identical.

use crate::{Registry, World};
use anyhow::Result;
use nostrsearch_core::event::NostrEvent;

/// Emit a realtime tick every this many events during backfill.
const TICK_EVERY: u64 = 50_000;

/// Backfill `reg` over an in-memory event set, honouring dependency stages.
/// `now` is the reference time for recency/trending; `now_wall` is real
/// wall-clock unix secs for refresh scheduling. Returns the materialized
/// [`World`] for reuse by the live tail.
pub fn backfill_in_memory(
    reg: &mut Registry,
    now: u64,
    now_wall: u64,
    events: &[NostrEvent],
) -> Result<World> {
    let mut sorted: Vec<&NostrEvent> = events.iter().collect();
    sorted.sort_by_key(|e| e.created_at);

    let stages = reg.stages()?;
    let mut world = World::new();

    reg.emit_snapshot(world.len());

    let mut since_tick = 0u64;
    for stage in &stages {
        for ev in &sorted {
            reg.observe_stage(stage, ev, now, &world);
            since_tick += 1;
            if since_tick >= TICK_EVERY {
                since_tick = 0;
                reg.emit_tick(world.len());
            }
        }
        // Refresh (if due) + publish this stage's producer outputs downstream.
        reg.materialize_stage(stage, now_wall, &mut world);
        reg.mark_backfilled(stage);
    }

    reg.emit_tick(world.len());
    Ok(world)
}
