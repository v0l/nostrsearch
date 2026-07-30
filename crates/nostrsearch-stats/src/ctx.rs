//! Shared precomputed [`World`] + the per-fold [`AnalysisCtx`] and the reusable
//! [`PublisherFilter`].
//!
//! Analyses stay pure folds: they never do I/O and never reach into each other
//! directly. *Producer* analyses publish their per-pubkey results into an owned
//! [`World`] via [`Analysis::contribute`]; downstream *consumer* analyses read
//! that `World` through [`AnalysisCtx`]. The `World` is separate owned storage,
//! so a consumer stage can borrow it while the registry mutably feeds the
//! consumers — no aliasing.

use crate::types::Pubkey;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Per-pubkey materialized signals. One struct per key (not three parallel
/// maps) to avoid duplicating the 32-byte key.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct PubkeyStat {
    pub followers: u32,
    pub wot_tier: u8,
    pub pagerank: f32,
}

/// Materialized cross-analysis state, keyed by [`Pubkey`].
#[derive(Debug, Clone, Default)]
pub struct World {
    stats: HashMap<Pubkey, PubkeyStat>,
}

impl World {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.stats.len()
    }
    pub fn is_empty(&self) -> bool {
        self.stats.is_empty()
    }

    // --- producer-side writers ---
    pub fn set_follower_count(&mut self, pk: Pubkey, count: u32) {
        self.stats.entry(pk).or_default().followers = count;
    }
    pub fn set_wot_tier(&mut self, pk: Pubkey, tier: u8) {
        self.stats.entry(pk).or_default().wot_tier = tier;
    }
    pub fn set_pagerank(&mut self, pk: Pubkey, score: f32) {
        self.stats.entry(pk).or_default().pagerank = score;
    }

    // --- consumer-side readers ---
    pub fn stat(&self, pk: &Pubkey) -> PubkeyStat {
        self.stats.get(pk).copied().unwrap_or_default()
    }
    pub fn follower_count(&self, pk: &Pubkey) -> u32 {
        self.stat(pk).followers
    }
    pub fn wot_tier(&self, pk: &Pubkey) -> u8 {
        self.stat(pk).wot_tier
    }
    pub fn pagerank(&self, pk: &Pubkey) -> f32 {
        self.stat(pk).pagerank
    }

    /// Iterate `(pubkey, wot_tier)` for all pubkeys with a non-zero tier —
    /// the compact set worth persisting into a [`WotIndex`](crate::wot::WotIndex).
    pub fn wot_iter(&self) -> impl Iterator<Item = (Pubkey, u8)> + '_ {
        self.stats
            .iter()
            .filter(|(_, s)| s.wot_tier > 0)
            .map(|(pk, s)| (*pk, s.wot_tier))
    }
}

/// Read-only side context for a single fold. Carries the current event's parsed
/// author / id so analyses don't re-parse hex, plus a reference to the
/// materialized [`World`].
pub struct AnalysisCtx<'a> {
    /// Reference "now" (unix seconds) for recency / trending windows.
    pub now: u64,
    /// Parsed author pubkey of the current event.
    pub author: Pubkey,
    /// Parsed id of the current event.
    pub event_id: Pubkey,
    world: Option<&'a World>,
}

impl<'a> AnalysisCtx<'a> {
    /// Full context for one event.
    pub fn new(now: u64, author: Pubkey, event_id: Pubkey, world: &'a World) -> Self {
        Self {
            now,
            author,
            event_id,
            world: Some(world),
        }
    }

    /// Context with no world / zero keys (unit tests of pure folds).
    pub fn bare(now: u64) -> Self {
        Self {
            now,
            author: Pubkey::ZERO,
            event_id: Pubkey::ZERO,
            world: None,
        }
    }

    /// Context backed by a world but no specific event (producer materialize).
    pub fn from_world(now: u64, world: &'a World) -> Self {
        Self {
            now,
            author: Pubkey::ZERO,
            event_id: Pubkey::ZERO,
            world: Some(world),
        }
    }

    pub fn wot_tier(&self, pk: &Pubkey) -> u8 {
        self.world.map(|w| w.wot_tier(pk)).unwrap_or(0)
    }
    pub fn is_trusted(&self, pk: &Pubkey) -> bool {
        self.wot_tier(pk) >= 1
    }
    pub fn followers(&self, pk: &Pubkey) -> u32 {
        self.world.map(|w| w.follower_count(pk)).unwrap_or(0)
    }
    pub fn pagerank(&self, pk: &Pubkey) -> f32 {
        self.world.map(|w| w.pagerank(pk)).unwrap_or(0.0)
    }

    /// WoT tier of the current event's author.
    pub fn author_tier(&self) -> u8 {
        self.wot_tier(&self.author)
    }
    /// Whether the current event's author is trusted.
    pub fn author_trusted(&self) -> bool {
        self.is_trusted(&self.author)
    }
}

/// Reusable publisher gate: lets a consumer analysis skip events from authors
/// below minimum follower / WoT thresholds (e.g. "don't count stats for users
/// with < 10 followers"). Check it at the top of `observe`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PublisherFilter {
    pub min_followers: u32,
    pub min_wot_tier: u8,
}

impl Default for PublisherFilter {
    fn default() -> Self {
        Self {
            min_followers: 0,
            min_wot_tier: 0,
        }
    }
}

impl PublisherFilter {
    pub fn min_followers(min: u32) -> Self {
        Self {
            min_followers: min,
            min_wot_tier: 0,
        }
    }
    pub fn min_wot(tier: u8) -> Self {
        Self {
            min_followers: 0,
            min_wot_tier: tier,
        }
    }

    /// Does the current event's author clear the thresholds?
    pub fn allows(&self, ctx: &AnalysisCtx) -> bool {
        ctx.followers(&ctx.author) >= self.min_followers
            && ctx.wot_tier(&ctx.author) >= self.min_wot_tier
    }
}
