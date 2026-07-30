//! Web-of-trust bridge: turns the materialized [`World`] into a compact,
//! shareable, hot-swappable tier lookup for the indexer's scoring hook.
//!
//! nostrsearch's `ShardManager::with_wot_lookup` expects a
//! `Fn(&str) -> u8 + Send + Sync` (pubkey hex → tier). [`SharedWot`] provides
//! exactly that, backed by a [`WotIndex`] that can be atomically replaced when
//! the stats pipeline recomputes WoT (e.g. after a pagerank refresh) — so
//! ingest picks up fresh trust without a restart.

use crate::ctx::World;
use crate::types::Pubkey;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

/// Compact `pubkey → tier` map (only non-zero tiers stored; default is 0).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WotIndex {
    tiers: HashMap<Pubkey, u8>,
}

impl WotIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build from a materialized world (keeps only non-zero tiers).
    pub fn from_world(world: &World) -> Self {
        Self {
            tiers: world.wot_iter().collect(),
        }
    }

    pub fn len(&self) -> usize {
        self.tiers.len()
    }
    pub fn is_empty(&self) -> bool {
        self.tiers.is_empty()
    }

    pub fn tier(&self, pk: &Pubkey) -> u8 {
        self.tiers.get(pk).copied().unwrap_or(0)
    }

    /// Tier for a 64-char hex pubkey (0 for unknown / malformed).
    pub fn tier_hex(&self, pubkey_hex: &str) -> u8 {
        match Pubkey::from_hex(pubkey_hex) {
            Some(pk) => self.tier(&pk),
            None => 0,
        }
    }

    /// Persist as bincode (32-byte keys, no length prefix).
    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        let bytes = bincode::serialize(self)?;
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, &bytes)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Load a persisted snapshot.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        Ok(bincode::deserialize(&bytes)?)
    }
}

/// Thread-safe, hot-swappable handle around a [`WotIndex`].
///
/// Cheap to clone (`Arc`). The indexer holds one via [`SharedWot::lookup`]; the
/// stats side calls [`SharedWot::replace`] to publish a freshly computed index.
#[derive(Clone, Default)]
pub struct SharedWot(Arc<RwLock<WotIndex>>);

impl SharedWot {
    pub fn new(index: WotIndex) -> Self {
        Self(Arc::new(RwLock::new(index)))
    }

    pub fn empty() -> Self {
        Self::default()
    }

    /// Atomically swap in a new index (e.g. after a pagerank refresh).
    pub fn replace(&self, index: WotIndex) {
        *self.0.write().unwrap() = index;
    }

    pub fn tier_hex(&self, pubkey_hex: &str) -> u8 {
        self.0.read().unwrap().tier_hex(pubkey_hex)
    }

    pub fn len(&self) -> usize {
        self.0.read().unwrap().len()
    }
    pub fn is_empty(&self) -> bool {
        self.0.read().unwrap().is_empty()
    }

    /// A `Fn(&str) -> u8 + Send + Sync` closure for
    /// `ShardManager::with_wot_lookup`. Reads the latest published index.
    pub fn lookup(&self) -> impl Fn(&str) -> u8 + Send + Sync + 'static {
        let inner = self.0.clone();
        move |pubkey_hex: &str| inner.read().unwrap().tier_hex(pubkey_hex)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_world_keeps_only_nonzero_and_looks_up_by_hex() {
        let hi = "aa".repeat(32);
        let lo = "bb".repeat(32);
        let mut w = World::new();
        w.set_wot_tier(Pubkey::from_hex(&hi).unwrap(), 3);
        w.set_wot_tier(Pubkey::from_hex(&lo).unwrap(), 0);

        let idx = WotIndex::from_world(&w);
        assert_eq!(idx.len(), 1, "tier-0 entries are dropped");
        assert_eq!(idx.tier_hex(&hi), 3);
        assert_eq!(idx.tier_hex(&lo), 0);
        assert_eq!(idx.tier_hex("garbage"), 0);
    }

    #[test]
    fn shared_lookup_reflects_replace() {
        let hi = "cc".repeat(32);
        let shared = SharedWot::empty();
        let f = shared.lookup();
        assert_eq!(f(&hi), 0);

        let mut w = World::new();
        w.set_wot_tier(Pubkey::from_hex(&hi).unwrap(), 4);
        shared.replace(WotIndex::from_world(&w));
        assert_eq!(f(&hi), 4, "closure sees the hot-swapped index");
    }

    #[test]
    fn save_load_roundtrip() {
        let hi = "dd".repeat(32);
        let mut w = World::new();
        w.set_wot_tier(Pubkey::from_hex(&hi).unwrap(), 2);
        let idx = WotIndex::from_world(&w);

        let mut path = std::env::temp_dir();
        path.push(format!("wot-{}.bin", std::process::id()));
        idx.save(&path).unwrap();
        let back = WotIndex::load(&path).unwrap();
        assert_eq!(back.tier_hex(&hi), 2);
        let _ = std::fs::remove_file(&path);
    }
}
