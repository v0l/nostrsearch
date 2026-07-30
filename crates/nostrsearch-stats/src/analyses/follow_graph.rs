//! Follow-graph producer: consumes kind-3 contact lists and maintains follower
//! counts per pubkey incrementally, contributing `follower_count` and a coarse
//! WoT tier to the [`World`].
//!
//! The adjacency itself lives in a shared on-disk [`GraphStore`] — a few
//! million contact lists at a few hundred follows each is ~1B edges, which is
//! tens of gigabytes as in-memory hash sets. Only the per-pubkey follower
//! counts stay resident, which scale with the number of *pubkeys* (millions),
//! not edges (billions).

use crate::graph::SharedGraph;
use crate::types::Pubkey;
use crate::{Analysis, AnalysisCtx, AttachCtx, World};
use nostrsearch_core::event::NostrEvent;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const KIND_CONTACTS: &[u16] = &[3];

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FollowGraph {
    /// followed pubkey → follower count. Resident; sized by pubkeys, not edges.
    counts: HashMap<Pubkey, u32>,
    /// Adjacency lives on disk and is shared with other analyses.
    #[serde(skip)]
    store: Option<SharedGraph>,
}

impl FollowGraph {
    fn tier(count: u32) -> u8 {
        match count {
            0..=9 => 0,
            10..=99 => 1,
            100..=999 => 2,
            1_000..=9_999 => 3,
            _ => 4,
        }
    }

    fn apply_delta(&mut self, followed: &[Pubkey], delta: i64) {
        for p in followed {
            let c = self.counts.entry(*p).or_default();
            *c = (*c as i64 + delta).max(0) as u32;
        }
    }
}

impl Analysis for FollowGraph {
    type Output = HashMap<Pubkey, u32>;

    fn name(&self) -> &'static str {
        "follow_graph"
    }

    fn attach(&mut self, ctx: &AttachCtx) -> anyhow::Result<()> {
        self.store = Some(ctx.graph.clone());
        Ok(())
    }

    fn kinds(&self) -> Option<&[u16]> {
        Some(KIND_CONTACTS)
    }

    fn observe(&mut self, ev: &NostrEvent, _ctx: &AnalysisCtx) -> bool {
        let Some(author) = Pubkey::from_hex(&ev.pubkey) else {
            return false;
        };
        let Some(store) = self.store.clone() else {
            // Silently dropping events here would look like an empty graph, so
            // make the misconfiguration obvious instead.
            static WARNED: std::sync::Once = std::sync::Once::new();
            WARNED.call_once(|| {
                tracing::error!(
                    "follow_graph has no graph store attached; call Registry::attach_all \
                     (Registry::load does this) before observing — the follow graph will be empty"
                );
            });
            return false;
        };

        // Contact lists are replaceable: keep only the newest per author.
        match store.created_at(&author) {
            Ok(Some(prev)) if ev.created_at <= prev => return false,
            Err(e) => {
                tracing::warn!(error = %e, "graph read failed");
                return false;
            }
            _ => {}
        }

        let new_follows: Vec<Pubkey> = ev.tag_values("p").filter_map(Pubkey::from_hex).collect();

        // Decrement the superseded list, increment the new one, so counts stay
        // correct without ever holding the whole graph in memory.
        if let Ok(Some(old)) = store.get(&author) {
            let old_follows = old.follows;
            self.apply_delta(&old_follows, -1);
        }
        self.apply_delta(&new_follows, 1);

        if let Err(e) = store.put(&author, ev.created_at, &new_follows) {
            tracing::warn!(error = %e, "graph write failed");
        }
        true
    }

    fn merge(&mut self, other: Self) {
        // Adjacency is shared on disk, so only the resident counts merge.
        for (pk, n) in other.counts {
            *self.counts.entry(pk).or_default() += n;
        }
    }

    fn contribute(&self, world: &mut World) {
        for (pk, &count) in &self.counts {
            world.set_follower_count(*pk, count);
            world.set_wot_tier(*pk, Self::tier(count));
        }
    }

    fn snapshot(&self) -> Self::Output {
        self.counts.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::GraphStore;
    use std::sync::Arc;

    fn pk_hex(seed: u8) -> String {
        format!("{seed:02x}").repeat(32)
    }
    fn contacts(author: u8, created_at: u64, follows: &[u8]) -> NostrEvent {
        NostrEvent {
            id: format!("{author:02x}").repeat(32),
            pubkey: pk_hex(author),
            created_at,
            kind: 3,
            tags: follows.iter().map(|p| vec!["p".into(), pk_hex(*p)]).collect(),
            content: String::new(),
            sig: "c".repeat(128),
        }
    }
    fn tmpdir() -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "nsfg-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        p
    }

    #[test]
    fn incremental_replace_adjusts_counts_on_disk() {
        let dir = tmpdir();
        let store = Arc::new(GraphStore::open(&dir).unwrap());
        let ctx = AnalysisCtx::bare(0);

        let mut g = FollowGraph::default();
        g.attach(&AttachCtx {
            graph: store.clone(),
        })
        .unwrap();

        g.observe(&contacts(1, 10, &[3, 4]), &ctx);
        g.observe(&contacts(2, 10, &[3]), &ctx);
        // author 1 replaces its list, dropping 4
        g.observe(&contacts(1, 20, &[3]), &ctx);
        // an older list for author 1 must be ignored
        assert!(!g.observe(&contacts(1, 5, &[7]), &ctx));

        let mut world = World::new();
        g.contribute(&mut world);
        assert_eq!(world.follower_count(&Pubkey::from_hex(&pk_hex(3)).unwrap()), 2);
        assert_eq!(world.follower_count(&Pubkey::from_hex(&pk_hex(4)).unwrap()), 0);
        assert_eq!(world.follower_count(&Pubkey::from_hex(&pk_hex(7)).unwrap()), 0);

        drop(g);
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
