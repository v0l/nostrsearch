//! Follow-graph producer: consumes kind-3 contact lists, maintains follower
//! counts per pubkey **incrementally** (handling replaceable-event updates),
//! and contributes `follower_count` + a coarse WoT tier into the [`World`].
//!
//! Incremental: when an author replaces their contact list we diff old vs new
//! follows and adjust counts, so no full recompute is needed — this analysis is
//! cheap and has no refresh interval.

use crate::types::Pubkey;
use crate::{Analysis, AnalysisCtx, World};
use nostrsearch_core::event::NostrEvent;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

const KIND_CONTACTS: &[u16] = &[3];

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FollowGraph {
    /// author → set of followed pubkeys (latest contact list only).
    follows: HashMap<Pubkey, HashSet<Pubkey>>,
    /// author → created_at of the held contact list (for replace ordering).
    seen_at: HashMap<Pubkey, u64>,
    /// followed pubkey → follower count (maintained incrementally).
    counts: HashMap<Pubkey, u32>,
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

    fn apply_delta(&mut self, followed: &HashSet<Pubkey>, delta: i64) {
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

    fn kinds(&self) -> Option<&[u16]> {
        Some(KIND_CONTACTS)
    }

    fn observe(&mut self, ev: &NostrEvent, _ctx: &AnalysisCtx) -> bool {
        let author = match Pubkey::from_hex(&ev.pubkey) {
            Some(a) => a,
            None => return false,
        };
        if let Some(&ts) = self.seen_at.get(&author) {
            if ev.created_at <= ts {
                return false; // stale / older contact list
            }
        }
        let new_follows: HashSet<Pubkey> = ev
            .tag_values("p")
            .filter_map(Pubkey::from_hex)
            .collect();

        // decrement old, increment new (incremental replace)
        if let Some(old) = self.follows.get(&author).cloned() {
            self.apply_delta(&old, -1);
        }
        self.apply_delta(&new_follows, 1);

        self.seen_at.insert(author, ev.created_at);
        self.follows.insert(author, new_follows);
        true
    }

    fn merge(&mut self, other: Self) {
        // Rebuild by replaying newest-wins contact lists, then recompute counts.
        for (author, ts) in other.seen_at {
            let take = self.seen_at.get(&author).map(|&c| ts > c).unwrap_or(true);
            if take {
                if let Some(f) = other.follows.get(&author) {
                    self.seen_at.insert(author, ts);
                    self.follows.insert(author, f.clone());
                }
            }
        }
        self.counts.clear();
        for followed in self.follows.values() {
            for p in followed {
                *self.counts.entry(*p).or_default() += 1;
            }
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

    fn pk(seed: u8) -> String {
        format!("{:02x}", seed).repeat(32)
    }
    fn contacts(author: u8, created_at: u64, follows: &[u8]) -> NostrEvent {
        NostrEvent {
            id: format!("{:02x}", author).repeat(32),
            pubkey: pk(author),
            created_at,
            kind: 3,
            tags: follows.iter().map(|p| vec!["p".into(), pk(*p)]).collect(),
            content: String::new(),
            sig: "c".repeat(128),
        }
    }

    #[test]
    fn incremental_replace_adjusts_counts() {
        let ctx = AnalysisCtx::bare(0);
        let mut g = FollowGraph::default();
        g.observe(&contacts(1, 10, &[3, 4]), &ctx);
        g.observe(&contacts(2, 10, &[3]), &ctx);
        // author 1 replaces list, dropping 4
        g.observe(&contacts(1, 20, &[3]), &ctx);

        let mut world = World::new();
        g.contribute(&mut world);
        assert_eq!(world.follower_count(&Pubkey::from_hex(&pk(3)).unwrap()), 2);
        assert_eq!(world.follower_count(&Pubkey::from_hex(&pk(4)).unwrap()), 0);
    }
}
