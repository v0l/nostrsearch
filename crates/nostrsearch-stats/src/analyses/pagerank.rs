//! Pagerank / WoT producer — the canonical *expensive, periodically-refreshed*
//! analysis.
//!
//! `observe` is cheap: it just accumulates the follow graph from kind-3 events
//! (incrementally, newest-wins). The actual pagerank vector is expensive to
//! compute, so we do **not** recompute it on every follow change — instead
//! [`refresh`](Analysis::refresh) runs a bounded power iteration on a schedule
//! ([`refresh_interval`] = 24h by default), and [`contribute`] publishes the
//! last computed ranks + a derived WoT tier into the [`World`].
//!
//! (Exact incremental pagerank on a mutating graph is possible — push-based /
//! Monte-Carlo random-walk schemes update only affected nodes — but it's
//! involved; periodic recompute is the pragmatic default. A future incremental
//! impl would keep this same trait surface and just shorten the interval.)

use crate::types::Pubkey;
use crate::{Analysis, AnalysisCtx, World};
use nostrsearch_core::event::NostrEvent;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

const KIND_CONTACTS: &[u16] = &[3];
const DAMPING: f32 = 0.85;
const ITERATIONS: usize = 20;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pagerank {
    /// author → followed set (newest-wins), the raw graph.
    follows: HashMap<Pubkey, HashSet<Pubkey>>,
    seen_at: HashMap<Pubkey, u64>,
    /// Last computed ranks (refreshed on a schedule, not per event).
    ranks: HashMap<Pubkey, f32>,
    /// Refresh cadence in seconds.
    interval_secs: u64,
}

impl Default for Pagerank {
    fn default() -> Self {
        Self {
            follows: HashMap::new(),
            seen_at: HashMap::new(),
            ranks: HashMap::new(),
            interval_secs: 24 * 3600,
        }
    }
}

impl Pagerank {
    pub fn with_interval(mut self, d: Duration) -> Self {
        self.interval_secs = d.as_secs().max(1);
        self
    }

    /// Coarse WoT tier from a pagerank score (relative to the graph's max).
    fn tier(score: f32, max: f32) -> u8 {
        if max <= 0.0 {
            return 0;
        }
        let r = score / max;
        match r {
            x if x >= 0.5 => 4,
            x if x >= 0.2 => 3,
            x if x >= 0.05 => 2,
            x if x >= 0.01 => 1,
            _ => 0,
        }
    }
}

impl Analysis for Pagerank {
    type Output = Vec<(Pubkey, f32)>;

    fn name(&self) -> &'static str {
        "pagerank"
    }

    fn kinds(&self) -> Option<&[u16]> {
        Some(KIND_CONTACTS)
    }

    fn refresh_interval(&self) -> Option<Duration> {
        Some(Duration::from_secs(self.interval_secs))
    }

    fn observe(&mut self, ev: &NostrEvent, _ctx: &AnalysisCtx) -> bool {
        let author = match Pubkey::from_hex(&ev.pubkey) {
            Some(a) => a,
            None => return false,
        };
        if let Some(&ts) = self.seen_at.get(&author) {
            if ev.created_at <= ts {
                return false;
            }
        }
        let follows: HashSet<Pubkey> = ev.tag_values("p").filter_map(Pubkey::from_hex).collect();
        self.seen_at.insert(author, ev.created_at);
        self.follows.insert(author, follows);
        true
    }

    fn merge(&mut self, other: Self) {
        for (author, ts) in other.seen_at {
            let take = self.seen_at.get(&author).map(|&c| ts > c).unwrap_or(true);
            if take {
                if let Some(f) = other.follows.get(&author) {
                    self.seen_at.insert(author, ts);
                    self.follows.insert(author, f.clone());
                }
            }
        }
    }

    /// Expensive: power-iterate pagerank over the accumulated graph.
    fn refresh(&mut self) {
        // Build a dense node index over everyone who appears (author or followed).
        let mut idx: HashMap<Pubkey, usize> = HashMap::new();
        for (a, fs) in &self.follows {
            idx.entry(*a).or_insert_with(|| 0);
            for f in fs {
                idx.entry(*f).or_insert_with(|| 0);
            }
        }
        let n = idx.len();
        if n == 0 {
            self.ranks.clear();
            return;
        }
        for (i, v) in idx.values_mut().enumerate() {
            *v = i;
        }
        // out-adjacency by index
        let mut out: Vec<Vec<usize>> = vec![Vec::new(); n];
        for (a, fs) in &self.follows {
            let ai = idx[a];
            for f in fs {
                out[ai].push(idx[f]);
            }
        }

        let base = (1.0 - DAMPING) / n as f32;
        let mut rank = vec![1.0f32 / n as f32; n];
        for _ in 0..ITERATIONS {
            let mut next = vec![base; n];
            let mut dangling = 0.0f32;
            for i in 0..n {
                if out[i].is_empty() {
                    dangling += rank[i];
                    continue;
                }
                let share = DAMPING * rank[i] / out[i].len() as f32;
                for &j in &out[i] {
                    next[j] += share;
                }
            }
            // redistribute dangling mass uniformly
            let dangle = DAMPING * dangling / n as f32;
            for v in next.iter_mut() {
                *v += dangle;
            }
            rank = next;
        }

        self.ranks = idx.into_iter().map(|(pk, i)| (pk, rank[i])).collect();
    }

    fn contribute(&self, world: &mut World) {
        let max = self.ranks.values().copied().fold(0.0f32, f32::max);
        for (pk, &score) in &self.ranks {
            world.set_pagerank(*pk, score);
            world.set_wot_tier(*pk, Self::tier(score, max));
        }
    }

    fn snapshot(&self) -> Self::Output {
        let mut v: Vec<(Pubkey, f32)> = self.ranks.iter().map(|(k, s)| (*k, *s)).collect();
        v.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        v.truncate(1000);
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk(seed: u8) -> String {
        format!("{:02x}", seed).repeat(32)
    }
    fn contacts(author: u8, follows: &[u8]) -> NostrEvent {
        NostrEvent {
            id: format!("{:02x}", author).repeat(32),
            pubkey: pk(author),
            created_at: 10,
            kind: 3,
            tags: follows.iter().map(|p| vec!["p".into(), pk(*p)]).collect(),
            content: String::new(),
            sig: "c".repeat(128),
        }
    }

    #[test]
    fn hub_ranks_highest_after_refresh() {
        let ctx = AnalysisCtx::bare(0);
        let mut pr = Pagerank::default();
        // everyone follows node 9 → it should rank highest
        for a in 1..=5u8 {
            pr.observe(&contacts(a, &[9]), &ctx);
        }
        pr.refresh();
        let mut world = World::new();
        pr.contribute(&mut world);
        let hub = Pubkey::from_hex(&pk(9)).unwrap();
        let leaf = Pubkey::from_hex(&pk(1)).unwrap();
        assert!(world.pagerank(&hub) > world.pagerank(&leaf));
        assert!(world.wot_tier(&hub) >= world.wot_tier(&leaf));
    }
}
