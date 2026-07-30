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

use crate::graph::SharedGraph;
use crate::types::Pubkey;
use crate::{Analysis, AnalysisCtx, AttachCtx, World};
use nostrsearch_core::event::NostrEvent;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

const KIND_CONTACTS: &[u16] = &[3];
const DAMPING: f32 = 0.85;
const ITERATIONS: usize = 20;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pagerank {
    /// Last computed ranks (refreshed on a schedule, not per event).
    ranks: HashMap<Pubkey, f32>,
    /// Refresh cadence in seconds.
    interval_secs: u64,
    /// The adjacency is read from the shared on-disk graph that `follow_graph`
    /// maintains; pagerank keeps no copy of its own.
    #[serde(skip)]
    store: Option<SharedGraph>,
}

impl Default for Pagerank {
    fn default() -> Self {
        Self {
            ranks: HashMap::new(),
            interval_secs: 24 * 3600,
            store: None,
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

    fn attach(&mut self, ctx: &AttachCtx) -> anyhow::Result<()> {
        self.store = Some(ctx.graph.clone());
        Ok(())
    }

    /// No per-event work: `follow_graph` already writes every contact list to
    /// the shared store, so pagerank reads it at refresh time instead of
    /// keeping a second copy of the adjacency.
    fn observe(&mut self, _ev: &NostrEvent, _ctx: &AnalysisCtx) -> bool {
        false
    }

    fn merge(&mut self, _other: Self) {
        // Adjacency is shared on disk; ranks are recomputed by `refresh`.
    }

    /// Expensive: power-iterate pagerank over the shared on-disk graph.
    ///
    /// Two streaming passes build a dense node index and the out-adjacency, so
    /// only the index and rank vectors are resident (sized by pubkeys, not
    /// edges); the edges themselves stay in RocksDB.
    fn refresh(&mut self) {
        let Some(store) = self.store.clone() else {
            return;
        };

        // Pass 1: dense node index over everyone who appears.
        let mut idx: HashMap<Pubkey, usize> = HashMap::new();
        store.for_each(|author, f| {
            idx.entry(author).or_insert(0);
            for p in &f.follows {
                idx.entry(*p).or_insert(0);
            }
        });
        let n = idx.len();
        if n == 0 {
            self.ranks.clear();
            return;
        }
        for (i, v) in idx.values_mut().enumerate() {
            *v = i;
        }
        // Pass 2: out-adjacency by index.
        let mut out: Vec<Vec<usize>> = vec![Vec::new(); n];
        store.for_each(|author, f| {
            let ai = idx[&author];
            for p in &f.follows {
                out[ai].push(idx[p]);
            }
        });

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
    use crate::graph::GraphStore;
    use std::sync::Arc;

    fn pk(seed: u8) -> Pubkey {
        crate::types::Hash32([seed; 32])
    }

    #[test]
    fn hub_ranks_highest_after_refresh() {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "nspr-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = Arc::new(GraphStore::open(&dir).unwrap());

        // `follow_graph` owns writes; pagerank only reads the shared store.
        for a in 1..=5u8 {
            store.put(&pk(a), 10, &[pk(9)]).unwrap();
        }

        let mut pr = Pagerank::default();
        pr.attach(&AttachCtx {
            graph: store.clone(),
        })
        .unwrap();
        pr.refresh();

        let mut world = World::new();
        pr.contribute(&mut world);
        assert!(
            world.pagerank(&pk(9)) > world.pagerank(&pk(1)),
            "the hub everyone follows should rank highest"
        );
        assert!(world.wot_tier(&pk(9)) >= world.wot_tier(&pk(1)));

        drop(pr);
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
