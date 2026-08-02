//! Per-kind event counts, split by web-of-trust. Optionally gates publishers
//! via a [`PublisherFilter`] — when set it depends on `follow_graph` so
//! follower/WoT data is materialized first.

use super::counter::TrustedCount;
use crate::{Analysis, AnalysisCtx, PublisherFilter};
use nostrsearch_core::event::NostrEvent;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Per-kind trusted/untrusted counts. Alias of the shared [`TrustedCount`].
pub type KindCount = TrustedCount;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KindBreakdown {
    counts: HashMap<u16, KindCount>,
    #[serde(default)]
    filter: Option<PublisherFilter>,
}

impl KindBreakdown {
    /// Only count events whose author clears `filter` (depends on follow_graph).
    pub fn filtered(filter: PublisherFilter) -> Self {
        Self {
            counts: HashMap::new(),
            filter: Some(filter),
        }
    }
}

impl Analysis for KindBreakdown {
    type Output = HashMap<u16, KindCount>;

    fn name(&self) -> &'static str {
        "kind_breakdown"
    }

    fn deps(&self) -> &'static [&'static str] {
        if self.filter.is_some() {
            &["follow_graph"]
        } else {
            &[]
        }
    }

    fn observe(&mut self, ev: &NostrEvent, ctx: &AnalysisCtx) -> bool {
        if let Some(f) = &self.filter
            && !f.allows(ctx)
        {
            return false; // filtered out (reported in metrics)
        }
        self.counts
            .entry(ev.kind)
            .or_default()
            .incr(ctx.author_trusted(), 1);
        true
    }

    fn merge(&mut self, other: Self) {
        for (kind, oc) in other.counts {
            self.counts.entry(kind).or_default().merge(oc);
        }
    }

    fn snapshot(&self) -> Self::Output {
        self.counts.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Pubkey;

    fn ev(kind: u16, pubkey: &str) -> NostrEvent {
        NostrEvent {
            id: "a".repeat(64),
            pubkey: pubkey.into(),
            created_at: 1_700_000_000,
            kind,
            tags: vec![],
            content: String::new(),
            sig: "c".repeat(128),
        }
    }

    #[test]
    fn counts_split_by_trust_and_merge() {
        let trusted = "b".repeat(64);
        let untrusted = "f".repeat(64);
        let mut world = crate::World::new();
        world.set_wot_tier(Pubkey::from_hex(&trusted).unwrap(), 2);

        let mk = |pk: &str| {
            let a = Pubkey::from_hex(pk).unwrap();
            AnalysisCtx::new(1_700_000_100, a, Pubkey::ZERO, &world)
        };

        let mut a = KindBreakdown::default();
        a.observe(&ev(1, &trusted), &mk(&trusted));
        a.observe(&ev(1, &untrusted), &mk(&untrusted));

        let mut b = KindBreakdown::default();
        b.observe(&ev(1, &trusted), &mk(&trusted));
        b.observe(&ev(7, &untrusted), &mk(&untrusted));

        a.merge(b);
        let out = a.snapshot();
        assert_eq!(out[&1].trusted, 2);
        assert_eq!(out[&1].untrusted, 1);
        assert_eq!(out[&7].untrusted, 1);
    }
}
