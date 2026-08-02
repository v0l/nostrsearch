//! Relays the network advertises, and how many distinct people advertise each.
//!
//! This exists to feed the scraper its target list. It used to derive that by
//! scanning the index at startup: open all 365 shards, query each for kind
//! 10002, and fetch a *stored document* per hit to read its pubkey and url
//! tags. Stored-field access is a random read plus a block decompress per
//! document, so it ran for five to ten minutes of solid disk load — and it ran
//! on every boot, because the timer guarding it was an in-process `Instant`
//! that a deploy resets.
//!
//! Every one of those events streams past the indexer anyway. Folding them as
//! they pass turns a repeated full-index scan into a lookup, and the result is
//! persisted, reset and rebuilt by the same machinery as every other report.
//!
//! Distinct advertisers are counted with a HyperLogLog rather than a set of
//! pubkeys. A set is what the old scan built, in memory, for the duration of
//! the scan; keeping one *persisted* per relay would grow without bound in the
//! one direction that matters, since relay lists are exactly what a spammer can
//! emit cheaply. Precision 8 costs 256 bytes per relay for roughly 6% error,
//! which is far below the resolution anything downstream needs: the count picks
//! a scrape order and applies a "mentioned by at least N people" threshold.

use crate::hll::Hll;
use crate::{Analysis, AnalysisCtx};
use nostrsearch_core::event::NostrEvent;
use nostrsearch_core::relay::normalize_relay_url;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// NIP-65 relay list metadata.
const KIND_RELAY_LIST: &[u16] = &[10_002];

/// Maximum relays tracked.
///
/// Relay URLs come from event tags, so they are attacker-controlled: an
/// unbounded map is an OOM waiting for someone to publish relay lists full of
/// unique hostnames. The real network is a few thousand relays, and the
/// scraper only ever uses the top slice of this by advertiser count.
pub const MAX_RELAYS: usize = 8192;

/// What is known about one relay.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RelayStats {
    /// Distinct pubkeys advertising this relay, estimated.
    pub advertisers: u64,
    /// Newest relay list that mentioned it.
    pub last_seen: u64,
    /// Estimator backing `advertisers`. Not part of the report output.
    #[serde(default)]
    hll: Hll,
}

/// Relay advertisement counts, keyed by normalized URL.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Relays {
    data: HashMap<String, RelayStats>,
}

impl Relays {
    /// Relays ordered by advertiser count, most advertised first.
    ///
    /// This is the scraper's target list: it takes a prefix and applies its own
    /// minimum-advertiser threshold.
    pub fn ranked(&self) -> Vec<(String, u64)> {
        let mut out: Vec<(String, u64)> = self
            .data
            .iter()
            .map(|(url, s)| (url.clone(), s.advertisers))
            .collect();
        out.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        out
    }
}

impl Analysis for Relays {
    type Output = HashMap<String, RelayStats>;

    fn name(&self) -> &'static str {
        "relays"
    }

    fn kinds(&self) -> Option<&[u16]> {
        Some(KIND_RELAY_LIST)
    }

    fn observe(&mut self, ev: &NostrEvent, ctx: &AnalysisCtx) -> bool {
        let mut kept = false;
        for raw in ev.tag_values("r").chain(ev.tag_values("u")) {
            let Some(url) = normalize_relay_url(raw) else {
                continue;
            };
            // Once full, existing relays still update: refusing those would
            // freeze the counts that decide scrape order the moment some
            // spammer fills the map.
            if !self.data.contains_key(&url) && self.data.len() >= MAX_RELAYS {
                continue;
            }
            let e = self.data.entry(url).or_default();
            e.hll.insert(&ctx.author);
            e.advertisers = e.hll.len();
            e.last_seen = e.last_seen.max(ev.created_at);
            kept = true;
        }
        kept
    }

    fn merge(&mut self, other: Self) {
        for (url, os) in other.data {
            if !self.data.contains_key(&url) && self.data.len() >= MAX_RELAYS {
                continue;
            }
            let e = self.data.entry(url).or_default();
            // Merging the sketches is what makes the estimate correct across
            // shards: summing the counts would count anyone advertising a
            // relay in two shards twice.
            e.hll.merge(&os.hll);
            e.advertisers = e.hll.len();
            e.last_seen = e.last_seen.max(os.last_seen);
        }
    }

    fn snapshot(&self) -> Self::Output {
        self.data.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::World;
    use crate::types::Hash32;

    fn relay_list(author: u8, urls: &[&str]) -> NostrEvent {
        NostrEvent {
            id: format!("{author:02x}").repeat(32),
            pubkey: format!("{author:02x}").repeat(32),
            created_at: 1_700_000_000 + author as u64,
            kind: 10_002,
            tags: urls.iter().map(|u| vec!["r".into(), (*u).into()]).collect(),
            content: String::new(),
            sig: "c".repeat(128),
        }
    }

    fn ctx_for<'a>(ev: &NostrEvent, world: &'a World) -> AnalysisCtx<'a> {
        AnalysisCtx::new(
            1_700_100_000,
            Hash32::from_hex(&ev.pubkey).unwrap(),
            Hash32::from_hex(&ev.id).unwrap(),
            world,
        )
    }

    #[test]
    fn counts_distinct_advertisers_not_advertisements() {
        let world = World::new();
        let mut r = Relays::default();

        // One author republishing their list must not inflate the count: relay
        // lists are replaceable, so this is the normal case, not an edge one.
        for _ in 0..10 {
            let ev = relay_list(1, &["wss://relay.damus.io"]);
            r.observe(&ev, &ctx_for(&ev, &world));
        }
        assert_eq!(r.data["wss://relay.damus.io"].advertisers, 1);

        for a in 2..=5u8 {
            let ev = relay_list(a, &["wss://relay.damus.io"]);
            r.observe(&ev, &ctx_for(&ev, &world));
        }
        assert_eq!(r.data["wss://relay.damus.io"].advertisers, 5);
    }

    #[test]
    fn urls_are_normalized_and_unreachable_ones_dropped() {
        let world = World::new();
        let mut r = Relays::default();

        // The same relay written three ways is one relay.
        for (i, u) in [
            "wss://Relay.Damus.io/",
            "wss://relay.damus.io",
            "WSS://RELAY.DAMUS.IO/",
        ]
        .iter()
        .enumerate()
        {
            let ev = relay_list(i as u8 + 1, &[u]);
            r.observe(&ev, &ctx_for(&ev, &world));
        }
        assert_eq!(r.data.len(), 1, "one relay, three spellings: {:?}", r.data);
        assert_eq!(r.data["wss://relay.damus.io"].advertisers, 3);

        // Nothing the scraper could connect to from a server.
        let ev = relay_list(9, &["ws://localhost:8080", "wss://x.onion", "http://a.com"]);
        assert!(!r.observe(&ev, &ctx_for(&ev, &world)));
        assert_eq!(r.data.len(), 1);
    }

    #[test]
    fn the_map_is_bounded_but_known_relays_keep_updating() {
        let world = World::new();
        let mut r = Relays::default();
        for i in 0..MAX_RELAYS {
            r.data
                .insert(format!("wss://r{i}.example.com"), RelayStats::default());
        }
        let full = r.data.len();

        // A flood of unique hostnames must not grow the map.
        let ev = relay_list(1, &["wss://spam1.example.com", "wss://spam2.example.com"]);
        r.observe(&ev, &ctx_for(&ev, &world));
        assert_eq!(r.data.len(), full, "map must stay bounded");

        // But a relay already tracked still accumulates, or the ordering the
        // scraper depends on would freeze as soon as the map filled.
        let ev = relay_list(7, &["wss://r0.example.com"]);
        assert!(r.observe(&ev, &ctx_for(&ev, &world)));
        assert_eq!(r.data["wss://r0.example.com"].advertisers, 1);
    }

    #[test]
    fn ranked_orders_by_advertisers() {
        let world = World::new();
        let mut r = Relays::default();
        for a in 1..=5u8 {
            let ev = relay_list(a, &["wss://popular.example.com"]);
            r.observe(&ev, &ctx_for(&ev, &world));
        }
        let ev = relay_list(9, &["wss://quiet.example.com"]);
        r.observe(&ev, &ctx_for(&ev, &world));

        let ranked = r.ranked();
        assert_eq!(ranked[0].0, "wss://popular.example.com");
        assert_eq!(ranked[0].1, 5);
        assert_eq!(ranked[1].0, "wss://quiet.example.com");
    }
}
