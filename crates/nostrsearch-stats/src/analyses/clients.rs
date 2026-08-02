//! Client-tag usage stats — which apps publish what, and when they were last
//! seen.
//!
//! Ported from nostr-dashboard's `reports/clients.rs` (report name
//! `client_tags`). Events with no `client` tag are bucketed under
//! [`NO_CLIENT`].
//!
//! The `client` tag is attacker-controlled freeform text, so upstream's plain
//! `HashMap<String, _>` grows without bound in a long-lived ingest process: a
//! spammer emitting a unique client name per event is an OOM. Two defences:
//! keys are [`normalize`]d (lower-cased, version suffix stripped, length
//! capped) and the map is bounded by [`MAX_CLIENTS`], past which new names
//! land in [`OTHER_CLIENT`].

use crate::{Analysis, AnalysisCtx};
use nostrsearch_core::event::NostrEvent;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Bucket name for events published without a `client` tag.
pub const NO_CLIENT: &str = "N/A";

/// Bucket for client names seen after the map is full.
pub const OTHER_CLIENT: &str = "(other)";

/// Maximum distinct client names tracked. Real client diversity is in the
/// low hundreds; anything beyond this is spam or version noise.
pub const MAX_CLIENTS: usize = 1024;

/// Longest client name kept; longer ones are truncated before bucketing.
const MAX_CLIENT_LEN: usize = 64;

/// Per-client totals.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClientStats {
    /// Total events seen from this client.
    pub sum: u64,
    /// `created_at` of the most recent event from this client.
    pub last_note: u64,
    /// Event count per kind.
    pub kinds: HashMap<u16, u64>,
}

impl ClientStats {
    fn merge(&mut self, other: Self) {
        self.sum += other.sum;
        self.last_note = self.last_note.max(other.last_note);
        for (kind, n) in other.kinds {
            *self.kinds.entry(kind).or_default() += n;
        }
    }
}

/// Client-tag breakdown.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Clients {
    data: HashMap<String, ClientStats>,
    /// Client buckets touched since the last drain (realtime-only; not
    /// checkpointed).
    #[serde(skip)]
    dirty: HashSet<String>,
}

impl Clients {
    pub fn get(&self, client: &str) -> Option<&ClientStats> {
        self.data.get(client)
    }
}

/// Normalize a raw `client` tag value into a stable bucket name.
///
/// Collapses the variants that would otherwise fragment one client across many
/// buckets: case, a `" - "`/`"@"`/`":"` version suffix (more-speech uses the
/// first, many others the rest), and surrounding whitespace. NIP-89 handler
/// coordinates (`31990:<pubkey>:<d>`) are kept whole so they stay resolvable.
pub fn normalize(client: &str) -> String {
    let c = client.trim();

    // NIP-89 `a`-coordinate style value: keep as-is (minus case).
    if c.starts_with("31990:") {
        return c.to_lowercase();
    }

    let base = c
        .split(" - ")
        .next()
        .and_then(|s| s.split('@').next())
        .map(|s| s.trim())
        .unwrap_or(c);

    let mut out = base.to_lowercase();
    if out.len() > MAX_CLIENT_LEN {
        // Truncate on a char boundary.
        let end = out
            .char_indices()
            .take_while(|(i, _)| *i <= MAX_CLIENT_LEN)
            .last()
            .map(|(i, _)| i)
            .unwrap_or(0);
        out.truncate(end);
    }
    out
}

impl Analysis for Clients {
    type Output = HashMap<String, ClientStats>;

    fn name(&self) -> &'static str {
        "client_tags"
    }

    fn observe(&mut self, ev: &NostrEvent, _ctx: &AnalysisCtx) -> bool {
        let client = match ev.tag_values("client").next().map(normalize) {
            Some(c) if !c.is_empty() => c,
            _ => NO_CLIENT.to_string(),
        };

        // Bound the key space: once full, fold unknown names into `(other)`
        // so a spammer cannot grow this map without limit.
        let key = if self.data.len() >= MAX_CLIENTS && !self.data.contains_key(&client) {
            OTHER_CLIENT.to_string()
        } else {
            client
        };

        self.dirty.insert(key.clone());
        let stats = self.data.entry(key).or_default();
        stats.sum += 1;
        stats.last_note = stats.last_note.max(ev.created_at);
        *stats.kinds.entry(ev.kind).or_default() += 1;
        true
    }

    fn merge(&mut self, other: Self) {
        for (client, os) in other.data {
            // Respect the cap across merges too.
            let key = if self.data.len() >= MAX_CLIENTS && !self.data.contains_key(&client) {
                OTHER_CLIENT.to_string()
            } else {
                client
            };
            self.dirty.insert(key.clone());
            self.data.entry(key).or_default().merge(os);
        }
    }

    fn snapshot(&self) -> Self::Output {
        self.data.clone()
    }

    /// Emits only the clients that published since the last drain.
    fn drain_delta(&mut self) -> Option<serde_json::Value> {
        if self.dirty.is_empty() {
            return None;
        }
        let patch: serde_json::Map<String, serde_json::Value> = self
            .dirty
            .drain()
            .filter_map(|name| {
                let stats = self.data.get(&name)?;
                Some((name, serde_json::to_value(stats).ok()?))
            })
            .collect();
        Some(serde_json::Value::Object(patch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(kind: u16, created_at: u64, client: Option<&str>) -> NostrEvent {
        NostrEvent {
            id: "a".repeat(64),
            pubkey: "b".repeat(64),
            created_at,
            kind,
            tags: client
                .map(|c| vec![vec!["client".to_string(), c.to_string()]])
                .unwrap_or_default(),
            content: String::new(),
            sig: "c".repeat(128),
        }
    }

    #[test]
    fn buckets_clients_tracks_last_note_and_merges() {
        let ctx = AnalysisCtx::bare(1_700_000_100);
        let mut a = Clients::default();
        a.observe(&ev(1, 1_700_000_000, Some("snort")), &ctx);
        a.observe(&ev(7, 1_700_000_050, Some("snort")), &ctx);
        a.observe(&ev(1, 1_700_000_000, None), &ctx);

        let mut b = Clients::default();
        b.observe(&ev(1, 1_700_000_900, Some("snort")), &ctx);
        // more-speech versions collapse to one bucket
        b.observe(&ev(1, 1_700_000_000, Some("more-speech - 1.2.3")), &ctx);

        a.merge(b);
        let out = a.snapshot();
        assert_eq!(out["snort"].sum, 3);
        assert_eq!(out["snort"].kinds[&1], 2);
        assert_eq!(out["snort"].kinds[&7], 1);
        assert_eq!(out["snort"].last_note, 1_700_000_900);
        assert_eq!(out[NO_CLIENT].sum, 1);
        assert_eq!(out["more-speech"].sum, 1);
    }

    #[test]
    fn normalizes_case_and_version_suffixes() {
        assert_eq!(normalize("Snort"), "snort");
        assert_eq!(normalize("  snort  "), "snort");
        assert_eq!(normalize("more-speech - 1.2.3"), "more-speech");
        assert_eq!(normalize("damus@1.5"), "damus");
        // NIP-89 coordinates survive intact
        assert_eq!(
            normalize(&format!("31990:{}:app", "a".repeat(64))),
            format!("31990:{}:app", "a".repeat(64))
        );
        // over-long names are truncated, not dropped
        assert!(normalize(&"x".repeat(500)).len() <= MAX_CLIENT_LEN + 1);
    }

    #[test]
    fn variants_collapse_into_one_bucket() {
        let ctx = AnalysisCtx::bare(1_700_000_100);
        let mut a = Clients::default();
        a.observe(&ev(1, 1_700_000_000, Some("Snort")), &ctx);
        a.observe(&ev(1, 1_700_000_000, Some("snort")), &ctx);
        a.observe(&ev(1, 1_700_000_000, Some("snort - 0.2")), &ctx);
        assert_eq!(a.snapshot()["snort"].sum, 3);
    }

    #[test]
    fn key_space_is_bounded_against_spam() {
        let ctx = AnalysisCtx::bare(1_700_000_100);
        let mut a = Clients::default();
        for i in 0..(MAX_CLIENTS + 500) {
            a.observe(&ev(1, 1_700_000_000, Some(&format!("spam-{i}"))), &ctx);
        }
        let out = a.snapshot();
        assert!(out.len() <= MAX_CLIENTS + 1, "map grew to {}", out.len());
        assert_eq!(out[OTHER_CLIENT].sum, 500);
    }
}
