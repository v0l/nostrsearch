//! Canonical Nostr event representation (NIP-01).

use serde::{Deserialize, Serialize};

/// A single Nostr event as it appears on the wire / in hole.v0l.io JSONL dumps.
///
/// We keep this deliberately serde-only and dependency-light (no `nostr` crate)
/// because the indexer parses ~763 GiB of JSONL and the hot path must avoid
/// any unnecessary crypto or allocation. Signature verification is explicitly
/// out of scope for a read-mostly search index over an archival dump.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NostrEvent {
    /// 32-byte event id, lowercase hex.
    pub id: String,
    /// 32-byte author public key, lowercase hex.
    pub pubkey: String,
    /// Unix timestamp (seconds).
    pub created_at: u64,
    /// Event kind.
    pub kind: u16,
    /// Tag array — each tag is a list of strings, tags[0] is the tag name.
    #[serde(default)]
    pub tags: Vec<Vec<String>>,
    /// Event content.
    #[serde(default)]
    pub content: String,
    /// 64-byte Schnorr signature, lowercase hex.
    pub sig: String,
}

impl NostrEvent {
    /// Iterate over the *values* of all tags with the given single-letter name.
    ///
    /// `tag_values('t')` yields every hashtag, `tag_values('p')` every mentioned
    /// pubkey, etc. Tags with no value (length 1) are skipped.
    pub fn tag_values(&self, name: &str) -> impl Iterator<Item = &str> {
        self.tags.iter().filter_map(move |t| {
            if t.len() >= 2 && t[0] == name {
                Some(t[1].as_str())
            } else {
                None
            }
        })
    }

    /// The `d` identifier of a parameterized-replaceable (addressable) event.
    pub fn d_tag(&self) -> Option<&str> {
        self.tag_values("d").next()
    }

    /// Whether this kind is replaceable per NIP-01 (kind 0, 3, 10000-19999).
    pub fn is_replaceable(&self) -> bool {
        self.kind == 0 || self.kind == 3 || (10_000..20_000).contains(&self.kind)
    }

    /// Whether this kind is parameterized-replaceable / addressable (30000-39999).
    pub fn is_addressable(&self) -> bool {
        (30_000..40_000).contains(&self.kind)
    }

    /// Whether this is an ephemeral event (20000-29999) — never stored by relays.
    pub fn is_ephemeral(&self) -> bool {
        (20_000..30_000).contains(&self.kind)
    }

    /// Whether this is a deletion request (kind 5).
    pub fn is_deletion(&self) -> bool {
        self.kind == 5
    }

    /// Whether the event carries human-readable, full-text-searchable content.
    ///
    /// This drives which kinds get their `content` tokenized vs stored raw.
    /// The caller decides policy; this just classifies.
    pub fn is_text_kind(&self) -> bool {
        matches!(
            self.kind,
            0          // profile metadata
            | 1        // short text note
            | 1111     // generic reply / comment (NIP-22)
            | 9802     // highlight (NIP-84)
            | 1063     // file metadata (NIP-94)
            | 30023    // long-form content (NIP-23)
            | 30024    // draft long-form
            | 30078    // app-specific data (often descriptive)
            | 30402    // classified listing (NIP-99)
            | 34550    // community definition (NIP-72)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(kind: u16, tags: Vec<Vec<&str>>) -> NostrEvent {
        NostrEvent {
            id: "a".repeat(64),
            pubkey: "b".repeat(64),
            created_at: 1_700_000_000,
            kind,
            tags: tags
                .into_iter()
                .map(|t| t.into_iter().map(String::from).collect())
                .collect(),
            content: "hello".into(),
            sig: "c".repeat(128),
        }
    }

    #[test]
    fn tag_values_filters_and_skips_short_tags() {
        let e = ev(
            1,
            vec![vec!["t", "nostr"], vec!["t"], vec!["p", "x"], vec!["t", "bitcoin"]],
        );
        let t: Vec<&str> = e.tag_values("t").collect();
        assert_eq!(t, vec!["nostr", "bitcoin"]);
    }

    #[test]
    fn kind_classification() {
        assert!(ev(0, vec![]).is_replaceable());
        assert!(ev(3, vec![]).is_replaceable());
        assert!(ev(10_002, vec![]).is_replaceable());
        assert!(ev(30_023, vec![]).is_addressable());
        assert!(ev(20_000, vec![]).is_ephemeral());
        assert!(ev(5, vec![]).is_deletion());
        assert!(ev(1, vec![]).is_text_kind());
        assert!(!ev(4, vec![]).is_text_kind());
    }
}
