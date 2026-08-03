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
    ///
    /// The rule is "would a human read this field as prose", not "is this kind
    /// interesting": media kinds carry captions, voice notes carry
    /// transcripts, git patches carry commit messages. The one deliberate
    /// *removal* is kind 30078 (app-specific data), whose content is routinely
    /// base64 or NIP-44 ciphertext -- exactly the term-dictionary pollution
    /// this classification exists to avoid.
    pub fn is_text_kind(&self) -> bool {
        matches!(
            self.kind,
            0          // profile metadata (JSON: name/about/nip05)
            | 1        // short text note
            | 20       // picture (NIP-68) -- caption
            | 21       // video (NIP-71) -- description
            | 22       // short video (NIP-71)
            | 1063     // file metadata (NIP-94)
            | 1111     // generic reply / comment (NIP-22)
            | 1222     // voice message (NIP-A0) -- transcript
            | 1244     // voice reply (NIP-A0)
            | 1617     // git patch (NIP-34) -- commit message + diff
            | 1621     // git issue (NIP-34)
            | 1622     // git reply (NIP-34)
            | 9802     // highlight (NIP-84)
            | 30017    // stall (NIP-15)
            | 30018    // product (NIP-15)
            | 30023    // long-form content (NIP-23)
            | 30024    // draft long-form
            | 30311    // live event (NIP-53) -- summary
            | 30402    // classified listing (NIP-99)
            | 31922    // date-based calendar event (NIP-52)
            | 31923    // time-based calendar event (NIP-52)
            | 34550 // community definition (NIP-72)
        )
    }

    /// The first value of a multi-character tag (`title`, `summary`, `alt`,
    /// `name`, ...).
    ///
    /// [`tag_values`](Self::tag_values) already matches on the whole name, so
    /// this is just the "at most one" case spelled out at the call site.
    pub fn tag_value(&self, name: &str) -> Option<&str> {
        self.tag_values(name).next()
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
            vec![
                vec!["t", "nostr"],
                vec!["t"],
                vec!["p", "x"],
                vec!["t", "bitcoin"],
            ],
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

    #[test]
    fn text_kinds_cover_media_voice_git_and_commerce() {
        // Kinds whose content is prose a user would search for, and which the
        // original list omitted entirely.
        for k in [
            20, 21, 22, 1222, 1244, 1617, 1621, 1622, 30017, 30018, 30311, 31922, 31923,
        ] {
            assert!(ev(k, vec![]).is_text_kind(), "kind {k} should be text");
        }
        // Encrypted or opaque payloads must stay out of the term dictionary.
        for k in [4, 1059, 30078] {
            assert!(!ev(k, vec![]).is_text_kind(), "kind {k} should not be text");
        }
    }
}
