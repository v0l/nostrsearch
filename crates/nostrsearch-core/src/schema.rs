//! Tantivy index schema tuned for Nostr.
//!
//! Design goals, in order:
//!
//! 1. **Tags are first-class.** Nostr "advanced search" is dominated by tag
//!    lookups (`#t` hashtags, `e`/`p` references, `a` addresses, `d`
//!    identifiers). Every tag value goes into a dedicated `STRING` field so it
//!    is a pure term lookup (no analysis, exact match).
//!
//! 2. **Fast fields for filter + score signals.** `created_at`, `kind`,
//!    `wot_tier` are columnar (`FAST`) so range filters, recency decay and WoT
//!    boosts read from the fast-field store instead of deserializing stored
//!    docs per hit (moar's `searcher.doc()` per-hit approach does not scale).
//!
//! 3. **Two content paths.** Only genuinely human-text kinds get their content
//!    tokenized for BM25; the other ~90% of the corpus (encrypted DMs,
//!    gift-wraps, ephemeral app blobs) is still fully indexed for *metadata*
//!    but does not pollute the term dictionary with base64/ciphertext. The user
//!    chose "index everything" — we index every event, but tokenizing
//!    ciphertext buys nothing and costs terabytes of term space.
//!
//! 4. **Stored = hydrate source of truth.** `content` is `STORED` so search
//!    results can be hydrated straight from the index without a second
//!    datastore (moar needed LMDB beside Tantivy; we do not).

use tantivy::schema::*;

/// The full Tantivy schema for a Nostr event shard.
#[derive(Clone, Copy)]
pub struct NostrSchema {
    /// 32-byte event id, hex. Exact-match lookup + dedup key.
    pub event_id: Field,
    /// 32-byte author pubkey, hex. Exact-match + filter.
    pub pubkey: Field,
    /// Event kind. Indexed + fast (filter, facet, sort).
    pub kind: Field,
    /// Unix seconds. Indexed + fast (range filter, recency scoring).
    pub created_at: Field,
    /// Full-text analyzed content (only populated for text kinds).
    pub content: Field,
    /// Raw content, stored for hydration. Not indexed.
    pub raw_content: Field,
    /// Web-of-trust tier (0 = unknown). Fast field for score boost.
    pub wot_tier: Field,
    /// `t` tag values (hashtags), lowercased.
    pub tag_t: Field,
    /// `e` tag values (referenced event ids, hex).
    pub tag_e: Field,
    /// `p` tag values (referenced pubkeys, hex).
    pub tag_p: Field,
    /// `a` tag values (address coordinates `kind:pubkey:d`).
    pub tag_a: Field,
    /// `d` tag value (addressable identifier).
    pub tag_d: Field,
    /// `g` tag values (geohash) — enables location search.
    pub tag_g: Field,
    /// `r`/`u` referenced URLs, stored for link search / analytics.
    pub tag_url: Field,
    /// `l` label values (NIP-32).
    pub tag_l: Field,
    /// Language hint if derivable (from `content` analysis upstream). Stored.
    pub lang: Field,
    /// Whether the event is deleted (by a kind-5) at index time. Fast flag.
    pub deleted: Field,
    /// Whether a newer replaceable/addressable version exists. Fast flag.
    pub superseded: Field,
    /// `created_at` bucketed to the day — cheap coarse time facet.
    pub day: Field,
}

/// Tokenizer name for Nostr content.
pub const CONTENT_TOKENIZER: &str = "nostr_content";

impl NostrSchema {
    /// Build the schema and the corresponding Tantivy `Schema`.
    pub fn build() -> (Schema, Self) {
        let mut b = Schema::builder();

        // --- exact-match identity fields ---
        let event_id = b.add_text_field("event_id", STRING | STORED);
        let pubkey = b.add_text_field("pubkey", STRING | STORED);

        // --- fast / filter / facet fields ---
        let kind = b.add_u64_field("kind", INDEXED | FAST | STORED);
        let created_at = b.add_u64_field("created_at", INDEXED | FAST | STORED);
        let day = b.add_u64_field("day", FAST);
        let wot_tier = b.add_u64_field("wot_tier", FAST);
        let deleted = b.add_u64_field("deleted", FAST);
        let superseded = b.add_u64_field("superseded", FAST);

        // --- full-text content ---
        let content_opts = TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer(CONTENT_TOKENIZER)
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            )
            .set_stored();
        let content = b.add_text_field("content", content_opts);

        // raw content, stored only (hydration for non-text kinds & snippets)
        let raw_content = b.add_text_field("raw_content", STORED);

        // --- tag fields: pure term lookups, multi-valued ---
        let tag_t = b.add_text_field("tag_t", STRING);
        let tag_e = b.add_text_field("tag_e", STRING);
        let tag_p = b.add_text_field("tag_p", STRING);
        let tag_a = b.add_text_field("tag_a", STRING);
        let tag_d = b.add_text_field("tag_d", STRING);
        let tag_g = b.add_text_field("tag_g", STRING);
        let tag_url = b.add_text_field("tag_url", STRING | STORED);
        let tag_l = b.add_text_field("tag_l", STRING);

        let lang = b.add_text_field("lang", STRING | STORED);

        let schema = b.build();

        (
            schema,
            Self {
                event_id,
                pubkey,
                kind,
                created_at,
                day,
                wot_tier,
                deleted,
                superseded,
                content,
                raw_content,
                tag_t,
                tag_e,
                tag_p,
                tag_a,
                tag_d,
                tag_g,
                tag_url,
                tag_l,
                lang,
            },
        )
    }

    /// Register the content tokenizer on an index.
    ///
    /// Nostr content is messy: URLs, `nostr:` bech32 refs, `#hashtags`,
    /// `@mentions`, emoji, mixed scripts. A `SimpleTokenizer` + lowercase +
    /// (optional) stopword removal is a sane baseline; we deliberately do NOT
    /// stem by default because Nostr is multi-lingual and stemming the wrong
    /// language is worse than none. Language-specific analyzers can be layered
    /// on later via the `lang` field.
    pub fn register_tokenizers(index: &tantivy::Index) {
        use tantivy::tokenizer::*;
        let analyzer = TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(RemoveLongFilter::limit(256))
            .filter(LowerCaser)
            .build();
        index.tokenizers().register(CONTENT_TOKENIZER, analyzer);
    }

    /// Materialize a [`NostrEvent`] into a Tantivy document.
    pub fn to_document(
        &self,
        ev: &crate::event::NostrEvent,
        wot_tier: u8,
        deleted: bool,
        superseded: bool,
        lang: Option<&str>,
    ) -> tantivy::TantivyDocument {
        let mut doc = tantivy::TantivyDocument::new();

        doc.add_text(self.event_id, &ev.id);
        doc.add_text(self.pubkey, &ev.pubkey);
        doc.add_u64(self.kind, ev.kind as u64);
        doc.add_u64(self.created_at, ev.created_at);
        doc.add_u64(self.day, ev.created_at / 86_400);
        doc.add_u64(self.wot_tier, wot_tier as u64);
        doc.add_u64(self.deleted, deleted as u64);
        doc.add_u64(self.superseded, superseded as u64);

        doc.add_text(self.raw_content, &ev.content);
        if ev.is_text_kind() {
            doc.add_text(self.content, &ev.content);
        }
        if let Some(l) = lang {
            doc.add_text(self.lang, l);
        }

        for v in ev.tag_values("t") {
            doc.add_text(self.tag_t, v.to_lowercase());
        }
        for v in ev.tag_values("e") {
            doc.add_text(self.tag_e, v);
        }
        for v in ev.tag_values("p") {
            doc.add_text(self.tag_p, v);
        }
        for v in ev.tag_values("a") {
            doc.add_text(self.tag_a, v);
        }
        if let Some(d) = ev.d_tag() {
            doc.add_text(self.tag_d, d);
        }
        for v in ev.tag_values("g") {
            doc.add_text(self.tag_g, v);
        }
        // `r` (NIP-10/NIP-23 refs) and `u` (NIP-61/NIP-99) both carry URLs.
        for v in ev.tag_values("r").chain(ev.tag_values("u")) {
            doc.add_text(self.tag_url, v);
        }
        for v in ev.tag_values("l") {
            doc.add_text(self.tag_l, v);
        }

        doc
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_builds_and_all_fields_present() {
        let (schema, ns) = NostrSchema::build();
        // every field handle must resolve back to a name
        for f in [
            ns.event_id,
            ns.pubkey,
            ns.kind,
            ns.created_at,
            ns.content,
            ns.tag_t,
            ns.tag_e,
            ns.tag_p,
            ns.tag_a,
            ns.tag_d,
            ns.tag_g,
            ns.tag_url,
            ns.tag_l,
            ns.lang,
        ] {
            let entry = schema.get_field_entry(f);
            assert!(
                entry.field_type().is_indexed(),
                "{} must be indexed or it cannot be queried",
                entry.name()
            );
        }

        // These are deliberately not indexed. They are read back per-document
        // during scoring and filtering (`day` for date bucketing, `wot_tier`
        // for the trust boost, the two flags for suppression), never matched as
        // query terms, so they carry FAST without INDEXED.
        for f in [ns.day, ns.wot_tier, ns.deleted, ns.superseded] {
            let entry = schema.get_field_entry(f);
            assert!(
                entry.field_type().is_fast(),
                "{} is read per-document during scoring, so it must be fast",
                entry.name()
            );
        }

        // `raw_content` is returned verbatim and never searched; `content` is
        // the analyzed copy that serves queries.
        let raw = schema.get_field_entry(ns.raw_content);
        assert!(raw.is_stored(), "raw_content must be stored to be returned");
        assert!(!raw.field_type().is_indexed());
    }

    #[test]
    fn document_has_tags_and_flags() {
        let (_, ns) = NostrSchema::build();
        let ev = crate::event::NostrEvent {
            id: "a".repeat(64),
            pubkey: "b".repeat(64),
            created_at: 1_700_000_000,
            kind: 1,
            tags: vec![
                vec!["t".into(), "Nostr".into()],
                vec!["e".into(), "deadbeef".into()],
                vec!["g".into(), "u4pruydqqvj".into()],
            ],
            content: "gm #nostr".into(),
            sig: "c".repeat(128),
        };
        let doc = ns.to_document(&ev, 2, false, false, Some("en"));
        let vals: Vec<_> = doc.get_all(ns.tag_t).collect();
        assert_eq!(vals.len(), 1);
        // hashtag is lowercased
        assert_eq!(vals[0].as_str(), Some("nostr"));
        assert_eq!(doc.get_first(ns.wot_tier).unwrap().as_u64(), Some(2));
    }
}
