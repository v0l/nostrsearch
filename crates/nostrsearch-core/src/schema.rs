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
//! 4. **The most searchable text is often in a tag, not in `content`.** For
//!    long-form posts, listings, live events and calendar entries the `title`
//!    and `summary` tags carry the point of the event; for profiles the name
//!    and nip05 live inside a JSON blob. Those are extracted into their own
//!    analyzed fields rather than left to chance.
//!
//! 5. **Stored once.** Exactly one copy of the content is stored, for snippets
//!    and for nodes with no archive attached. Complete signed events (tags and
//!    `sig` included, as a NIP-50 relay must return) are hydrated by id from
//!    the archive, which records each event's shard and offset — duplicating
//!    763 GiB of JSON inside the index to avoid one O(1) lookup is not a trade
//!    worth making.
//!
//! ## Deliberately absent
//!
//! There are no `deleted` / `superseded` columns. Nothing in this system
//! computes either one — there is no kind-5 deletion pass and no
//! replaceable-version tracking — so the columns only ever held zero, and the
//! `exclude_deleted` / `exclude_superseded` query parameters that read them
//! could not do anything but error (they were `FAST` without `INDEXED`) or,
//! once "fixed", match every document while looking like a working filter.
//!
//! Neither is worth indexing even if it were computed, because this is a full
//! archive and both questions are already answerable from what is indexed:
//!
//! - **Superseded** is a derived view, not a fact about an event. The version
//!   history of a replaceable event *is* the query `authors:<pk> kind:<k>
//!   #d:<id>` ordered by `created_at` — the newest row is the live version and
//!   every older row is superseded, by definition. Baking that into a column
//!   would freeze one answer at index time, and it would be wrong the moment
//!   the next version arrives (which is exactly what a *rewriting* index
//!   cannot cheaply fix over 900M events).
//! - **Deleted** is likewise a claim someone published: a kind-5 event naming
//!   its target in an `e` tag, which stays searchable as an ordinary event.
//!   An archive's job is to record that the request happened, not to enact it.
//!
//! Callers that want either view compose it from the index; the policy stays
//! with them.
//!
//! There is likewise no `day` field: it was written on every document and read
//! by nothing (neither `scoring.rs` nor `query.rs`), and `created_at` is
//! already a fast field that any date bucketing can be derived from.

use tantivy::schema::*;

/// The full Tantivy schema for a Nostr event shard.
#[derive(Clone, Copy)]
pub struct NostrSchema {
    /// 32-byte event id, lowercase hex. Exact-match lookup + dedup key.
    pub event_id: Field,
    /// 32-byte author pubkey, lowercase hex. Exact-match + filter.
    pub pubkey: Field,
    /// Event kind. Indexed + fast (filter, facet, sort).
    pub kind: Field,
    /// Unix seconds. Indexed + fast (range filter, recency scoring).
    pub created_at: Field,
    /// Full-text analyzed content (only populated for text kinds). Not stored;
    /// [`raw_content`](Self::raw_content) is the single stored copy.
    pub content: Field,
    /// Raw content, stored for hydration. Not indexed.
    pub raw_content: Field,
    /// Analyzed title-ish text: `title`/`subject` tags, profile `name` and
    /// `display_name`. Short, high-signal, and worth boosting over body text.
    pub title: Field,
    /// Analyzed secondary text: `summary`/`alt`/`description` tags and the
    /// profile `about` blurb.
    pub summary: Field,
    /// NIP-05 identifier, lowercased and stored whole (`bob@example.com` is
    /// one term, not three).
    pub nip05: Field,
    /// Web-of-trust tier (0 = unknown). Fast field for score boost.
    pub wot_tier: Field,
    /// `t` tag values (hashtags), lowercased.
    pub tag_t: Field,
    /// `e` tag values (referenced event ids, lowercase hex).
    pub tag_e: Field,
    /// `p` tag values (referenced pubkeys, lowercase hex).
    pub tag_p: Field,
    /// `a` tag values (address coordinates `kind:pubkey:d`).
    pub tag_a: Field,
    /// `d` tag value (addressable identifier).
    pub tag_d: Field,
    /// `g` tag values (geohash) **and every prefix of them**, so a prefix or
    /// radius search is a single term lookup.
    pub tag_g: Field,
    /// `r`/`u` referenced URLs, stored for link search / analytics.
    pub tag_url: Field,
    /// Normalized host of each referenced URL (`example.com`), so links can be
    /// searched by domain.
    pub tag_host: Field,
    /// `l` label values (NIP-32).
    pub tag_l: Field,
    /// Detected language of the content (ISO 639-1, e.g. `en`). Populated by
    /// the writer; absent when detection is not confident.
    pub lang: Field,
}

/// Tokenizer name for Nostr content.
pub const CONTENT_TOKENIZER: &str = "nostr_content";

/// Longest geohash prefix indexed. Level 9 is ~5 m; beyond that a "prefix
/// search" is a point lookup and the extra terms buy nothing.
const MAX_GEOHASH_PRECISION: usize = 9;

/// Shortest geohash prefix indexed. Level 1 is a ~5000 km cell — matching it
/// is barely narrower than matching everything, and it would be a very hot
/// posting list.
const MIN_GEOHASH_PRECISION: usize = 2;

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
        let wot_tier = b.add_u64_field("wot_tier", FAST);

        // --- full-text content ---
        //
        // Analyzed but NOT stored: `raw_content` below is the stored copy, and
        // storing both duplicated the entire corpus inside the index.
        let analyzed = TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer(CONTENT_TOKENIZER)
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        );
        let content = b.add_text_field("content", analyzed.clone());
        let title = b.add_text_field("title", analyzed.clone() | STORED);
        let summary = b.add_text_field("summary", analyzed);

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
        let tag_host = b.add_text_field("tag_host", STRING);
        let tag_l = b.add_text_field("tag_l", STRING);
        let nip05 = b.add_text_field("nip05", STRING | STORED);

        let lang = b.add_text_field("lang", STRING | STORED);

        let schema = b.build();

        (
            schema,
            Self {
                event_id,
                pubkey,
                kind,
                created_at,
                wot_tier,
                content,
                raw_content,
                title,
                summary,
                nip05,
                tag_t,
                tag_e,
                tag_p,
                tag_a,
                tag_d,
                tag_g,
                tag_url,
                tag_host,
                tag_l,
                lang,
            },
        )
    }

    /// Fields searched by bare terms in a query string, with their boosts.
    ///
    /// Free text used to be parsed against `content` alone, so a search for a
    /// long-form post's title could not match it. Titles are short and
    /// deliberate, so they outrank body text; summaries sit between the two.
    pub fn free_text_fields(&self) -> Vec<(Field, f32)> {
        vec![(self.title, 3.0), (self.summary, 1.5), (self.content, 1.0)]
    }

    /// Register the content tokenizer on an index.
    ///
    /// Nostr content is messy: URLs, `nostr:` bech32 refs, `#hashtags`,
    /// `@mentions`, emoji, mixed scripts. We deliberately do NOT stem, because
    /// Nostr is multi-lingual and stemming the wrong language is worse than
    /// none — but we do have to segment scripts that are written without
    /// spaces, which [`NostrTokenizer`](crate::tokenizer::NostrTokenizer)
    /// handles per script run.
    ///
    /// Filters: token length cap (see
    /// [`MAX_TOKEN_BYTES`](crate::tokenizer::MAX_TOKEN_BYTES)), lowercasing,
    /// and ASCII folding so `café` and `cafe` are the same term.
    pub fn register_tokenizers(index: &tantivy::Index) {
        use tantivy::tokenizer::*;
        let analyzer = TextAnalyzer::builder(crate::tokenizer::NostrTokenizer)
            .filter(RemoveLongFilter::limit(crate::tokenizer::MAX_TOKEN_BYTES))
            .filter(LowerCaser)
            .filter(AsciiFoldingFilter)
            .build();
        index.tokenizers().register(CONTENT_TOKENIZER, analyzer);
    }

    /// Materialize a [`NostrEvent`](crate::event::NostrEvent) into a Tantivy
    /// document.
    ///
    /// `lang` is the writer's language detection (`None` when it was not
    /// confident); everything else is derived from the event itself.
    pub fn to_document(
        &self,
        ev: &crate::event::NostrEvent,
        wot_tier: u8,
        lang: Option<&str>,
    ) -> tantivy::TantivyDocument {
        let mut doc = tantivy::TantivyDocument::new();

        // Hex identifiers are matched exactly, so they are normalized on the
        // way in; the planner normalizes the query side to match. Without
        // this, an event published with uppercase hex is simply unfindable.
        doc.add_text(self.event_id, normalize_hex(&ev.id));
        doc.add_text(self.pubkey, normalize_hex(&ev.pubkey));
        doc.add_u64(self.kind, ev.kind as u64);
        doc.add_u64(self.created_at, ev.created_at);
        doc.add_u64(self.wot_tier, wot_tier as u64);

        doc.add_text(self.raw_content, &ev.content);
        if ev.is_text_kind() {
            doc.add_text(self.content, &ev.content);
        }
        if let Some(l) = lang {
            doc.add_text(self.lang, l);
        }

        // --- title-ish text -------------------------------------------------
        // NIP-23/52/53/99 all put the headline in a `title` tag; NIP-17 and
        // mail-like kinds use `subject`.
        for name in ["title", "subject"] {
            if let Some(v) = ev.tag_value(name) {
                doc.add_text(self.title, v);
            }
        }
        for name in ["summary", "description", "alt"] {
            if let Some(v) = ev.tag_value(name) {
                doc.add_text(self.summary, v);
            }
        }

        // --- profiles: the searchable parts live inside the JSON blob -------
        // Kind 0 content is `{"name":..,"display_name":..,"about":..,"nip05":..}`.
        // Tokenizing the blob happens to index the values, but it also indexes
        // the keys and shreds `bob@example.com` into three terms, so the
        // fields that people actually search by are extracted properly.
        if ev.kind == 0
            && let Ok(serde_json::Value::Object(p)) =
                serde_json::from_str::<serde_json::Value>(&ev.content)
        {
            {
                for key in ["name", "display_name", "displayName", "username"] {
                    if let Some(v) = p.get(key).and_then(|v| v.as_str()) {
                        doc.add_text(self.title, v);
                    }
                }
                for key in ["about", "bio"] {
                    if let Some(v) = p.get(key).and_then(|v| v.as_str()) {
                        doc.add_text(self.summary, v);
                    }
                }
                if let Some(v) = p.get("nip05").and_then(|v| v.as_str()) {
                    doc.add_text(self.nip05, normalize_nip05(v));
                }
                if let Some(v) = p.get("website").and_then(|v| v.as_str())
                    && let Some(h) = url_host(v)
                {
                    doc.add_text(self.tag_host, h);
                }
            }
        }

        // --- tags -----------------------------------------------------------
        for v in ev.tag_values("t") {
            doc.add_text(self.tag_t, v.to_lowercase());
        }
        for v in ev.tag_values("e") {
            doc.add_text(self.tag_e, normalize_hex(v));
        }
        for v in ev.tag_values("p") {
            doc.add_text(self.tag_p, normalize_hex(v));
        }
        for v in ev.tag_values("a") {
            doc.add_text(self.tag_a, normalize_coordinate(v));
        }
        if let Some(d) = ev.d_tag() {
            doc.add_text(self.tag_d, d);
        }
        // Every prefix of the geohash, so "everything within this cell" is one
        // term lookup instead of a scan: `u4pruy` matches any event whose
        // geohash starts with it, at any precision.
        for v in ev.tag_values("g") {
            for p in geohash_prefixes(v) {
                doc.add_text(self.tag_g, p);
            }
        }
        // `r` (NIP-10/NIP-23 refs) and `u` (NIP-61/NIP-99) both carry URLs.
        for v in ev.tag_values("r").chain(ev.tag_values("u")) {
            doc.add_text(self.tag_url, v);
            if let Some(h) = url_host(v) {
                doc.add_text(self.tag_host, h);
            }
        }
        for v in ev.tag_values("l") {
            doc.add_text(self.tag_l, v);
        }

        doc
    }
}

/// Lowercase a hex identifier so exact-match lookups are case-insensitive.
///
/// Only touched when it actually is hex: `e` tags in the wild sometimes carry
/// a relay hint or junk, and lowercasing that would change a value the caller
/// may be matching verbatim.
pub fn normalize_hex(s: &str) -> String {
    if s.bytes().all(|b| b.is_ascii_hexdigit()) {
        s.to_ascii_lowercase()
    } else {
        s.to_string()
    }
}

/// Normalize an `a` coordinate (`kind:pubkey:d`): the pubkey is hex and
/// case-insensitive, the `d` identifier is neither and is left alone.
pub fn normalize_coordinate(s: &str) -> String {
    let mut parts = s.splitn(3, ':');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(k), Some(pk), rest) => {
            let mut out = format!("{}:{}", k, normalize_hex(pk));
            if let Some(d) = rest {
                out.push(':');
                out.push_str(d);
            }
            out
        }
        _ => s.to_string(),
    }
}

/// Normalize a NIP-05 identifier: lowercase, and expand the `_@domain` form
/// that clients display as bare `domain`.
pub fn normalize_nip05(s: &str) -> String {
    let s = s.trim().to_lowercase();
    match s.strip_prefix("_@") {
        Some(domain) => domain.to_string(),
        None => s,
    }
}

/// Host of a URL, lowercased, with `www.` and any port removed.
///
/// Deliberately hand-rolled rather than pulling in a URL parser: the input is
/// arbitrary user-supplied tag text, most of which is not a valid URL, and the
/// only part we want is the authority.
pub fn url_host(url: &str) -> Option<String> {
    let rest = url
        .split_once("://")
        .map(|(_, r)| r)
        .unwrap_or_else(|| url.trim_start_matches("//"));
    let authority = rest
        .split(['/', '?', '#'])
        .next()
        .unwrap_or("")
        // strip userinfo
        .rsplit('@')
        .next()
        .unwrap_or("");
    let host = authority.split(':').next().unwrap_or("").to_lowercase();
    let host = host.strip_prefix("www.").unwrap_or(&host);
    // A host has to have a dot and at least one label character, or it is a
    // relative path we mis-parsed.
    if host.contains('.') && !host.starts_with('.') && !host.ends_with('.') {
        Some(host.to_string())
    } else {
        None
    }
}

/// Every indexable prefix of a geohash, longest last.
///
/// Geohash is designed so that a shared prefix means spatial containment, so
/// indexing the truncations turns "within this cell" into one term lookup.
pub fn geohash_prefixes(g: &str) -> Vec<String> {
    let g = g.trim().to_lowercase();
    // Base32 geohash alphabet: no a, i, l, o.
    if g.is_empty() || !g.bytes().all(|b| b.is_ascii_alphanumeric()) {
        return Vec::new();
    }
    let chars: Vec<char> = g.chars().take(MAX_GEOHASH_PRECISION).collect();
    (MIN_GEOHASH_PRECISION..=chars.len())
        .map(|n| chars[..n].iter().collect())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev_with(kind: u16, content: &str, tags: Vec<Vec<&str>>) -> crate::event::NostrEvent {
        crate::event::NostrEvent {
            id: "a".repeat(64),
            pubkey: "b".repeat(64),
            created_at: 1_700_000_000,
            kind,
            tags: tags
                .into_iter()
                .map(|t| t.into_iter().map(String::from).collect())
                .collect(),
            content: content.into(),
            sig: "c".repeat(128),
        }
    }

    fn texts(doc: &tantivy::TantivyDocument, f: Field) -> Vec<String> {
        use tantivy::schema::Value;
        doc.get_all(f)
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect()
    }

    #[test]
    fn schema_builds_and_all_queryable_fields_are_indexed() {
        let (schema, ns) = NostrSchema::build();
        for f in [
            ns.event_id,
            ns.pubkey,
            ns.kind,
            ns.created_at,
            ns.content,
            ns.title,
            ns.summary,
            ns.nip05,
            ns.tag_t,
            ns.tag_e,
            ns.tag_p,
            ns.tag_a,
            ns.tag_d,
            ns.tag_g,
            ns.tag_url,
            ns.tag_host,
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

        // `wot_tier` is read per-document during scoring, never matched.
        assert!(schema.get_field_entry(ns.wot_tier).field_type().is_fast());
    }

    #[test]
    fn there_are_no_flag_columns_to_lie_with() {
        // `deleted`/`superseded` were written as a constant 0 by the only
        // caller that existed, so any filter reading them was either an error
        // (not indexed) or a no-op that matched the whole corpus. They are
        // gone; kind-5 deletions remain searchable as ordinary events.
        let (schema, _) = NostrSchema::build();
        for name in ["deleted", "superseded", "day"] {
            assert!(
                schema.get_field(name).is_err(),
                "{name} is back in the schema without anything to put in it"
            );
        }
    }

    #[test]
    fn content_is_stored_exactly_once() {
        let (schema, ns) = NostrSchema::build();
        // The analyzed copy serves queries and is not stored...
        assert!(!schema.get_field_entry(ns.content).is_stored());
        // ...`raw_content` is the single stored copy returned to callers.
        let raw = schema.get_field_entry(ns.raw_content);
        assert!(raw.is_stored());
        assert!(!raw.field_type().is_indexed());

        // And a text-kind document really does carry only one copy.
        let ev = ev_with(1, "gm nostr", vec![]);
        let doc = ns.to_document(&ev, 0, None);
        assert_eq!(texts(&doc, ns.raw_content), vec!["gm nostr"]);
        assert_eq!(texts(&doc, ns.content), vec!["gm nostr"]);
        assert!(!schema.get_field_entry(ns.content).is_stored());
    }

    #[test]
    fn hex_identifiers_are_normalized_on_the_way_in() {
        let mut ev = ev_with(
            1,
            "x",
            vec![vec!["e", &"D".repeat(64)], vec!["p", &"AB".repeat(32)]],
        );
        ev.id = "A".repeat(64);
        ev.pubkey = "B".repeat(64);
        let (_, ns) = NostrSchema::build();
        let doc = ns.to_document(&ev, 0, None);
        assert_eq!(texts(&doc, ns.event_id), vec!["a".repeat(64)]);
        assert_eq!(texts(&doc, ns.pubkey), vec!["b".repeat(64)]);
        assert_eq!(texts(&doc, ns.tag_e), vec!["d".repeat(64)]);
        assert_eq!(texts(&doc, ns.tag_p), vec!["ab".repeat(32)]);
    }

    #[test]
    fn coordinates_lowercase_the_pubkey_but_not_the_identifier() {
        let pk = "A".repeat(64);
        assert_eq!(
            normalize_coordinate(&format!("30023:{pk}:My-Slug")),
            format!("30023:{}:My-Slug", "a".repeat(64))
        );
    }

    #[test]
    fn titles_and_summaries_are_searchable() {
        let (_, ns) = NostrSchema::build();
        let ev = ev_with(
            30_023,
            "the body",
            vec![
                vec!["title", "Bitcoin Conference"],
                vec!["summary", "a talk about payments"],
            ],
        );
        let doc = ns.to_document(&ev, 0, None);
        assert_eq!(texts(&doc, ns.title), vec!["Bitcoin Conference"]);
        assert_eq!(texts(&doc, ns.summary), vec!["a talk about payments"]);
    }

    #[test]
    fn profile_fields_come_out_of_the_json_blob() {
        let (_, ns) = NostrSchema::build();
        let ev = ev_with(
            0,
            r#"{"name":"bob","display_name":"Bob T","about":"builder","nip05":"Bob@Example.com","website":"https://www.example.com/x"}"#,
            vec![],
        );
        let doc = ns.to_document(&ev, 0, None);
        assert_eq!(texts(&doc, ns.title), vec!["bob", "Bob T"]);
        assert_eq!(texts(&doc, ns.summary), vec!["builder"]);
        // One term, lowercased -- not three tokens around the @ and the dot.
        assert_eq!(texts(&doc, ns.nip05), vec!["bob@example.com"]);
        assert_eq!(texts(&doc, ns.tag_host), vec!["example.com"]);
    }

    #[test]
    fn nip05_underscore_form_is_the_bare_domain() {
        assert_eq!(normalize_nip05("_@example.com"), "example.com");
        assert_eq!(normalize_nip05("Bob@Example.com"), "bob@example.com");
    }

    #[test]
    fn geohash_indexes_every_prefix_so_containment_is_one_lookup() {
        assert_eq!(
            geohash_prefixes("u4pruy"),
            vec!["u4", "u4p", "u4pr", "u4pru", "u4pruy"]
        );
        // Over-precise input is truncated rather than exploding the term count.
        assert_eq!(geohash_prefixes("u4pruydqqvj").len(), 8);
        assert!(geohash_prefixes("").is_empty());
        assert!(geohash_prefixes("not a geohash!").is_empty());
    }

    #[test]
    fn urls_yield_a_searchable_host() {
        assert_eq!(
            url_host("https://www.Example.com/a/b?c=1"),
            Some("example.com".into())
        );
        assert_eq!(
            url_host("http://user:pw@host.example.org:8080/x"),
            Some("host.example.org".into())
        );
        assert_eq!(
            url_host("wss://relay.damus.io"),
            Some("relay.damus.io".into())
        );
        assert_eq!(url_host("not-a-url"), None);
        assert_eq!(url_host("/relative/path"), None);
    }

    #[test]
    fn document_has_tags_and_scoring_signals() {
        let (_, ns) = NostrSchema::build();
        let ev = ev_with(
            1,
            "gm #nostr",
            vec![
                vec!["t", "Nostr"],
                vec!["e", "deadbeef"],
                vec!["g", "u4pruydqqvj"],
                vec!["r", "https://example.com/post"],
            ],
        );
        let doc = ns.to_document(&ev, 2, Some("en"));
        assert_eq!(texts(&doc, ns.tag_t), vec!["nostr"]); // lowercased
        assert_eq!(texts(&doc, ns.tag_host), vec!["example.com"]);
        assert_eq!(texts(&doc, ns.lang), vec!["en"]);
        use tantivy::schema::Value;
        assert_eq!(doc.get_first(ns.wot_tier).unwrap().as_u64(), Some(2));
    }
}
