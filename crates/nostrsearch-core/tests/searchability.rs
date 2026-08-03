//! End-to-end searchability: index real documents, run planned queries, and
//! assert on what comes back.
//!
//! Every test here corresponds to a case from the schema review (issue #1)
//! that returned wrong or empty results against a live endpoint. The unit
//! tests next to each module check the pieces; these check that the pieces
//! compose into a query that actually matches the document it should.

use nostrsearch_core::event::NostrEvent;
use nostrsearch_core::query::{QueryPlanner, SearchFilter};
use nostrsearch_core::schema::NostrSchema;
use nostrsearch_core::shard::ShardId;
use tantivy::Index;
use tantivy::collector::TopDocs;

fn ev(kind: u16, content: &str, tags: Vec<Vec<&str>>) -> NostrEvent {
    NostrEvent {
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

/// Build a one-shard in-RAM index over `events`.
fn index_of(events: &[NostrEvent]) -> (Index, NostrSchema) {
    let (schema, ns) = NostrSchema::build();
    let index = Index::create_in_ram(schema);
    NostrSchema::register_tokenizers(&index);
    {
        let mut w = index.writer(15_000_000).unwrap();
        for e in events {
            let lang = if e.is_text_kind() {
                nostrsearch_core::lang::detect(&e.content)
            } else {
                None
            };
            w.add_document(ns.to_document(e, 0, lang)).unwrap();
        }
        w.commit().unwrap();
    }
    (index, ns)
}

/// Run a filter and return how many documents matched.
fn count(index: &Index, ns: &NostrSchema, filter: &SearchFilter) -> usize {
    let planner = QueryPlanner::new(ns, index, ShardId::new(2023, 1));
    let planned = planner.plan(filter).expect("plan");
    let searcher = index.reader().unwrap().searcher();
    searcher
        .search(&planned.query, &TopDocs::with_limit(100))
        .expect("search")
        .len()
}

fn search(q: &str) -> SearchFilter {
    SearchFilter {
        search: Some(q.to_string()),
        limit: 100,
        ..Default::default()
    }
}

// --- #2: CJK ---------------------------------------------------------------

#[test]
fn japanese_content_is_findable_by_substring() {
    let (index, ns) = index_of(&[
        ev(1, "今日はビットコインの会議です", vec![]),
        ev(1, "unrelated english note", vec![]),
    ]);
    // The bug: SimpleTokenizer emitted the whole sentence as one token, so
    // this returned 0.
    assert_eq!(count(&index, &ns, &search("ビットコイン")), 1);
    assert_eq!(count(&index, &ns, &search("会議")), 1);
    // A substring that is not in the text must still not match.
    assert_eq!(count(&index, &ns, &search("ラーメン")), 0);
}

#[test]
fn korean_and_chinese_are_findable_too() {
    let (index, ns) = index_of(&[
        ev(1, "비트코인은 돈이다", vec![]),
        ev(1, "比特币会议在东京举行", vec![]),
    ]);
    assert_eq!(count(&index, &ns, &search("비트코인")), 1);
    assert_eq!(count(&index, &ns, &search("比特币")), 1);
}

// --- #12: token filters ----------------------------------------------------

#[test]
fn accents_fold_so_cafe_matches_café() {
    let (index, ns) = index_of(&[ev(1, "meet me at the café tomorrow", vec![])]);
    assert_eq!(count(&index, &ns, &search("cafe")), 1);
    assert_eq!(count(&index, &ns, &search("café")), 1);
}

#[test]
fn giant_tokens_are_dropped_rather_than_indexed() {
    // A data-URI in a profile: previously up to 256 bytes per token, every one
    // of them a unique term in the dictionary.
    let blob = format!("data:image/png;base64,{}", "A".repeat(4096));
    let (index, ns) = index_of(&[ev(1, &format!("pic {blob}"), vec![])]);
    assert_eq!(count(&index, &ns, &search("pic")), 1);
    assert_eq!(count(&index, &ns, &search(&"A".repeat(4096))), 0);
}

// --- #10: default operator + fields ----------------------------------------

#[test]
fn bare_terms_are_anded_not_ored() {
    let (index, ns) = index_of(&[
        ev(1, "bitcoin conference in prague", vec![]),
        ev(1, "bitcoin price is up", vec![]),
        ev(1, "a conference about gardening", vec![]),
    ]);
    // OR would return all three; the user asking for both words wants one.
    assert_eq!(count(&index, &ns, &search("bitcoin conference")), 1);
    // Explicit OR is still available.
    assert_eq!(count(&index, &ns, &search("bitcoin OR conference")), 3);
}

// --- #6: titles, summaries, profiles ---------------------------------------

#[test]
fn long_form_titles_are_searchable() {
    let (index, ns) = index_of(&[ev(
        30_023,
        "the body text mentions nothing useful",
        vec![
            vec!["title", "Understanding Lightning Channels"],
            vec!["summary", "a practical guide to routing"],
        ],
    )]);
    assert_eq!(count(&index, &ns, &search("lightning channels")), 1);
    assert_eq!(count(&index, &ns, &search("routing")), 1);
}

#[test]
fn profiles_are_searchable_by_name_and_nip05() {
    let (index, ns) = index_of(&[ev(
        0,
        r#"{"name":"bob","display_name":"Bob the Builder","about":"I build things","nip05":"bob@example.com"}"#,
        vec![],
    )]);
    assert_eq!(count(&index, &ns, &search("bob")), 1);
    assert_eq!(count(&index, &ns, &search("builder")), 1);
    // Exact identifier lookup: one term, not three shredded pieces.
    let by_nip05 = SearchFilter {
        nip05: vec!["Bob@Example.com".into()],
        limit: 10,
        ..Default::default()
    };
    assert_eq!(count(&index, &ns, &by_nip05), 1);
    assert_eq!(count(&index, &ns, &search("nip05:bob@example.com")), 1);
}

// --- #7: geohash prefixes --------------------------------------------------

#[test]
fn geohash_prefix_matches_everything_in_the_cell() {
    let (index, ns) = index_of(&[
        ev(1, "at the venue", vec![vec!["g", "u4pruydqqvj"]]),
        ev(1, "nearby", vec![vec!["g", "u4pruyabcde"]]),
        ev(1, "far away", vec![vec!["g", "9q8yyzabcde"]]),
    ]);
    let in_cell = |g: &str| SearchFilter {
        tag_g: vec![g.to_string()],
        limit: 10,
        ..Default::default()
    };
    // Coarse cell contains both nearby events...
    assert_eq!(count(&index, &ns, &in_cell("u4pruy")), 2);
    // ...a finer one only the exact match...
    assert_eq!(count(&index, &ns, &in_cell("u4pruydqq")), 1);
    // ...and a different cell neither.
    assert_eq!(count(&index, &ns, &in_cell("9q8yy")), 1);
    // Same via the query grammar.
    assert_eq!(count(&index, &ns, &search("geo:u4pruy")), 2);
}

// --- #8: URL hosts ---------------------------------------------------------

#[test]
fn links_are_searchable_by_domain() {
    let (index, ns) = index_of(&[
        ev(
            1,
            "read this",
            vec![vec!["r", "https://www.example.com/a/b?x=1"]],
        ),
        ev(1, "and this", vec![vec!["r", "https://example.com/other"]]),
        ev(1, "not this", vec![vec!["r", "https://other.org/page"]]),
    ]);
    // `www.` and the path must not split the two example.com links apart.
    assert_eq!(count(&index, &ns, &search("site:example.com")), 2);
    assert_eq!(count(&index, &ns, &search("site:https://example.com/")), 2);
    assert_eq!(count(&index, &ns, &search("site:other.org")), 1);
}

// --- #9: hex + npub normalization ------------------------------------------

#[test]
fn uppercase_hex_matches_on_either_side() {
    let mut e = ev(1, "hello", vec![vec!["p", &"AB".repeat(32)]]);
    e.pubkey = "C".repeat(64);
    let (index, ns) = index_of(&[e]);

    let by_author = |a: &str| SearchFilter {
        authors: vec![a.to_string()],
        limit: 10,
        ..Default::default()
    };
    assert_eq!(count(&index, &ns, &by_author(&"C".repeat(64))), 1);
    assert_eq!(count(&index, &ns, &by_author(&"c".repeat(64))), 1);

    let by_p = |p: &str| SearchFilter {
        tag_p: vec![p.to_string()],
        limit: 10,
        ..Default::default()
    };
    assert_eq!(count(&index, &ns, &by_p(&"ab".repeat(32))), 1);
    assert_eq!(count(&index, &ns, &by_p(&"AB".repeat(32))), 1);
}

#[test]
fn author_accepts_npub_as_documented() {
    const NPUB: &str = "npub1sg6plzptd64u62a878hep2kev88swjh3tw00gjsfl8f237lmu63q0uf63m";
    const HEX: &str = "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2";

    let mut e = ev(1, "hello", vec![]);
    e.pubkey = HEX.to_string();
    let (index, ns) = index_of(&[e]);

    // The grammar has always documented `author:<hex|npub>`; the npub form
    // used to be pushed through raw and match nothing.
    assert_eq!(count(&index, &ns, &search(&format!("author:{NPUB}"))), 1);
    assert_eq!(count(&index, &ns, &search(&format!("author:{HEX}"))), 1);

    // Structured form too.
    let f = SearchFilter {
        authors: vec![NPUB.into()],
        limit: 10,
        ..Default::default()
    };
    assert_eq!(count(&index, &ns, &f), 1);
}

#[test]
fn an_unusable_author_matches_nothing_rather_than_everything() {
    let (index, ns) = index_of(&[ev(1, "hello", vec![]), ev(1, "world", vec![])]);
    // Dropping the clause would return the whole corpus for a filter that
    // explicitly restricted the author.
    let f = SearchFilter {
        authors: vec!["definitely-not-a-pubkey".into()],
        limit: 10,
        ..Default::default()
    };
    assert_eq!(count(&index, &ns, &f), 0);
    assert_eq!(
        count(&index, &ns, &search("author:definitely-not-a-key")),
        0
    );
}

// --- #3: lang --------------------------------------------------------------

#[test]
fn lang_filter_matches_detected_language() {
    let (index, ns) = index_of(&[
        ev(
            1,
            "just bought some sats on the exchange, feeling good about the price today",
            vec![],
        ),
        ev(
            1,
            "Ich habe heute einen langen Spaziergang im Park gemacht und Kaffee getrunken.",
            vec![],
        ),
    ]);
    let by_lang = |l: &str| SearchFilter {
        lang: Some(l.to_string()),
        limit: 10,
        ..Default::default()
    };
    // The bug: the writer passed `None` for every document, so this was 0.
    assert_eq!(count(&index, &ns, &by_lang("en")), 1);
    assert_eq!(count(&index, &ns, &by_lang("de")), 1);
    assert_eq!(count(&index, &ns, &by_lang("fr")), 0);
    // And through the query grammar, case-insensitively.
    assert_eq!(count(&index, &ns, &search("lang:EN")), 1);
}

// --- #1: version history without a `superseded` column ---------------------

#[test]
fn replaceable_version_history_is_a_query_not_a_column() {
    let author = "d".repeat(64);
    let mut v1 = ev(
        30_023,
        "first draft",
        vec![vec!["d", "my-article"], vec!["title", "My Article"]],
    );
    v1.pubkey = author.clone();
    v1.created_at = 1_700_000_000;
    let mut v2 = ev(
        30_023,
        "second draft",
        vec![vec!["d", "my-article"], vec!["title", "My Article"]],
    );
    v2.pubkey = author.clone();
    v2.created_at = 1_700_000_500;
    // An unrelated article by the same author.
    let mut other = ev(30_023, "different", vec![vec!["d", "other"]]);
    other.pubkey = author.clone();

    let (index, ns) = index_of(&[v1, v2, other]);

    // author + kind + #d is the whole version history; the newest is live and
    // the rest are superseded, by definition.
    let history = SearchFilter {
        authors: vec![author],
        kinds: vec![30_023],
        tag_d: vec!["my-article".into()],
        limit: 10,
        ..Default::default()
    };
    assert_eq!(count(&index, &ns, &history), 2);
}

#[test]
fn deletion_requests_stay_searchable_as_ordinary_events() {
    let target = "e".repeat(64);
    let (index, ns) = index_of(&[
        ev(1, "a note someone later regretted", vec![]),
        ev(5, "deleting", vec![vec!["e", &target]]),
    ]);
    // A caller that wants to apply deletions can find the requests; the
    // archive does not enact them.
    let deletions = SearchFilter {
        kinds: vec![5],
        tag_e: vec![target.to_uppercase()],
        limit: 10,
        ..Default::default()
    };
    assert_eq!(count(&index, &ns, &deletions), 1);
}

// --- #11: text kinds -------------------------------------------------------

#[test]
fn newly_classified_text_kinds_have_searchable_content() {
    let (index, ns) = index_of(&[
        ev(21, "a video about lightning routing", vec![]), // NIP-71
        ev(1222, "voice note transcript about nostr", vec![]), // NIP-A0
        ev(1617, "fix the memory leak in the reader", vec![]), // NIP-34 patch
        ev(31_923, "meetup at the pub", vec![]),           // NIP-52
    ]);
    assert_eq!(count(&index, &ns, &search("routing")), 1);
    assert_eq!(count(&index, &ns, &search("transcript")), 1);
    assert_eq!(count(&index, &ns, &search("memory leak")), 1);
    assert_eq!(count(&index, &ns, &search("meetup")), 1);
}

#[test]
fn opaque_payload_kinds_stay_out_of_the_term_dictionary() {
    // kind 30078 is routinely base64 / NIP-44 ciphertext.
    let (index, ns) = index_of(&[
        ev(30_078, "gardening tips application blob", vec![]),
        ev(4, "gardening tips encrypted", vec![]),
    ]);
    assert_eq!(count(&index, &ns, &search("gardening")), 0);
}
