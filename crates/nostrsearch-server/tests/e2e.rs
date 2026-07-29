//! End-to-end: index real hole.v0l.io events → search them via the registry.
//!
//! Uses a small sample of the real dump (or synthetic fallback if absent).

use nostr_archive_cursor::NostrCursor;
use nostrsearch_core::event::NostrEvent;
use nostrsearch_core::query::SearchFilter;
use nostrsearch_core::scoring::ScoreWeights;
use nostrsearch_indexer::{ShardManager, ShardWriterConfig};
use nostrsearch_server::ShardRegistry;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

fn sample_path() -> Option<PathBuf> {
    let p = PathBuf::from("/tmp/e2e_sample.jsonl");
    p.exists().then_some(p)
}

#[test]
fn index_and_search_real_events() {
    let dir = tempfile::tempdir().unwrap();
    let index_root = dir.path().join("index");

    // --- ingest ---
    let mut indexed = 0usize;
    let mut kind1_content: Option<String> = None;
    {
        let cfg = ShardWriterConfig {
            commit_every_docs: 5_000,
            ..Default::default()
        };
        if let Some(path) = sample_path() {
            // drive ingest via nostr-archive-cursor over the sample's dir
            let dir = path.parent().unwrap().to_path_buf();
            let mgr = Arc::new(Mutex::new(ShardManager::new(&index_root, cfg)));
            let mgr_cb = mgr.clone();
            let seen = Arc::new(Mutex::new(0usize));
            let seen_cb = seen.clone();
            let k1 = Arc::new(Mutex::new(None::<String>));
            let k1_cb = k1.clone();
            NostrCursor::new(dir)
                .with_parallelism(1)
                .with_dedupe(false)
                .walk_with_chunked_sync(
                    move |events: Vec<nostr_archive_cursor::NostrEventBorrowed>| {
                        let mut m = mgr_cb.lock().unwrap();
                        for ev in &events {
                            let owned = NostrEvent {
                                id: ev.id.to_string(),
                                pubkey: ev.pubkey.to_string(),
                                created_at: ev.created_at,
                                kind: ev.kind as u16,
                                tags: ev.tags.iter().map(|t| t.iter().map(|s| s.to_string()).collect()).collect(),
                                content: ev.content.to_string(),
                                sig: ev.sig.to_string(),
                            };
                            if owned.kind == 1 && owned.content.split_whitespace().count() >= 3 {
                                let mut g = k1_cb.lock().unwrap();
                                if g.is_none() {
                                    *g = Some(owned.content.clone());
                                }
                            }
                            m.index_event(&owned).unwrap();
                            *seen_cb.lock().unwrap() += 1;
                        }
                    },
                    1000,
                );
            indexed = *seen.lock().unwrap();
            kind1_content = k1.lock().unwrap().clone();
            mgr.lock().unwrap().commit_all().unwrap();
        } else {
            // synthetic fallback
            let mut mgr = ShardManager::new(&index_root, cfg);
            for i in 0..1000u64 {
                let ev = NostrEvent {
                    id: format!("{:064x}", i),
                    pubkey: "b".repeat(64),
                    created_at: 1_700_000_000 + i,
                    kind: 1,
                    tags: vec![vec!["t".into(), "nostr".into()]],
                    content: format!("hello nostr note number {i} about bitcoin"),
                    sig: "c".repeat(128),
                };
                mgr.index_event(&ev).unwrap();
                indexed += 1;
            }
            kind1_content = Some("hello nostr note number 5 about bitcoin".into());
            mgr.commit_all().unwrap();
        }
    }
    assert!(indexed > 0, "indexed some events");
    eprintln!("indexed {indexed} events");

    // --- search ---
    let mut reg = ShardRegistry::open(&index_root, ScoreWeights::default()).unwrap();
    let stats = reg.stats();
    eprintln!("stats: {} docs across {} shards", stats.total_docs, stats.shard_count);
    assert_eq!(stats.total_docs as usize, indexed);

    // full-text search for a word we know exists in kind-1 content
    if let Some(content) = kind1_content {
        // pick a distinctive-ish term from the middle of the note
        let term = content
            .split_whitespace()
            .filter(|w| w.len() >= 5 && w.chars().all(|c| c.is_alphanumeric()))
            .last()
            .unwrap_or("nostr")
            .to_lowercase();
        eprintln!("searching for term: {term}");

        let filter = SearchFilter {
            search: Some(term.clone()),
            kinds: vec![1],
            limit: 10,
            ..Default::default()
        };
        let hits = reg.search(&filter).unwrap();
        eprintln!("full-text hits: {}", hits.len());
        assert!(!hits.is_empty(), "expected at least one full-text hit for '{term}'");
        assert!(hits.iter().all(|h| h.kind == 1));
    }

    // metadata-only: recent kind-1 notes, no text query
    let meta = SearchFilter {
        kinds: vec![1],
        limit: 20,
        ..Default::default()
    };
    let hits = reg.search(&meta).unwrap();
    eprintln!("metadata hits: {}", hits.len());
    assert!(!hits.is_empty());
    // ordered by created_at desc
    for w in hits.windows(2) {
        assert!(w[0].created_at >= w[1].created_at, "results sorted by recency");
    }
}
