//! Shard readers must be bounded.
//!
//! They used to be cached in a plain `HashMap` that never evicted, so a
//! long-lived server accumulated one Tantivy `Index` — and one reload watcher
//! thread — per month the corpus covers, for the life of the process. On a
//! public node also serving archive downloads and relay websockets, that
//! contributed to descriptor exhaustion ("Too many open files").

use nostrsearch_core::query::SearchFilter;
use nostrsearch_core::scoring::ScoreWeights;
use nostrsearch_server::ShardRegistry;

fn tempdir(tag: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "nsreaders-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// Build `count` empty monthly shards on disk.
fn make_shards(root: &std::path::Path, count: u32) {
    let (schema, _) = nostrsearch_core::schema::NostrSchema::build();
    for i in 0..count {
        let (year, month) = (2020 + i / 12, (i % 12) + 1);
        let dir = root.join(format!("{year:04}-{month:02}"));
        std::fs::create_dir_all(&dir).unwrap();
        let index = tantivy::Index::create_in_dir(&dir, schema.clone()).unwrap();
        // A committed writer gives the shard a valid meta.json.
        let mut w: tantivy::IndexWriter = index.writer(15_000_000).unwrap();
        w.commit().unwrap();
    }
}

#[test]
fn open_readers_stay_under_the_cap() {
    // Explicit cap rather than the env var: tests run in parallel and would
    // otherwise race on the same process-wide variable.
    let root = tempdir("cap");
    make_shards(&root, 30);

    let reg = ShardRegistry::open_with_capacity(&root, ScoreWeights::default(), 8).unwrap();

    // `stats()` touches every shard on disk — previously this pinned all 30
    // readers open permanently.
    let stats = reg.stats();
    assert_eq!(stats.shard_count, 30, "all shards counted");
    assert!(
        stats.open_readers <= 8,
        "readers unbounded: {} open",
        stats.open_readers
    );
    assert_eq!(stats.max_open_readers, 8);

    // Repeated queries across the whole range keep it bounded too.
    for _ in 0..3 {
        let _ = reg.search(&SearchFilter {
            limit: 5,
            ..Default::default()
        });
    }
    assert!(
        reg.open_readers() <= 8,
        "readers grew after searching: {}",
        reg.open_readers()
    );
}

/// `/stats` is polled by the console every few seconds. Counting documents
/// must not open readers, or every poll evicts the shards searches are using.
#[test]
fn stats_counts_documents_without_opening_readers() {
    let root = tempdir("stats");
    make_shards(&root, 4);
    let dir = root.join("2020-02");
    let index = tantivy::Index::open_in_dir(&dir).unwrap();
    nostrsearch_core::schema::NostrSchema::register_tokenizers(&index);
    let (_, ns) = nostrsearch_core::schema::NostrSchema::build();
    let mut w: tantivy::IndexWriter = index.writer(15_000_000).unwrap();
    for i in 0..3u64 {
        let ev = nostrsearch_core::event::NostrEvent {
            id: format!("{i:064x}"),
            pubkey: "a".repeat(64),
            created_at: 1_580_601_600 + i,
            kind: 1,
            tags: vec![],
            content: "hello".into(),
            sig: String::new(),
        };
        w.add_document(ns.to_document(&ev, 0, None)).unwrap();
    }
    w.commit().unwrap();

    let reg = ShardRegistry::open_with_capacity(&root, ScoreWeights::default(), 8).unwrap();
    let stats = reg.stats();
    assert_eq!(stats.total_docs, 3);
    assert_eq!(stats.shard_count, 4);
    assert_eq!(reg.open_readers(), 0);
}

/// Eviction must not break correctness: a shard is transparently reopened.
#[test]
fn evicted_shards_are_reopened_on_demand() {
    let root = tempdir("reopen");
    make_shards(&root, 6);

    let reg = ShardRegistry::open_with_capacity(&root, ScoreWeights::default(), 2).unwrap();
    let first = reg.stats().total_docs;
    // Forces repeated open/evict cycles.
    let second = reg.stats().total_docs;

    assert_eq!(first, second, "doc totals must survive reader eviction");
    assert_eq!(reg.stats().shard_count, 6);
    assert!(reg.open_readers() <= 2);
}
