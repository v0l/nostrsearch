//! Concurrent writers against a shard cap far below the corpus span.
//!
//! Eviction used to drop the map's handle while another thread still held one.
//! The writer stayed alive, kept tantivy's directory lockfile, and the next
//! thread to want that month failed to open it with LockBusy -- losing every
//! event in the batch. Archive files each span many months, so with enough
//! reader threads this happened continuously.

use nostrsearch_core::event::NostrEvent;
use nostrsearch_indexer::shard_writer::{ShardManager, ShardWriterConfig};
use std::sync::Arc;

fn event(id: u64, created_at: u64) -> NostrEvent {
    NostrEvent {
        id: format!("{id:064x}"),
        pubkey: format!("{:064x}", id % 97),
        created_at,
        kind: 1,
        tags: Vec::new(),
        content: format!("event {id}"),
        sig: String::new(),
    }
}

/// Month starts from 2021-01 through 2025-12, as unix seconds.
fn month_starts() -> Vec<u64> {
    let mut out = Vec::new();
    // 2021-01-01T00:00:00Z, stepping ~30.4 days keeps us landing in distinct
    // months without pulling in a date library.
    let base = 1_609_459_200u64;
    for i in 0..60u64 {
        out.push(base + i * 2_629_746);
    }
    out
}

#[test]
fn concurrent_writers_survive_eviction() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = ShardWriterConfig {
        // Far below the number of months touched: forces constant eviction,
        // which is the condition the race needed.
        max_open_shards: 4,
        heap_bytes: 15 * 1024 * 1024,
        writer_threads: 1,
        ..ShardWriterConfig::default()
    };
    let mgr = Arc::new(ShardManager::new(dir.path().to_path_buf(), cfg));

    let months = month_starts();
    let threads: Vec<_> = (0..8u64)
        .map(|t| {
            let mgr = mgr.clone();
            let months = months.clone();
            std::thread::spawn(move || {
                let mut failures: Vec<String> = Vec::new();
                for round in 0..40u64 {
                    // Each thread walks the months on a different offset, so
                    // they collide on shards constantly rather than settling
                    // into disjoint working sets.
                    let m = months[((t * 7 + round) as usize) % months.len()];
                    let ev = event(t * 10_000 + round, m);
                    if let Err(e) = mgr.index_event(&ev) {
                        failures.push(e.to_string());
                    }
                }
                failures
            })
        })
        .collect();

    let mut failures: Vec<String> = Vec::new();
    for h in threads {
        failures.extend(h.join().unwrap());
    }

    assert!(
        failures.is_empty(),
        "indexing failed under concurrent eviction ({} errors), first: {}",
        failures.len(),
        failures.first().map(String::as_str).unwrap_or("")
    );

    mgr.commit_all().unwrap();
}
