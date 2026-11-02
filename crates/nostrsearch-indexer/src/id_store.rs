//! Persistent seen-event-id set, so a restarted backfill skips events that are
//! already committed to the index instead of duplicating them.
//!
//! Tantivy has no unique-key constraint on the write path, so without this a
//! restart must wipe the whole index. The store lives *inside the index root*
//! (`.dedupe/`, invisible to `YYYY-MM` shard discovery), so wiping the index
//! also wipes the seen-set — the two can never disagree.
//!
//! Crash consistency: callers must only [`flush`](IdStore::flush) ids *after*
//! committing the corresponding documents to Tantivy. An id that reaches the
//! store before its document is durable would become a permanent hole on the
//! next resume.

use rocksdb::{BlockBasedOptions, Cache, DB, Options, WriteBatch};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

const DEFAULT_CACHE_MB: usize = 3072;
const DEFAULT_MAX_OPEN_FILES: i32 = 1024;
const LOOKUP_CHUNK: usize = 256;
const LOOKUP_THREADS: usize = 32;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Lookups {
    pub single: u64,
    pub batches: u64,
    pub batched_ids: u64,
}

pub struct IdStore {
    db: DB,
    single: AtomicU64,
    batches: AtomicU64,
    batched_ids: AtomicU64,
}

impl IdStore {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let cache_mb = std::env::var("DEDUPE_CACHE_MB")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_CACHE_MB);
        Self::open_with_cache(path, cache_mb)
    }

    pub fn open_with_cache(path: &Path, cache_mb: usize) -> anyhow::Result<Self> {
        let mut bb = BlockBasedOptions::default();
        // Random 32-byte point lookups: a bloom filter makes the all-miss
        // first run cheap, a modest cache keeps hot index blocks resident.
        bb.set_bloom_filter(10.0, false);
        bb.set_block_cache(&Cache::new_lru_cache(cache_mb * 1024 * 1024));
        // Without this the bloom filters and index blocks this store depends
        // on sit outside the 64 MB cache, one set per open SST, unbounded --
        // so the cache size described what was bounded rather than what was
        // resident. The dedupe set is corpus-sized, so that is the difference
        // between 64 MB and gigabytes.
        bb.set_cache_index_and_filter_blocks(true);
        bb.set_pin_l0_filter_and_index_blocks_in_cache(true);
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_block_based_table_factory(&bb);
        opts.set_write_buffer_size(64 * 1024 * 1024);
        opts.set_max_background_jobs(2);
        // Bounded descriptor use: the default (-1) holds an fd per SST file,
        // which grows without limit as the dedupe set does.
        opts.set_max_open_files(DEFAULT_MAX_OPEN_FILES);
        Ok(Self {
            db: DB::open(&opts, path)?,
            single: AtomicU64::new(0),
            batches: AtomicU64::new(0),
            batched_ids: AtomicU64::new(0),
        })
    }

    /// Has this event id already been committed to the index?
    /// Membership for many ids at once.
    ///
    /// A rebuild asks this per event across hundreds of millions of them, and
    /// one-at-a-time point reads cap at roughly 13k/s: every lookup is a hit
    /// (the corpus already holds these events), and a hit is exactly what a
    /// bloom filter cannot make cheap. `multi_get` hands RocksDB the whole
    /// batch, so the block reads are issued together and sorted keys share
    /// index blocks.
    pub fn contains_batch(&self, ids: &[[u8; 32]]) -> Vec<bool> {
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.batched_ids.fetch_add(ids.len() as u64, Ordering::Relaxed);
        let mut order: Vec<usize> = (0..ids.len()).collect();
        order.sort_unstable_by_key(|&i| ids[i]);
        let mut found = vec![false; ids.len()];
        let threads = LOOKUP_THREADS.min(order.len().div_ceil(LOOKUP_CHUNK)).max(1);
        let per_thread = order.len().div_ceil(threads).max(1);
        let answers: Vec<Vec<bool>> = std::thread::scope(|scope| {
            order
                .chunks(per_thread)
                .map(|part| {
                    scope.spawn(move || {
                        let mut out = Vec::with_capacity(part.len());
                        for chunk in part.chunks(LOOKUP_CHUNK) {
                            out.extend(
                                self.db
                                    .multi_get(chunk.iter().map(|&i| ids[i].as_slice()))
                                    .into_iter()
                                    .map(|r| matches!(r, Ok(Some(_)))),
                            );
                        }
                        out
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|h| h.join().expect("dedupe lookup thread"))
                .collect()
        });
        for (&i, hit) in order.iter().zip(answers.into_iter().flatten()) {
            found[i] = hit;
        }
        found
    }

    pub fn contains(&self, id: &[u8; 32]) -> bool {
        self.single.fetch_add(1, Ordering::Relaxed);
        self.db
            .get_pinned(id.as_slice())
            .map(|v| v.is_some())
            .unwrap_or(false)
    }

    pub fn lookups(&self) -> Lookups {
        Lookups {
            single: self.single.load(Ordering::Relaxed),
            batches: self.batches.load(Ordering::Relaxed),
            batched_ids: self.batched_ids.load(Ordering::Relaxed),
        }
    }

    /// Durably record a batch of ids. Call only after the documents they refer
    /// to have been committed to Tantivy.
    pub fn flush<'a>(&self, ids: impl Iterator<Item = &'a [u8; 32]>) -> anyhow::Result<()> {
        let mut batch = WriteBatch::default();
        for id in ids {
            batch.put(id, []);
        }
        self.db.write(batch)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u64) -> [u8; 32] {
        let mut k = [0u8; 32];
        k[..8].copy_from_slice(&n.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes());
        k[8..16].copy_from_slice(&n.to_le_bytes());
        k
    }

    #[test]
    fn batch_answers_match_single_lookups_in_input_order() {
        let dir = tempfile::tempdir().unwrap();
        let store = IdStore::open_with_cache(dir.path(), 16).unwrap();
        let held: Vec<[u8; 32]> = (0..20_000).filter(|n| n % 3 == 0).map(id).collect();
        store.flush(held.iter()).unwrap();
        let probe: Vec<[u8; 32]> = (0..20_000).rev().map(id).collect();
        let got = store.contains_batch(&probe);
        assert_eq!(got.len(), probe.len());
        for (n, hit) in (0..20_000u64).rev().zip(got) {
            assert_eq!(hit, n % 3 == 0, "id {n}");
            assert_eq!(hit, store.contains(&id(n)));
        }
    }

    #[test]
    fn empty_batch_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let store = IdStore::open_with_cache(dir.path(), 16).unwrap();
        assert!(store.contains_batch(&[]).is_empty());
    }
}
