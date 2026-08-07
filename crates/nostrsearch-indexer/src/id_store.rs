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

pub struct IdStore {
    db: DB,
}

impl IdStore {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let mut bb = BlockBasedOptions::default();
        // Random 32-byte point lookups: a bloom filter makes the all-miss
        // first run cheap, a modest cache keeps hot index blocks resident.
        bb.set_bloom_filter(10.0, false);
        bb.set_block_cache(&Cache::new_lru_cache(64 * 1024 * 1024));
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
        opts.set_max_open_files(256);
        Ok(Self {
            db: DB::open(&opts, path)?,
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
        self.db
            .multi_get(ids.iter().map(|i| i.as_slice()))
            .into_iter()
            .map(|r| matches!(r, Ok(Some(_))))
            .collect()
    }

    pub fn contains(&self, id: &[u8; 32]) -> bool {
        self.db
            .get_pinned(id.as_slice())
            .map(|v| v.is_some())
            .unwrap_or(false)
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
