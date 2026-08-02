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
