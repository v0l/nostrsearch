//! On-disk follow graph backed by RocksDB.
//!
//! The graph is the one piece of analysis state that scales with the *corpus*
//! rather than with the number of pubkeys: a few million contact lists at a few
//! hundred follows each is on the order of a billion edges. Held as
//! `HashMap<Pubkey, HashSet<Pubkey>>` that is ~64 bytes per edge of hash-table
//! overhead — tens of gigabytes, and it was previously stored *twice* (once in
//! the follow-graph analysis, once in pagerank).
//!
//! Here each author maps to a packed value with no per-edge overhead:
//!
//! ```text
//! key:   32-byte author pubkey
//! value: 8-byte created_at (LE) || N x 32-byte followed pubkey
//! ```
//!
//! That is 32 bytes per edge on disk, and only the RocksDB block cache is
//! resident. One [`GraphStore`] is shared by every analysis that needs the
//! graph, so there is a single copy.

use crate::types::{Hash32, Pubkey};
use anyhow::{Context, Result};
use rocksdb::{DB, Options};
use std::path::Path;
use std::sync::Arc;

/// A contact list: when it was published, and who it follows.
pub struct Follows {
    pub created_at: u64,
    pub follows: Vec<Pubkey>,
}

/// RocksDB-backed adjacency store.
pub struct GraphStore {
    db: DB,
}

impl std::fmt::Debug for GraphStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("GraphStore")
    }
}

impl GraphStore {
    /// Open (or create) the store at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        // Bound resident memory: the graph is far larger than RAM, so cap the
        // write buffers and let the block cache do the rest.
        opts.set_write_buffer_size(64 * 1024 * 1024);
        opts.set_max_write_buffer_number(3);
        opts.set_target_file_size_base(128 * 1024 * 1024);
        // Values are packed pubkeys; compression saves little and costs CPU on
        // a hot ingest path.
        opts.set_compression_type(rocksdb::DBCompressionType::None);
        // Bypass the page cache for flush and compaction. Under a cgroup the
        // OOM killer counts page cache, and dirty pages cannot be reclaimed
        // until written back, so a high-throughput writer can be killed while
        // its RSS is still small.
        opts.set_use_direct_io_for_flush_and_compaction(true);
        // RocksDB defaults to -1 (keep every SST open). At corpus scale the
        // graph is thousands of SSTs, so that alone can exhaust the process's
        // descriptor budget and surface as "Too many open files" somewhere
        // unrelated (an HTTP accept, a Tantivy open). Cap it and let RocksDB
        // use its table cache instead.
        opts.set_max_open_files(256);
        let db = DB::open(&opts, path.as_ref())
            .with_context(|| format!("opening graph store at {}", path.as_ref().display()))?;
        Ok(Self { db })
    }

    /// Fetch an author's contact list.
    pub fn get(&self, author: &Pubkey) -> Result<Option<Follows>> {
        let Some(raw) = self.db.get(author.as_bytes())? else {
            return Ok(None);
        };
        Ok(Some(decode(&raw)))
    }

    /// Only the timestamp, avoiding decoding the whole follow list — the hot
    /// path just needs to know whether an incoming contact list is newer.
    pub fn created_at(&self, author: &Pubkey) -> Result<Option<u64>> {
        let Some(raw) = self.db.get(author.as_bytes())? else {
            return Ok(None);
        };
        if raw.len() < 8 {
            return Ok(None);
        }
        let mut ts = [0u8; 8];
        ts.copy_from_slice(&raw[..8]);
        Ok(Some(u64::from_le_bytes(ts)))
    }

    /// Replace an author's contact list.
    /// Delete every adjacency record.
    ///
    /// Needed for reset to mean anything: contact lists are replaceable, so
    /// `FollowGraph` drops any event not newer than what it already holds. A
    /// reset that left the store populated made every replayed contact list
    /// look stale, and the graph could never be re-derived from the archive.
    pub fn clear(&self) -> Result<()> {
        let mut batch = rocksdb::WriteBatch::default();
        let mut n = 0u64;
        for kv in self.db.iterator(rocksdb::IteratorMode::Start) {
            let Ok((k, _)) = kv else { break };
            batch.delete(&k);
            n += 1;
            // Bounded batches: the graph holds millions of authors and one
            // batch for all of them is a large allocation plus a long stall.
            if batch.len() >= 10_000 {
                self.db.write(std::mem::take(&mut batch))?;
            }
        }
        self.db.write(batch)?;
        tracing::info!(authors = n, "cleared follow graph");
        Ok(())
    }

    pub fn put(&self, author: &Pubkey, created_at: u64, follows: &[Pubkey]) -> Result<()> {
        let mut buf = Vec::with_capacity(8 + follows.len() * 32);
        buf.extend_from_slice(&created_at.to_le_bytes());
        for f in follows {
            buf.extend_from_slice(f.as_bytes());
        }
        self.db.put(author.as_bytes(), buf)?;
        Ok(())
    }

    /// Stream every `(author, follows)` pair. Used by pagerank, which needs a
    /// full pass but never the whole graph resident at once.
    pub fn for_each<F: FnMut(Pubkey, &Follows)>(&self, mut f: F) {
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        for item in iter.flatten() {
            let (k, v) = item;
            if k.len() != 32 {
                continue;
            }
            let mut key = [0u8; 32];
            key.copy_from_slice(&k);
            let decoded = decode(&v);
            f(Hash32(key), &decoded);
        }
    }

    /// Number of stored contact lists (authors), not edges.
    pub fn author_count(&self) -> usize {
        self.db
            .property_int_value("rocksdb.estimate-num-keys")
            .ok()
            .flatten()
            .unwrap_or(0) as usize
    }
}

fn decode(raw: &[u8]) -> Follows {
    if raw.len() < 8 {
        return Follows {
            created_at: 0,
            follows: Vec::new(),
        };
    }
    let mut ts = [0u8; 8];
    ts.copy_from_slice(&raw[..8]);
    let body = &raw[8..];
    let mut follows = Vec::with_capacity(body.len() / 32);
    for chunk in body.chunks_exact(32) {
        let mut pk = [0u8; 32];
        pk.copy_from_slice(chunk);
        follows.push(Hash32(pk));
    }
    Follows {
        created_at: u64::from_le_bytes(ts),
        follows,
    }
}

/// Shared handle passed to analyses via [`crate::AttachCtx`].
pub type SharedGraph = Arc<GraphStore>;

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(tag: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "nsgraph-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        p
    }

    fn pk(b: u8) -> Pubkey {
        Hash32([b; 32])
    }

    #[test]
    fn roundtrip_and_iterate() {
        let dir = tmp("rt");
        let g = GraphStore::open(&dir).unwrap();
        g.put(&pk(1), 100, &[pk(9), pk(8)]).unwrap();
        g.put(&pk(2), 200, &[pk(9)]).unwrap();

        let a = g.get(&pk(1)).unwrap().unwrap();
        assert_eq!(a.created_at, 100);
        assert_eq!(a.follows, vec![pk(9), pk(8)]);
        assert_eq!(g.created_at(&pk(2)).unwrap(), Some(200));
        assert!(g.get(&pk(7)).unwrap().is_none());

        let mut edges = 0;
        g.for_each(|_, f| edges += f.follows.len());
        assert_eq!(edges, 3);

        drop(g);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn put_replaces_previous_list() {
        let dir = tmp("replace");
        let g = GraphStore::open(&dir).unwrap();
        g.put(&pk(1), 100, &[pk(9), pk(8)]).unwrap();
        g.put(&pk(1), 200, &[pk(9)]).unwrap();
        let a = g.get(&pk(1)).unwrap().unwrap();
        assert_eq!(a.created_at, 200);
        assert_eq!(a.follows, vec![pk(9)]);
        drop(g);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
