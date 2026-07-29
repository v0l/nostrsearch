//! Per-shard index writers with no global lock and scheduled commits.
//!
//! Each monthly shard owns:
//!   - its own Tantivy `Index` (in `<root>/<YYYY-MM>/`)
//!   - its own `IndexWriter` (created once, reused)
//!   - a commit policy (commit every N docs or T seconds, whichever first)
//!
//! The [`ShardManager`] routes events to the right shard and lazily opens
//! shards on first write. Because Nostr archival dumps are roughly
//! time-ordered, only a handful of shards are "hot" at once; cold shards hold
//! no writer and can be finalized + offloaded.

use nostrsearch_core::event::NostrEvent;
use nostrsearch_core::schema::NostrSchema;
use nostrsearch_core::shard::ShardId;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tantivy::{Index, IndexWriter, TantivyDocument};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ShardError {
    #[error("tantivy: {0}")]
    Tantivy(#[from] tantivy::TantivyError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("open directory: {0}")]
    OpenDirectory(#[from] tantivy::directory::error::OpenDirectoryError),
}

/// Tunables for a shard writer.
#[derive(Debug, Clone)]
pub struct ShardWriterConfig {
    /// Tantivy writer heap per shard (bytes). Total memory ≈
    /// `heap_bytes × num_hot_shards`.
    pub heap_bytes: usize,
    /// Commit after this many docs per shard.
    pub commit_every_docs: u64,
    /// ...or after this much wall time, whichever comes first.
    pub commit_every: Duration,
    /// Number of writer threads per shard.
    pub writer_threads: usize,
}

impl Default for ShardWriterConfig {
    fn default() -> Self {
        Self {
            heap_bytes: 256 * 1_000_000, // 256 MB per hot shard
            commit_every_docs: 100_000,
            commit_every: Duration::from_secs(30),
            writer_threads: 2,
        }
    }
}

/// A single open shard: index + writer + commit bookkeeping.
struct OpenShard {
    index: Index,
    writer: IndexWriter,
    schema: NostrSchema,
    docs_since_commit: u64,
    last_commit: Instant,
    total_docs: u64,
}

impl OpenShard {
    fn open(dir: &Path, cfg: &ShardWriterConfig) -> Result<Self, ShardError> {
        std::fs::create_dir_all(dir)?;
        let (schema, ns) = NostrSchema::build();
        let index = Index::open_or_create(tantivy::directory::MmapDirectory::open(dir)?, schema)?;
        NostrSchema::register_tokenizers(&index);
        let writer = index.writer_with_num_threads(cfg.writer_threads, cfg.heap_bytes)?;
        Ok(Self {
            index,
            writer,
            schema: ns,
            docs_since_commit: 0,
            last_commit: Instant::now(),
            total_docs: 0,
        })
    }

    fn add(&mut self, doc: TantivyDocument) -> Result<(), ShardError> {
        self.writer.add_document(doc)?;
        self.docs_since_commit += 1;
        self.total_docs += 1;
        Ok(())
    }

    fn should_commit(&self, cfg: &ShardWriterConfig) -> bool {
        self.docs_since_commit >= cfg.commit_every_docs
            || (self.docs_since_commit > 0 && self.last_commit.elapsed() >= cfg.commit_every)
    }

    fn commit(&mut self) -> Result<(), ShardError> {
        if self.docs_since_commit == 0 {
            return Ok(());
        }
        self.writer.commit()?;
        self.docs_since_commit = 0;
        self.last_commit = Instant::now();
        Ok(())
    }
}

/// Routes events to per-month shards and manages their lifecycle.
pub struct ShardManager {
    root: PathBuf,
    cfg: ShardWriterConfig,
    shards: HashMap<ShardId, OpenShard>,
    /// WoT tier lookup hook — maps pubkey hex → tier. Pluggable so the WoT
    /// graph can be injected without the indexer depending on it.
    wot_lookup: Option<Box<dyn Fn(&str) -> u8 + Send + Sync>>,
}

impl ShardManager {
    pub fn new(root: impl Into<PathBuf>, cfg: ShardWriterConfig) -> Self {
        Self {
            root: root.into(),
            cfg,
            shards: HashMap::new(),
            wot_lookup: None,
        }
    }

    /// Inject a web-of-trust tier lookup.
    pub fn with_wot_lookup(mut self, f: impl Fn(&str) -> u8 + Send + Sync + 'static) -> Self {
        self.wot_lookup = Some(Box::new(f));
        self
    }

    fn shard_dir(&self, id: ShardId) -> PathBuf {
        self.root.join(id.name())
    }

    /// Open (or return) the shard for an event timestamp.
    fn shard_for(&mut self, ts: u64) -> Result<&mut OpenShard, ShardError> {
        let id = ShardId::from_timestamp(ts);
        if !self.shards.contains_key(&id) {
            let dir = self.shard_dir(id);
            tracing::info!(shard = %id, path = %dir.display(), "opening shard");
            let shard = OpenShard::open(&dir, &self.cfg)?;
            self.shards.insert(id, shard);
        }
        Ok(self.shards.get_mut(&id).unwrap())
    }

    /// Index one event. `deleted`/`superseded` are computed by the caller's
    /// mutability policy (default: both false — "index everything").
    pub fn index_event(&mut self, ev: &NostrEvent) -> Result<ShardId, ShardError> {
        self.index_event_with_flags(ev, false, false)
    }

    /// Index one event with explicit mutability flags.
    pub fn index_event_with_flags(
        &mut self,
        ev: &NostrEvent,
        deleted: bool,
        superseded: bool,
    ) -> Result<ShardId, ShardError> {
        let wot = self
            .wot_lookup
            .as_ref()
            .map(|f| f(&ev.pubkey))
            .unwrap_or(0);

        // route by timestamp first (borrows self mutably)
        let id = ShardId::from_timestamp(ev.created_at);
        let cfg = self.cfg.clone();
        let shard = self.shard_for(ev.created_at)?;
        let doc = shard.schema.to_document(ev, wot, deleted, superseded, None);
        shard.add(doc)?;

        if shard.should_commit(&cfg) {
            shard.commit()?;
        }
        Ok(id)
    }

    /// Flush any shard whose time-based commit deadline has passed.
    pub fn tick(&mut self) -> Result<(), ShardError> {
        for shard in self.shards.values_mut() {
            if shard.should_commit(&self.cfg) {
                shard.commit()?;
            }
        }
        Ok(())
    }

    /// Commit all open shards (call before shutdown or offload).
    pub fn commit_all(&mut self) -> Result<(), ShardError> {
        for (id, shard) in self.shards.iter_mut() {
            shard.commit()?;
            tracing::info!(shard = %id, docs = shard.total_docs, "committed shard");
        }
        Ok(())
    }

    /// Close and drop the writer for a shard, freeing its heap. The on-disk
    /// index remains searchable. Use to bound memory when many shards are open.
    pub fn close_shard(&mut self, id: ShardId) -> Result<(), ShardError> {
        if let Some(mut shard) = self.shards.remove(&id) {
            shard.commit()?;
            tracing::info!(shard = %id, "closed shard writer");
        }
        Ok(())
    }

    /// Total docs indexed across all open shards.
    pub fn total_docs(&self) -> u64 {
        self.shards.values().map(|s| s.total_docs).sum()
    }

    /// Number of currently-open shards.
    pub fn open_shard_count(&self) -> usize {
        self.shards.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(ts: u64, content: &str) -> NostrEvent {
        NostrEvent {
            id: format!("{:064x}", ts),
            pubkey: "b".repeat(64),
            created_at: ts,
            kind: 1,
            tags: vec![vec!["t".into(), "nostr".into()]],
            content: content.into(),
            sig: "c".repeat(128),
        }
    }

    #[test]
    fn routes_events_to_monthly_shards() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = ShardWriterConfig {
            commit_every_docs: 2,
            ..Default::default()
        };
        let mut mgr = ShardManager::new(dir.path(), cfg);

        // 2023-11-14 and 2023-12-01 land in different shards
        let nov = 1_700_000_000u64;
        let dec = 1_701_400_000u64;
        let s1 = mgr.index_event(&ev(nov, "gm")).unwrap();
        let s2 = mgr.index_event(&ev(dec, "gn")).unwrap();
        assert_ne!(s1, s2);
        assert_eq!(mgr.open_shard_count(), 2);

        mgr.commit_all().unwrap();
        assert_eq!(mgr.total_docs(), 2);

        // shard dirs exist on disk
        assert!(dir.path().join(s1.name()).exists());
        assert!(dir.path().join(s2.name()).exists());
    }

    #[test]
    fn commits_on_doc_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = ShardWriterConfig {
            commit_every_docs: 1, // commit every doc
            ..Default::default()
        };
        let mut mgr = ShardManager::new(dir.path(), cfg);
        let ts = 1_700_000_000u64;
        mgr.index_event(&ev(ts, "one")).unwrap();
        mgr.index_event(&ev(ts + 1, "two")).unwrap();
        // auto-committed; docs visible to a fresh reader
        let shard_dir = dir.path().join(ShardId::from_timestamp(ts).name());
        let (schema, _) = NostrSchema::build();
        let index = Index::open_in_dir(&shard_dir).unwrap_or_else(|_| {
            Index::open_or_create(tantivy::directory::MmapDirectory::open(&shard_dir).unwrap(), schema)
                .unwrap()
        });
        let reader = index.reader().unwrap();
        assert_eq!(reader.searcher().num_docs(), 2);
    }
}
