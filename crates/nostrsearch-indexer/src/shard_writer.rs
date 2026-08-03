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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
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
    /// Number of writer threads per shard. Total indexing threads is this
    /// times the number of open shards, so keep it at 1 unless very few shards
    /// are open.
    pub writer_threads: usize,
    /// Maximum shards held open at once. Each open shard costs `heap_bytes` of
    /// writer heap, so the product bounds writer memory. When the cap is
    /// exceeded the least-recently-written shard is committed and closed, and
    /// reopens transparently if more events route to it.
    ///
    /// Set this at or above the number of months the corpus spans. Eviction is
    /// a safety valve, not a steady state: dumps are often not date-ordered, so
    /// a cap below the corpus span makes every few events evict a shard that is
    /// immediately needed again, and each eviction costs a commit + fsync.
    pub max_open_shards: usize,
}

impl ShardWriterConfig {
    /// Total writer arena across every open shard.
    ///
    /// The two knobs *multiply*, which is the whole hazard: `heap_bytes` reads
    /// like a per-process budget but is charged per open shard, so raising
    /// either one scales total memory. 512 MB with 64 shards is 32 GB.
    pub fn total_heap_bytes(&self) -> usize {
        self.heap_bytes.saturating_mul(self.max_open_shards.max(1))
    }

    /// Shrink the per-shard heap so the total fits in `budget_bytes`, keeping
    /// the shard count (which is tuned for the corpus layout, not memory).
    ///
    /// Returns the previous per-shard heap when it had to be reduced. A slower
    /// ingest is strictly better than one the OOM killer stops.
    pub fn fit_to_budget(&mut self, budget_bytes: usize) -> Option<usize> {
        if self.total_heap_bytes() <= budget_bytes {
            return None;
        }
        let shards = self.max_open_shards.max(1);
        let previous = self.heap_bytes;
        // Tantivy refuses arenas below a few MB; keep a sane floor.
        self.heap_bytes = (budget_bytes / shards).max(15 * 1_000_000);
        Some(previous)
    }
}

impl Default for ShardWriterConfig {
    fn default() -> Self {
        Self {
            // Per *open shard*, so this multiplies by `max_open_shards`:
            // 64 x 64 MB is ~4 GB. Raise it only alongside that cap.
            heap_bytes: 64 * 1_000_000, // 64 MB per hot shard
            commit_every_docs: 100_000,
            commit_every: Duration::from_secs(30),
            // One thread per shard: parallelism comes from having many shards
            // open, and each extra writer thread adds arena and merge overhead
            // on top of the nominal heap budget. At 44 open shards, two
            // threads each meant 88 indexing threads.
            writer_threads: 1,
            // Archives are not necessarily date-ordered: a dump directory can
            // interleave every month of the corpus, in which case a small cap
            // evicts a shard that is needed again immediately and every
            // eviction pays a full commit + fsync. Hold a whole multi-year
            // corpus open instead — 64 x 64 MB is ~4 GB.
            max_open_shards: 64,
        }
    }
}

/// A single open shard: index + writer + commit bookkeeping.
///
/// Everything here is shared rather than owned exclusively, so several threads
/// can index into the same shard at once. Tantivy's `add_document` takes
/// `&self` and hands the document to the writer's own threads, so the only
/// thing needing exclusivity is `commit`.
///
/// This matters because the alternative -- one lock around the whole pipeline
/// -- serialized every event in the corpus onto a single core while dozens of
/// reader threads sat blocked on it, and the disk went idle.
struct OpenShard {
    /// Value of `ShardManager::tick` when this shard was last written to.
    last_used: AtomicU64,
    #[allow(dead_code)]
    index: Index,
    /// Read to add, write to commit.
    writer: RwLock<IndexWriter>,
    schema: NostrSchema,
    docs_since_commit: AtomicU64,
    /// Millis since the manager's epoch, so the commit deadline needs no lock.
    last_commit_ms: AtomicU64,
    total_docs: AtomicU64,
}

impl OpenShard {
    fn open(dir: &Path, cfg: &ShardWriterConfig) -> Result<Self, ShardError> {
        std::fs::create_dir_all(dir)?;
        let (schema, ns) = NostrSchema::build();
        let index = Index::open_or_create(tantivy::directory::MmapDirectory::open(dir)?, schema)?;
        NostrSchema::register_tokenizers(&index);
        let writer = index.writer_with_num_threads(cfg.writer_threads, cfg.heap_bytes)?;
        Ok(Self {
            last_used: AtomicU64::new(0),
            index,
            writer: RwLock::new(writer),
            schema: ns,
            docs_since_commit: AtomicU64::new(0),
            last_commit_ms: AtomicU64::new(now_ms()),
            total_docs: AtomicU64::new(0),
        })
    }

    /// Queue a document. Concurrent with other adds to the same shard.
    fn add(&self, doc: TantivyDocument) -> Result<(), ShardError> {
        // A read guard: `add_document` only needs `&self`, and Tantivy's own
        // writer threads do the indexing. Taking a write guard here would
        // reintroduce the serialization this exists to remove.
        self.writer.read().unwrap().add_document(doc)?;
        self.docs_since_commit.fetch_add(1, Ordering::Relaxed);
        self.total_docs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn should_commit(&self, cfg: &ShardWriterConfig) -> bool {
        let n = self.docs_since_commit.load(Ordering::Relaxed);
        n >= cfg.commit_every_docs
            || (n > 0
                && now_ms().saturating_sub(self.last_commit_ms.load(Ordering::Relaxed))
                    >= cfg.commit_every.as_millis() as u64)
    }

    /// Commit. Exclusive: Tantivy's `commit` needs `&mut`, and adds must not
    /// interleave with it.
    fn commit(&self) -> Result<(), ShardError> {
        let mut w = self.writer.write().unwrap();
        // Re-check under the guard: another thread may have committed while
        // this one waited, and an empty commit still costs an fsync.
        if self.docs_since_commit.load(Ordering::Relaxed) == 0 {
            return Ok(());
        }
        w.commit()?;
        self.docs_since_commit.store(0, Ordering::Relaxed);
        self.last_commit_ms.store(now_ms(), Ordering::Relaxed);
        Ok(())
    }
}

/// Milliseconds since process start, for lock-free commit deadlines.
fn now_ms() -> u64 {
    use std::sync::OnceLock;
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    EPOCH.get_or_init(Instant::now).elapsed().as_millis() as u64
}

/// Routes events to per-month shards and manages their lifecycle.
pub struct ShardManager {
    root: PathBuf,
    cfg: ShardWriterConfig,
    /// Open shards, each independently writable.
    ///
    /// The map lock is held only to find or open a shard -- never across the
    /// indexing itself, which is the expensive part and now runs concurrently
    /// across threads and shards.
    shards: Mutex<HashMap<ShardId, Arc<OpenShard>>>,
    /// Monotonic counter used to pick the least-recently-written shard.
    tick: AtomicU64,
    /// Total evictions, used to detect thrashing.
    evictions: AtomicU64,
    /// WoT tier lookup hook — maps pubkey hex → tier. Pluggable so the WoT
    /// graph can be injected without the indexer depending on it.
    wot_lookup: Option<Box<dyn Fn(&str) -> u8 + Send + Sync>>,
}

impl ShardManager {
    pub fn new(root: impl Into<PathBuf>, cfg: ShardWriterConfig) -> Self {
        Self {
            root: root.into(),
            cfg,
            shards: Mutex::new(HashMap::new()),
            tick: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
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

    /// Find or open the shard for `ts`, returning a handle usable without the
    /// map lock.
    fn shard_for(&self, ts: u64) -> Result<Arc<OpenShard>, ShardError> {
        let id = ShardId::from_timestamp(ts);
        let tick = self.tick.fetch_add(1, Ordering::Relaxed) + 1;
        {
            let map = self.shards.lock().unwrap();
            if let Some(s) = map.get(&id) {
                s.last_used.store(tick, Ordering::Relaxed);
                return Ok(s.clone());
            }
        }
        // Not open. Evict outside the map lock where possible, then insert;
        // another thread may have won the race, in which case take theirs.
        self.evict_if_needed()?;
        let dir = self.shard_dir(id);
        let mut map = self.shards.lock().unwrap();
        if let Some(s) = map.get(&id) {
            s.last_used.store(tick, Ordering::Relaxed);
            return Ok(s.clone());
        }
        tracing::info!(shard = %id, path = %dir.display(), "opening shard");
        let shard = Arc::new(OpenShard::open(&dir, &self.cfg)?);
        shard.last_used.store(tick, Ordering::Relaxed);
        map.insert(id, shard.clone());
        Ok(shard)
    }

    /// Commit and close the least-recently-written shard while over the cap.
    fn evict_if_needed(&self) -> Result<(), ShardError> {
        let cap = self.cfg.max_open_shards.max(1);
        // An eviction can decline to close -- the shard may have been taken
        // between choosing it and closing it -- so this cannot loop until the
        // map is under the cap or it would spin whenever every shard is busy.
        for _ in 0..cap.saturating_add(1) {
            // Pick a victim under the map lock, then release it: closing a
            // shard commits and fsyncs, which must not block every other
            // thread's shard lookup.
            let victim = {
                let map = self.shards.lock().unwrap();
                if map.len() < cap {
                    return Ok(());
                }
                // Only shards nobody is writing to. A handle held by another
                // thread keeps the writer -- and so tantivy's directory
                // lockfile -- alive after the map drops it, and the next
                // thread to want that month then fails to open it with
                // LockBusy and loses its events. Being briefly over the cap is
                // the harmless side of that trade.
                map.iter()
                    .filter(|(_, s)| Arc::strong_count(s) == 1)
                    .min_by_key(|(_, s)| s.last_used.load(Ordering::Relaxed))
                    .map(|(id, _)| *id)
            };
            // Every open shard is in use: nothing can be evicted safely.
            let Some(id) = victim else { return Ok(()) };

            let n = self.evictions.fetch_add(1, Ordering::Relaxed) + 1;
            // Sustained eviction means the cap is below the corpus span and
            // every eviction is paying a commit for nothing.
            if n == 100 {
                tracing::warn!(
                    max_open_shards = cap,
                    "evicting shard writers repeatedly — the archive spans more months \
                     than the open-shard cap, so each eviction pays a commit and is \
                     immediately reopened; raise --max-open-shards to the number of \
                     months in the corpus"
                );
            }
            tracing::debug!(shard = %id, "evicting shard writer");
            self.close_shard(id)?;
        }
        Ok(())
    }

    /// Index one event.
    pub fn index_event(&self, ev: &NostrEvent) -> Result<ShardId, ShardError> {
        let wot = self.wot_lookup.as_ref().map(|f| f(&ev.pubkey)).unwrap_or(0);

        // Language is detected here, not in the schema, because it is a
        // property of the *text* and only text kinds have any. It used to be
        // passed as an unconditional `None`, which left `lang:` matching
        // nothing at all.
        let lang = if ev.is_text_kind() {
            nostrsearch_core::lang::detect(&ev.content)
        } else {
            None
        };

        let id = ShardId::from_timestamp(ev.created_at);
        let shard = self.shard_for(ev.created_at)?;
        let doc = shard.schema.to_document(ev, wot, lang);
        // Outside every map lock: this is the expensive part, and holding a
        // lock across it is what pinned the whole ingest to one core.
        shard.add(doc)?;

        if shard.should_commit(&self.cfg) {
            shard.commit()?;
        }
        Ok(id)
    }

    /// Flush any shard whose time-based commit deadline has passed.
    pub fn tick(&self) -> Result<(), ShardError> {
        // Snapshot the handles, then commit without the map lock.
        let shards: Vec<Arc<OpenShard>> = self.shards.lock().unwrap().values().cloned().collect();
        for shard in shards {
            if shard.should_commit(&self.cfg) {
                shard.commit()?;
            }
        }
        Ok(())
    }

    /// Commit all open shards (call before shutdown or offload).
    pub fn commit_all(&self) -> Result<(), ShardError> {
        // Commit shards in parallel: each holds an independent writer and the
        // cost is dominated by per-shard serialization + fsync. Committing ~90
        // shards sequentially stalled the whole pipeline for 15-20s at every
        // checkpoint (the caller holds the pipeline lock); in parallel the
        // stall is the slowest single shard.
        let shards: Vec<(ShardId, Arc<OpenShard>)> = self
            .shards
            .lock()
            .unwrap()
            .iter()
            .map(|(id, s)| (*id, s.clone()))
            .collect();
        let results: Vec<(ShardId, u64, Result<(), ShardError>)> = std::thread::scope(|scope| {
            let handles: Vec<_> = shards
                .iter()
                .map(|(id, shard)| {
                    let id = *id;
                    let shard = shard.clone();
                    scope.spawn(move || {
                        let dirty = shard.docs_since_commit.load(Ordering::Relaxed) > 0;
                        let res = shard.commit();
                        let total = shard.total_docs.load(Ordering::Relaxed);
                        (id, if dirty { total } else { 0 }, res)
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        for (id, docs, res) in results {
            res?;
            if docs > 0 {
                tracing::info!(shard = %id, docs, "committed shard");
            }
        }
        Ok(())
    }

    /// Close and drop the writer for a shard, freeing its heap. The on-disk
    /// index remains searchable. Use to bound memory when many shards are open.
    pub fn close_shard(&self, id: ShardId) -> Result<(), ShardError> {
        // Commit before removing, holding only a handle: the fsync is the slow
        // part and must not block lookups of other shards.
        let shard = match self.shards.lock().unwrap().get(&id) {
            Some(s) => s.clone(),
            None => return Ok(()),
        };
        shard.commit()?;

        // Remove and drop under one lock. Dropping outside it leaves a window
        // where the shard is gone from the map but its writer still holds
        // tantivy's lockfile, and a thread that looks up that month in the
        // window will miss, try to open the directory, and fail with LockBusy.
        //
        // `shard` is a second handle, so release it first and re-check the
        // count: another thread may have taken one since the commit, and
        // dropping the map's handle then would not close the writer at all.
        drop(shard);
        let mut map = self.shards.lock().unwrap();
        match map.get(&id) {
            Some(s) if Arc::strong_count(s) == 1 => {
                drop(map.remove(&id));
                tracing::debug!(shard = %id, "closed shard writer");
            }
            _ => return Ok(()),
        }
        Ok(())
    }

    /// Total docs indexed across all open shards.
    pub fn total_docs(&self) -> u64 {
        self.shards
            .lock()
            .unwrap()
            .values()
            .map(|s| s.total_docs.load(Ordering::Relaxed))
            .sum()
    }

    /// Number of currently-open shards.
    pub fn open_shard_count(&self) -> usize {
        self.shards.lock().unwrap().len()
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
            Index::open_or_create(
                tantivy::directory::MmapDirectory::open(&shard_dir).unwrap(),
                schema,
            )
            .unwrap()
        });
        let reader = index.reader().unwrap();
        assert_eq!(reader.searcher().num_docs(), 2);
    }
}

#[cfg(test)]
mod eviction_tests {
    use super::*;

    fn ev_at(year: i32, month: u32, id: &str) -> NostrEvent {
        // first second of the given month
        let ts = chrono::NaiveDate::from_ymd_opt(year, month, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp() as u64;
        NostrEvent {
            id: id.into(),
            pubkey: "b".repeat(64),
            created_at: ts,
            kind: 1,
            tags: vec![],
            content: "hello".into(),
            sig: "c".repeat(128),
        }
    }

    /// Writing across many months must not hold every shard writer open —
    /// each costs `heap_bytes`, so an unbounded backfill over a multi-year
    /// corpus exhausts memory and gets OOM-killed.
    #[test]
    fn open_shards_are_capped_and_evicted() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = ShardWriterConfig {
            heap_bytes: 15 * 1_000_000, // tantivy minimum-ish, keep test light
            max_open_shards: 3,
            writer_threads: 1,
            ..Default::default()
        };
        let mut mgr = ShardManager::new(dir.path(), cfg);

        // 12 distinct monthly shards
        for m in 1..=12u32 {
            mgr.index_event(&ev_at(2024, m, &format!("{m:064x}")))
                .expect("index");
            assert!(
                mgr.open_shard_count() <= 3,
                "open shards {} exceeded cap after month {}",
                mgr.open_shard_count(),
                m
            );
        }

        // Evicted shards must still hold their data.
        mgr.commit_all().unwrap();
        let mut found = 0;
        for m in 1..=12u32 {
            let d = dir.path().join(format!("2024-{m:02}"));
            if d.exists() {
                found += 1;
            }
        }
        assert_eq!(found, 12, "every month should have been written to disk");
    }
}

#[cfg(test)]
mod heap_budget_tests {
    use super::*;

    #[test]
    fn per_shard_heap_multiplies_by_shard_count() {
        let cfg = ShardWriterConfig {
            heap_bytes: 512 * 1_000_000,
            max_open_shards: 64,
            ..Default::default()
        };
        // The trap that OOM-killed ingest: two reasonable-looking knobs.
        assert_eq!(cfg.total_heap_bytes(), 32_768_000_000);
    }

    #[test]
    fn fitting_to_a_budget_keeps_shard_count_and_shrinks_heap() {
        let mut cfg = ShardWriterConfig {
            heap_bytes: 512 * 1_000_000,
            max_open_shards: 64,
            ..Default::default()
        };
        // Half of a 24 GiB limit.
        let budget = 12_000_000_000usize;
        let was = cfg.fit_to_budget(budget).expect("should have reduced");

        assert_eq!(was, 512 * 1_000_000);
        assert_eq!(cfg.max_open_shards, 64, "shard count is tuned for layout");
        assert!(cfg.total_heap_bytes() <= budget, "still over budget");
        assert!(
            cfg.heap_bytes >= 15_000_000,
            "must stay above tantivy's floor"
        );
    }

    #[test]
    fn a_config_already_within_budget_is_untouched() {
        let mut cfg = ShardWriterConfig::default();
        let before = cfg.heap_bytes;
        assert!(cfg.fit_to_budget(100_000_000_000).is_none());
        assert_eq!(cfg.heap_bytes, before);
    }

    #[test]
    fn the_default_matches_the_documented_budget() {
        // ~4 GB, the figure the k8s manifest and docs quote.
        let cfg = ShardWriterConfig::default();
        assert_eq!(cfg.total_heap_bytes(), 4_096_000_000);
    }
}
