//! Read-side shard registry: opens monthly indices and fans queries out.

use nostrsearch_core::query::{QueryPlanner, SearchFilter};
use nostrsearch_core::schema::NostrSchema;
use nostrsearch_core::scoring::{CompositeCollector, ScoreWeights, ScoredDoc};
use nostrsearch_core::shard::ShardId;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tantivy::collector::TopDocs;
use tantivy::schema::Value;
use tantivy::{Index, IndexReader, TantivyDocument};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("tantivy: {0}")]
    Tantivy(#[from] tantivy::TantivyError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("query: {0}")]
    Query(#[from] tantivy::query::QueryParserError),
}

/// One open, searchable shard (read-only).
struct ShardReader {
    id: ShardId,
    index: Index,
    reader: IndexReader,
    schema: NostrSchema,
}

/// A hydrated search hit.
///
/// The index stores what it needs to *rank* an event, not the event itself:
/// `tags` and `sig` are not in the index at all, so these fields alone cannot
/// reconstruct anything a client could verify. [`event`](Self::event) carries
/// the complete signed event when the node has an archive to fetch it from --
/// which is what a NIP-50 relay has to return -- and is absent on a node with
/// no archive attached.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SearchHit {
    pub event_id: String,
    pub pubkey: String,
    pub kind: u16,
    pub created_at: u64,
    pub score: f32,
    pub content: String,
    /// Title / display name, when the event has one (long-form posts,
    /// listings, calendar entries, profiles).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Detected language (ISO 639-1), when detection was confident.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lang: Option<String>,
    pub shard: String,
    /// The complete signed event, hydrated by id from the archive.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<serde_json::Value>,
}

/// Maximum shard readers held open at once, before the least-recently-used is
/// evicted (`MAX_OPEN_SHARD_READERS`).
///
/// Readers were previously cached without bound: every month a query touched
/// stayed open for the life of the process, and each one costs a Tantivy
/// `Index` plus a `ReloadPolicy::OnCommitWithDelay` watcher thread. Over a
/// multi-year corpus that is hundreds of threads and a steadily growing
/// resource footprint on a long-lived server.
fn max_open_readers() -> usize {
    std::env::var("MAX_OPEN_SHARD_READERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(48)
}

/// The shard registry: lazily opens indices under a root and answers queries
/// by fanning out to the pruned shard set and merging top-k.
///
/// Open readers are an LRU cache, not a permanent map — see
/// [`max_open_readers`]. Eviction only drops this registry's handle; an
/// `Arc<ShardReader>` already handed to an in-flight query stays alive until
/// that query finishes.
pub struct ShardRegistry {
    root: PathBuf,
    shards: HashMap<ShardId, Arc<ShardReader>>,
    /// Monotonic clock for LRU ordering: shard -> tick of last use.
    used: HashMap<ShardId, u64>,
    clock: u64,
    max_open: usize,
    earliest: ShardId,
    weights: ScoreWeights,
}

impl ShardRegistry {
    /// Open a registry over a root dir, discovering existing `YYYY-MM` shards.
    /// Reader capacity comes from `MAX_OPEN_SHARD_READERS`.
    pub fn open(root: impl Into<PathBuf>, weights: ScoreWeights) -> Result<Self, RegistryError> {
        Self::open_with_capacity(root, weights, max_open_readers())
    }

    /// As [`open`](Self::open) with an explicit reader cap, bypassing the
    /// environment (tests, or a caller that knows its own descriptor budget).
    pub fn open_with_capacity(
        root: impl Into<PathBuf>,
        weights: ScoreWeights,
        max_open: usize,
    ) -> Result<Self, RegistryError> {
        let root = root.into();
        let mut reg = Self {
            root,
            shards: HashMap::new(),
            used: HashMap::new(),
            clock: 0,
            max_open: max_open.max(1),
            earliest: ShardId::new(2020, 11), // nostr genesis-ish default
            weights,
        };
        reg.discover()?;
        Ok(reg)
    }

    /// Scan the root for shard directories and record the earliest.
    pub fn discover(&mut self) -> Result<(), RegistryError> {
        if !self.root.exists() {
            return Ok(());
        }
        let mut earliest: Option<ShardId> = None;
        for entry in std::fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some(id) = ShardId::parse(&name) {
                earliest = Some(match earliest {
                    Some(e) => e.min(id),
                    None => id,
                });
            }
        }
        if let Some(e) = earliest {
            self.earliest = e;
        }
        Ok(())
    }

    /// Number of shard readers currently held open.
    pub fn open_readers(&self) -> usize {
        self.shards.len()
    }

    /// Note a use of `id` for LRU ordering.
    fn touch(&mut self, id: ShardId) {
        self.clock += 1;
        self.used.insert(id, self.clock);
    }

    /// Drop least-recently-used readers until at most `max_open` remain.
    fn evict_to_capacity(&mut self) {
        while self.shards.len() > self.max_open {
            let Some(victim) = self
                .used
                .iter()
                .filter(|(id, _)| self.shards.contains_key(id))
                .min_by_key(|(_, tick)| **tick)
                .map(|(id, _)| *id)
            else {
                break;
            };
            self.shards.remove(&victim);
            self.used.remove(&victim);
            tracing::debug!(shard = %victim.name(), "evicted shard reader (LRU)");
        }
    }

    /// Open (or return cached) a shard reader.
    fn shard(&mut self, id: ShardId) -> Option<Arc<ShardReader>> {
        if let Some(s) = self.shards.get(&id).cloned() {
            self.touch(id);
            return Some(s);
        }
        let dir = self.root.join(id.name());
        if !dir.exists() {
            return None;
        }
        let index = Index::open_in_dir(&dir).ok()?;
        NostrSchema::register_tokenizers(&index);
        let (_, ns) = NostrSchema::build();
        let reader = index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::OnCommitWithDelay)
            .try_into()
            .ok()?;
        let sr = Arc::new(ShardReader {
            id,
            index,
            reader,
            schema: ns,
        });
        self.shards.insert(id, sr.clone());
        self.touch(id);
        self.evict_to_capacity();
        Some(sr)
    }

    /// Execute a search filter: plan, prune shards, fan out, merge, hydrate.
    pub fn search(&mut self, filter: &SearchFilter) -> Result<Vec<SearchHit>, RegistryError> {
        // Plan against any open shard's index (schema is identical across
        // shards, so the QueryParser only needs one). If no shard exists yet,
        // there is nothing to search.
        // A registry opened against an empty index has `earliest` at its
        // default, so no anchor exists yet. Shards created afterwards (by a
        // writer in this process or another) would then never be found. Re-run
        // discovery before giving up so a live node picks up the first shard
        // without a restart.
        let anchor = self
            .shards
            .values()
            .next()
            .cloned()
            .or_else(|| self.shard(self.earliest))
            .or_else(|| {
                let _ = self.discover();
                self.shard(self.earliest)
            });
        let Some(anchor) = anchor else {
            return Ok(Vec::new());
        };

        let planner = QueryPlanner::new(&anchor.schema, &anchor.index, self.earliest);
        let planned = planner.plan(filter)?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Resolve the readers first -- that needs `&mut self` because a shard
        // is opened and cached on first touch -- then query them concurrently.
        //
        // This loop used to run the shards one at a time. Each one is a full
        // Tantivy search, and a text query with no date bound plans across every
        // shard on the node: 374 of them here, ~70ms each, so a single query
        // took 25-40 seconds. Worse, the caller holds one global registry mutex
        // for the whole call, so that query also blocked /stats, /admin/* and
        // every other search -- a 401 took seven seconds to come back.
        //
        // Shards are independent indices and a `Searcher` is `Sync`, so there is
        // nothing to serialise here but the merge at the end.
        let resolved: Vec<Arc<ShardReader>> = planned
            .shards
            .iter()
            .filter_map(|id| self.shard(*id))
            .collect();

        let weights = self.weights;
        let limit = filter.limit;
        let text_query = filter.search.is_some();
        let query = &planned.query;
        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .min(resolved.len().max(1));

        let next = std::sync::atomic::AtomicUsize::new(0);
        let mut hits: Vec<SearchHit> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..workers)
                .map(|_| {
                    let (next, resolved) = (&next, &resolved);
                    scope.spawn(move || {
                        let mut local: Vec<SearchHit> = Vec::new();
                        loop {
                            let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let Some(shard) = resolved.get(i) else { break };
                            Self::search_one(
                                shard, query, text_query, limit, weights, now, &mut local,
                            );
                        }
                        local
                    })
                })
                .collect();
            handles
                .into_iter()
                .filter_map(|h| h.join().ok())
                .flatten()
                .collect()
        });
        let _ = &mut hits;

        // Merge across shards: composite score for text queries, created_at
        // (encoded in score) for metadata queries.
        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        hits.truncate(filter.limit);
        Ok(hits)
    }

    /// Query one shard, appending its hits. Runs on a worker thread.
    fn search_one(
        shard: &Arc<ShardReader>,
        query: &dyn tantivy::query::Query,
        text_query: bool,
        limit: usize,
        weights: ScoreWeights,
        now: u64,
        out: &mut Vec<SearchHit>,
    ) {
        {
            let searcher = shard.reader.searcher();

            // Full-text query → composite score; metadata-only → recent first.
            let scored: Vec<ScoredDoc> = if text_query {
                let collector = CompositeCollector {
                    limit,
                    weights,
                    now_ts: now,
                    created_at_field: "created_at".to_string(),
                    wot_tier_field: "wot_tier".to_string(),
                };
                match searcher.search(query, &collector) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(shard = %shard.id, error = %e, "shard search failed");
                        return;
                    }
                }
            } else {
                match searcher.search(
                    query,
                    &TopDocs::with_limit(limit)
                        .order_by_fast_field::<u64>("created_at", tantivy::Order::Desc),
                ) {
                    Ok(v) => v
                        .into_iter()
                        .map(|(ts, addr): (u64, tantivy::DocAddress)| ScoredDoc {
                            segment_ord: addr.segment_ord,
                            doc_id: addr.doc_id,
                            score: ts as f32,
                        })
                        .collect(),
                    Err(e) => {
                        tracing::warn!(shard = %shard.id, error = %e, "shard search failed");
                        return;
                    }
                }
            };

            // Hydrate from stored fields.
            for sd in scored.into_iter().take(limit) {
                if let Ok(h) = hydrate(&searcher, &shard.schema, sd.address(), sd.score, shard.id) {
                    out.push(h);
                }
            }
        }
    }

    /// Every shard id present on disk, plus any already-open ones.
    pub fn all_shard_ids(&mut self) -> Vec<ShardId> {
        let _ = self.discover();
        let mut ids: std::collections::BTreeSet<ShardId> = self.shards.keys().copied().collect();
        if self.root.exists() {
            if let Ok(rd) = std::fs::read_dir(&self.root) {
                for entry in rd.flatten() {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if let Some(id) = ShardId::parse(&name) {
                        ids.insert(id);
                    }
                }
            }
        }
        ids.into_iter().collect()
    }

    /// Fetch a single event by id across all shards.
    pub fn get_event(&mut self, event_id: &str) -> Result<Option<SearchHit>, RegistryError> {
        let filter = SearchFilter {
            limit: 1,
            ..Default::default()
        };
        // Brute-force across shards by term on event_id. Enumerate shard dirs
        // from disk (not just already-open readers) so a node that started
        // against an empty index still finds events written afterwards.
        let ids: Vec<ShardId> = self.all_shard_ids();
        for id in ids {
            if let Some(shard) = self.shard(id) {
                let searcher = shard.reader.searcher();
                let term = tantivy::Term::from_field_text(shard.schema.event_id, event_id);
                let q =
                    tantivy::query::TermQuery::new(term, tantivy::schema::IndexRecordOption::Basic);
                let top = searcher.search(&q, &TopDocs::with_limit(1))?;
                if let Some((_score, addr)) = top.into_iter().next() {
                    let _ = filter;
                    return hydrate(&searcher, &shard.schema, addr, 1.0, shard.id).map(Some);
                }
            }
        }
        Ok(None)
    }

    /// Cluster stats. Scans the root for shard dirs (not just already-open
    /// readers) so a freshly-opened registry reports the full corpus.
    pub fn stats(&mut self) -> RegistryStats {
        // (re)discover in case shards were added since open
        let _ = self.discover();
        let mut ids: Vec<ShardId> = Vec::new();
        if self.root.exists() {
            if let Ok(rd) = std::fs::read_dir(&self.root) {
                for entry in rd.flatten() {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if let Some(id) = ShardId::parse(&name) {
                        ids.push(id);
                    }
                }
            }
        }
        ids.sort();

        let mut per_shard = Vec::new();
        let mut total = 0u64;
        for id in ids {
            if let Some(shard) = self.shard(id) {
                let searcher = shard.reader.searcher();
                let n = searcher.num_docs();
                total += n;
                per_shard.push(ShardStat {
                    shard: id.name(),
                    docs: n,
                });
            }
        }
        let (nofile_soft, _) = nostrsearch_indexer::mem::raise_nofile();
        RegistryStats {
            total_docs: total,
            shard_count: per_shard.len(),
            open_readers: self.shards.len(),
            max_open_readers: self.max_open,
            open_fds: nostrsearch_indexer::mem::open_fds(),
            nofile_soft,
            memory: MemoryStats::collect(),
            shards: per_shard,
        }
    }
}

fn hydrate(
    searcher: &tantivy::Searcher,
    schema: &NostrSchema,
    addr: tantivy::DocAddress,
    score: f32,
    shard: ShardId,
) -> Result<SearchHit, RegistryError> {
    let doc: TantivyDocument = searcher.doc(addr)?;
    let get_text = |f: tantivy::schema::Field| {
        doc.get_first(f)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    };
    let get_u64 =
        |f: tantivy::schema::Field| doc.get_first(f).and_then(|v| v.as_u64()).unwrap_or(0);
    let opt_text = |f: tantivy::schema::Field| {
        doc.get_first(f)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    };
    Ok(SearchHit {
        event_id: get_text(schema.event_id),
        pubkey: get_text(schema.pubkey),
        kind: get_u64(schema.kind) as u16,
        created_at: get_u64(schema.created_at),
        score,
        content: get_text(schema.raw_content),
        title: opt_text(schema.title),
        lang: opt_text(schema.lang),
        shard: shard.name(),
        // Filled in by the caller from the archive; the index has no signed
        // event to give.
        event: None,
    })
}

#[derive(Debug, serde::Serialize)]
pub struct RegistryStats {
    pub total_docs: u64,
    pub shard_count: usize,
    /// Shard readers currently held open (bounded by `MAX_OPEN_SHARD_READERS`).
    pub open_readers: usize,
    pub max_open_readers: usize,
    /// Descriptors this process holds, and its soft limit — the pair you want
    /// when diagnosing "Too many open files".
    pub open_fds: Option<usize>,
    pub nofile_soft: u64,
    pub memory: MemoryStats,
    pub shards: Vec<ShardStat>,
}

/// Memory, split the way it actually needs reading on a search node.
///
/// `rss_mb` alone is misleading here: Tantivy mmaps every segment, so resident
/// file-backed pages inflate RSS without being "used" in any sense that
/// matters — the kernel reclaims them on demand. The number to watch is
/// `cgroup_anon_mb` (real heap), while `cgroup_file_mb` is page cache doing
/// its job. Both count toward `cgroup_limit_mb`, which is why a node can look
/// enormous and still be healthy.
#[derive(Debug, serde::Serialize)]
pub struct MemoryStats {
    pub rss_mb: u64,
    pub peak_rss_mb: u64,
    /// Total charged to the cgroup (anon + page cache).
    pub cgroup_current_mb: Option<u64>,
    /// Anonymous memory: heaps, writer arenas, in-RAM maps.
    pub cgroup_anon_mb: Option<u64>,
    /// Page cache, including mmap'd index segments. Reclaimable.
    pub cgroup_file_mb: Option<u64>,
    pub cgroup_limit_mb: Option<u64>,
}

impl MemoryStats {
    pub fn collect() -> Self {
        let (rss_mb, peak_rss_mb) = nostrsearch_indexer::mem::rss_mb();
        let usage = nostrsearch_indexer::mem::cgroup_usage_mb();
        Self {
            rss_mb,
            peak_rss_mb,
            cgroup_current_mb: usage.map(|(c, _, _)| c),
            cgroup_anon_mb: usage.map(|(_, a, _)| a),
            cgroup_file_mb: usage.map(|(_, _, f)| f),
            cgroup_limit_mb: nostrsearch_indexer::mem::cgroup_limit_mb(),
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct ShardStat {
    pub shard: String,
    pub docs: u64,
}
