//! Unified ingest CLI: one pipeline for **static JSONL archives** and the
//! **live relay firehose**, feeding both the Tantivy index and the stats/WoT
//! engine.
//!
//! - `--input-dir <dir>`   backfill from JSONL dumps (via nostr-archive-cursor)
//! - `--relays <url>`      tail the live firehose (repeatable)
//! - both                  backfill first, then switch to live tailing
//!
//! Web-of-trust is bootstrapped from the same stream and hot-swapped into the
//! index scoring signal every `--wot-refresh-every` events.
//!
//! Usage:
//!   ingest --index-root ./data/index --input-dir ./dumps
//!   ingest --index-root ./data/index --relays wss://relay.damus.io --relays wss://nos.lol
//!   ingest --index-root ./data/index --input-dir ./dumps --relays wss://relay.damus.io
//!
//! ## Full rebuild
//!
//! `--rebuild` runs both migrations in the only order that works, then the
//! normal backfill:
//!
//!   1. rebuild the **archive** id index, so every event's location (shard +
//!      frame offset) is recorded and single-frame imports are reframed;
//!   2. wipe the **Tantivy** index and the dedupe store;
//!   3. re-ingest the corpus into a fresh index.
//!
//! ```text
//! ingest --index-root ./data/index --input-dir ./dumps --rebuild
//! ```
//!
//! Step 1 is what makes an event fetchable by id without scanning; step 2
//! starts the Tantivy index from empty so the pass rebuilds it rather than
//! adding to what is already there.

use clap::Parser;
use nostrsearch_indexer::{Pipeline, PipelineConfig, ShardWriterConfig};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Unified ingest: archive backfill + live firehose, into the index and the
/// stats/WoT engine.
///
/// Defaults come from the same environment the server node reads (INDEX_ROOT,
/// STATE_DIR, WOT_OUT, ARCHIVE_DIR, RELAYS, WOT_REFRESH_EVERY,
/// MAX_OPEN_SHARDS, WRITER_THREADS); flags override them.
#[derive(Parser, Debug)]
#[command(name = "ingest", version)]
struct Args {
    /// Index output root
    #[arg(long, value_name = "DIR")]
    index_root: Option<PathBuf>,

    /// JSONL dumps to backfill (.jsonl/.json/.zst/.gz/.bz2)
    #[arg(long, value_name = "DIR")]
    input_dir: Option<PathBuf>,

    /// Live firehose relay (repeatable); appends to $RELAYS
    #[arg(long = "relays", alias = "relay", value_name = "URL")]
    relays: Vec<String>,

    /// Also archive firehose events here as .jsonl.zst + id index
    #[arg(long, value_name = "DIR")]
    archive_dir: Option<PathBuf>,

    /// Analysis state store
    #[arg(long, value_name = "DIR")]
    state_dir: Option<PathBuf>,

    /// Do not persist analysis state
    #[arg(long, conflicts_with = "state_dir")]
    no_state: bool,

    /// Also write a WoT snapshot here
    #[arg(long, value_name = "FILE")]
    wot_out: Option<PathBuf>,

    /// Do not write a WoT snapshot
    #[arg(long, conflicts_with = "wot_out")]
    no_wot_out: bool,

    /// Re-materialize and hot-swap the WoT every N events
    #[arg(long, value_name = "N")]
    wot_refresh_every: Option<u64>,

    /// Tantivy writer heap per shard, in MB.
    ///
    /// Charged per *open shard*, so this multiplies by --max-open-shards.
    #[arg(long, value_name = "MB", default_value_t = 64)]
    heap_mb: usize,

    /// Commit every N docs per shard
    #[arg(long, value_name = "N", default_value_t = 200_000)]
    commit_docs: u64,

    /// Checkpoint interval, in seconds.
    ///
    /// A kill re-processes at most one checkpoint window. Shorten it when
    /// restarts are expected; each checkpoint commits every open shard.
    #[arg(long, value_name = "SECS", default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..))]
    checkpoint_secs: u64,

    /// Archive files read in parallel [default: available cores]
    #[arg(long, value_name = "N", default_value_t = 0, hide_default_value = true)]
    parallelism: usize,

    /// Events per read chunk
    #[arg(long, value_name = "N", default_value_t = 2_000)]
    chunk_size: usize,

    /// Disable archive event-id dedup
    #[arg(long)]
    no_dedupe: bool,

    /// Shard writers held open; total writer heap is this x --heap-mb
    #[arg(long, value_name = "N")]
    max_open_shards: Option<usize>,

    /// Indexing threads per shard; total is this x open shards
    #[arg(long, value_name = "N")]
    writer_threads: Option<usize>,

    /// Do not sort batches before indexing
    #[arg(long)]
    no_sort: bool,

    /// Full migration: --rebuild-archive-index then --reindex, then ingest
    #[arg(long)]
    rebuild: bool,

    /// Rebuild the archive id index first, recording each event's location and
    /// reframing single-frame dumps so lookups decode one frame (O(n))
    #[arg(long)]
    rebuild_archive_index: bool,

    /// Force a full RocksDB compaction of the archive index afterwards
    #[arg(long)]
    compact_archive_index: bool,

    /// Wipe the Tantivy index + dedupe store first, then rebuild it from the
    /// archive. DESTROYS the existing index.
    #[arg(long)]
    reindex: bool,

    /// Serve a status page and the archive files on this address.
    ///
    /// Defaults to $BIND, the same variable the server node reads; off when
    /// neither is set. A backfill takes hours, and for that whole time the
    /// node would otherwise refuse connections on its port.
    #[arg(long, value_name = "ADDR")]
    bind: Option<String>,

    /// Exit when the backfill finishes.
    ///
    /// The default is to idle, because anything with a restart policy of
    /// Always treats a clean exit as a reason to run the whole ingest again.
    /// Use this for a batch Job.
    #[arg(long)]
    exit_when_done: bool,
}

/// The parsed arguments, merged with the environment contract.
///
/// Defaults live in [`nostrsearch_indexer::env`] rather than in clap's `env`
/// attribute: the server node reads the same variables through those helpers,
/// and they treat an empty value as unset, which clap does not. A container
/// that sets `ARCHIVE_DIR=""` to mean "off" would otherwise get an archive
/// directory named "".
struct Config {
    index_root: PathBuf,
    input_dir: Option<PathBuf>,
    relays: Vec<String>,
    archive_dir: Option<PathBuf>,
    state_dir: Option<PathBuf>,
    wot_out: Option<PathBuf>,
    wot_refresh_every: u64,
    heap_mb: usize,
    commit_docs: u64,
    checkpoint_secs: u64,
    parallelism: usize,
    chunk_size: usize,
    dedupe: bool,
    max_open_shards: usize,
    writer_threads: usize,
    sort_batches: bool,
    rebuild_archive_index: bool,
    compact_archive_index: bool,
    reindex: bool,
    exit_when_done: bool,
    bind: Option<String>,
}

impl Config {
    fn load() -> Result<Self, String> {
        Self::from_args(Args::parse())
    }

    fn from_args(a: Args) -> Result<Self, String> {
        use nostrsearch_indexer::env;

        // Relays accumulate: the image sets a baseline in $RELAYS and a flag
        // adds to it, which is how the two were combined before clap.
        let mut relays = env::relays();
        relays.extend(a.relays);

        let cfg = Self {
            index_root: a.index_root.unwrap_or_else(env::index_root),
            input_dir: a.input_dir,
            relays,
            archive_dir: a.archive_dir.or_else(env::archive_dir),
            state_dir: if a.no_state {
                None
            } else {
                Some(a.state_dir.unwrap_or_else(env::state_dir))
            },
            wot_out: if a.no_wot_out {
                None
            } else {
                Some(a.wot_out.unwrap_or_else(env::wot_out))
            },
            wot_refresh_every: a.wot_refresh_every.unwrap_or_else(env::wot_refresh_every),
            heap_mb: a.heap_mb,
            commit_docs: a.commit_docs,
            checkpoint_secs: a.checkpoint_secs,
            parallelism: a.parallelism,
            chunk_size: a.chunk_size,
            dedupe: !a.no_dedupe,
            max_open_shards: a.max_open_shards.unwrap_or_else(env::max_open_shards),
            writer_threads: a.writer_threads.unwrap_or_else(env::writer_threads),
            sort_batches: !a.no_sort,
            rebuild_archive_index: a.rebuild_archive_index || a.rebuild,
            compact_archive_index: a.compact_archive_index,
            reindex: a.reindex || a.rebuild,
            exit_when_done: a.exit_when_done,
            bind: a.bind.or_else(env::bind),
        };

        if cfg.input_dir.is_none() && cfg.relays.is_empty() {
            return Err("provide --input-dir <dir> and/or --relays <url>".into());
        }
        Ok(cfg)
    }

    /// Directory holding the archive dumps + their id index.
    ///
    /// `--archive-dir` when set (the firehose's own corpus), otherwise the
    /// backfill source: in the unified deployment they are the same directory,
    /// and rebuilding the index of a directory we are not reading would be a
    /// silent no-op.
    fn archive_index_dir(&self) -> Option<&PathBuf> {
        self.archive_dir.as_ref().or(self.input_dir.as_ref())
    }
}

fn main() -> anyhow::Result<()> {
    // Arguments first: `--help` and `--version` exit here, and doing it before
    // the subscriber is installed keeps them free of log lines.
    let args = Config::load().map_err(anyhow::Error::msg)?;

    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    // `ingest=info` covers this binary's own logs: a bin's tracing target is
    // its crate name, so a filter listing only the libraries silently hides
    // every line the CLI itself emits -- including the rebuild progress and
    // the warning that the index was wiped.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "nostrsearch=info,ingest=info,nostr_archive_cursor=warn".into()
            }),
        )
        .init();

    let (soft, hard) = nostrsearch_indexer::mem::raise_nofile();
    tracing::info!(
        nofile_soft = soft,
        nofile_hard = hard,
        "file descriptor limit"
    );

    // The runtime comes up first so the status service can be listening before
    // any of the long work starts. The rebuilds below are blocking, but they
    // run on this thread while the runtime's workers serve HTTP, so the port
    // answers throughout — a rebuild is the longest phase there is, and it was
    // the phase most likely to be mistaken for a hung container.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    if let Some(bind) = args.bind.as_deref() {
        rt.block_on(nostrsearch_indexer::serve::spawn(
            bind,
            args.archive_index_dir().map(|p| p.as_path()),
        ))?;
    }

    // ── Phase 0: rebuilds, before anything opens the index ─────────────────
    // Both run here rather than inside `run()` because they must happen before
    // `Pipeline::new` takes the index root, and because the archive rebuild is
    // long, blocking, single-threaded work with no reason to hold a runtime.
    if args.rebuild_archive_index {
        rebuild_archive_index(&args)?;
    }
    if args.reindex {
        wipe_index(&args)?;
    }

    let cfg = PipelineConfig {
        index_root: args.index_root.clone(),
        shard: ShardWriterConfig {
            heap_bytes: args.heap_mb * 1_000_000,
            commit_every_docs: args.commit_docs,
            max_open_shards: args.max_open_shards,
            writer_threads: args.writer_threads.max(1),
            ..Default::default()
        },
        state_dir: args.state_dir.clone(),
        wot_refresh_every: args.wot_refresh_every,
        min_refresh_interval: nostrsearch_indexer::env::min_refresh_interval(),
        persist_interval: nostrsearch_indexer::env::persist_interval(),
        wot_out: args.wot_out.clone(),
    };
    // Writer heap is charged per open shard, so --heap-mb and
    // --max-open-shards multiply. Log the product and, when the cgroup
    // publishes a limit, keep it to half of that: a slower ingest beats one
    // the OOM killer stops. Half, not all, because the stats maps, the id
    // buffer and the archive cursor also need room.
    let mut cfg = cfg;
    let total_gb = cfg.shard.total_heap_bytes() as f64 / 1e9;
    if let Some(limit_mb) = nostrsearch_indexer::mem::cgroup_limit_mb() {
        let budget = (limit_mb as usize * 1_000_000) / 2;
        if let Some(was) = cfg.shard.fit_to_budget(budget) {
            tracing::warn!(
                requested_heap_mb = was / 1_000_000,
                using_heap_mb = cfg.shard.heap_bytes / 1_000_000,
                max_open_shards = cfg.shard.max_open_shards,
                requested_total_gb = format!("{total_gb:.1}"),
                cgroup_limit_mb = limit_mb,
                "writer heap would exceed half the cgroup limit; reduced per-shard heap"
            );
        }
    }
    tracing::info!(
        heap_mb = cfg.shard.heap_bytes / 1_000_000,
        max_open_shards = cfg.shard.max_open_shards,
        total_writer_heap_gb = format!("{:.1}", cfg.shard.total_heap_bytes() as f64 / 1e9),
        "writer heap budget"
    );

    let mut pipeline = Pipeline::new(cfg)?;
    if args.reindex {
        // The analyses fold every event they see. Replaying the whole corpus
        // over state that already counted it would double every total, so a
        // reindex resets them and lets them rebuild from the same pass.
        let reset = pipeline.reset_all_analyses();
        tracing::info!(analyses = ?reset, "reset analyses for reindex");
    }
    let pipeline = Arc::new(Mutex::new(pipeline));

    rt.block_on(run(args, pipeline))
}

/// Rebuild the archive's id index from every dump in the directory.
///
/// Index values now record *where* each event lives (shard + frame offset +
/// length), which is what lets a search hit be hydrated into a complete signed
/// event without scanning the corpus. An index written by an older version
/// still opens and still dedupes, but it carries no locations, so this is the
/// migration that makes lookups O(1). It also reframes archives that are one
/// giant zstd frame -- the usual shape of a dump produced elsewhere -- because
/// otherwise every lookup into them decompresses from byte zero.
///
/// O(n) over the corpus: hours on a 763 GiB archive. Deliberately explicit,
/// never automatic.
fn rebuild_archive_index(args: &Config) -> anyhow::Result<()> {
    let Some(dir) = args.archive_index_dir() else {
        anyhow::bail!("--rebuild-archive-index needs --archive-dir or --input-dir");
    };
    if !dir.is_dir() {
        anyhow::bail!("archive dir {} is not a directory", dir.display());
    }

    let started = Instant::now();
    tracing::info!(dir = %dir.display(), "rebuilding archive id index (O(n) over the corpus)");

    // The rebuild's parallel walk decompresses one shard per reader thread,
    // each with its own decode buffer, so the core count is a memory
    // multiplier. Left to itself it uses every core -- 80 on the production
    // host -- which is how a rebuild that scanned happily for an hour died the
    // moment it reached the parallel walk. Honour --parallelism here for the
    // same reason the backfill does.
    let parallelism = if args.parallelism == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    } else {
        args.parallelism
    };
    let index = nostr_archive_cursor::RocksDbIndex::open(dir.join("index-rocksdb"))?;
    let mut db: nostr_archive_cursor::DefaultJsonFilesDatabase =
        nostr_archive_cursor::JsonFilesDatabase::new_with_index(dir, index.clone())?
            .with_rebuild_parallelism(parallelism);
    tracing::info!(parallelism, "archive rebuild parallelism");

    // Frame sidecars first: without them a rebuilt index records offsets into
    // shards nobody can seek into.
    let built = db.rebuild_missing_frame_indexes();
    if built > 0 {
        tracing::info!(shards = built, "built missing frame sidecars");
    }
    db.rebuild_index()?;
    tracing::info!(
        events = db.count_keys(),
        elapsed_s = format!("{:.0}", started.elapsed().as_secs_f64()),
        "archive id index rebuilt"
    );

    if args.compact_archive_index {
        // Data written by older versions is uncompressed, and RocksDB will not
        // rewrite it on its own (manual compaction skips the bottommost level,
        // which in a mostly-static index is where everything lives).
        tracing::info!("compacting the archive index");
        index.compact();
        tracing::info!("archive index compaction complete");
    }
    Ok(())
}

/// Delete the Tantivy shards and the dedupe store so the next pass rebuilds
/// them from scratch.
///
/// The dedupe store goes with them: it records what has already been indexed,
/// so leaving it behind would make the rebuild skip the entire corpus it is
/// supposed to re-add.
///
/// Only shard directories (`YYYY-MM`) and `.dedupe` are removed; anything else
/// under the root belongs to someone else and is left alone.
fn wipe_index(args: &Config) -> anyhow::Result<()> {
    if args.input_dir.is_none() {
        anyhow::bail!(
            "--reindex without --input-dir would delete the index and have nothing \
             to rebuild it from"
        );
    }
    let root = &args.index_root;
    if !root.exists() {
        return Ok(());
    }

    let mut removed = 0usize;
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        let is_shard = nostrsearch_core::shard::ShardId::parse(&name).is_some();
        if (is_shard || name == ".dedupe") && entry.path().is_dir() {
            std::fs::remove_dir_all(entry.path())?;
            removed += 1;
        }
    }
    tracing::warn!(
        root = %root.display(),
        removed,
        "wiped the index for a full reindex"
    );
    Ok(())
}

async fn run(args: Config, pipeline: Arc<Mutex<Pipeline>>) -> anyhow::Result<()> {
    let total = Arc::new(AtomicU64::new(0));
    // The engine's live counters, shared with the progress reporter below.
    // Reporting off `total` alone would print nothing until the run ended,
    // which on a multi-hour archive pass is no reporting at all.
    let progress =
        std::sync::Arc::new(nostrsearch_indexer::archive_ingest::IngestProgress::default());
    let started = Instant::now();
    // Set once the work is finished, so the progress reporter stops rather
    // than printing the same final line every 5s for as long as the process
    // idles.
    let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Persistent event-id dedupe, so a restarted backfill resumes instead of
    // duplicating everything (Tantivy has no unique key on writes). Lives
    // inside the index root so wiping the index also wipes the seen-set.
    let id_store = if args.dedupe && args.input_dir.is_some() {
        Some(Arc::new(nostrsearch_indexer::id_store::IdStore::open(
            &args.index_root.join(".dedupe"),
        )?))
    } else {
        None
    };
    // Ids indexed since the last checkpoint. Mutated only while holding the
    // pipeline mutex; flushed by the checkpoint task, which holds that mutex
    // across commit + flush so an id is only recorded once its document is
    // durable. A hard kill therefore re-processes at most one checkpoint
    // window — redundant work, never holes; duplicates only if a shard
    // happened to auto-commit inside that window.
    let pending_ids: Arc<Mutex<std::collections::HashSet<[u8; 32]>>> =
        Arc::new(Mutex::new(std::collections::HashSet::new()));

    // In a container this binary is PID 1, and PID 1 ignores SIGTERM unless a
    // handler is installed — so `kubectl delete` / probe kills hung for the
    // whole grace period and then SIGKILLed us (exit 137) with no log line and
    // no state flush. Handle it: log, flush what we can, exit 143.
    {
        let pipe = pipeline.clone();
        let id_store_sig = id_store.clone();
        let pending_sig = pending_ids.clone();
        tokio::spawn(async move {
            let mut term =
                match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                    Ok(t) => t,
                    Err(e) => {
                        tracing::warn!(error = %e, "no SIGTERM handler");
                        return;
                    }
                };
            term.recv().await;
            tracing::warn!("SIGTERM received; flushing index and stats before exit");
            let store = id_store_sig.clone();
            let pending = pending_sig.clone();
            let flushed = tokio::task::spawn_blocking(move || {
                let mut p = pipe.lock().unwrap();
                p.finish()?;
                // Everything is committed; record the seen-ids so the next
                // run resumes instead of re-processing this window.
                if let Some(store) = store {
                    let ids = std::mem::take(&mut *pending.lock().unwrap());
                    store.flush(ids.iter())?;
                }
                anyhow::Ok(())
            })
            .await;
            match flushed {
                Ok(Ok(())) => tracing::info!("flushed; exiting"),
                other => tracing::warn!(?other, "flush on SIGTERM failed"),
            }
            std::process::exit(143);
        });
    }

    // progress reporter
    {
        let total_prog = total.clone();
        let live = progress.clone();
        let limit_mb = nostrsearch_indexer::mem::cgroup_limit_mb();
        if let Some(l) = limit_mb {
            tracing::info!(limit_mb = l, "cgroup memory limit");
        }
        // Force periodic writeback of the index filesystem. Dirty pages are
        // charged to the cgroup and cannot be reclaimed until they reach disk,
        // so a writer that dirties faster than writeback retires can be
        // OOM-killed while its own memory is modest.
        let sync_root = args.index_root.clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_secs(10));
                nostrsearch_indexer::mem::syncfs(&sync_root);
            }
        });

        let stage_pipe = pipeline.clone();
        let reporter_done = finished.clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_secs(5));
                if reporter_done.load(Ordering::Relaxed) {
                    return;
                }
                // Prefer the engine's live count; fall back to the final
                // total once the run has handed it over.
                let live_n = live.indexed.load(Ordering::Relaxed);
                let done = if live_n > 0 {
                    live_n
                } else {
                    total_prog.load(Ordering::Relaxed)
                };
                // Serial fold time comes from the pipeline; the reader stages
                // are summed across threads, so they are thread-seconds and are
                // labelled to say so. Reporting only the fold made every
                // slowdown look like the fold.
                let fold = stage_pipe
                    .try_lock()
                    .map(|p| format!("  fold={:.0}s", p.stage_secs().0))
                    .unwrap_or_default();
                let stages = format!(
                    "{fold}  [thread-s parse={:.0} dedupe={:.0} index={:.0}]",
                    live.parse_ns.load(Ordering::Relaxed) as f64 / 1e9,
                    live.dedupe_ns.load(Ordering::Relaxed) as f64 / 1e9,
                    live.index_ns.load(Ordering::Relaxed) as f64 / 1e9,
                );
                if done > 0 {
                    let rate = done as f64 / started.elapsed().as_secs_f64();
                    let (rss, peak) = nostrsearch_indexer::mem::rss_mb();
                    // cgroup usage, not RSS, is what the OOM killer measures: it
                    // includes page cache, which heavy dump reads and index writes
                    // fill even while RSS stays flat.
                    let cg = nostrsearch_indexer::mem::cgroup_usage_mb()
                        .map(|(cur, anon, file)| {
                            let pct = limit_mb
                                .map(|l| {
                                    format!(" {:.0}% of {}MB", cur as f64 / l as f64 * 100.0, l)
                                })
                                .unwrap_or_default();
                            let d = nostrsearch_indexer::mem::cgroup_dirty_mb()
                                .map(|(dirty, wb, slab)| {
                                    format!(" dirty={dirty}MB wb={wb}MB slab={slab}MB")
                                })
                                .unwrap_or_default();
                            format!("  cgroup={cur}MB (anon={anon}MB cache={file}MB{d}){pct}")
                        })
                        .unwrap_or_default();
                    eprintln!(
                        "  processed={done}  rate={rate:.0}/s  elapsed={:.0}s{stages}  rss={rss}MB peak={peak}MB{cg}",
                        started.elapsed().as_secs_f64()
                    );
                }
            }
        });
    }

    // ── Phase 1: backfill from the archive (blocking) ──────────────────────
    if let Some(input_dir) = args.input_dir.clone() {
        if !input_dir.is_dir() {
            anyhow::bail!("--input-dir {} is not a directory", input_dir.display());
        }
        let parallelism = if args.parallelism == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        } else {
            args.parallelism
        };
        tracing::info!(dir = %input_dir.display(), parallelism, "starting archive backfill");

        let chunk_size = args.chunk_size;
        let sort_batches = args.sort_batches;

        // Checkpointing belongs to the engine, which owns the ids and the
        // commit that makes them safe to record. A second checkpoint task here
        // committed all open shards on its own timer while flushing a buffer
        // the engine no longer filled -- paying the fsyncs, recording nothing.

        // The engine is shared with the server's admin ingest: one reader,
        // one staging scheme, one set of id-store rules. They used to be two
        // implementations with two sets of bugs.
        let cancel = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        nostrsearch_indexer::archive_ingest::ingest(
            pipeline.clone(),
            nostrsearch_indexer::archive_ingest::IngestOptions {
                input_dir: input_dir.clone(),
                parallelism,
                chunk_size,
                sort_batches,
                dedupe: args.dedupe,
                checkpoint_every: std::time::Duration::from_secs(args.checkpoint_secs),
                ..Default::default()
            },
            id_store.clone(),
            pending_ids.clone(),
            progress.clone(),
            cancel,
        )
        .await?;
        total.store(progress.indexed.load(Ordering::Relaxed), Ordering::Relaxed);
        tracing::info!("archive backfill complete");
    }

    // ── Phase 2: live firehose tail (runs until stopped) ───────────────────
    if !args.relays.is_empty() {
        // If we skipped backfill, still switch the pipeline into live mode.
        if args.input_dir.is_none() {
            pipeline.lock().unwrap().go_live();
        }

        // periodic commit for durability while tailing
        {
            let pipe = pipeline.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
                loop {
                    tick.tick().await;
                    if let Err(e) = pipe.lock().unwrap().commit() {
                        tracing::warn!(error = %e, "periodic commit failed");
                    }
                }
            });
        }

        let pipe = pipeline.clone();
        let total_cb = total.clone();
        let mut fh = nostrsearch_indexer::firehose::FirehoseConfig::new(args.relays.clone());
        if let Some(dir) = &args.archive_dir {
            fh = fh.with_archive(dir);
        }
        tracing::info!(
            relays = args.relays.len(),
            archive = args.archive_dir.is_some(),
            "starting firehose tail"
        );
        nostrsearch_indexer::firehose::run(&fh, move |ev| {
            pipe.lock().unwrap().process(ev);
            total_cb.fetch_add(1, Ordering::Relaxed);
        })
        .await?;
    } else {
        // archive-only run: final flush
        pipeline.lock().unwrap().finish()?;
    }

    let done = total.load(Ordering::Relaxed);
    let secs = started.elapsed().as_secs_f64();
    finished.store(true, Ordering::Relaxed);
    println!(
        "ingest complete: {} events in {:.1}s = {:.0}/s",
        done,
        secs,
        done as f64 / secs
    );

    if args.exit_when_done {
        return Ok(());
    }

    // Everything is committed and flushed; from here the process exists only
    // to stay alive.
    //
    // Exiting 0 is the wrong end state under a Deployment: the container is
    // restarted, and a restarted ingest re-walks the entire corpus. The dedupe
    // store means that is redundant work rather than duplicate documents, but
    // on a 763 GiB archive it is hours of IO per restart, forever. Idling makes
    // the pod a no-op until something actually asks it to stop.
    //
    // SIGTERM is still handled (the task installed above flushes and exits
    // 143), so `kubectl delete` / `rollout restart` terminate promptly.
    tracing::info!(
        "ingest finished; idling (pass --exit-when-done to exit instead). \
         SIGTERM will shut down cleanly."
    );
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
    }
}
