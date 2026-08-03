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

use nostrsearch_indexer::{Pipeline, PipelineConfig, ShardWriterConfig};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

struct Args {
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
}

impl Args {
    fn parse() -> Result<Self, String> {
        // Defaults come from the same environment contract the server node
        // uses (INDEX_ROOT, STATE_DIR, WOT_OUT, ARCHIVE_DIR, RELAYS), so the
        // container image configures every entry point once. Flags override.
        use nostrsearch_indexer::env;
        let mut index_root = env::index_root();
        let mut input_dir = None;
        let mut relays = env::relays();
        let mut archive_dir = env::archive_dir();
        let mut state_dir = Some(env::state_dir());
        let mut wot_out = Some(env::wot_out());
        let mut wot_refresh_every = env::wot_refresh_every();
        // Charged per *open shard*, so this multiplies by --max-open-shards.
        // Was 512, which at the default 64 shards is a 32 GB arena.
        let mut heap_mb = 64usize;
        let mut commit_docs = 200_000u64;
        let mut checkpoint_secs = 60u64;
        let mut parallelism = 0usize;
        let mut chunk_size = 2_000usize;
        let mut dedupe = true;
        let mut max_open_shards = nostrsearch_indexer::env::max_open_shards();
        let mut writer_threads = nostrsearch_indexer::env::writer_threads();
        let mut sort_batches = true;

        let mut it = std::env::args().skip(1);
        while let Some(a) = it.next() {
            match a.as_str() {
                "--index-root" => {
                    index_root = PathBuf::from(it.next().ok_or("--index-root value")?)
                }
                "--input-dir" => {
                    input_dir = Some(PathBuf::from(it.next().ok_or("--input-dir value")?))
                }
                "--relays" | "--relay" => relays.push(it.next().ok_or("--relays value")?),
                "--archive-dir" => {
                    archive_dir = Some(PathBuf::from(it.next().ok_or("--archive-dir value")?))
                }
                "--state-dir" => {
                    state_dir = Some(PathBuf::from(it.next().ok_or("--state-dir value")?))
                }
                "--no-state" => state_dir = None,
                "--wot-out" => wot_out = Some(PathBuf::from(it.next().ok_or("--wot-out value")?)),
                "--no-wot-out" => wot_out = None,
                "--wot-refresh-every" => {
                    wot_refresh_every = it
                        .next()
                        .ok_or("--wot-refresh-every value")?
                        .parse()
                        .map_err(|_| "bad wot-refresh-every")?
                }
                "--heap-mb" => {
                    heap_mb = it
                        .next()
                        .ok_or("--heap-mb value")?
                        .parse()
                        .map_err(|_| "bad heap")?
                }
                // A kill re-processes at most one checkpoint window, and any
                // shard that auto-committed inside it contributes duplicate
                // documents. Shorten this when restarts are expected; each
                // checkpoint commits every open shard, so it is not free.
                "--checkpoint-secs" => {
                    checkpoint_secs = it
                        .next()
                        .ok_or("--checkpoint-secs value")?
                        .parse()
                        .map_err(|_| "bad checkpoint-secs")?
                }
                "--commit-docs" => {
                    commit_docs = it
                        .next()
                        .ok_or("--commit-docs value")?
                        .parse()
                        .map_err(|_| "bad commit-docs")?
                }
                "--parallelism" => {
                    parallelism = it
                        .next()
                        .ok_or("--parallelism value")?
                        .parse()
                        .map_err(|_| "bad parallelism")?
                }
                "--chunk-size" => {
                    chunk_size = it
                        .next()
                        .ok_or("--chunk-size value")?
                        .parse()
                        .map_err(|_| "bad chunk-size")?
                }
                "--no-dedupe" => dedupe = false,
                "--max-open-shards" => {
                    max_open_shards = it
                        .next()
                        .ok_or("--max-open-shards value")?
                        .parse()
                        .map_err(|_| "bad max-open-shards")?
                }
                "--writer-threads" => {
                    writer_threads = it
                        .next()
                        .ok_or("--writer-threads value")?
                        .parse()
                        .map_err(|_| "bad writer-threads")?
                }
                "--no-sort" => sort_batches = false,
                "-h" | "--help" => {
                    println!("{}", help());
                    std::process::exit(0);
                }
                other => return Err(format!("unknown arg: {other}")),
            }
        }

        if input_dir.is_none() && relays.is_empty() {
            return Err("provide --input-dir <dir> and/or --relays <url>".into());
        }
        Ok(Self {
            index_root,
            input_dir,
            relays,
            archive_dir,
            state_dir,
            wot_out,
            wot_refresh_every,
            heap_mb,
            commit_docs,
            checkpoint_secs: checkpoint_secs.max(1),
            parallelism,
            chunk_size,
            dedupe,
            max_open_shards,
            writer_threads,
            sort_batches,
        })
    }
}

fn help() -> String {
    "nostrsearch unified ingest (archive + firehose → index + stats/WoT)\n\
     \n\
     Defaults are read from the same env vars as the server node:\n\
     INDEX_ROOT, STATE_DIR, WOT_OUT, ARCHIVE_DIR, RELAYS, WOT_REFRESH_EVERY.\n\
     Flags below override them.\n\
     \n\
     --index-root <dir>        index output root ($INDEX_ROOT, ./data/index)\n\
     --input-dir <dir>         JSONL dumps to backfill (.jsonl/.json/.zst/.gz/.bz2)\n\
     --relays <url>            live firehose relay (repeatable)\n\
     --archive-dir <dir>       ALSO archive firehose events as .jsonl.zst + id index\n\
     \x20                        (absorbs nostrhole: produces the hole.v0l.io corpus)\n\
     --state-dir <dir>         analysis state store (default ./data/stats)  | --no-state\n\
     --wot-out <file>          also write WoT snapshot here (default ./data/wot.bin) | --no-wot-out\n\
     --wot-refresh-every <n>   re-materialize + hot-swap WoT every N events (default 1000000)\n\
     --heap-mb <n>             tantivy writer heap per shard (default 512)\n\
     --commit-docs <n>         commit every N docs per shard (default 200000)\n\
     --parallelism <n>         archive files read in parallel (default: num cores)\n\
     --chunk-size <n>          events per read chunk (default 2000)\n\
     --no-dedupe               disable archive event-id dedup\n\
     --max-open-shards <n>     shard writers held open ($MAX_OPEN_SHARDS, 64);\n\
     \x20                        total writer heap is n x --heap-mb\n\
     --writer-threads <n>      indexing threads per shard ($WRITER_THREADS, 1);\n\
     \x20                        total threads is n x open shards"
        .to_string()
}

fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "nostrsearch=info,nostr_archive_cursor=warn".into()),
        )
        .init();

    let (soft, hard) = nostrsearch_indexer::mem::raise_nofile();
    tracing::info!(
        nofile_soft = soft,
        nofile_hard = hard,
        "file descriptor limit"
    );

    let args = Args::parse().map_err(anyhow::Error::msg)?;

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

    let pipeline = Arc::new(Mutex::new(Pipeline::new(cfg)?));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(run(args, pipeline))
}

async fn run(args: Args, pipeline: Arc<Mutex<Pipeline>>) -> anyhow::Result<()> {
    let total = Arc::new(AtomicU64::new(0));
    // The engine's live counters, shared with the progress reporter below.
    // Reporting off `total` alone would print nothing until the run ended,
    // which on a multi-hour archive pass is no reporting at all.
    let progress =
        std::sync::Arc::new(nostrsearch_indexer::archive_ingest::IngestProgress::default());
    let started = Instant::now();

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
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_secs(5));
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
    println!(
        "ingest complete: {} events in {:.1}s = {:.0}/s",
        done,
        secs,
        done as f64 / secs
    );
    Ok(())
}
