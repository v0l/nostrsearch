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

use nostr_archive_cursor::NostrCursor;
use nostrsearch_core::event::NostrEvent;
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
    parallelism: usize,
    chunk_size: usize,
    dedupe: bool,
    max_open_shards: usize,
    writer_threads: usize,
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
        let mut heap_mb = 512usize;
        let mut commit_docs = 200_000u64;
        let mut parallelism = 0usize;
        let mut chunk_size = 2_000usize;
        let mut dedupe = true;
        let mut max_open_shards = nostrsearch_indexer::env::max_open_shards();
        let mut writer_threads = nostrsearch_indexer::env::writer_threads();

        let mut it = std::env::args().skip(1);
        while let Some(a) = it.next() {
            match a.as_str() {
                "--index-root" => index_root = PathBuf::from(it.next().ok_or("--index-root value")?),
                "--input-dir" => input_dir = Some(PathBuf::from(it.next().ok_or("--input-dir value")?)),
                "--relays" | "--relay" => relays.push(it.next().ok_or("--relays value")?),
                "--archive-dir" => archive_dir = Some(PathBuf::from(it.next().ok_or("--archive-dir value")?)),
                "--state-dir" => state_dir = Some(PathBuf::from(it.next().ok_or("--state-dir value")?)),
                "--no-state" => state_dir = None,
                "--wot-out" => wot_out = Some(PathBuf::from(it.next().ok_or("--wot-out value")?)),
                "--no-wot-out" => wot_out = None,
                "--wot-refresh-every" => wot_refresh_every = it.next().ok_or("--wot-refresh-every value")?.parse().map_err(|_| "bad wot-refresh-every")?,
                "--heap-mb" => heap_mb = it.next().ok_or("--heap-mb value")?.parse().map_err(|_| "bad heap")?,
                "--commit-docs" => commit_docs = it.next().ok_or("--commit-docs value")?.parse().map_err(|_| "bad commit-docs")?,
                "--parallelism" => parallelism = it.next().ok_or("--parallelism value")?.parse().map_err(|_| "bad parallelism")?,
                "--chunk-size" => chunk_size = it.next().ok_or("--chunk-size value")?.parse().map_err(|_| "bad chunk-size")?,
                "--no-dedupe" => dedupe = false,
                "--max-open-shards" => max_open_shards = it.next().ok_or("--max-open-shards value")?.parse().map_err(|_| "bad max-open-shards")?,
                "--writer-threads" => writer_threads = it.next().ok_or("--writer-threads value")?.parse().map_err(|_| "bad writer-threads")?,
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
            parallelism,
            chunk_size,
            dedupe,
            max_open_shards,
            writer_threads,
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
    let pipeline = Arc::new(Mutex::new(Pipeline::new(cfg)?));

    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    rt.block_on(run(args, pipeline))
}

async fn run(args: Args, pipeline: Arc<Mutex<Pipeline>>) -> anyhow::Result<()> {
    let total = Arc::new(AtomicU64::new(0));
    let started = Instant::now();

    // progress reporter
    {
        let total_prog = total.clone();
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

        std::thread::spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_secs(5));
            let done = total_prog.load(Ordering::Relaxed);
            if done > 0 {
                let rate = done as f64 / started.elapsed().as_secs_f64();
                let (rss, peak) = nostrsearch_indexer::mem::rss_mb();
                // cgroup usage, not RSS, is what the OOM killer measures: it
                // includes page cache, which heavy dump reads and index writes
                // fill even while RSS stays flat.
                let cg = nostrsearch_indexer::mem::cgroup_usage_mb()
                    .map(|(cur, anon, file)| {
                        let pct = limit_mb
                            .map(|l| format!(" {:.0}% of {}MB", cur as f64 / l as f64 * 100.0, l))
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
                    "  processed={done}  rate={rate:.0}/s  elapsed={:.0}s  rss={rss}MB peak={peak}MB{cg}",
                    started.elapsed().as_secs_f64()
                );
            }
        });
    }

    // ── Phase 1: backfill from the archive (blocking) ──────────────────────
    if let Some(input_dir) = args.input_dir.clone() {
        if !input_dir.is_dir() {
            anyhow::bail!("--input-dir {} is not a directory", input_dir.display());
        }
        let parallelism = if args.parallelism == 0 {
            std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4)
        } else {
            args.parallelism
        };
        tracing::info!(dir = %input_dir.display(), parallelism, "starting archive backfill");

        let pipe = pipeline.clone();
        let total_cb = total.clone();
        let chunk_size = args.chunk_size;
        let dedupe = args.dedupe;
        tokio::task::spawn_blocking(move || {
            let cursor = NostrCursor::new(input_dir)
                .with_parallelism(parallelism)
                .with_dedupe(dedupe);
            cursor.walk_with_chunked_sync(
                move |events: Vec<nostr_archive_cursor::NostrEventBorrowed>| {
                    let mut batch: Vec<NostrEvent> = events.iter().map(to_core).collect();
                    // Group each chunk by time so events land shard-by-shard.
                    // Archives are not necessarily date-ordered, and writing in
                    // arbitrary month order thrashes the open-shard set: each
                    // switch can evict a writer that is needed again a moment
                    // later, paying a commit + fsync every time.
                    batch.sort_unstable_by_key(|e| e.created_at);
                    let mut p = pipe.lock().unwrap();
                    for ev in &batch {
                        p.process(ev);
                    }
                    drop(p);
                    total_cb.fetch_add(batch.len() as u64, Ordering::Relaxed);
                },
                chunk_size,
            );
        })
        .await?;

        // finalize backfill and (if no firehose) commit + exit
        pipeline.lock().unwrap().go_live();
        pipeline.lock().unwrap().commit()?;
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

fn to_core(ev: &nostr_archive_cursor::NostrEventBorrowed) -> NostrEvent {
    NostrEvent {
        id: ev.id.to_string(),
        pubkey: ev.pubkey.to_string(),
        created_at: ev.created_at,
        kind: ev.kind as u16,
        tags: ev.tags.iter().map(|t| t.iter().map(|s| s.to_string()).collect()).collect(),
        content: ev.content.to_string(),
        sig: ev.sig.to_string(),
    }
}
