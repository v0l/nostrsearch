//! Ingest CLI: load Nostr JSONL archives into sharded Tantivy indices.
//!
//! Uses `nostr-archive-cursor` (`NostrCursor`) to walk a directory of dumps —
//! handling `.jsonl`/`.json`/`.zst`/`.gz`/`.bz2`, parallel chunked reads, and
//! event-id dedup — and routes each event into the time-sharded `ShardManager`.
//!
//! Usage:
//!   ingest --index-root ./data/index --input-dir /core/Backup/nostr/source
//!   ingest --index-root ./data/index --input-dir ./dumps --parallelism 8 --no-dedupe

use nostr_archive_cursor::NostrCursor;
use nostrsearch_core::event::NostrEvent;
use nostrsearch_indexer::{ShardManager, ShardWriterConfig};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Debug)]
struct Args {
    index_root: PathBuf,
    input_dir: PathBuf,
    heap_mb: usize,
    commit_docs: u64,
    parallelism: usize,
    chunk_size: usize,
    dedupe: bool,
}

impl Args {
    fn parse() -> Result<Self, String> {
        let mut index_root = PathBuf::from("./data/index");
        let mut input_dir = None;
        let mut heap_mb = 512usize;
        let mut commit_docs = 200_000u64;
        let mut parallelism = 0usize; // 0 = auto
        let mut chunk_size = 2_000usize;
        let mut dedupe = true;

        let mut it = std::env::args().skip(1);
        while let Some(a) = it.next() {
            match a.as_str() {
                "--index-root" => index_root = PathBuf::from(it.next().ok_or("--index-root value")?),
                "--input-dir" => input_dir = Some(PathBuf::from(it.next().ok_or("--input-dir value")?)),
                "--heap-mb" => heap_mb = it.next().ok_or("--heap-mb value")?.parse().map_err(|_| "bad heap")?,
                "--commit-docs" => commit_docs = it.next().ok_or("--commit-docs value")?.parse().map_err(|_| "bad commit-docs")?,
                "--parallelism" => parallelism = it.next().ok_or("--parallelism value")?.parse().map_err(|_| "bad parallelism")?,
                "--chunk-size" => chunk_size = it.next().ok_or("--chunk-size value")?.parse().map_err(|_| "bad chunk-size")?,
                "--no-dedupe" => dedupe = false,
                "-h" | "--help" => {
                    println!("{}", help());
                    std::process::exit(0);
                }
                other => return Err(format!("unknown arg: {other}")),
            }
        }

        let input_dir = input_dir.ok_or("missing --input-dir <dir>")?;
        Ok(Self {
            index_root,
            input_dir,
            heap_mb,
            commit_docs,
            parallelism,
            chunk_size,
            dedupe,
        })
    }
}

fn help() -> String {
    "nostrsearch ingest (via nostr-archive-cursor)\n\
     --index-root <dir>     index output root (default ./data/index)\n\
     --input-dir <dir>      directory of .jsonl/.json/.zst/.gz/.bz2 dumps\n\
     --heap-mb <n>          tantivy writer heap per shard (default 512)\n\
     --commit-docs <n>      commit every N docs per shard (default 200000)\n\
     --parallelism <n>      files read in parallel (default: num cores)\n\
     --chunk-size <n>       events per read chunk (default 2000)\n\
     --no-dedupe            disable event-id dedup (faster, may index dupes)"
        .to_string()
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "nostrsearch=info,nostr_archive_cursor=warn".into()),
        )
        .init();

    let args = Args::parse().map_err(anyhow::Error::msg)?;

    if !args.input_dir.is_dir() {
        anyhow::bail!("--input-dir {} is not a directory", args.input_dir.display());
    }

    let cfg = ShardWriterConfig {
        heap_bytes: args.heap_mb * 1_000_000,
        commit_every_docs: args.commit_docs,
        ..Default::default()
    };

    let manager = Arc::new(Mutex::new(ShardManager::new(&args.index_root, cfg)));
    let total = Arc::new(AtomicU64::new(0));
    let started = Instant::now();

    // progress reporter
    let total_prog = total.clone();
    std::thread::spawn(move || loop {
        std::thread::sleep(std::time::Duration::from_secs(5));
        let done = total_prog.load(Ordering::Relaxed);
        if done > 0 {
            let rate = done as f64 / started.elapsed().as_secs_f64();
            eprintln!("  indexed={done}  rate={rate:.0}/s  elapsed={:.0}s", started.elapsed().as_secs_f64());
        }
    });

    let parallelism = if args.parallelism == 0 {
        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4)
    } else {
        args.parallelism
    };

    tracing::info!(
        dir = %args.input_dir.display(),
        parallelism,
        chunk_size = args.chunk_size,
        dedupe = args.dedupe,
        "starting ingest"
    );

    // The cursor drives parallel file reads; the callback converts each
    // borrowed event into our core type and feeds the shard manager.
    let total_cb = total.clone();
    let manager_cb = manager.clone();
    let cursor = NostrCursor::new(args.input_dir.clone())
        .with_parallelism(parallelism)
        .with_dedupe(args.dedupe);

    cursor.walk_with_chunked_sync(
        move |events: Vec<nostr_archive_cursor::NostrEventBorrowed>| {
            let mut batch: Vec<NostrEvent> = Vec::with_capacity(events.len());
            for ev in &events {
                batch.push(NostrEvent {
                    id: ev.id.to_string(),
                    pubkey: ev.pubkey.to_string(),
                    created_at: ev.created_at,
                    kind: ev.kind as u16,
                    tags: ev
                        .tags
                        .iter()
                        .map(|t| t.iter().map(|s| s.to_string()).collect())
                        .collect(),
                    content: ev.content.to_string(),
                    sig: ev.sig.to_string(),
                });
            }
            let mut mgr = manager_cb.lock().unwrap();
            for ev in &batch {
                if let Err(e) = mgr.index_event(ev) {
                    tracing::warn!(error = %e, "index_event failed");
                }
            }
            drop(mgr);
            total_cb.fetch_add(batch.len() as u64, Ordering::Relaxed);
        },
        args.chunk_size,
    );

    // final commit
    manager.lock().unwrap().commit_all()?;

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
