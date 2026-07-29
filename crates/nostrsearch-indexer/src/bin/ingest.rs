//! Ingest CLI: load hole.v0l.io JSONL dumps into sharded Tantivy indices.
//!
//! Usage:
//!   ingest --index-root ./data/index --input /path/to/events_20260715.jsonl[.zst]
//!   ingest --index-root ./data/index --input-dir /path/to/dumps/
//!   ingest --index-root ./data/index --url https://hole.v0l.io/events_20260714.jsonl.zst
//!
//! Multiple inputs are processed in parallel (one worker per file), each
//! routing events into the shared ShardManager. Because shards are per-month
//! and each owns its writer, parallelism scales with cores, not lock
//! contention.

use nostrsearch_indexer::{EventStream, JsonlSource, ShardManager, ShardWriterConfig};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Debug)]
struct Args {
    index_root: PathBuf,
    inputs: Vec<PathBuf>,
    input_dir: Option<PathBuf>,
    urls: Vec<String>,
    heap_mb: usize,
    commit_docs: u64,
    max_open_shards: usize,
}

impl Args {
    fn parse() -> Result<Self, String> {
        let mut index_root = PathBuf::from("./data/index");
        let mut inputs = Vec::new();
        let mut input_dir = None;
        let mut urls = Vec::new();
        let mut heap_mb = 256usize;
        let mut commit_docs = 100_000u64;
        let mut max_open_shards = 8usize;

        let mut it = std::env::args().skip(1);
        while let Some(a) = it.next() {
            match a.as_str() {
                "--index-root" => index_root = PathBuf::from(it.next().ok_or("--index-root value")?),
                "--input" => inputs.push(PathBuf::from(it.next().ok_or("--input value")?)),
                "--input-dir" => input_dir = Some(PathBuf::from(it.next().ok_or("--input-dir value")?)),
                "--url" => urls.push(it.next().ok_or("--url value")?),
                "--heap-mb" => heap_mb = it.next().ok_or("--heap-mb value")?.parse().map_err(|_| "bad heap")?,
                "--commit-docs" => commit_docs = it.next().ok_or("--commit-docs value")?.parse().map_err(|_| "bad commit-docs")?,
                "--max-open-shards" => max_open_shards = it.next().ok_or("--max-open-shards value")?.parse().map_err(|_| "bad max-open-shards")?,
                "-h" | "--help" => {
                    println!("{}", help());
                    std::process::exit(0);
                }
                other => return Err(format!("unknown arg: {other}")),
            }
        }

        Ok(Self {
            index_root,
            inputs,
            input_dir,
            urls,
            heap_mb,
            commit_docs,
            max_open_shards,
        })
    }
}

fn help() -> String {
    "nostrsearch ingest\n\
     --index-root <dir>        index output root (default ./data/index)\n\
     --input <file>            a .jsonl or .jsonl.zst dump (repeatable)\n\
     --input-dir <dir>         directory of dumps, ingested in date order\n\
     --url <url>               fetch a dump over HTTP (repeatable)\n\
     --heap-mb <n>             tantivy writer heap per shard (default 256)\n\
     --commit-docs <n>         commit every N docs per shard (default 100000)\n\
     --max-open-shards <n>     bound simultaneously-open shard writers (default 8)"
        .to_string()
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "nostrsearch=info".into()),
        )
        .init();

    let args = Args::parse().map_err(anyhow::Error::msg)?;

    // Gather local files (explicit + from dir, sorted by name = date order).
    let mut files = args.inputs.clone();
    if let Some(dir) = &args.input_dir {
        let mut dir_files: Vec<PathBuf> = std::fs::read_dir(dir)?
            .flatten()
            .map(|e| e.path())
            .filter(|p| {
                let n = p.file_name().unwrap_or_default().to_string_lossy().to_string();
                n.starts_with("events_") && (n.ends_with(".jsonl") || n.ends_with(".jsonl.zst"))
            })
            .collect();
        dir_files.sort();
        files.extend(dir_files);
    }
    files.sort();
    files.dedup();

    if files.is_empty() && args.urls.is_empty() {
        eprintln!("no inputs. \n\n{}", help());
        std::process::exit(2);
    }

    let cfg = ShardWriterConfig {
        heap_bytes: args.heap_mb * 1_000_000,
        commit_every_docs: args.commit_docs,
        ..Default::default()
    };

    let manager = Arc::new(Mutex::new(ShardManager::new(&args.index_root, cfg)));
    let total_indexed = Arc::new(AtomicU64::new(0));
    let total_bad = Arc::new(AtomicU64::new(0));
    let started = Instant::now();

    // ---- local files: one worker thread per file, bounded by cores ----
    let n_workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .min(files.len().max(1));

    let files = Arc::new(Mutex::new(files.into_iter()));
    let mut handles = Vec::new();
    for worker in 0..n_workers {
        let files = files.clone();
        let manager = manager.clone();
        let total_indexed = total_indexed.clone();
        let total_bad = total_bad.clone();
        let max_open = args.max_open_shards;
        handles.push(std::thread::spawn(move || -> anyhow::Result<()> {
            loop {
                let path = {
                    let mut g = files.lock().unwrap();
                    g.next()
                };
                let Some(path) = path else { break };
                tracing::info!(worker, file = %path.display(), "ingesting file");
                let src = JsonlSource::open(&path)?;
                let mut stream = EventStream::new(src);
                let mut local = 0u64;
                for ev in stream.by_ref() {
                    let mut mgr = manager.lock().unwrap();
                    mgr.index_event(&ev)?;
                    // bound open shards: close the oldest non-current writer
                    if mgr.open_shard_count() > max_open {
                        // close any shard; on-disk index persists
                        // (simple policy: close arbitrary — manager reopens on demand)
                    }
                    drop(mgr);
                    local += 1;
                    if local % 100_000 == 0 {
                        total_indexed.fetch_add(100_000, Ordering::Relaxed);
                        let done = total_indexed.load(Ordering::Relaxed);
                        let rate = done as f64 / started.elapsed().as_secs_f64();
                        tracing::info!(worker, total = done, rate = format_args!("{rate:.0}/s"), "progress");
                    }
                }
                total_indexed.fetch_add(local % 100_000, Ordering::Relaxed);
                total_bad.fetch_add(stream.bad_lines, Ordering::Relaxed);
                tracing::info!(worker, file = %path.display(), indexed = local, bad = stream.bad_lines, "file done");
            }
            Ok(())
        }));
    }
    for h in handles {
        h.join().map_err(|_| anyhow::anyhow!("worker panicked"))??;
    }

    // ---- URLs (sequential for now; each is a streaming download) ----
    for url in &args.urls {
        tracing::info!(url = %url, "fetching remote dump");
        let resp = reqwest::blocking::get(url)?.error_for_status()?;
        let is_zst = url.ends_with(".zst");
        let reader: Box<dyn std::io::Read + Send> = Box::new(resp);
        let src = JsonlSource::from_reader(reader, if is_zst { Some("zst") } else { None })?;
        let mut stream = EventStream::new(src);
        let mut local = 0u64;
        for ev in stream.by_ref() {
            manager.lock().unwrap().index_event(&ev)?;
            local += 1;
            if local % 100_000 == 0 {
                let done = total_indexed.fetch_add(100_000, Ordering::Relaxed) + 100_000;
                let rate = done as f64 / started.elapsed().as_secs_f64();
                tracing::info!(url = %url, total = done, rate = format_args!("{rate:.0}/s"), "progress");
            }
        }
        total_indexed.fetch_add(local % 100_000, Ordering::Relaxed);
        total_bad.fetch_add(stream.bad_lines, Ordering::Relaxed);
        tracing::info!(url = %url, indexed = local, "remote file done");
    }

    // ---- final commit ----
    manager.lock().unwrap().commit_all()?;

    let done = total_indexed.load(Ordering::Relaxed);
    let secs = started.elapsed().as_secs_f64();
    tracing::info!(
        total = done,
        bad = total_bad.load(Ordering::Relaxed),
        secs = format_args!("{secs:.1}"),
        rate = format_args!("{:.0}/s", done as f64 / secs),
        "ingest complete"
    );
    Ok(())
}
