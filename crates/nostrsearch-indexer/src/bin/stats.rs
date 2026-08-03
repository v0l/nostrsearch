//! Stats/WoT backfill CLI: stream a Nostr JSONL archive through the analysis
//! pipeline and emit a web-of-trust snapshot for search scoring.
//!
//! Streams dumps via `nostr-archive-cursor` (same source as `ingest`), folds
//! kind-3 contact lists through the `FollowGraph` + `Pagerank` producers,
//! materializes the `World`, and writes a compact `WotIndex` that `ingest
//! --wot <file>` consumes to populate the `wot_tier` scoring signal.
//!
//! Analysis state is persisted (resumable, additive) under `--state-dir`.
//!
//! Usage:
//!   stats --input-dir ./dumps --state-dir ./data/stats --wot-out ./data/wot.bin

use clap::Parser;
use nostr_archive_cursor::NostrCursor;
use nostrsearch_core::event::NostrEvent;
use nostrsearch_stats::analyses::{FollowGraph, Pagerank};
use nostrsearch_stats::{Registry, StatStore, World, WotIndex};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Stats/WoT backfill: stream a Nostr JSONL archive through the analysis
/// pipeline and emit a web-of-trust snapshot for search scoring.
///
/// Defaults come from $STATE_DIR / $WOT_OUT, the same variables the server
/// node and `ingest` read; flags override them.
#[derive(clap::Parser, Debug)]
#[command(name = "stats", version)]
struct Args {
    /// Directory of .jsonl/.json/.zst/.gz/.bz2 dumps
    #[arg(long, value_name = "DIR", required = true)]
    input_dir: PathBuf,

    /// Analysis state store
    #[arg(long, value_name = "DIR", default_value_os_t = nostrsearch_indexer::env::state_dir())]
    state_dir: PathBuf,

    /// Output WoT snapshot
    #[arg(long, value_name = "FILE", default_value_os_t = nostrsearch_indexer::env::wot_out())]
    wot_out: PathBuf,

    /// Files read in parallel [default: available cores]
    #[arg(long, value_name = "N", default_value_t = 0, hide_default_value = true)]
    parallelism: usize,

    /// Events per read chunk
    #[arg(long, value_name = "N", default_value_t = 2_000)]
    chunk_size: usize,

    /// Disable event-id dedup
    #[arg(long = "no-dedupe", action = clap::ArgAction::SetFalse)]
    dedupe: bool,
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn main() -> anyhow::Result<()> {
    // Before the subscriber, so --help/--version print without log lines.
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "nostrsearch=info,stats=info,nostr_archive_cursor=warn".into()),
        )
        .init();

    if !args.input_dir.is_dir() {
        anyhow::bail!(
            "--input-dir {} is not a directory",
            args.input_dir.display()
        );
    }

    let store = StatStore::new(&args.state_dir)?;

    // WoT producers. Both are stage-0 (independent), so a single streaming pass
    // is correct. Add dependent consumers via the staged runner, not here.
    let mut registry = Registry::new();
    registry
        .register(FollowGraph::default())
        .register(Pagerank::default());
    registry.load(&store)?;

    if !registry.needs_backfill() {
        tracing::info!("all analyses already backfilled; recomputing from persisted state");
    }

    let now = now_secs();
    let registry = Arc::new(Mutex::new(registry));
    let world = World::new(); // producers don't read the world during observe
    let total = Arc::new(AtomicU64::new(0));
    let started = Instant::now();

    // progress reporter
    let total_prog = total.clone();
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(std::time::Duration::from_secs(5));
            let done = total_prog.load(Ordering::Relaxed);
            if done > 0 {
                let rate = done as f64 / started.elapsed().as_secs_f64();
                eprintln!(
                    "  scanned={done}  rate={rate:.0}/s  elapsed={:.0}s",
                    started.elapsed().as_secs_f64()
                );
            }
        }
    });

    let parallelism = if args.parallelism == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    } else {
        args.parallelism
    };

    tracing::info!(dir = %args.input_dir.display(), parallelism, "starting stats backfill");

    let total_cb = total.clone();
    let registry_cb = registry.clone();
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
            let mut reg = registry_cb.lock().unwrap();
            for ev in &batch {
                reg.observe_backfill(ev, now, &world);
            }
            drop(reg);
            total_cb.fetch_add(batch.len() as u64, Ordering::Relaxed);
        },
        args.chunk_size,
    );

    // Materialize producers (runs pagerank's scheduled refresh) → World → WoT.
    let mut reg = Arc::try_unwrap(registry)
        .map_err(|_| anyhow::anyhow!("registry still shared"))?
        .into_inner()
        .unwrap();
    let mut world = World::new();
    let now_wall = now_secs();
    reg.materialize_all(now_wall, &mut world)?;
    reg.mark_all_backfilled()?;
    reg.persist(&store)?;

    let wot = WotIndex::from_world(&world);
    if let Some(parent) = args.wot_out.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    wot.save(&args.wot_out)?;

    let done = total.load(Ordering::Relaxed);
    let secs = started.elapsed().as_secs_f64();
    println!(
        "stats complete: scanned {} events in {:.1}s = {:.0}/s; WoT entries={} → {}",
        done,
        secs,
        done as f64 / secs,
        wot.len(),
        args.wot_out.display()
    );
    Ok(())
}
