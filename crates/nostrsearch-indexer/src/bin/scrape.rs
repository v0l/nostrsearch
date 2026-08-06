//! Standalone full-network historical scrape (see `scrape` module docs).
//!
//! Owns its own [`Pipeline`] + `.dedupe` store, so it must not run while
//! another process holds the index writers. For continuous gap-filling inside
//! the unified server, the same engine runs there as a background task; this
//! binary exists for dedicated backfill sessions (Deployment scaled to 0).
//!
//! Usage:
//!   scrape --index-root ./data/index                     # discover + scrape
//!   scrape --discover                                    # refresh targets, print, exit
//!   scrape --relay wss://relay.damus.io --min-date 2023-01-01

use nostrsearch_core::event::NostrEvent;
use nostrsearch_indexer::id_store::IdStore;
use nostrsearch_indexer::scrape::{
    RelayInfo, ScrapeConfig, ScrapeState, Sink, discover_relays, normalize_relay_url, parse_date,
};
use nostrsearch_indexer::{Pipeline, PipelineConfig, ShardWriterConfig};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Fill index gaps by scraping the whole relay network.
///
/// Owns its own pipeline and `.dedupe` store, so it must not run while another
/// process holds the index writers.
#[derive(clap::Parser, Debug)]
#[command(name = "scrape", version)]
struct Args {
    /// Index root
    #[arg(long, value_name = "DIR", default_value_os_t = nostrsearch_indexer::env::index_root())]
    index_root: PathBuf,

    /// Pipeline + scrape state
    #[arg(long, value_name = "DIR", default_value_os_t = nostrsearch_indexer::env::state_dir())]
    state_dir: PathBuf,

    /// Refresh relay targets from kind-10002, print them, and exit
    #[arg(long = "discover")]
    discover_only: bool,

    /// Refresh targets, then scrape
    #[arg(long)]
    rediscover: bool,

    /// Stop walking backwards at this date
    #[arg(long, value_name = "YYYY-MM-DD", default_value = "2022-01-01",
          value_parser = parse_date_arg)]
    min_date: u64,

    /// Scrape the top N relays by advertiser count; 0 for no limit
    #[arg(long, value_name = "N", default_value_t = 0)]
    max_relays: usize,

    /// Minimum distinct advertisers for a relay to qualify
    #[arg(long, value_name = "N", default_value_t = 3)]
    min_sources: u32,

    /// Relays scraped in parallel
    #[arg(long, value_name = "N", default_value_t = 12)]
    concurrency: usize,

    /// Smallest bisection window, in minutes
    #[arg(long = "floor-mins", value_name = "N", default_value_t = 10,
          value_parser = clap::value_parser!(u64).range(1..))]
    floor_mins: u64,

    /// Stop a relay after N consecutive empty days
    #[arg(long, value_name = "N", default_value_t = 14)]
    birthday_days: u32,

    /// Extra seed relay (repeatable)
    #[arg(long = "relay", value_name = "URL")]
    seed_relays: Vec<String>,

    /// Tantivy writer heap per shard, in MB (charged per open shard)
    #[arg(long, value_name = "MB", default_value_t = 64)]
    heap_mb: usize,

    /// Commit every N docs per shard
    #[arg(long, value_name = "N", default_value_t = 100_000)]
    commit_docs: u64,

    /// Shard writers held open
    #[arg(long, value_name = "N", default_value_t = nostrsearch_indexer::env::max_open_shards())]
    max_open_shards: usize,

    /// Do not write a WoT snapshot
    #[arg(long)]
    no_wot_out: bool,

    /// Write the WoT snapshot here
    #[arg(long, value_name = "FILE", conflicts_with = "no_wot_out",
          default_value_os_t = nostrsearch_indexer::env::wot_out())]
    wot_out: PathBuf,
}

impl Args {
    /// Smallest bisection window, in seconds.
    fn floor_secs(&self) -> u64 {
        self.floor_mins * 60
    }

    /// Where to write the WoT snapshot, or `None` for --no-wot-out.
    fn wot_out(&self) -> Option<PathBuf> {
        (!self.no_wot_out).then(|| self.wot_out.clone())
    }
}

/// `YYYY-MM-DD` to a unix timestamp, for clap's value parser.
fn parse_date_arg(s: &str) -> Result<u64, String> {
    parse_date(s).ok_or_else(|| format!("expected YYYY-MM-DD, got {s:?}"))
}

/// Pipeline-backed sink for standalone runs: `.dedupe`-gated, checkpointed.
struct PipelineSink {
    pipeline: Arc<Mutex<Pipeline>>,
    store: Arc<IdStore>,
    /// Ids indexed since the last checkpoint (not yet durable in the store).
    pending: Mutex<HashSet<[u8; 32]>>,
    seen: AtomicU64,
    new: AtomicU64,
}

impl PipelineSink {
    /// Commit the index, then durably record the pending ids — in that order,
    /// so a crash re-fetches a window instead of leaving holes.
    fn checkpoint(&self, finish: bool) -> anyhow::Result<()> {
        let mut p = self.pipeline.lock().unwrap();
        if finish {
            p.finish()?;
        } else {
            p.commit()?;
        }
        let ids = std::mem::take(&mut *self.pending.lock().unwrap());
        self.store.flush(ids.iter())?;
        Ok(())
    }
}

impl Sink for PipelineSink {
    async fn missing(&self, ids: Vec<[u8; 32]>) -> Vec<[u8; 32]> {
        tokio::task::block_in_place(|| {
            let pending = self.pending.lock().unwrap();
            ids.into_iter()
                .filter(|id| !pending.contains(id) && !self.store.contains(id))
                .collect()
        })
    }

    async fn process(&self, events: Vec<nostr_sdk::Event>) -> u64 {
        self.seen.fetch_add(events.len() as u64, Ordering::Relaxed);
        tokio::task::block_in_place(|| {
            let mut p = self.pipeline.lock().unwrap();
            let mut pending = self.pending.lock().unwrap();
            let mut new = 0u64;
            for ev in &events {
                let core = to_core(ev);
                let Some(id) = hex32(&core.id) else { continue };
                if pending.contains(&id) || self.store.contains(&id) {
                    continue;
                }
                p.process(&core);
                pending.insert(id);
                new += 1;
            }
            self.new.fetch_add(new, Ordering::Relaxed);
            new
        })
    }
}

fn hex32(hex: &str) -> Option<[u8; 32]> {
    nostrsearch_indexer::archive_ingest::hex32(hex)
}

fn to_core(ev: &nostr_sdk::Event) -> NostrEvent {
    nostrsearch_indexer::firehose::to_core(ev)
}

fn main() -> anyhow::Result<()> {
    // Before the subscriber, so --help/--version print without log lines.
    let args = <Args as clap::Parser>::parse();

    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,nostr_relay_pool=warn,nostr_sdk=warn".into()),
        )
        .init();
    let state = Arc::new(ScrapeState::open(&args.state_dir.join("scrape"))?);

    // Target discovery from kind-10002 lists already in the index.
    let have_targets = !state.relays().is_empty();
    if args.discover_only || args.rediscover || !have_targets {
        tracing::info!(root = %args.index_root.display(), "discovering relays from kind-10002");
        let found = discover_relays(&args.index_root)?;
        let existing: std::collections::HashMap<String, RelayInfo> =
            state.relays().into_iter().collect();
        let mut kept = 0;
        for (url, sources) in found
            .iter()
            .filter(|(_, n)| *n >= args.min_sources)
            .take(if args.max_relays == 0 {
                usize::MAX
            } else {
                args.max_relays
            })
        {
            let mut info = existing.get(url).cloned().unwrap_or_default();
            info.sources = *sources;
            state.put_relay(url, &info);
            kept += 1;
        }
        tracing::info!(found = found.len(), kept, "relay discovery complete");
    }
    for url in &args.seed_relays {
        if let Some(u) = normalize_relay_url(url) {
            let mut info = RelayInfo::default();
            info.sources = u32::MAX; // manual seeds always qualify
            state.put_relay(&u, &info);
        }
    }
    if args.discover_only {
        for (url, info) in state.relays() {
            println!("{url}\t{}", info.sources);
        }
        return Ok(());
    }

    let cfg = PipelineConfig {
        index_root: args.index_root.clone(),
        shard: ShardWriterConfig {
            heap_bytes: args.heap_mb * 1_000_000,
            commit_every_docs: args.commit_docs,
            max_open_shards: args.max_open_shards,
            writer_threads: 1,
            ..Default::default()
        },
        state_dir: Some(args.state_dir.clone()),
        wot_refresh_every: nostrsearch_indexer::env::wot_refresh_every(),
        min_refresh_interval: nostrsearch_indexer::env::min_refresh_interval(),
        persist_interval: nostrsearch_indexer::env::persist_interval(),
        wot_out: args.wot_out(),
    };
    let pipeline = Arc::new(Mutex::new(Pipeline::new(cfg)?));
    // Scraped events are new arrivals: fold them as live, not backfill.
    pipeline.lock().unwrap().go_live();

    let sink = Arc::new(PipelineSink {
        pipeline,
        store: Arc::new(IdStore::open(&args.index_root.join(".dedupe"))?),
        pending: Mutex::new(HashSet::new()),
        seen: AtomicU64::new(0),
        new: AtomicU64::new(0),
    });

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(run(args, state, sink))
}

async fn run(args: Args, state: Arc<ScrapeState>, sink: Arc<PipelineSink>) -> anyhow::Result<()> {
    // SIGTERM (PID 1 in a container): checkpoint and exit cleanly.
    {
        let sink = sink.clone();
        tokio::spawn(async move {
            let Ok(mut term) =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            else {
                return;
            };
            term.recv().await;
            tracing::warn!("SIGTERM: checkpointing before exit");
            let _ = tokio::task::spawn_blocking(move || sink.checkpoint(true)).await;
            std::process::exit(143);
        });
    }

    // Periodic checkpoint + progress line.
    {
        let sink = sink.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
            tick.tick().await;
            loop {
                tick.tick().await;
                let sink = sink.clone();
                if let Err(e) = tokio::task::spawn_blocking(move || sink.checkpoint(false)).await {
                    tracing::warn!(error = ?e, "checkpoint failed");
                }
            }
        });
    }
    {
        let sink = sink.clone();
        tokio::spawn(async move {
            let started = std::time::Instant::now();
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                let (rss, _) = nostrsearch_indexer::mem::rss_mb();
                eprintln!(
                    "  scraped seen={} new={} elapsed={:.0}s rss={rss}MB",
                    sink.seen.load(Ordering::Relaxed),
                    sink.new.load(Ordering::Relaxed),
                    started.elapsed().as_secs_f64(),
                );
            }
        });
    }

    let cfg = ScrapeConfig {
        min_date: args.min_date,
        floor_secs: args.floor_secs(),
        concurrency: args.concurrency,
        empty_days_limit: args.birthday_days,
    };
    nostrsearch_indexer::scrape::run_pass(state, sink.clone(), cfg).await;

    tracing::info!("final checkpoint");
    tokio::task::spawn_blocking(move || sink.checkpoint(true)).await??;
    Ok(())
}
