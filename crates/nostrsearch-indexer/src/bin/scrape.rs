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
    discover_relays, normalize_relay_url, parse_date, RelayInfo, ScrapeConfig, ScrapeState, Sink,
};
use nostrsearch_indexer::{Pipeline, PipelineConfig, ShardWriterConfig};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

struct Args {
    index_root: PathBuf,
    state_dir: PathBuf,
    discover_only: bool,
    rediscover: bool,
    min_date: u64,
    max_relays: usize,
    min_sources: u32,
    concurrency: usize,
    floor_secs: u64,
    birthday_days: u32,
    seed_relays: Vec<String>,
    heap_mb: usize,
    commit_docs: u64,
    max_open_shards: usize,
    wot_out: Option<PathBuf>,
}

impl Args {
    fn parse() -> Result<Self, String> {
        use nostrsearch_indexer::env;
        let mut a = Args {
            index_root: env::index_root(),
            state_dir: env::state_dir(),
            discover_only: false,
            rediscover: false,
            min_date: parse_date("2022-01-01").unwrap(),
            max_relays: 300,
            min_sources: 3,
            concurrency: 12,
            floor_secs: 600,
            birthday_days: 14,
            seed_relays: Vec::new(),
            heap_mb: 64,
            commit_docs: 100_000,
            max_open_shards: env::max_open_shards(),
            wot_out: Some(env::wot_out()),
        };
        let mut it = std::env::args().skip(1);
        while let Some(arg) = it.next() {
            let mut val = |name: &str| it.next().ok_or(format!("{name} needs a value"));
            match arg.as_str() {
                "--index-root" => a.index_root = val("--index-root")?.into(),
                "--state-dir" => a.state_dir = val("--state-dir")?.into(),
                "--discover" => a.discover_only = true,
                "--rediscover" => a.rediscover = true,
                "--min-date" => {
                    a.min_date = parse_date(&val("--min-date")?).ok_or("bad --min-date")?
                }
                "--max-relays" => {
                    a.max_relays = val("--max-relays")?.parse().map_err(|_| "bad max-relays")?
                }
                "--min-sources" => {
                    a.min_sources = val("--min-sources")?.parse().map_err(|_| "bad min-sources")?
                }
                "--concurrency" => {
                    a.concurrency = val("--concurrency")?.parse().map_err(|_| "bad concurrency")?
                }
                "--floor-mins" => {
                    a.floor_secs = val("--floor-mins")?
                        .parse::<u64>()
                        .map_err(|_| "bad floor-mins")?
                        * 60
                }
                "--birthday-days" => {
                    a.birthday_days = val("--birthday-days")?
                        .parse()
                        .map_err(|_| "bad birthday-days")?
                }
                "--relay" => a.seed_relays.push(val("--relay")?),
                "--heap-mb" => a.heap_mb = val("--heap-mb")?.parse().map_err(|_| "bad heap-mb")?,
                "--commit-docs" => {
                    a.commit_docs = val("--commit-docs")?.parse().map_err(|_| "bad commit-docs")?
                }
                "--max-open-shards" => {
                    a.max_open_shards = val("--max-open-shards")?
                        .parse()
                        .map_err(|_| "bad max-open-shards")?
                }
                "--no-wot-out" => a.wot_out = None,
                "--help" | "-h" => {
                    return Err("scrape: fill index gaps from the whole relay network\n\
     --index-root <dir>    index root ($INDEX_ROOT)\n\
     --state-dir <dir>     pipeline + scrape state ($STATE_DIR)\n\
     --discover            refresh relay targets from kind-10002, print, exit\n\
     --rediscover          refresh targets, then scrape\n\
     --min-date <d>        stop walking backwards at YYYY-MM-DD (2022-01-01)\n\
     --max-relays <n>      scrape the top n relays by advertisers (300)\n\
     --min-sources <n>     min distinct advertisers to qualify (3)\n\
     --concurrency <n>     relays scraped in parallel (12)\n\
     --floor-mins <n>      smallest bisection window (10)\n\
     --birthday-days <n>   stop a relay after n consecutive empty days (14)\n\
     --relay <url>         extra seed relay (repeatable)"
                        .into());
                }
                other => return Err(format!("unknown arg: {other}")),
            }
        }
        Ok(a)
    }
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
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(hex.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

fn to_core(ev: &nostr_sdk::Event) -> NostrEvent {
    NostrEvent {
        id: ev.id.to_hex(),
        pubkey: ev.pubkey.to_hex(),
        created_at: ev.created_at.as_secs(),
        kind: ev.kind.as_u16(),
        tags: ev.tags.iter().map(|t| t.clone().to_vec()).collect(),
        content: ev.content.clone(),
        sig: ev.sig.to_string(),
    }
}

fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider().install_default().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,nostr_relay_pool=warn,nostr_sdk=warn".into()),
        )
        .init();
    let args = match Args::parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(2);
        }
    };

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
            .take(args.max_relays)
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
        wot_out: args.wot_out.clone(),
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

async fn run(
    args: Args,
    state: Arc<ScrapeState>,
    sink: Arc<PipelineSink>,
) -> anyhow::Result<()> {
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
                if let Err(e) = tokio::task::spawn_blocking(move || sink.checkpoint(false)).await
                {
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
        floor_secs: args.floor_secs,
        concurrency: args.concurrency,
        empty_days_limit: args.birthday_days,
    };
    nostrsearch_indexer::scrape::run_pass(state, sink.clone(), cfg).await;

    tracing::info!("final checkpoint");
    tokio::task::spawn_blocking(move || sink.checkpoint(true)).await??;
    Ok(())
}
