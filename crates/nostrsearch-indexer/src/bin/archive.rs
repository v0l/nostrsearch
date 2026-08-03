//! Archive maintenance CLI — absorbs nostrhole's maintenance commands.
//!
//! Operates on the `.jsonl.zst` corpus + RocksDB id index that the unified
//! ingest writes and the server publishes.
//!
//! Usage:
//!   archive --dir /data/archive --stats
//!   archive --dir /data/archive --index-new
//!   archive --dir /data/archive --rebuild-index
//!   archive --dir /data/archive --compact

use clap::Parser;
use nostr_archive_cursor::{DefaultJsonFilesDatabase, JsonFilesDatabase, RocksDbIndex};
use std::path::PathBuf;

/// Archive maintenance: the .jsonl.zst corpus + its RocksDB id index.
///
/// The directory defaults to $ARCHIVE_DIR, the same variable the server node
/// and `ingest` read.
#[derive(Parser, Debug)]
#[command(name = "archive", version)]
struct Args {
    /// Archive directory (.jsonl.zst files + id index)
    #[arg(long, value_name = "DIR")]
    dir: Option<PathBuf>,

    /// Report file count / sizes / indexed event count
    #[arg(long)]
    stats: bool,

    /// Index shards that are new or changed since the last pass.
    ///
    /// Incremental, and reframes single-frame imports so lookups decode one
    /// frame instead of the whole file.
    #[arg(long)]
    index_new: bool,

    /// Wipe and rebuild the id index from all archive files (O(n)).
    ///
    /// Records every event's location and the cached count.
    #[arg(long)]
    rebuild_index: bool,

    /// Force a full RocksDB compaction of the index.
    ///
    /// Recompresses data written by older versions; about -24% on a static
    /// index.
    #[arg(long)]
    compact: bool,

    /// Print where an event lives (shard file, offset, length)
    #[arg(long, value_name = "ID")]
    locate: Option<String>,
}

impl Args {
    /// The archive directory, from `--dir` or $ARCHIVE_DIR.
    fn dir(&self) -> Result<PathBuf, String> {
        self.dir
            .clone()
            .or_else(nostrsearch_indexer::env::archive_dir)
            .ok_or_else(|| "missing --dir <archive dir> (or set ARCHIVE_DIR)".to_string())
    }

    /// At least one action, or there is nothing to do.
    fn check_has_action(&self) -> Result<(), String> {
        if self.stats
            || self.index_new
            || self.rebuild_index
            || self.compact
            || self.locate.is_some()
        {
            Ok(())
        } else {
            Err("pick one of --stats / --index-new / --rebuild-index / --compact / --locate".into())
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Before the subscriber, so --help/--version print without log lines.
    let args = Args::parse();
    args.check_has_action().map_err(anyhow::Error::msg)?;
    let dir = args.dir().map_err(anyhow::Error::msg)?;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "nostrsearch=info,archive=info".into()),
        )
        .init();

    // Open the index ourselves so `--compact` has a handle to it; the database
    // wraps the same one, exactly as `DefaultJsonFilesDatabase::new` would.
    let index = RocksDbIndex::open(dir.join("index-rocksdb"))?;
    let mut db: DefaultJsonFilesDatabase = JsonFilesDatabase::new_with_index(&dir, index.clone())?;

    if args.stats {
        let files = db.list_files().await?;
        let total: u64 = files.iter().map(|f| f.size).sum();
        println!("archive dir : {}", dir.display());
        println!("files       : {}", files.len());
        println!(
            "total size  : {:.2} GiB",
            total as f64 / 1024.0 / 1024.0 / 1024.0
        );
        println!("indexed ids : {}", db.count_keys());
        println!("index empty : {}", db.is_index_empty());
    }

    if args.index_new {
        tracing::info!("indexing new/changed shards");
        let r = db.index_new_shards()?;
        println!(
            "shards: {} (unchanged {}, indexed {}, reframed {}), new events: {}",
            r.shards, r.unchanged, r.indexed, r.reframed, r.new_events
        );
    }

    if args.rebuild_index {
        tracing::info!("rebuilding id index from all archive files (O(n) scan)");
        db.rebuild_index()?;
        println!("index rebuild complete: {} events indexed", db.count_keys());
    }

    if let Some(id) = &args.locate {
        let id = nostr_sdk::prelude::EventId::from_hex(id.trim())
            .map_err(|e| anyhow::anyhow!("invalid event id: {e}"))?;
        match db.locate(&id)? {
            Some(loc) => {
                let shard = db
                    .shard_path(loc.shard)
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| format!("shard#{:016x} (not in this directory)", loc.shard));
                println!(
                    "{id}\n  shard  : {shard}\n  offset : {}\n  len    : {}",
                    loc.offset, loc.len
                );
            }
            // Either the id is unknown, or its index entry predates locations
            // (v0, `created_at` only) -- `--rebuild-index` fixes the latter.
            None => println!("{id}: no recorded location"),
        }
    }

    if args.compact {
        tracing::info!("compacting the id index (one full rewrite)");
        index.compact();
        println!("index compaction complete");
    }

    Ok(())
}
