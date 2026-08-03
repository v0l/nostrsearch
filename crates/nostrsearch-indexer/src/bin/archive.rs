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

use nostr_archive_cursor::{DefaultJsonFilesDatabase, JsonFilesDatabase, RocksDbIndex};
use std::path::PathBuf;

struct Args {
    dir: PathBuf,
    stats: bool,
    index_new: bool,
    rebuild_index: bool,
    compact: bool,
    locate: Option<String>,
}

impl Args {
    fn parse() -> Result<Self, String> {
        // Same env contract as the other binaries; --dir overrides.
        let mut dir = nostrsearch_indexer::env::archive_dir();
        let mut stats = false;
        let mut index_new = false;
        let mut rebuild_index = false;
        let mut compact = false;
        let mut locate = None;

        let mut it = std::env::args().skip(1);
        while let Some(a) = it.next() {
            match a.as_str() {
                "--dir" => dir = Some(PathBuf::from(it.next().ok_or("--dir value")?)),
                "--stats" => stats = true,
                "--index-new" => index_new = true,
                "--compact" => compact = true,
                "--locate" => locate = Some(it.next().ok_or("--locate <event id hex>")?),
                "--rebuild-index" => rebuild_index = true,
                "-h" | "--help" => {
                    println!("{}", help());
                    std::process::exit(0);
                }
                other => return Err(format!("unknown arg: {other}")),
            }
        }
        let dir = dir.ok_or("missing --dir <archive dir> (or set ARCHIVE_DIR)")?;
        if !(stats || index_new || rebuild_index || compact || locate.is_some()) {
            return Err(
                "pick one of --stats / --index-new / --rebuild-index / --compact / --locate".into(),
            );
        }
        Ok(Self {
            dir,
            stats,
            index_new,
            rebuild_index,
            compact,
            locate,
        })
    }
}

fn help() -> String {
    "nostrsearch archive maintenance\n\
     --dir <dir>        archive directory (.jsonl.zst files + id index)\n\
     --stats            report file count / sizes / indexed event count\n\
     --index-new        index shards that are new or changed since the last pass\n\
     \x20                 (incremental; reframes single-frame imports so lookups\n\
     \x20                 decode one frame instead of the whole file)\n\
     --rebuild-index    wipe and rebuild the id index from all archive files (O(n));\n\
     \x20                 records every event's location and the cached count\n\
     --compact          force a full RocksDB compaction of the index (recompresses\n\
     \x20                 data written by older versions; ~-24% on a static index)\n\
     --locate <id>      print where an event lives (shard file, offset, length)"
        .to_string()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "nostrsearch=info,archive=info".into()),
        )
        .init();

    let args = Args::parse().map_err(anyhow::Error::msg)?;
    // Open the index ourselves so `--compact` has a handle to it; the database
    // wraps the same one, exactly as `DefaultJsonFilesDatabase::new` would.
    let index = RocksDbIndex::open(args.dir.join("index-rocksdb"))?;
    let mut db: DefaultJsonFilesDatabase =
        JsonFilesDatabase::new_with_index(&args.dir, index.clone())?;

    if args.stats {
        let files = db.list_files().await?;
        let total: u64 = files.iter().map(|f| f.size).sum();
        println!("archive dir : {}", args.dir.display());
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
