//! Archive maintenance CLI — absorbs nostrhole's maintenance commands.
//!
//! Operates on the `.jsonl.zst` corpus + RocksDB id index that the unified
//! ingest writes and the server publishes.
//!
//! Usage:
//!   archive --dir /data/archive --stats
//!   archive --dir /data/archive --rebuild-index
//!   archive --dir /data/archive --repair-count

use nostr_archive_cursor::DefaultJsonFilesDatabase;
use std::path::PathBuf;

struct Args {
    dir: PathBuf,
    stats: bool,
    rebuild_index: bool,
}

impl Args {
    fn parse() -> Result<Self, String> {
        // Same env contract as the other binaries; --dir overrides.
        let mut dir = nostrsearch_indexer::env::archive_dir();
        let mut stats = false;
        let mut rebuild_index = false;

        let mut it = std::env::args().skip(1);
        while let Some(a) = it.next() {
            match a.as_str() {
                "--dir" => dir = Some(PathBuf::from(it.next().ok_or("--dir value")?)),
                "--stats" => stats = true,
                "--rebuild-index" => rebuild_index = true,
                "-h" | "--help" => {
                    println!("{}", help());
                    std::process::exit(0);
                }
                other => return Err(format!("unknown arg: {other}")),
            }
        }
        let dir = dir.ok_or("missing --dir <archive dir> (or set ARCHIVE_DIR)")?;
        if !(stats || rebuild_index) {
            return Err("pick one of --stats / --rebuild-index".into());
        }
        Ok(Self {
            dir,
            stats,
            rebuild_index,
        })
    }
}

fn help() -> String {
    "nostrsearch archive maintenance\n\
     --dir <dir>        archive directory (.jsonl.zst files + id index)\n\
     --stats            report file count / sizes / indexed event count\n\
     --rebuild-index    wipe and rebuild the id index from all archive files (O(n));\n\
     \x20                 also recomputes the cached event count"
        .to_string()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "nostrsearch=info".into()),
        )
        .init();

    let args = Args::parse().map_err(anyhow::Error::msg)?;
    let mut db = DefaultJsonFilesDatabase::new(&args.dir)?;

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

    if args.rebuild_index {
        tracing::info!("rebuilding id index from all archive files (O(n) scan)");
        db.rebuild_index()?;
        println!("index rebuild complete: {} events indexed", db.count_keys());
    }

    Ok(())
}
