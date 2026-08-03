//! Archive serving — absorbs nostrhole's HTTP role.
//!
//! Serves the raw `.jsonl.zst` corpus files (what `hole.v0l.io` publishes) plus
//! a directory listing, from the same `JsonFilesDatabase` the unified ingest
//! writes. Ported from nostrhole's hyper handler to axum so it shares a router,
//! middleware, and port with whatever else the process is serving.
//!
//! This lives in its own crate because two binaries serve these routes: the
//! server node, and `ingest`, which publishes the corpus from the same process
//! that is writing it during a long backfill. The server depends on the
//! indexer, so the shared half cannot live there without a dependency cycle.
//!
//! Routes:
//! - `GET /archive`            — HTML listing of archive files + totals
//! - `GET /archive/files`      — JSON listing (name, size, timestamp)
//! - `GET /archive/event/{id}` — one event by id, straight from the index
//! - `GET /archive/{file}`     — stream one archive file

pub mod theme;

use axum::body::Body;
use axum::extract::{Path as AxPath, State};
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use nostr_archive_cursor::{DefaultJsonFilesDatabase, IndexReport};
use nostr_sdk::prelude::EventId;
use serde::Serialize;
use std::sync::Arc;
use tokio_util::io::ReaderStream;

/// Shared handle to the archive.
///
/// **Lock note:** the archive's RocksDB id index is exclusive to one process.
/// Serving files is pure filesystem work, so by default we open in *index-free*
/// mode — the server can publish the corpus while the ingest process holds the
/// index lock and writes to it. Only [`open_with_index`](ArchiveState::open_with_index)
/// takes the lock, which is required for the relay (it must persist events).
#[derive(Clone)]
pub struct ArchiveState {
    pub dir: std::path::PathBuf,
    /// Present only when this process owns the index lock.
    pub db: Option<Arc<DefaultJsonFilesDatabase>>,
}

impl ArchiveState {
    /// Index-free: list and serve archive files from the filesystem. Does not
    /// take the RocksDB lock, so it can run alongside a writing ingest process.
    pub fn open(dir: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        if !dir.is_dir() {
            std::fs::create_dir_all(&dir)?;
        }
        Ok(Self { dir, db: None })
    }

    /// Open with the RocksDB id index (exclusive lock). Needed when this
    /// process also runs the relay, which persists inbound events.
    pub fn open_with_index(dir: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        Ok(Self {
            db: Some(Arc::new(DefaultJsonFilesDatabase::new(&dir)?)),
            dir,
        })
    }

    /// Indexed event count, when this process holds the index.
    pub fn event_count(&self) -> Option<u64> {
        self.db.as_ref().map(|d| d.count_keys())
    }

    /// Index shards that appeared or changed since the last pass, in the
    /// background.
    ///
    /// The index now stores *where* each event lives (shard + offset), so a
    /// shard dropped into the directory by an external backup has to be read
    /// once before its events can be fetched by id. This is incremental —
    /// unchanged shards cost one `stat` each — so it is safe on every start,
    /// unlike the O(n) `rebuild_index`.
    ///
    /// No-op without the index lock.
    pub fn spawn_index_new_shards(&self) -> Option<tokio::task::JoinHandle<()>> {
        let db = self.db.clone()?;
        Some(tokio::task::spawn_blocking(move || {
            match db.index_new_shards() {
                Ok(IndexReport {
                    shards,
                    unchanged,
                    indexed,
                    reframed,
                    new_events,
                    repaired,
                }) => tracing::info!(
                    shards,
                    unchanged,
                    indexed,
                    reframed,
                    new_events,
                    repaired,
                    "archive shard indexing pass complete"
                ),
                Err(e) => tracing::error!(error = %e, "archive shard indexing failed"),
            }
        }))
    }

    /// Fetch one event's raw JSON line by id. `None` when this process has no
    /// index, or the archive does not hold the event.
    pub async fn event_raw(&self, id: &EventId) -> Option<Vec<u8>> {
        self.db.as_ref()?.get_raw(id).await
    }

    /// Fetch many events by hex id, in the order given.
    ///
    /// This is what turns a search hit into something a client can verify: the
    /// index holds no `tags` and no `sig`, so the signed event has to come
    /// from the corpus. Batched deliberately -- one index `multi_get`, reads
    /// grouped by (shard, frame) so each frame is decoded once for the whole
    /// page, on the blocking pool.
    pub async fn events_by_hex_ids(&self, ids: &[String]) -> Vec<Option<serde_json::Value>> {
        let Some(db) = self.db.as_ref() else {
            return vec![None; ids.len()];
        };
        // Keep the mapping from request position to the ids we could parse, so
        // a malformed id yields `None` in its own slot rather than shifting
        // every later event onto the wrong hit.
        let mut slots: Vec<Option<usize>> = Vec::with_capacity(ids.len());
        let mut wanted: Vec<EventId> = Vec::with_capacity(ids.len());
        for id in ids {
            match EventId::from_hex(id) {
                Ok(eid) => {
                    slots.push(Some(wanted.len()));
                    wanted.push(eid);
                }
                Err(_) => slots.push(None),
            }
        }

        let raws = db.get_many_raw(&wanted).await;
        slots
            .into_iter()
            .map(|slot| {
                let raw = raws.get(slot?)?.as_ref()?;
                serde_json::from_slice(raw).ok()
            })
            .collect()
    }
}

#[derive(Serialize)]
pub struct ArchiveFileInfo {
    pub name: String,
    pub size: u64,
    pub timestamp: i64,
}

/// Archive routes, to be nested under `/archive`.
pub fn router(state: ArchiveState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/files", get(files_json))
        .route("/stats", get(stats_json))
        .route("/event/{id}", get(serve_event))
        .route("/{file}", get(serve_file))
        .with_state(state)
        // Nested under /archive, so the pages link /archive/style.css.
        .merge(theme::css_router())
}

/// `GET /archive/stats`
///
/// The archive's own totals, as JSON. `total_events` is the count of distinct
/// event ids the archive index holds, which is the only available answer to
/// "how much should be in the search index" -- comparing it against the index's
/// document count is what makes an ingest verifiable rather than hopeful.
async fn stats_json(State(st): State<ArchiveState>) -> Result<Json<ArchiveStats>, Response> {
    let files = list(&st).await?;
    Ok(Json(ArchiveStats {
        files: files.len(),
        total_size: files.iter().map(|f| f.size).sum(),
        total_events: st.event_count(),
    }))
}

#[derive(serde::Serialize)]
pub struct ArchiveStats {
    pub files: usize,
    pub total_size: u64,
    /// `None` when this process does not hold the archive index.
    pub total_events: Option<u64>,
}

/// JSON listing of archive files, newest first.
async fn files_json(
    State(st): State<ArchiveState>,
) -> Result<Json<Vec<ArchiveFileInfo>>, Response> {
    let mut files = list(&st).await?;
    files.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    Ok(Json(files))
}

/// List archive files straight from the filesystem (no index lock needed).
/// Extensions the archive cursor can read; anything else in the directory is
/// internal (RocksDB id index, lock files) and must not be published.
const DUMP_EXTS: &[&str] = &[
    ".jsonl",
    ".jsonl.gz",
    ".jsonl.zst",
    ".jsonl.zstd",
    ".jsonl.bz2",
    ".json",
    ".json.gz",
    ".json.zst",
    ".json.zstd",
    ".json.bz2",
];

fn is_archive_dump(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    DUMP_EXTS.iter().any(|e| lower.ends_with(e))
}

async fn list(st: &ArchiveState) -> Result<Vec<ArchiveFileInfo>, Response> {
    let dir = st.dir.clone();
    let entries = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<ArchiveFileInfo>> {
        let mut out = Vec::new();
        for e in std::fs::read_dir(&dir)? {
            let e = e?;
            let meta = e.metadata()?;
            if !meta.is_file() {
                continue;
            }
            let name = match e.file_name().to_str() {
                Some(n) => n.to_string(),
                None => continue,
            };
            // Publish archive dumps, never index/internal files.
            //
            // This used to require an `events_` prefix, which silently hid any
            // dump not following the daily naming convention -- including a
            // ~200 GB `combined.jsonl` holding most of the historical corpus.
            // The ingest cursor has no such filter and reads every top-level
            // file, so the listing was claiming an archive far smaller than the
            // one actually being indexed.
            //
            // Match on readable extensions instead (the set the cursor
            // supports), which keeps RocksDB's internals out without making
            // assumptions about how a dump is named.
            if !is_archive_dump(&name) {
                continue;
            }
            let timestamp = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            out.push(ArchiveFileInfo {
                name,
                size: meta.len(),
                timestamp,
            });
        }
        Ok(out)
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("join: {e}")).into_response())?
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("list failed: {e}"),
        )
            .into_response()
    })?;
    Ok(entries)
}

/// `GET /archive/event/{id}`
///
/// One event, by hex id, as its original JSON line. Served from the id index:
/// the index value records the shard, frame offset and length, so this reads
/// and decodes a single frame rather than scanning the corpus.
async fn serve_event(
    State(st): State<ArchiveState>,
    AxPath(id): AxPath<String>,
) -> Result<Response, Response> {
    if st.db.is_none() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "this node does not hold the archive index",
        )
            .into_response());
    }
    let id = EventId::from_hex(id.trim())
        .map_err(|_| (StatusCode::BAD_REQUEST, "invalid event id").into_response())?;
    let raw = st
        .event_raw(&id)
        .await
        .ok_or_else(|| (StatusCode::NOT_FOUND, "no such event").into_response())?;
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        raw,
    )
        .into_response())
}

/// Stream one archive file by name.
async fn serve_file(
    State(st): State<ArchiveState>,
    AxPath(file): AxPath<String>,
) -> Result<Response, Response> {
    // Reject traversal and anything that isn't an archive dump.
    if file.contains("..")
        || file.contains('/')
        || file.contains('\\')
        || !file.starts_with("events_")
    {
        return Err((StatusCode::BAD_REQUEST, "invalid file name").into_response());
    }
    let path = st.dir.join(&file);
    let meta = tokio::fs::metadata(&path)
        .await
        .map_err(|_| (StatusCode::NOT_FOUND, "no such archive file").into_response())?;
    if !meta.is_file() {
        return Err((StatusCode::NOT_FOUND, "no such archive file").into_response());
    }

    let handle = tokio::fs::File::open(&path)
        .await
        .map_err(|_| (StatusCode::NOT_FOUND, "cannot open file").into_response())?;

    let body = Body::from_stream(ReaderStream::new(handle));
    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/octet-stream".to_string()),
            (header::CONTENT_LENGTH, meta.len().to_string()),
        ],
        body,
    )
        .into_response())
}

/// HTML landing page listing the archive (nostrhole's index page).
async fn index(State(st): State<ArchiveState>) -> Result<Html<String>, Response> {
    let mut files = list(&st).await?;
    files.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

    let total_size: u64 = files.iter().map(|f| f.size).sum();
    // Only known when this process holds the index (i.e. relay mode).
    let total_events = st.event_count();

    let rows = if files.is_empty() {
        "<li class=\"empty\">no archive files yet</li>".to_string()
    } else {
        files
            .iter()
            .map(|f| {
                format!(
                    "<li><a href=\"/archive/{name}\">{name}</a>\
                     <span class=\"s\">{size}</span></li>",
                    name = f.name,
                    size = theme::human_size(f.size),
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    // The event count is only knowable with the index open, and a readout that
    // says "0" when the answer is "not measurable here" is a lie -- so it says
    // where the number went instead.
    let events = match total_events {
        Some(n) => format!("<div class=\"v\">{n}</div>"),
        None => "<div class=\"v mute\">not indexed here</div>".to_string(),
    };

    Ok(Html(format!(
        r#"<!doctype html>
<html lang="en"><head>{head}
<style>
/* A listing is a register: ruled rows, sizes right-aligned so the digits form
   a column you can scan down. Only this page has one. */
.files {{ list-style: none; margin: 18px 0 0; padding: 0; }}
.files li {{ display: grid; grid-template-columns: 1fr auto; align-items: baseline;
  gap: 16px; padding: 8px 0; border-bottom: 1px solid var(--rule); }}
.files li:last-child {{ border-bottom: 0; }}
.files a {{ text-decoration: none; }}
.files a:hover {{ text-decoration: underline; }}
.files .s {{ color: var(--slate); font-variant-numeric: tabular-nums; }}
.files .empty {{ color: var(--slate); padding: 8px 0; }}
</style></head>
<body class="page">
<main>
  <div class="wordmark">nostr<span>search</span><small>Event archive</small></div>

  <section class="panel">
    <div class="plate">Corpus <em>{files_n} files</em></div>

    <p class="panel-note">Raw events as newline-delimited JSON, zstd-compressed, one
    file per month. Every file is a complete shard: download and replay it, no
    API required.</p>

    <div class="readouts">
      <div class="readout"><div class="v">{files_n}</div>
        <div class="k">Files</div></div>
      <div class="readout"><div class="v">{total}</div>
        <div class="k">Total size</div></div>
      <div class="readout">{events}
        <div class="k">Events</div></div>
    </div>

    <ul class="files">
{rows}
    </ul>
  </section>
</main>
</body></html>"#,
        head = theme::head("nostr event archive", "/archive/style.css"),
        files_n = files.len(),
        total = theme::human_size(total_size),
        events = events,
        rows = rows,
    )))
}

#[cfg(test)]
mod listing_tests {
    use super::is_archive_dump;

    #[test]
    fn publishes_dumps_whatever_they_are_named() {
        // The daily convention...
        assert!(is_archive_dump("events_20260802.jsonl.zst"));
        assert!(is_archive_dump("events_20250820.jsonl.zstd"));
        // ...and the merged historical archive, which an `events_` prefix
        // filter hid entirely despite being the largest file present.
        assert!(is_archive_dump("combined.jsonl"));
        assert!(is_archive_dump("combined.jsonl.zst"));
        assert!(is_archive_dump("2023-backup.json.gz"));
        assert!(is_archive_dump("OLD_DUMP.JSONL"));
    }

    #[test]
    fn never_publishes_index_internals() {
        // RocksDB id index and lock files must stay private.
        for name in [
            "LOCK",
            "CURRENT",
            "IDENTITY",
            "MANIFEST-000004",
            "OPTIONS-000026",
            "000018.sst",
            "000017.log",
            "LOG.old.1699999999",
            "wot.bin",
        ] {
            assert!(!is_archive_dump(name), "would have published {name}");
        }
    }
}
