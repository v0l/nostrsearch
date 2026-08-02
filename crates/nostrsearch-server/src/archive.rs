//! Archive serving — absorbs nostrhole's HTTP role.
//!
//! Serves the raw `.jsonl.zst` corpus files (what `hole.v0l.io` publishes) plus
//! a directory listing, from the same `JsonFilesDatabase` the unified ingest
//! writes. Ported from nostrhole's hyper handler to axum so it shares the
//! search server's router, middleware, and port.
//!
//! Routes:
//! - `GET /archive`            — HTML listing of archive files + totals
//! - `GET /archive/files`      — JSON listing (name, size, timestamp)
//! - `GET /archive/{file}`     — stream one archive file

use axum::body::Body;
use axum::extract::{Path as AxPath, State};
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use nostr_archive_cursor::DefaultJsonFilesDatabase;
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
        .route("/{file}", get(serve_file))
        .with_state(state)
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
            // Only publish archive dumps, never index/internal files.
            if !name.starts_with("events_") {
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

    let links = files
        .iter()
        .map(|f| {
            format!(
                "<li><a href=\"/archive/{}\">{}</a> <span class=\"s\">{:.2} MiB</span></li>",
                f.name,
                f.name,
                f.size as f64 / 1024.0 / 1024.0
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    Ok(Html(format!(
        r#"<!doctype html>
<html><head><meta charset="utf-8"><title>nostr archive</title>
<style>
body{{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;max-width:52rem;margin:3rem auto;padding:0 1rem;background:#0d0d0f;color:#d8d8dc}}
h1{{font-size:1.25rem;letter-spacing:.02em}}
.meta{{color:#8a8a93;margin-bottom:2rem}}
ul{{list-style:none;padding:0}}
li{{padding:.35rem 0;border-bottom:1px solid #1d1d22}}
a{{color:#7aa2f7;text-decoration:none}} a:hover{{text-decoration:underline}}
.s{{color:#6a6a73;float:right}}
</style></head><body>
<h1>nostr event archive</h1>
<div class="meta">{files_n} files &middot; {events}{total_gib:.2} GiB</div>
<ul>
{links}
</ul>
</body></html>"#,
        files_n = files.len(),
        events = total_events
            .map(|n| format!("{n} events &middot; "))
            .unwrap_or_default(),
        total_gib = total_size as f64 / 1024.0 / 1024.0 / 1024.0,
        links = links,
    )))
}
