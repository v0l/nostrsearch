//! A small HTTP service for `ingest` to run while it works.
//!
//! A full backfill takes hours to days, and for that whole time the node has
//! nothing on its port: the search index is half-built, so running the server
//! node against it would answer queries from a partial corpus and look like
//! data loss. But the archive dumps are complete and sitting on disk the
//! entire time, and a bare connection-refused is a worse answer than "still
//! working, here is the corpus".
//!
//! So this serves two things and nothing else:
//! - `GET /` — a static page saying an ingest is in progress
//! - `GET /archive/*` — the corpus, via the shared [`nostrsearch_archive`]
//!   routes the server node uses
//!
//! The archive handle is opened **index-free** ([`ArchiveState::open`]), which
//! takes no RocksDB lock — the ingest process itself holds that lock, and two
//! handles to it in one process would deadlock rather than merely fail.
//! `GET /archive/event/{id}` therefore answers 503 here rather than pretending
//! the event is missing; file listing and downloads, which are pure filesystem
//! work, are unaffected.

use axum::Router;
use axum::response::Html;
use axum::routing::get;
use std::path::Path;

/// Routes for the ingest-time service: landing page, health, archive files.
pub fn router(archive_dir: Option<&Path>) -> anyhow::Result<Router> {
    // The stylesheet is mounted at the root here, not only under /archive, so
    // the status page is styled even when this ingest has no archive to serve.
    let mut app = Router::new()
        .route("/", get(landing))
        .route("/healthz", get(|| async { "ok" }))
        .merge(nostrsearch_archive::theme::css_router());

    if let Some(dir) = archive_dir {
        // Index-free: see the module note. The ingest process holds the lock.
        let state = nostrsearch_archive::ArchiveState::open(dir)?;
        app = app.merge(Router::new().nest("/archive", nostrsearch_archive::router(state)));
    }
    Ok(app)
}

/// Bind and serve until the process exits.
///
/// Returns once the listener is bound, so the caller can start the backfill;
/// the server runs on the same runtime as the ingest. A bind failure is
/// returned rather than logged, because a port that is already taken usually
/// means a second ingest is running against the same index.
pub async fn spawn(bind: &str, archive_dir: Option<&Path>) -> anyhow::Result<()> {
    let app = router(archive_dir)?;
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let local = listener.local_addr()?;
    tracing::info!(
        addr = %local,
        archive = archive_dir.is_some(),
        "serving ingest status page"
    );
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!(error = %e, "ingest status server stopped");
        }
    });
    Ok(())
}

/// `GET /` — a static holding page.
///
/// Deliberately static: it must not read the index or the pipeline, both of
/// which are locked by the ingest, and a status page that blocks on the thing
/// it reports about is worse than no page.
async fn landing() -> Html<&'static str> {
    Html(page())
}

/// The rendered page, built once.
///
/// Only the `<head>` varies, and only because it is assembled by
/// [`nostrsearch_archive::theme`]; `OnceLock` keeps it a single allocation for
/// the life of the process.
fn page() -> &'static str {
    static PAGE: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    PAGE.get_or_init(|| {
        TEMPLATE.replace(
            "{{head}}",
            &nostrsearch_archive::theme::head("nostrsearch — ingest in progress", "/style.css"),
        )
    })
    .as_str()
}

/// The status page.
///
/// Chrome, palette and type come from the console's own stylesheet, served by
/// [`nostrsearch_archive::theme`] at `/style.css` — so a node that is still
/// building its index looks like the same product as the console that appears
/// once it has one.
const TEMPLATE: &str = r#"<!doctype html>
<html lang="en"><head>{{head}}
<style>
/* The scan bar. Not a spinner: a band travelling along a ruled scale, the way
   a tape reader draws — it says work is moving through a corpus, which is what
   is happening, rather than merely that something is busy.

   One element moves, and it moves by transform alone. Transform and opacity
   are the only properties a browser can animate on the compositor; anything
   else — colour, size, position — repaints every frame. An earlier version
   animated `background` across 48 separate ticks, which is 48 repaints per
   frame forever, and cost about half a GPU to say "please wait".

   The ticks are a repeating gradient rather than elements: painted once, and
   nothing to keep in the DOM. */
.scan { position: relative; height: 34px; overflow: hidden;
  border-bottom: 1px solid var(--rule-strong);
  background-image: repeating-linear-gradient(to right,
    var(--rule-strong) 0 2px, transparent 2px 12px);
  background-repeat: no-repeat;
  background-position: bottom left;
  background-size: 100% 12px; }
.scan::after { content: ""; position: absolute; inset: 0; width: 30%;
  background: linear-gradient(90deg, transparent,
    color-mix(in srgb, var(--patina) 55%, transparent) 35%,
    color-mix(in srgb, var(--brass) 45%, transparent) 65%, transparent);
  transform: translateX(-110%);
  animation: scan 2.6s linear infinite; }
@keyframes scan { to { transform: translateX(440%); } }

.scan-legend { display: flex; justify-content: space-between; font-size: 10px;
  letter-spacing: .16em; text-transform: uppercase; color: var(--slate);
  margin: 8px 0 20px; }

/* Motion here is decoration; the page reads the same standing still. */
@media (prefers-reduced-motion: reduce) {
  .scan::after { animation: none; opacity: .35; transform: none; width: 100%; }
}
</style></head>
<body class="page">
<main>
  <div class="wordmark">nostr<span>search</span><small>Node status</small></div>

  <section class="panel">
    <div class="plate">Ingest <em>in progress</em></div>

    <div class="scan"></div>
    <div class="scan-legend"><span>reading archive</span><span>building index</span></div>

    <p>This node is still indexing its corpus. Search stays offline until the
    pass finishes &mdash; answering queries from a half-built index returns
    quietly incomplete results, which is worse than an honest wait.</p>

    <p>The archive itself is complete and served right now:
    <a href="/archive">browse the corpus &rarr;</a></p>

    <p class="panel-note">Progress is written to the ingest log, not to this
    page: the page is static so it can never block on the index it describes.</p>

    <div class="readouts">
      <div class="readout"><div class="v mute"><span class="chip bad">Offline</span></div>
        <div class="k">Search</div></div>
      <div class="readout"><div class="v mute"><span class="chip ok">Serving</span></div>
        <div class="k">Archive</div></div>
      <div class="readout"><div class="v mute"><span class="chip warn">Building</span></div>
        <div class="k">Index</div></div>
    </div>
  </section>
</main>
</body></html>
"#;
