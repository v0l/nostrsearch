//! The unified node: **one process, one shared index**.
//!
//! Runs every role together — archive relay, search API, archive HTTP, and the
//! live firehose — over a single [`Pipeline`] and a single archive database.
//! This is what nostrhole did (one process owning the corpus), extended with
//! search.
//!
//! ```text
//!    firehose (client + archive DB) ─┐
//!                                    ├─► mpsc ─► writer task
//!    relay (LocalRelay + NodeDb) ────┘          (Pipeline: index + stats + WoT)
//!                                                        │
//!         HTTP: /search ◄── ShardRegistry (same dir, auto-reload on commit)
//!               /archive, /  (relay websocket)
//! ```
//!
//! Writes are serialized through an mpsc channel rather than a shared mutex, so
//! the relay and firehose never block each other on Tantivy commits. Because a
//! single process owns the archive's RocksDB index *and* the Tantivy writer,
//! there is no cross-process lock contention.
//!
//! Archiving is performed by whoever owns the nostr-sdk database (the firehose
//! client, or [`NodeDb`] for relay writes); the [`Pipeline`] only indexes and
//! folds stats, so events are never archived twice.

use nostr_archive_cursor::DefaultJsonFilesDatabase;
use nostr_sdk::prelude::*;
use nostrsearch_core::event::NostrEvent;
use nostrsearch_indexer::{Pipeline, PipelineConfig};
use std::sync::Arc;
use tokio::sync::mpsc;

/// Convert an sdk event into the canonical corpus event.
pub fn to_core(ev: &Event) -> NostrEvent {
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

/// Handle used by event producers (firehose, relay) to submit events to the
/// single writer task. Cloneable and cheap.
#[derive(Clone)]
pub struct EventSink(mpsc::Sender<NostrEvent>);

impl EventSink {
    /// Submit an event for indexing + stats, waiting for queue capacity.
    ///
    /// Every producer archives *before* submitting, so dropping here would
    /// leave a permanent hole: the archive says "have it", the scraper will
    /// never re-fetch it, and the index/stats never see it. Backpressure is
    /// the correct behavior — a saturated writer slows the relay socket,
    /// firehose, or scraper instead of silently losing events.
    pub async fn send(&self, ev: NostrEvent) {
        if self.0.send(ev).await.is_err() {
            tracing::warn!("writer task gone; event not indexed");
        }
    }

    /// Non-blocking submit for sync contexts. Drops (with a warning) if the
    /// writer is saturated — prefer [`send`](Self::send) wherever possible.
    pub fn submit(&self, ev: NostrEvent) {
        if let Err(e) = self.0.try_send(ev) {
            match e {
                mpsc::error::TrySendError::Full(_) => {
                    tracing::warn!("writer queue full; dropping event from index/stats")
                }
                mpsc::error::TrySendError::Closed(_) => {
                    tracing::error!("writer task closed; event not indexed")
                }
            }
        }
    }
}

/// A request that must run on the writer task, because that task is the sole
/// owner of the [`Pipeline`].
pub enum WriterCmd {
    /// Discard an analysis's state so it re-derives from incoming events.
    ResetAnalysis {
        name: String,
        reply: tokio::sync::oneshot::Sender<bool>,
    },
    /// Per-analysis progress.
    Status {
        reply: tokio::sync::oneshot::Sender<Vec<nostrsearch_stats::AnalysisStatus>>,
    },
}

/// Control handle for the writer task. Cloneable and cheap.
#[derive(Clone)]
pub struct WriterCtl(mpsc::Sender<WriterCmd>);

impl WriterCtl {
    /// Reset one analysis. `Ok(false)` = no analysis by that name.
    pub async fn reset_analysis(&self, name: &str) -> Result<bool, &'static str> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.0
            .send(WriterCmd::ResetAnalysis {
                name: name.to_string(),
                reply: tx,
            })
            .await
            .map_err(|_| "writer task gone")?;
        rx.await.map_err(|_| "writer task dropped the request")
    }

    pub async fn status(&self) -> Result<Vec<nostrsearch_stats::AnalysisStatus>, &'static str> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.0
            .send(WriterCmd::Status { reply: tx })
            .await
            .map_err(|_| "writer task gone")?;
        rx.await.map_err(|_| "writer task dropped the request")
    }
}

/// Handle for shutting the writer down cleanly.
///
/// Without this, a SIGTERM (what Docker/k8s send on every deploy) kills the
/// runtime mid-interval and everything indexed since the last commit is lost
/// from the search index — the events survive in the archive, but the index
/// silently drifts from it on each restart.
pub struct WriterHandle {
    shutdown: tokio::sync::watch::Sender<bool>,
    join: tokio::task::JoinHandle<()>,
}

impl WriterHandle {
    /// Signal the writer to flush and stop, then wait for it.
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        if let Err(e) = self.join.await {
            tracing::warn!(error = %e, "writer task join failed");
        }
    }
}

/// Spawn the single writer task that owns the [`Pipeline`].
///
/// Returns the sink producers use plus a [`WriterHandle`] for a clean flush.
/// The task commits on an interval so search readers (which use
/// `ReloadPolicy::OnCommitWithDelay`) pick up new events.
pub fn spawn_writer(
    cfg: PipelineConfig,
    queue_size: usize,
    commit_every: std::time::Duration,
) -> anyhow::Result<(EventSink, WriterHandle)> {
    let (sink, handle, _ctl) = spawn_writer_with_reports(cfg, queue_size, commit_every, None)?;
    Ok((sink, handle))
}

/// As [`spawn_writer`], but also publishes analysis snapshots into `reports`
/// on each commit tick so the HTTP layer can serve them without touching the
/// pipeline (which this task owns exclusively).
pub fn spawn_writer_with_reports(
    cfg: PipelineConfig,
    queue_size: usize,
    commit_every: std::time::Duration,
    reports: Option<crate::reports::ReportStore>,
) -> anyhow::Result<(EventSink, WriterHandle, WriterCtl)> {
    let mut pipeline = Pipeline::new(cfg)?;
    // Live tail semantics: everything from here on is realtime.
    pipeline.go_live();

    let (tx, mut rx) = mpsc::channel::<NostrEvent>(queue_size);
    let (sd_tx, mut sd_rx) = tokio::sync::watch::channel(false);
    // Small: these are rare operator actions, not a data path.
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<WriterCmd>(16);

    let join = tokio::spawn(async move {
        let mut tick = tokio::time::interval(commit_every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await; // consume the immediate first tick

        // Deltas stream far more often than commits: they are cheap (only what
        // changed) and their whole point is to make the dashboard move in
        // something close to realtime, which a 30s commit cadence cannot do.
        let mut delta_tick = tokio::time::interval(DELTA_INTERVAL);
        delta_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        delta_tick.tick().await;

        let mut dirty = false;

        loop {
            // The commit timer must be independent of event arrival: a purely
            // event-driven commit would leave the last events uncommitted (and
            // therefore unsearchable) for the whole of a quiet period.
            tokio::select! {
                maybe = rx.recv() => match maybe {
                    Some(ev) => {
                        pipeline.process(&ev);
                        dirty = true;
                    }
                    None => break, // all senders dropped
                },
                _ = tick.tick() => {
                    if dirty {
                        if let Err(e) = pipeline.commit() {
                            tracing::warn!(error = %e, "commit failed");
                        }
                        publish_reports(&pipeline, reports.as_ref());
                        dirty = false;
                    }
                }
                _ = delta_tick.tick() => {
                    if let Some(store) = reports.as_ref() {
                        let deltas = pipeline.drain_report_deltas();
                        store.apply_deltas(unix_now(), deltas);
                    }
                }
                Some(cmd) = cmd_rx.recv() => match cmd {
                    WriterCmd::ResetAnalysis { name, reply } => {
                        let ok = pipeline.reset_analysis(&name);
                        // Republish immediately so the dashboard reflects the
                        // now-empty report rather than the stale one.
                        publish_reports(&pipeline, reports.as_ref());
                        let _ = reply.send(ok);
                    }
                    WriterCmd::Status { reply } => {
                        let _ = reply.send(pipeline.analyses_status());
                    }
                },
                _ = sd_rx.changed() => {
                    if *sd_rx.borrow() {
                        tracing::info!("writer shutting down; draining queue");
                        // Drain anything already queued so a deploy doesn't
                        // drop in-flight events.
                        while let Ok(ev) = rx.try_recv() {
                            pipeline.process(&ev);
                        }
                        break;
                    }
                }
            }
        }

        if let Err(e) = pipeline.finish() {
            tracing::warn!(error = %e, "final flush failed");
        }
        publish_reports(&pipeline, reports.as_ref());
        tracing::info!("writer task stopped (flushed)");
    });

    Ok((
        EventSink(tx),
        WriterHandle {
            shutdown: sd_tx,
            join,
        },
        WriterCtl(cmd_tx),
    ))
}

/// How often the writer drains partial report changes for the live stream.
const DELTA_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Copy the pipeline's current analysis snapshots into the shared store.
///
/// Publishing replaces wholesale, so it also clears any drift accumulated by
/// incremental patches between commits.
fn publish_reports(pipeline: &Pipeline, reports: Option<&crate::reports::ReportStore>) {
    let Some(store) = reports else { return };
    store.publish(unix_now(), pipeline.reports());
}

/// Nostr database for the relay: archives to the corpus **and** forwards to the
/// writer task so relay-published events become searchable.
///
/// Without this, events published to our relay would be archived but never
/// indexed or folded into stats.
#[derive(Debug)]
pub struct NodeDb {
    inner: DefaultJsonFilesDatabase,
    sink: EventSink,
}

impl NodeDb {
    pub fn new(inner: DefaultJsonFilesDatabase, sink: EventSink) -> Self {
        Self { inner, sink }
    }
}

impl std::fmt::Debug for EventSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EventSink")
    }
}

impl NostrDatabase for NodeDb {
    fn backend(&self) -> Backend {
        self.inner.backend()
    }

    fn save_event<'a>(
        &'a self,
        event: &'a Event,
    ) -> BoxedFuture<'a, Result<SaveEventStatus, DatabaseError>> {
        Box::pin(async move {
            let status = self.inner.save_event(event).await?;
            // Only index genuinely new events (skip duplicates/rejects).
            if matches!(status, SaveEventStatus::Success) {
                self.sink.send(to_core(event)).await;
            }
            Ok(status)
        })
    }

    fn check_id<'a>(
        &'a self,
        event_id: &'a EventId,
    ) -> BoxedFuture<'a, Result<DatabaseEventStatus, DatabaseError>> {
        self.inner.check_id(event_id)
    }

    fn event_by_id<'a>(
        &'a self,
        id: &'a EventId,
    ) -> BoxedFuture<'a, Result<Option<Event>, DatabaseError>> {
        self.inner.event_by_id(id)
    }

    fn count(&self, f: Filter) -> BoxedFuture<'_, Result<usize, DatabaseError>> {
        self.inner.count(f)
    }

    fn query(&self, f: Filter) -> BoxedFuture<'_, Result<Events, DatabaseError>> {
        self.inner.query(f)
    }

    fn delete(&self, f: Filter) -> BoxedFuture<'_, Result<(), DatabaseError>> {
        self.inner.delete(f)
    }

    fn wipe(&self) -> BoxedFuture<'_, Result<(), DatabaseError>> {
        self.inner.wipe()
    }
}

/// Spawn the live firehose, archiving through the shared database and feeding
/// the same writer task the relay uses.
pub fn spawn_firehose(
    relays: Vec<String>,
    archive: Option<DefaultJsonFilesDatabase>,
    sink: EventSink,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = run_firehose(relays, archive, sink).await {
            tracing::error!(error = %e, "firehose task exited");
        }
    })
}

async fn run_firehose(
    relays: Vec<String>,
    archive: Option<DefaultJsonFilesDatabase>,
    sink: EventSink,
) -> anyhow::Result<()> {
    let mut builder = Client::builder().signer(Keys::generate());
    if let Some(db) = archive {
        builder = builder.database(db);
    }
    let client = builder.build();
    for r in &relays {
        client.add_relay(r.clone()).await?;
    }
    client.connect().await;
    tracing::info!(relays = relays.len(), "firehose connected");

    // Archive everything except ephemeral kinds (NIP-01: not stored by relays).
    let kinds: Vec<Kind> = (0u16..20_000)
        .chain(30_000..=u16::MAX)
        .map(Kind::Custom)
        .collect();

    loop {
        let filter = Filter::default()
            .kinds(kinds.clone())
            .since(Timestamp::now());
        if let Err(e) = client.subscribe(filter, None).await {
            tracing::error!(error = %e, "firehose subscribe failed; retrying in 5s");
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            continue;
        }

        let mut rx = client.notifications();
        loop {
            match rx.recv().await {
                Ok(RelayPoolNotification::Event { event, .. }) => sink.send(to_core(&event)).await,
                Ok(RelayPoolNotification::Shutdown) => return Ok(()),
                Ok(_) => {}
                Err(_) => break,
            }
        }
        tracing::warn!("firehose stream closed; reconnecting in 5s");
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

/// Shared archive handle for the node (one process = one index lock).
pub type SharedArchive = Arc<DefaultJsonFilesDatabase>;
