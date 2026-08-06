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
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

/// Convert an sdk event into the canonical corpus event.
///
/// Defined in the indexer crate (the firehose owns the type conversion); this
/// re-export keeps `crate::scraper`'s `use crate::node::to_core` unchanged.
pub use nostrsearch_indexer::firehose::to_core;

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

/// Ids indexed since the last commit, awaiting durable record.
///
/// The id-store answers "is this already in the index?", and ingest skips
/// anything it says yes to. So the store being *ahead* of the index is silent,
/// permanent data loss: every attempt to fill the gap is skipped as
/// already-done, and no amount of re-running helps.
///
/// The ordering rule that prevents it: **record only after the commit that
/// made the document searchable**. Crashing in that window leaves the store
/// *behind*, which costs a re-index of a few seconds of events and is
/// self-correcting.
#[derive(Default)]
struct IndexedIds(Vec<[u8; 32]>);

impl IndexedIds {
    fn note(&mut self, ev: &NostrEvent) {
        if let Some(id) = nostrsearch_indexer::archive_ingest::hex32(&ev.id) {
            self.0.push(id);
        }
    }

    /// Record everything indexed since the last commit. Call *after* it.
    fn flush(&mut self, store: Option<&Arc<nostrsearch_indexer::id_store::IdStore>>) {
        if self.0.is_empty() {
            return;
        }
        match store {
            Some(s) => {
                if let Err(e) = s.flush(self.0.iter()) {
                    tracing::warn!(error = %e, pending = self.0.len(), "recording indexed ids failed");
                    return;
                }
                self.0.clear();
            }
            None => self.0.clear(),
        }
    }
}

/// A request that must run on the writer task, because that task is the sole
/// owner of the [`Pipeline`].
pub enum WriterCmd {
    /// Discard an analysis's state so it re-derives from incoming events.
    ResetAnalysis {
        name: String,
        /// Reset names, and whether rebuilding them needs a corpus replay.
        reply: tokio::sync::oneshot::Sender<Result<Option<(Vec<&'static str>, bool)>, String>>,
    },
    /// Per-analysis progress.
    Status {
        reply: tokio::sync::oneshot::Sender<Vec<nostrsearch_stats::AnalysisStatus>>,
    },
    /// Discard every analysis and rebuild from the archive.
    ResetAll {
        reply: tokio::sync::oneshot::Sender<Result<Vec<&'static str>, String>>,
    },
    /// Relay targets for the scraper, ranked by advertiser count.
    RelayTargets {
        reply: tokio::sync::oneshot::Sender<Vec<(String, u64)>>,
    },
}

/// Control handle for the writer task. Cloneable and cheap.
#[derive(Clone)]
pub struct WriterCtl(mpsc::Sender<WriterCmd>);

impl WriterCtl {
    /// Discard every analysis so they rebuild from the archive.
    pub async fn reset_all(&self) -> Result<Result<Vec<&'static str>, String>, &'static str> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.0
            .send(WriterCmd::ResetAll { reply: tx })
            .await
            .map_err(|_| "writer is gone")?;
        rx.await.map_err(|_| "writer dropped the reply")
    }

    /// Relay targets from the `relays` report, ranked by advertiser count.
    ///
    /// Empty when nothing has folded a relay list yet, which is the caller's
    /// cue to fall back to scanning the index.
    pub async fn relay_targets(&self) -> Vec<(String, u64)> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self
            .0
            .send(WriterCmd::RelayTargets { reply: tx })
            .await
            .is_err()
        {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }
    /// Reset an analysis and everything that depends on it.
    ///
    /// `Ok(None)` = no analysis by that name.
    pub async fn reset_analysis(
        &self,
        name: &str,
    ) -> Result<Result<Option<(Vec<&'static str>, bool)>, String>, &'static str> {
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
    let (sink, handle, _ctl, _replay) =
        spawn_writer_with_reports(cfg, queue_size, commit_every, None, None)?;
    Ok((sink, handle))
}

/// As [`spawn_writer`], but also publishes analysis snapshots into `reports`
/// on each commit tick so the HTTP layer can serve them without touching the
/// pipeline (which this task owns exclusively).
///
/// `dedupe` is the id-store this node indexes against. The writer records the
/// id of everything it indexes, *after* the commit that made it searchable --
/// see [`IndexedIds`] for why the ordering is the whole point.
pub fn spawn_writer_with_reports(
    cfg: PipelineConfig,
    queue_size: usize,
    commit_every: std::time::Duration,
    reports: Option<crate::reports::ReportStore>,
    dedupe: Option<std::sync::Arc<nostrsearch_indexer::id_store::IdStore>>,
) -> anyhow::Result<(EventSink, WriterHandle, WriterCtl, Arc<Mutex<Pipeline>>)> {
    // Behind a mutex, not moved into the task: the archive ingest engine --
    // the same one the CLI runs -- needs access to this pipeline, and a
    // channel-only writer is exactly what forced a second, divergent ingest
    // implementation to exist. Contention is negligible: live traffic is a
    // handful of events a second, and ingest holds the lock per chunk.
    let pipeline = Arc::new(Mutex::new(Pipeline::new(cfg)?));
    // Live tail semantics: everything from here on is realtime.
    pipeline.lock().unwrap().go_live();

    // Publish once from the state just loaded, before any event arrives.
    //
    // Reports otherwise only reach the store on a commit tick that found work
    // to do, so a node restarting with a fully populated analysis state served
    // an empty /reports until the next live event happened to land -- and on a
    // node with no firehose attached, indefinitely. The data was on disk the
    // whole time.
    publish_reports(&pipeline.lock().unwrap(), reports.as_ref());

    let pipeline_for_task = pipeline.clone();

    let (tx, mut rx) = mpsc::channel::<NostrEvent>(queue_size);
    let (sd_tx, mut sd_rx) = tokio::sync::watch::channel(false);
    // Small: these are rare operator actions, not a data path.
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<WriterCmd>(16);
    let join = tokio::spawn(async move {
        let pipeline = pipeline_for_task;
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
        // Ids indexed since the last commit, recorded only once it succeeds.
        let mut indexed = IndexedIds::default();

        loop {
            // The commit timer must be independent of event arrival: a purely
            // event-driven commit would leave the last events uncommitted (and
            // therefore unsearchable) for the whole of a quiet period.
            tokio::select! {
                // Biased: every arm above the replay one is checked first, so
                // live events, commits and operator commands always win. A
                // replay only advances when the node is otherwise idle.
                biased;

                maybe = rx.recv() => match maybe {
                    Some(ev) => {
                        pipeline.lock().unwrap().process(&ev);
                        indexed.note(&ev);
                        dirty = true;
                    }
                    None => break, // all senders dropped
                },
                _ = tick.tick() => {
                    if dirty {
                        match pipeline.lock().unwrap().commit() {
                            // Committed, so the documents are searchable and
                            // their ids may now be claimed. Never before: a
                            // store ahead of the index makes those events
                            // unreachable to every future ingest.
                            Ok(()) => indexed.flush(dedupe.as_ref()),
                            Err(e) => tracing::warn!(error = %e, "commit failed"),
                        }
                        publish_reports(&pipeline.lock().unwrap(), reports.as_ref());
                        dirty = false;
                    }
                }
                _ = delta_tick.tick() => {
                    if let Some(store) = reports.as_ref() {
                        let deltas = pipeline.lock().unwrap().drain_report_deltas();
                        store.apply_deltas(unix_now(), deltas);
                    }
                }
                Some(cmd) = cmd_rx.recv() => match cmd {
                    WriterCmd::ResetAll { reply } => {
                        // A failed re-attach leaves the graph unreachable, so
                        // the reset is reported as the failure it is rather
                        // than as an empty success.
                        let res = pipeline.lock().unwrap().reset_all_analyses();
                        publish_reports(&pipeline.lock().unwrap(), reports.as_ref());
                        let _ = reply.send(res.map_err(|e| e.to_string()));
                    }
                    WriterCmd::RelayTargets { reply } => {
                        let _ = reply.send(pipeline.lock().unwrap().relay_targets());
                    }
                    WriterCmd::ResetAnalysis { name, reply } => {
                        let reset = pipeline.lock().unwrap().reset_analysis(&name);
                        let ok = match reset {
                            Ok(v) => v,
                            // Re-attach failed: the graph is unreachable and a
                            // rebuild now would write tier 0 across the corpus.
                            Err(e) => {
                                let _ = reply.send(Err(e.to_string()));
                                continue;
                            }
                        };
                        // Whether rebuilding what was reset actually requires
                        // reading the corpus, decided while the names are in
                        // hand rather than assumed by the caller.
                        let needs = ok.as_ref().is_none_or(|names| {
                            pipeline.lock().unwrap().names_need_corpus(names)
                        });
                        // A derived analysis rebuilds from other analyses'
                        // materialized output, which is already on disk. Do it
                        // now rather than leaving the operator watching an
                        // empty report until the next scheduled refresh.
                        if let Some(names) = ok.as_ref()
                            && !needs
                        {
                            let n = pipeline.lock().unwrap().refresh_now(names);
                            tracing::info!(
                                reset = ?names,
                                refreshed = n,
                                "rebuilt derived analyses without a corpus pass"
                            );
                            publish_reports(&pipeline.lock().unwrap(), reports.as_ref());
                        }
                        let ok = ok.map(|names| (names, needs));
                        // Republish immediately so the dashboard reflects the
                        // now-empty report rather than the stale one.
                        publish_reports(&pipeline.lock().unwrap(), reports.as_ref());
                        let _ = reply.send(Ok(ok));
                    }
                    WriterCmd::Status { reply } => {
                        let _ = reply.send(pipeline.lock().unwrap().analyses_status());
                    }
                },
                _ = sd_rx.changed() => {
                    if *sd_rx.borrow() {
                        tracing::info!("writer shutting down; draining queue");
                        // Drain anything already queued so a deploy doesn't
                        // drop in-flight events.
                        while let Ok(ev) = rx.try_recv() {
                            pipeline.lock().unwrap().process(&ev);
                            indexed.note(&ev);
                        }
                        break;
                    }
                }
            }
        }

        match pipeline.lock().unwrap().finish() {
            // Same rule on the way out: claim the ids only once the final
            // commit has made them searchable.
            Ok(()) => indexed.flush(dedupe.as_ref()),
            Err(e) => tracing::warn!(error = %e, "final flush failed"),
        }
        publish_reports(&pipeline.lock().unwrap(), reports.as_ref());
        tracing::info!("writer task stopped (flushed)");
    });

    Ok((
        EventSink(tx),
        WriterHandle {
            shutdown: sd_tx,
            join,
        },
        WriterCtl(cmd_tx),
        pipeline,
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
