//! Live relay firehose source — the **single** upstream subscription for the
//! whole system.
//!
//! Absorbs nostrhole's ingest role: when an archive directory is configured the
//! nostr-sdk `Client` gets a [`DefaultJsonFilesDatabase`] attached, so every
//! verified event is written to the `.jsonl.zst` archive (+ RocksDB id index)
//! *before* the notification is emitted. The same notification then drives the
//! search index and stats engine, so one subscription produces all three:
//!
//! ```text
//!                          ┌─► JsonFilesDatabase (archive, hole.v0l.io corpus)
//! relays ──► one client ───┼─► ShardManager      (Tantivy search index)
//!                          └─► stats Registry    (WoT / trending)
//! ```
//!
//! Ephemeral kinds (20000-29999) are dropped on arrival — per NIP-01 relays
//! aren't expected to store them, and archiving them would bloat the corpus
//! (same policy as nostrhole).

use nostr_archive_cursor::DefaultJsonFilesDatabase;
use nostr_sdk::prelude::*;
use nostrsearch_core::event::NostrEvent;
use std::path::Path;

/// Convert an sdk event to the canonical corpus event.
///
/// This is the single home for the `nostr_sdk::Event → NostrEvent` conversion;
/// the firehose, the server node and the scraper all funnel through it so the
/// field mapping (and any future change to it) lives in one place.
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

/// The live subscription. No kind list: enumerating every non-ephemeral kind
/// is a ~320 KB REQ that some relays (nos.lol) drop the connection over.
/// Ephemeral kinds are discarded on arrival instead, by [`EphemeralFilter`].
pub fn live_filter(kinds: Option<&[u16]>, since_now: bool) -> Filter {
    let mut filter = Filter::new();
    if since_now {
        filter = filter.since(Timestamp::now());
    }
    match kinds {
        Some(k) => filter.kinds(
            k.iter()
                .map(|v| Kind::Custom(*v))
                .filter(|k| !k.is_ephemeral()),
        ),
        None => filter,
    }
}

/// Rejects ephemeral events before the client archives or emits them.
#[derive(Debug)]
pub struct EphemeralFilter;

impl AdmitPolicy for EphemeralFilter {
    fn admit_event<'a>(
        &'a self,
        _relay_url: &'a RelayUrl,
        _subscription_id: &'a SubscriptionId,
        event: &'a Event,
    ) -> BoxedFuture<'a, Result<AdmitStatus, PolicyError>> {
        Box::pin(async move {
            Ok(if event.kind.is_ephemeral() {
                AdmitStatus::rejected("ephemeral")
            } else {
                AdmitStatus::Success
            })
        })
    }
}

/// Subscribe once and forward events until the client shuts down.
///
/// A lagged receiver keeps going: the subscription is still open, and
/// resubscribing would stack another one on top of it. Events missed while
/// lagging are left for the gap scraper.
pub async fn stream<F, Fut>(client: &Client, filter: Filter, mut on_event: F)
where
    F: FnMut(Box<Event>) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    loop {
        if let Err(e) = client.subscribe(filter.clone(), None).await {
            tracing::error!(error = %e, "firehose subscribe failed; retrying in 5s");
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            continue;
        }
        tracing::info!("firehose subscribed");
        let mut rx = client.notifications();
        loop {
            match rx.recv().await {
                Ok(RelayPoolNotification::Event { event, .. }) => on_event(event).await,
                Ok(RelayPoolNotification::Message {
                    message: RelayMessage::Notice(m),
                    relay_url,
                }) => tracing::warn!(relay = %relay_url, "notice: {}", m),
                Ok(RelayPoolNotification::Shutdown) => return,
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(skipped = n, "firehose lagged; continuing");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
        client.unsubscribe_all().await;
        tracing::warn!("firehose stream closed; resubscribing in 5s");
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

/// Options for the firehose.
pub struct FirehoseConfig {
    pub relays: Vec<String>,
    /// Archive directory. When set, received events are written to the JSONL
    /// archive + id index (nostrhole's role). `None` = index/stats only.
    pub archive_dir: Option<std::path::PathBuf>,
    /// Restrict to events created from connect time onward (normal tail).
    pub since_now: bool,
    /// Optional kind whitelist; ephemeral kinds are always dropped.
    pub kinds: Option<Vec<u16>>,
}

impl FirehoseConfig {
    pub fn new(relays: Vec<String>) -> Self {
        Self {
            relays,
            archive_dir: None,
            since_now: true,
            kinds: None,
        }
    }
    pub fn with_archive(mut self, dir: impl AsRef<Path>) -> Self {
        self.archive_dir = Some(dir.as_ref().to_path_buf());
        self
    }
}

/// Open the archive database (JSONL files + RocksDB id index).
pub fn open_archive(dir: impl AsRef<Path>) -> anyhow::Result<DefaultJsonFilesDatabase> {
    let db = DefaultJsonFilesDatabase::new(dir.as_ref())?;
    Ok(db)
}

/// Bring the id index up to date with the shards on disk, on a background
/// thread.
///
/// Index values carry each event's location (shard + frame offset + length), so
/// a shard has to be read once before its events can be fetched by id. The pass
/// is incremental — a shard whose size and mtime are unchanged is skipped after
/// one `stat` — so this is cheap enough to run on every start, and it reframes
/// single-frame imports (which would otherwise decode from byte zero on every
/// lookup) as it goes.
pub fn spawn_index_new_shards(db: &DefaultJsonFilesDatabase) -> std::thread::JoinHandle<()> {
    let db = db.clone();
    std::thread::spawn(move || match db.index_new_shards() {
        Ok(r) => tracing::info!(
            shards = r.shards,
            unchanged = r.unchanged,
            indexed = r.indexed,
            reframed = r.reframed,
            new_events = r.new_events,
            "archive shard indexing pass complete"
        ),
        Err(e) => tracing::error!(error = %e, "archive shard indexing failed"),
    })
}

/// Connect to `cfg.relays` and stream live events into `sink` until stopped.
///
/// When `cfg.archive_dir` is set, events are archived automatically by the
/// client's database integration before `sink` is invoked.
pub async fn run<F>(cfg: &FirehoseConfig, mut sink: F) -> anyhow::Result<()>
where
    F: FnMut(&NostrEvent) + Send,
{
    let mut builder = Client::builder()
        .signer(Keys::generate())
        .admit_policy(EphemeralFilter);

    if let Some(dir) = &cfg.archive_dir {
        let db = open_archive(dir)?;
        if db.is_index_empty() && !db.list_files().await?.is_empty() {
            tracing::info!("archive id index empty; indexing shards in background");
        }
        // Incremental either way: an empty index indexes everything, a warm one
        // only picks up shards that appeared or changed since the last pass.
        spawn_index_new_shards(&db);
        tracing::info!(dir = %dir.display(), events = db.count_keys(), "archiving enabled");
        builder = builder.database(db);
    }

    let client = builder.build();
    for r in &cfg.relays {
        client.add_relay(r.clone()).await?;
    }
    client.connect().await;
    tracing::info!(relays = cfg.relays.len(), "firehose connected");

    let filter = live_filter(cfg.kinds.as_deref(), cfg.since_now);
    // The archive DB has already persisted each event by the time it arrives.
    stream(&client, filter, |ev| {
        sink(&to_core(&ev));
        std::future::ready(())
    })
    .await;
    Ok(())
}
