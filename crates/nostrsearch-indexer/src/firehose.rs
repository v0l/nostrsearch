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
//! Ephemeral kinds (20000-29999) are excluded from the upstream filter — per
//! NIP-01 relays aren't expected to store them, and archiving them would bloat
//! the corpus (same policy as nostrhole).

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

/// All non-ephemeral kinds. Nostr filters are inclusive-only, so the
/// 20000-29999 ephemeral range is excluded by enumeration (as nostrhole does).
fn non_ephemeral_kinds() -> Vec<Kind> {
    (0u16..20_000)
        .chain(30_000..=u16::MAX)
        .map(Kind::Custom)
        .collect()
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
    let mut builder = Client::builder().signer(Keys::generate());

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

    loop {
        let mut filter = Filter::default();
        filter = match &cfg.kinds {
            // Explicit whitelist, minus any ephemeral kinds.
            Some(k) => filter.kinds(
                k.iter()
                    .map(|v| Kind::Custom(*v))
                    .filter(|k| !k.is_ephemeral()),
            ),
            None => filter.kinds(non_ephemeral_kinds()),
        };
        if cfg.since_now {
            filter = filter.since(Timestamp::now());
        }

        if let Err(e) = client.subscribe(filter, None).await {
            tracing::error!(error = %e, "firehose subscribe failed; retrying in 5s");
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            continue;
        }

        let mut rx = client.notifications();
        loop {
            match rx.recv().await {
                // The archive DB has already persisted this event at this point.
                Ok(RelayPoolNotification::Event { event, .. }) => sink(&to_core(&event)),
                Ok(RelayPoolNotification::Message { message, relay_url }) => {
                    if let RelayMessage::Notice(m) = message {
                        tracing::warn!(relay = %relay_url, "notice: {}", m);
                    }
                }
                Ok(RelayPoolNotification::Shutdown) => return Ok(()),
                Ok(_) => {}
                Err(_) => break, // stream closed → reconnect
            }
        }

        tracing::warn!("firehose stream closed; reconnecting in 5s");
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}
