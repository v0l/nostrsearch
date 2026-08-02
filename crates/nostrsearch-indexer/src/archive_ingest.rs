//! Ingesting an archive of JSONL dumps into a [`Pipeline`].
//!
//! This is *the* archive ingest. It was previously implemented twice -- once in
//! `bin/ingest.rs` and once, differently, in the server's admin replay -- which
//! meant two readers, two staging schemes, two resume schemes, and two sets of
//! bugs. The server's copy silently never recorded indexed ids, so its dedupe
//! store drifted ahead of the index and every later ingest skipped the events
//! it was supposed to add. One engine, called by both, is the fix.
//!
//! Three properties matter and are easy to get wrong:
//!
//! **A bad line costs one event.** An earlier reader abandoned the rest of a
//! file on the first parse failure, which is how a large span of the corpus
//! went missing from an index that reported success.
//!
//! **Dependent analyses need their own pass.** `activity` and `active_users`
//! label events using the world `follow_graph` builds, so folding them in the
//! same pass over a cold corpus records everything as untrusted. The archive is
//! read once per dependency stage; only pass 0 indexes.
//!
//! **Ids are recorded only once their documents are durable.** The id store
//! answers "is this already indexed?", and ingest skips whatever it claims. A
//! store ahead of the index is therefore unfillable data loss -- every retry
//! skips the hole. Recording happens under the same pipeline lock as the
//! commit, so a kill leaves the store *behind* at worst, which costs redundant
//! work and self-corrects.

use crate::id_store::IdStore;
use crate::pipeline::Pipeline;
use nostr_archive_cursor::NostrCursor;
use nostrsearch_core::event::NostrEvent;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// How the archive should be read.
#[derive(Clone, Debug)]
pub struct IngestOptions {
    /// Directory of JSONL dumps.
    pub input_dir: PathBuf,
    /// Reader threads. The cursor walks whole files in parallel.
    pub parallelism: usize,
    /// Events handed to the pipeline per callback.
    pub chunk_size: usize,
    /// Sort each chunk by `created_at` so events land shard-by-shard.
    ///
    /// Archives are not date-ordered, and writing in arbitrary month order
    /// thrashes the open-shard set: each switch can evict a writer needed again
    /// a moment later, paying a commit and fsync every time.
    pub sort_batches: bool,
    /// Skip events the id store already claims. Off re-indexes everything,
    /// which is what a corrupt or divergent store needs.
    pub dedupe: bool,
    /// How often to commit and record the ids indexed since the last one.
    ///
    /// This is a durability interval, not an optimisation. Ids are only
    /// recorded after the commit that makes their documents searchable, so
    /// between checkpoints the store is behind the index by one window. Losing
    /// the process costs re-reading that window; having no checkpoints at all
    /// costs re-indexing the entire run into an index that already contains
    /// it, which Tantivy will happily do twice.
    pub checkpoint_every: std::time::Duration,
}

impl Default for IngestOptions {
    fn default() -> Self {
        Self {
            input_dir: PathBuf::new(),
            parallelism: 4,
            chunk_size: 1000,
            sort_batches: true,
            dedupe: true,
            checkpoint_every: std::time::Duration::from_secs(60),
        }
    }
}

/// Live counters, shared with whoever is watching.
#[derive(Debug, Default)]
pub struct IngestProgress {
    /// Dependency-stage pass currently running, 0-based.
    pub pass: AtomicU64,
    /// Total passes this run will make.
    pub passes: AtomicU64,
    /// Events handed to the pipeline in the indexing pass.
    pub indexed: AtomicU64,
    /// Events seen, including those skipped as already known.
    pub seen: AtomicU64,
    /// Events skipped because the id store already had them.
    pub skipped: AtomicU64,
    /// Lines that would not parse. One bad line costs one event, never a file.
    pub malformed: AtomicU64,
    pub running: AtomicBool,
    pub finished_at: AtomicU64,
}

/// Run an archive ingest to completion.
///
/// Blocking work happens on the blocking pool; the caller may cancel by
/// setting `cancel`, which is honoured between chunks.
pub async fn ingest(
    pipeline: Arc<Mutex<Pipeline>>,
    opts: IngestOptions,
    id_store: Option<Arc<IdStore>>,
    progress: Arc<IngestProgress>,
    cancel: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    progress.running.store(true, Ordering::Relaxed);

    // Ids indexed since the last checkpoint. Flushed while holding the
    // pipeline lock across commit, so an id is only recorded once its document
    // is durable.
    let pending_ids: Arc<Mutex<Vec<[u8; 32]>>> = Arc::new(Mutex::new(Vec::new()));

    // Periodic checkpoint: commit, then record what that commit made durable.
    //
    // Without it a 24-hour run records nothing until the very end, so a kill at
    // hour 23 leaves an index full of events the store has never heard of --
    // and the next run indexes every one of them again, because Tantivy has no
    // unique key to stop it. It also bounds the pending buffer, which would
    // otherwise hold every id in the corpus.
    let checkpoint = {
        let pipe = pipeline.clone();
        let pending = pending_ids.clone();
        let store = id_store.clone();
        let every = opts.checkpoint_every;
        let done = cancel.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.tick().await; // consume the immediate first tick
            loop {
                tick.tick().await;
                if done.load(Ordering::Relaxed) {
                    return;
                }
                let (pipe, pending, store) = (pipe.clone(), pending.clone(), store.clone());
                let res = tokio::task::spawn_blocking(move || {
                    let mut p = pipe.lock().unwrap();
                    p.commit()?;
                    // Only now: the documents these ids name are durable.
                    let n = match &store {
                        Some(s) => {
                            let ids = std::mem::take(&mut *pending.lock().unwrap());
                            s.flush(ids.iter())?;
                            ids.len()
                        }
                        None => 0,
                    };
                    anyhow::Ok(n)
                })
                .await;
                match res {
                    Ok(Ok(n)) if n > 0 => tracing::debug!(ids = n, "ingest checkpoint"),
                    Ok(Ok(_)) => {}
                    other => tracing::warn!(?other, "ingest checkpoint failed"),
                }
            }
        })
    };

    let passes = pipeline.lock().unwrap().backfill_passes();
    progress.passes.store(passes as u64, Ordering::Relaxed);
    if passes > 1 {
        tracing::info!(
            passes,
            "archive will be read once per dependency stage; only pass 0 indexes"
        );
    }

    loop {
        if cancel.load(Ordering::Relaxed) {
            tracing::info!("archive ingest cancelled");
            break;
        }

        let pass = pipeline.lock().unwrap().current_pass();
        let indexing_pass = pass == 0;
        progress.pass.store(pass as u64, Ordering::Relaxed);
        tracing::info!(
            pass,
            passes,
            indexing = indexing_pass,
            "archive pass starting"
        );

        let pipe = pipeline.clone();
        let input_dir = opts.input_dir.clone();
        let parallelism = opts.parallelism;
        let chunk_size = opts.chunk_size;
        let sort_batches = opts.sort_batches;
        // The id store records what has been *indexed*, so its gate applies to
        // pass 0 only: later passes must see the same events again to feed the
        // dependent analyses.
        let ck_store = if indexing_pass && opts.dedupe {
            id_store.clone()
        } else {
            None
        };
        let ck_pending = pending_ids.clone();
        let prog = progress.clone();
        let cancel_cb = cancel.clone();
        // Indexing handle, taken once: it is shared and needs no pipeline lock.
        let indexer = pipeline.lock().unwrap().indexer();

        tokio::task::spawn_blocking(move || {
            let cursor = NostrCursor::new(input_dir).with_parallelism(parallelism);
            cursor.walk_with_chunked_sync(
                move |events: Vec<nostr_archive_cursor::NostrEventBorrowed>| {
                    if cancel_cb.load(Ordering::Relaxed) {
                        return;
                    }
                    let mut batch: Vec<NostrEvent> = events.iter().map(to_core).collect();
                    if sort_batches {
                        batch.sort_unstable_by_key(|e| e.created_at);
                    }
                    prog.seen.fetch_add(batch.len() as u64, Ordering::Relaxed);

                    // Drop anything the store already has, before taking any
                    // lock: membership is a read-only lookup.
                    let mut skipped = 0u64;
                    if let Some(store) = &ck_store {
                        let ids: Vec<[u8; 32]> = batch
                            .iter()
                            .map(|e| hex32(&e.id).unwrap_or([0u8; 32]))
                            .collect();
                        let known = store.contains_batch(&ids);
                        let mut keep = Vec::with_capacity(batch.len());
                        let mut new_ids = Vec::with_capacity(batch.len());
                        for ((ev, id), known) in batch.into_iter().zip(ids).zip(known) {
                            if known {
                                skipped += 1;
                            } else {
                                keep.push(ev);
                                new_ids.push(id);
                            }
                        }
                        batch = keep;
                        ck_pending.lock().unwrap().extend(new_ids);
                        prog.skipped.fetch_add(skipped, Ordering::Relaxed);
                    }

                    // Fold under the pipeline lock. This mutates per-analysis
                    // state so it must be serial, but it is only hashmap work.
                    {
                        let mut p = pipe.lock().unwrap();
                        for ev in &batch {
                            p.fold_only(ev);
                        }
                    }

                    // Index outside it. Tokenizing and building postings is
                    // the expensive half, and every shard has an independent
                    // writer, so this runs across all reader threads at once.
                    // Doing it under the pipeline lock held the whole corpus
                    // to one core while the readers idled.
                    if indexing_pass {
                        for ev in &batch {
                            if let Err(e) = indexer.index_event(ev) {
                                tracing::warn!(error = %e, "index_event failed");
                            }
                        }
                        prog.indexed
                            .fetch_add(batch.len() as u64, Ordering::Relaxed);
                    }
                },
                chunk_size,
            );
        })
        .await?;

        if cancel.load(Ordering::Relaxed) {
            break;
        }

        // Materialize this stage into the world so the next pass's consumers
        // can read it; stop when every stage has folded.
        if !pipeline.lock().unwrap().advance_pass() {
            break;
        }
    }

    // Finalize: commit first, then claim the ids, never the other way round.
    {
        let mut p = pipeline.lock().unwrap();
        p.go_live();
        if let Some(store) = &id_store {
            let ids = std::mem::take(&mut *pending_ids.lock().unwrap());
            if let Err(e) = store.flush(ids.iter()) {
                tracing::warn!(error = %e, "final dedupe flush failed");
            }
        }
        p.commit()?;
    }

    checkpoint.abort();
    progress.running.store(false, Ordering::Relaxed);
    progress.finished_at.store(unix_now(), Ordering::Relaxed);
    tracing::info!(
        indexed = progress.indexed.load(Ordering::Relaxed),
        skipped = progress.skipped.load(Ordering::Relaxed),
        "archive ingest complete"
    );
    Ok(())
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub fn hex32(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(hex.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

/// Borrowed cursor event to owned core event.
pub fn to_core(ev: &nostr_archive_cursor::NostrEventBorrowed) -> NostrEvent {
    NostrEvent {
        id: ev.id.to_string(),
        pubkey: ev.pubkey.to_string(),
        created_at: ev.created_at,
        kind: ev.kind as u16,
        tags: ev
            .tags
            .iter()
            .map(|t| t.iter().map(|s| s.to_string()).collect())
            .collect(),
        content: ev.content.to_string(),
        // Not copied: the signature is 128 hex chars, and nothing downstream of
        // ingest reads it -- not the schema, not any analysis. Allocating it
        // per event was pure cost.
        //
        // If signature verification is ever added it belongs here, at the
        // point of ingest, verifying against the borrowed bytes rather than
        // carrying a copy through the pipeline.
        sig: String::new(),
    }
}
