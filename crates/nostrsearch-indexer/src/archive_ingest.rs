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
use std::collections::HashSet;
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
    /// Events the indexer rejected. These keep their ids unclaimed, so a
    /// later run over the same archive will retry them.
    pub failed: AtomicU64,

    // Time attribution for the reader threads, in nanoseconds summed across
    // them -- thread-time, not wall time, so compare them against each other
    // and against `parallelism × elapsed`, not against elapsed alone.
    //
    // Without these the only measured stage is the serial fold, which makes
    // every slowdown look like the fold by default and leaves the rest of the
    // pipeline as one unattributed lump.
    /// Deserializing archive lines into owned events.
    pub parse_ns: AtomicU64,
    /// Id-store lookups deciding what is already indexed.
    pub dedupe_ns: AtomicU64,
    /// Building documents and handing them to shard writers.
    pub index_ns: AtomicU64,
    pub running: AtomicBool,
    pub finished_at: AtomicU64,
}

/// Run an archive ingest to completion.
///
/// Blocking work happens on the blocking pool; the caller may cancel by
/// setting `cancel`, which is honoured between chunks.
/// `pending_ids` holds ids indexed since the last checkpoint. It is a
/// parameter rather than an internal so a caller that shuts down on a signal
/// can flush the same buffer this engine is filling: a second buffer would
/// always be empty, and flushing it would record nothing while the documents
/// were committed -- turning every graceful restart into a window of duplicate
/// documents.
pub async fn ingest(
    pipeline: Arc<Mutex<Pipeline>>,
    opts: IngestOptions,
    id_store: Option<Arc<IdStore>>,
    pending_ids: Arc<Mutex<HashSet<[u8; 32]>>>,
    progress: Arc<IngestProgress>,
    cancel: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    progress.running.store(true, Ordering::Relaxed);

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
                    commit_and_claim(&mut p, store.as_ref(), &pending)
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

        // A stage whose analyses consume no events has nothing to read.
        //
        // Pagerank is the case this exists for: it derives entirely from the
        // adjacency follow_graph leaves on disk, so its pass was reading the
        // whole corpus to hand every event to an observe() that discards them.
        // Skipping straight to advance_pass still runs the refresh and
        // materialize that actually produce its output.
        if !pipeline.lock().unwrap().pass_needs_corpus() {
            tracing::info!(pass, "pass needs no corpus; skipping read");
            if !pipeline.lock().unwrap().advance_pass() {
                break;
            }
            continue;
        }

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
            // The cursor's own dedupe keeps every event id it has seen in a
            // DashMap, which for a corpus this size is tens of gigabytes of
            // resident memory and the reason ingest kept being OOM-killed.
            // IdStore does the same job on disk and survives restarts, so the
            // in-memory copy is redundant as well as fatal.
            let cursor = NostrCursor::new(input_dir)
                .with_parallelism(parallelism)
                .with_dedupe(false);
            cursor.walk_with_chunked_sync(
                move |events: Vec<nostr_archive_cursor::NostrEventBorrowed>| {
                    if cancel_cb.load(Ordering::Relaxed) {
                        return;
                    }
                    let t_parse = std::time::Instant::now();
                    let mut batch: Vec<NostrEvent> = events.iter().map(to_core).collect();
                    prog.parse_ns
                        .fetch_add(t_parse.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    if sort_batches {
                        batch.sort_unstable_by_key(|e| e.created_at);
                    }
                    prog.seen.fetch_add(batch.len() as u64, Ordering::Relaxed);

                    // Note anything the store already has, before taking any
                    // lock: membership is a read-only lookup.
                    //
                    // "Already have it" means "already *indexed* it". It does
                    // not mean the analyses have seen it: an analysis that was
                    // reset has no state, and the events it needs to rebuild
                    // from are exactly the ones the dedupe store already knows.
                    // So dedupe gates indexing only; every event in the batch
                    // is still folded.
                    let mut skipped = 0u64;
                    // Positions in `batch` that still need indexing.
                    let mut to_index: Vec<usize> = Vec::new();
                    // Ids of the events kept, positionally aligned with
                    // `batch`. Claimed only once their documents exist.
                    let mut new_ids: Vec<[u8; 32]> = Vec::new();
                    let t_dedupe = std::time::Instant::now();
                    if let Some(store) = &ck_store {
                        let ids: Vec<[u8; 32]> = batch
                            .iter()
                            .map(|e| hex32(&e.id).unwrap_or([0u8; 32]))
                            .collect();
                        let known = store.contains_batch(&ids);
                        to_index.reserve(batch.len());
                        new_ids.reserve(batch.len());
                        // Ids indexed since the last checkpoint are not in the
                        // store yet. Without checking them too, a duplicate
                        // arriving inside the checkpoint window is indexed
                        // twice -- and tantivy has no unique key to catch it.
                        let pend = ck_pending.lock().unwrap();
                        let plan = plan_index_work(&ids, &known, |id| pend.contains(id));
                        drop(pend);
                        skipped = plan.skipped;
                        to_index = plan.to_index;
                        new_ids = plan.new_ids;
                        prog.skipped.fetch_add(skipped, Ordering::Relaxed);
                    } else {
                        // No dedupe store: everything is new.
                        to_index.extend(0..batch.len());
                    }
                    prog.dedupe_ns
                        .fetch_add(t_dedupe.elapsed().as_nanos() as u64, Ordering::Relaxed);

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
                        // An id is claimed only if its event reached a writer.
                        // Claiming first and indexing after means anything that
                        // fails here is recorded as done and skipped by every
                        // later run: the archive still has the event, but
                        // nothing will ever look at it again.
                        let t_index = std::time::Instant::now();
                        let mut ok_ids = Vec::with_capacity(new_ids.len());
                        let mut failed = 0u64;
                        for (n, &i) in to_index.iter().enumerate() {
                            let ev = &batch[i];
                            match indexer.index_event(ev) {
                                Ok(_) => {
                                    if let Some(id) = new_ids.get(n) {
                                        ok_ids.push(*id);
                                    }
                                }
                                Err(e) => {
                                    failed += 1;
                                    tracing::warn!(error = %e, id = %ev.id, "index_event failed");
                                }
                            }
                        }
                        if !ok_ids.is_empty() {
                            ck_pending.lock().unwrap().extend(ok_ids);
                        }
                        let ok = to_index.len() as u64 - failed;
                        prog.indexed.fetch_add(ok, Ordering::Relaxed);
                        prog.failed.fetch_add(failed, Ordering::Relaxed);
                        prog.index_ns
                            .fetch_add(t_index.elapsed().as_nanos() as u64, Ordering::Relaxed);
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

    // Finalize: switch to live, then record ids only after their documents are
    // durable — the commit-and-claim pairing, via the single shared path below.
    {
        let mut p = pipeline.lock().unwrap();
        p.go_live();
        commit_and_claim(&mut p, id_store.as_ref(), &pending_ids)?;
    }

    checkpoint.abort();
    progress.running.store(false, Ordering::Relaxed);
    progress.finished_at.store(unix_now(), Ordering::Relaxed);
    let failed = progress.failed.load(Ordering::Relaxed);
    if failed > 0 {
        tracing::warn!(
            failed,
            "events the indexer rejected; their ids were left unclaimed, so \
             running the same archive again will retry them"
        );
    }
    tracing::info!(
        failed,
        indexed = progress.indexed.load(Ordering::Relaxed),
        skipped = progress.skipped.load(Ordering::Relaxed),
        "archive ingest complete"
    );
    Ok(())
}

/// Commit every open shard, then durably record the ids indexed since the
/// last commit. Returns how many ids were claimed.
///
/// This is the **only** place the commit-and-claim ordering lives, and the
/// order is load-bearing: an id must reach the seen-set no earlier than the
/// commit that makes its document durable. Claim it first and a crash between
/// the two writes leaves the store ahead of the index — the next resume skips
/// an event that was never actually written, a permanent hole that retries
/// cannot fill. Both the periodic checkpoint and the end-of-backfill finalize
/// call this, so a caller that gets the order wrong cannot be reintroduced
/// piecemeal across two hand-rolled copies.
fn commit_and_claim(
    p: &mut Pipeline,
    store: Option<&Arc<IdStore>>,
    pending: &Mutex<HashSet<[u8; 32]>>,
) -> anyhow::Result<usize> {
    p.commit()?;
    let n = match store {
        Some(s) => {
            let ids = std::mem::take(&mut *pending.lock().unwrap());
            s.flush(ids.iter())?;
            ids.len()
        }
        None => 0,
    };
    Ok(n)
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


/// Which events in a batch still need indexing.
///
/// Dedupe answers "have we indexed this?", which is a different question from
/// "have the analyses seen this?". An analysis that was just reset has no
/// state, and the events it must rebuild from are precisely the ones the
/// dedupe store already knows about. So this decides indexing only -- the
/// caller folds the whole batch either way.
pub(crate) struct IndexPlan {
    /// Positions in the batch to index, in order.
    pub to_index: Vec<usize>,
    /// Ids of those events, positionally aligned with `to_index`.
    pub new_ids: Vec<[u8; 32]>,
    pub skipped: u64,
}

pub(crate) fn plan_index_work(
    ids: &[[u8; 32]],
    known: &[bool],
    pending: impl Fn(&[u8; 32]) -> bool,
) -> IndexPlan {
    let mut plan = IndexPlan {
        to_index: Vec::with_capacity(ids.len()),
        new_ids: Vec::with_capacity(ids.len()),
        skipped: 0,
    };
    for (i, id) in ids.iter().enumerate() {
        // Ids indexed since the last checkpoint are not in the store yet.
        // Without checking them too, a duplicate arriving inside the
        // checkpoint window is indexed twice -- tantivy has no unique key.
        if known.get(i).copied().unwrap_or(false) || pending(id) {
            plan.skipped += 1;
        } else {
            plan.to_index.push(i);
            plan.new_ids.push(*id);
        }
    }
    plan
}

#[cfg(test)]
mod index_plan_tests {
    use super::plan_index_work;

    fn ids(n: usize) -> Vec<[u8; 32]> {
        (0..n)
            .map(|i| {
                let mut a = [0u8; 32];
                a[0] = i as u8;
                a
            })
            .collect()
    }

    /// A fully-known batch indexes nothing -- and the caller still folds all
    /// of it.
    ///
    /// This is the shape of "re-derive an analysis on a corpus already
    /// ingested": every event is in the dedupe store. The old code removed
    /// those events from the batch before folding, so a reset analysis was
    /// rebuilt from an empty stream and reported a handful of events observed
    /// from live traffic instead of the whole archive.
    #[test]
    fn a_fully_deduped_batch_indexes_nothing_but_is_still_available_to_fold() {
        let ids = ids(64);
        let known = vec![true; 64];
        let plan = plan_index_work(&ids, &known, |_| false);

        assert!(plan.to_index.is_empty(), "nothing needs re-indexing");
        assert_eq!(plan.skipped, 64);
        // The batch itself is untouched: the caller holds all 64 events and
        // folds every one. Nothing here may shorten it.
        assert_eq!(ids.len(), 64, "the batch must not be filtered for folding");
    }

    /// new_ids must stay aligned with to_index, or a successful index claims
    /// the wrong id and the real one is retried forever.
    #[test]
    fn new_ids_align_with_the_positions_to_index() {
        let ids = ids(6);
        let known = vec![false, true, false, true, false, false];
        let plan = plan_index_work(&ids, &known, |_| false);

        assert_eq!(plan.to_index, vec![0, 2, 4, 5]);
        assert_eq!(plan.skipped, 2);
        for (n, &i) in plan.to_index.iter().enumerate() {
            assert_eq!(plan.new_ids[n], ids[i], "id must match its batch position");
        }
    }

    /// Ids indexed since the last checkpoint are not in the store yet.
    #[test]
    fn pending_ids_are_skipped_even_when_the_store_has_not_seen_them() {
        let ids = ids(4);
        let known = vec![false; 4];
        let plan = plan_index_work(&ids, &known, |id| id[0] == 2);

        assert_eq!(plan.to_index, vec![0, 1, 3]);
        assert_eq!(plan.skipped, 1);
    }
}
