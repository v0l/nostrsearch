//! The node's archive ingest, exposed over the admin API.
//!
//! This is a thin wrapper over [`nostrsearch_indexer::archive_ingest`] -- the
//! same engine `nostrsearch-ingest` runs. It used to be a separate
//! implementation, and the two diverged exactly as one would expect: different
//! readers, different staging, different resume, different bugs. The server's
//! copy never recorded indexed ids, so its dedupe store drifted ahead of the
//! index and every subsequent ingest skipped the events it was meant to add.
//!
//! This file holds only what the API needs on top of the engine: one run at a
//! time, a cancel switch, and a status shape for the console.

use nostrsearch_indexer::archive_ingest::{IngestOptions, IngestProgress};
use nostrsearch_indexer::id_store::IdStore;
use nostrsearch_indexer::pipeline::Pipeline;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Everything the admin API needs to run an ingest.
#[derive(Clone)]
pub struct IngestCtx {
    pub pipeline: Arc<Mutex<Pipeline>>,
    pub dir: std::path::PathBuf,
    pub dedupe: Option<Arc<IdStore>>,
    pub state: IngestState,
}

/// One run's counters and cancel switch.
#[derive(Clone)]
struct Run {
    progress: Arc<IngestProgress>,
    cancel: Arc<AtomicBool>,
}

impl Default for Run {
    fn default() -> Self {
        Self {
            progress: Arc::new(IngestProgress::default()),
            cancel: Arc::new(AtomicBool::new(false)),
        }
    }
}

/// Shared handle to the running (or last) ingest.
///
/// Each run installs a fresh [`Run`], so a status read can never blend the
/// counters of two runs; `active` is what serializes them.
#[derive(Clone, Default)]
pub struct IngestState {
    current: Arc<Mutex<Run>>,
    active: Arc<AtomicBool>,
}

/// What `GET /admin/ingest` returns.
#[derive(Debug, Serialize)]
pub struct IngestStatus {
    pub running: bool,
    pub cancelled: bool,
    /// Dependency-stage pass, 0-based, and how many this run makes. The
    /// archive is read once per stage: dependents cannot fold in the same pass
    /// that builds the world they label events with.
    pub pass: u64,
    pub passes: u64,
    /// Events handed to the index in the indexing pass.
    pub indexed: u64,
    /// Events read, including those skipped as already known.
    pub seen: u64,
    /// Events the dedupe store already had.
    pub skipped: u64,
    pub finished_at: u64,
}

impl IngestState {
    pub fn status(&self) -> IngestStatus {
        let run = self.current.lock().unwrap().clone();
        let p = &run.progress;
        IngestStatus {
            running: p.running.load(Ordering::Relaxed),
            cancelled: run.cancel.load(Ordering::Relaxed),
            pass: p.pass.load(Ordering::Relaxed),
            passes: p.passes.load(Ordering::Relaxed),
            indexed: p.indexed.load(Ordering::Relaxed),
            seen: p.seen.load(Ordering::Relaxed),
            skipped: p.skipped.load(Ordering::Relaxed),
            finished_at: p.finished_at.load(Ordering::Relaxed),
        }
    }

    pub fn is_running(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    /// Ask the run to stop. Honoured between chunks.
    pub fn cancel(&self) -> bool {
        if !self.is_running() {
            return false;
        }
        self.current
            .lock()
            .unwrap()
            .cancel
            .store(true, Ordering::Relaxed);
        true
    }
}

/// Start an ingest. `Err` if one is already running.
///
/// `dedupe` off re-indexes everything the archive holds, which is what a store
/// that has drifted ahead of the index needs: with it on, every attempt to
/// refill a gap is skipped as already-done.
pub fn start(ctx: &IngestCtx, dedupe: bool, parallelism: usize) -> Result<(), String> {
    // Claim the slot first: two ingests over one pipeline would interleave
    // passes and produce a world neither of them intended.
    if ctx
        .state
        .active
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Err("an ingest is already running".into());
    }

    let run = Run::default();
    *ctx.state.current.lock().unwrap() = run.clone();

    let pipeline = ctx.pipeline.clone();
    let opts = IngestOptions {
        input_dir: ctx.dir.clone(),
        parallelism,
        dedupe,
        ..Default::default()
    };
    let id_store = ctx.dedupe.clone();
    let active = ctx.state.active.clone();

    tokio::spawn(async move {
        let r = nostrsearch_indexer::archive_ingest::ingest(
            pipeline,
            opts,
            id_store,
            run.progress.clone(),
            run.cancel.clone(),
        )
        .await;
        if let Err(e) = r {
            tracing::error!(error = %e, "archive ingest failed");
        }
        // Released last: the slot must outlive the engine, or a second run
        // could start while this one is still finalizing.
        active.store(false, Ordering::Release);
    });

    Ok(())
}
