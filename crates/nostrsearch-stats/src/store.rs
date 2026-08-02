//! Durable persistence for analyses: **binary** checkpoint state + progress.
//!
//! Layout (one directory):
//! ```text
//! <dir>/<name>.state.bin       # bincode of the whole analysis (checkpoint)
//! <dir>/<name>.progress.bin    # bincode of Progress (epoch + watermark + …)
//! ```
//! Binary (bincode + [`Hash32`](crate::types::Hash32) as raw 32 bytes) keeps
//! checkpoints compact and avoids building a giant `serde_json::Value` tree in
//! RAM at corpus scale. State is the *whole* analysis, so realtime folding
//! resumes exactly where it left off across restarts.

use crate::progress::Progress;
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct StatStore {
    dir: PathBuf,
}

impl StatStore {
    pub fn new(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("creating stat store dir {}", dir.display()))?;
        Ok(Self { dir })
    }

    /// Root directory of the store (external state, e.g. the graph, lives here).
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn state_path(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.state.bin"))
    }
    fn progress_path(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.progress.bin"))
    }

    /// Load saved `(state_bytes, progress)` for `name`, if present.
    pub fn load(&self, name: &str) -> Result<Option<(Vec<u8>, Progress)>> {
        let pp = self.progress_path(name);
        let sp = self.state_path(name);
        if !pp.exists() || !sp.exists() {
            return Ok(None);
        }
        let progress: Progress = bincode::deserialize(&std::fs::read(&pp)?)
            .with_context(|| format!("decoding {}", pp.display()))?;
        let state = std::fs::read(&sp)?;
        Ok(Some((state, progress)))
    }

    /// Persist `(state_bytes, progress)` for `name` via write+rename.
    pub fn save(&self, name: &str, state: &[u8], progress: &Progress) -> Result<()> {
        write_atomic(&self.state_path(name), state)?;
        write_atomic(&self.progress_path(name), &bincode::serialize(progress)?)?;
        Ok(())
    }
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// How far a rebuild has folded the archive.
///
/// A rebuild needs a resume point, and the per-analysis watermark cannot be it.
/// The watermark is a `created_at`, which orders a live stream but says nothing
/// about position in an archive: dump files are not sorted by time, so "highest
/// created_at folded" identifies no point to carry on from. Two events sharing
/// a timestamp can sit gigabytes apart.
///
/// The resume point is the id of the last event folded. An id is
/// self-validating in a way a byte offset is not: if the archive file is ever
/// rewritten, re-sorted or appended to, a stale offset still points *somewhere*
/// and resuming from it silently skips or repeats a span of events, corrupting
/// every counter with nothing to indicate it happened. A missing id is
/// detectable, and the rebuild can restart that file instead.
///
/// Resuming costs a linear re-read either way, so the id costs nothing: a plain
/// zstd frame cannot be opened part-way through, so the file has to be
/// decompressed from the start regardless. Scanning for the id needs only a
/// substring match against each raw line -- no JSON parsing, which is the
/// expensive part -- and `offset` is kept purely as a hint, to report progress
/// while skipping and to bound how far the scan looks before giving up.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct RebuildCheckpoint {
    /// Files fully folded, skipped entirely on resume.
    pub completed: Vec<String>,
    /// File in progress, if any.
    pub file: String,
    /// Id of the last event folded from `file`. Empty means start of file.
    pub last_id: String,
    /// Decompressed bytes folded, a hint only -- see the type docs.
    pub offset: u64,
}

impl StatStore {
    fn rebuild_path(&self) -> PathBuf {
        self.dir.join("rebuild.checkpoint.bin")
    }

    /// Load the rebuild checkpoint, if a rebuild was interrupted.
    pub fn load_rebuild(&self) -> Result<Option<RebuildCheckpoint>> {
        let p = self.rebuild_path();
        if !p.exists() {
            return Ok(None);
        }
        // A truncated or stale checkpoint must not wedge startup: losing it
        // costs a restart of the rebuild, while failing to start costs the node.
        match bincode::deserialize(&std::fs::read(&p)?) {
            Ok(cp) => Ok(Some(cp)),
            Err(e) => {
                tracing::warn!(error = %e, "discarding unreadable rebuild checkpoint");
                Ok(None)
            }
        }
    }

    /// Record how far the rebuild has folded.
    ///
    /// Must be written in the same persist as the analysis state it describes.
    /// A checkpoint ahead of the state re-reads nothing and silently loses
    /// events; behind it, events are folded twice and every counter inflates.
    pub fn save_rebuild(&self, cp: &RebuildCheckpoint) -> Result<()> {
        write_atomic(&self.rebuild_path(), &bincode::serialize(cp)?)
    }

    /// Drop the checkpoint once the rebuild finishes.
    pub fn clear_rebuild(&self) -> Result<()> {
        match std::fs::remove_file(self.rebuild_path()) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.into()),
            _ => Ok(()),
        }
    }
}
