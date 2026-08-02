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

impl StatStore {
    fn rebuild_path(&self) -> PathBuf {
        self.dir.join("rebuild.checkpoint.bin")
    }

    /// Delete the old global checkpoint left by earlier versions.
    ///
    /// Rebuild position is per-analysis now, in [`crate::Progress`]. The old
    /// file described a single shared position that could not represent
    /// analyses at different points, and clearing it on one analysis destroyed
    /// the resume point of a rebuild running for another.
    pub fn remove_legacy_rebuild_checkpoint(&self) {
        let _ = std::fs::remove_file(self.rebuild_path());
    }
}
