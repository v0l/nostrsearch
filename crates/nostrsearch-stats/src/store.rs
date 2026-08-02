//! Durable persistence for analyses: checkpoint state, progress, boundary set.
//!
//! Layout (one directory):
//! ```text
//! <dir>/<name>.state.bin       # bincode of the whole analysis (checkpoint)
//! <dir>/<name>.progress.json   # progress metadata, self-describing
//! <dir>/<name>.boundary.bin    # count-once id set, raw 32-byte keys
//! ```
//!
//! State stays binary: it is the *whole* analysis, so realtime folding resumes
//! exactly where it left off, and bincode avoids building a giant
//! `serde_json::Value` tree in RAM at corpus scale.
//!
//! Progress is split because its two halves want opposite things. The metadata
//! -- epoch, watermark, totals, rebuild position -- is about 100 bytes and
//! gains fields as the system grows, so it is stored as JSON: adding a field to
//! a bincode struct cannot read bytes written before it existed, which is
//! exactly how a field addition once stopped nodes from booting at all.
//!
//! The boundary set is the entire bulk (200k ids, 6.4 MB at the cap) and never
//! evolves -- it is fixed 32-byte keys. It gets its own file of raw
//! concatenated ids, which is smaller than any encoding of it inside a
//! document: JSON would more than double every persist, and persists happen
//! every five minutes per analysis.

use crate::progress::{Progress, ProgressMeta};
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
    /// Legacy bincode progress, read for migration and never written.
    fn legacy_progress_path(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.progress.bin"))
    }
    fn progress_path(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.progress.json"))
    }
    fn boundary_path(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.boundary.bin"))
    }

    /// Load saved `(state_bytes, progress)` for `name`, if present.
    pub fn load(&self, name: &str) -> Result<Option<(Vec<u8>, Progress)>> {
        let pp = self.progress_path(name);
        let legacy = self.legacy_progress_path(name);
        let sp = self.state_path(name);
        if !sp.exists() || (!pp.exists() && !legacy.exists()) {
            return Ok(None);
        }
        let state = std::fs::read(&sp)?;

        let mut progress: Progress = if pp.exists() {
            let meta: ProgressMeta = serde_json::from_slice(&std::fs::read(&pp)?)
                .with_context(|| format!("decoding {}", pp.display()))?;
            meta.into()
        } else {
            // Written before progress was split. Bincode is positional, so a
            // struct that gained a field cannot read older bytes -- try each
            // layout that build could have written, newest first.
            let raw = std::fs::read(&legacy)?;
            tracing::info!(analysis = name, "migrating progress to the split format");
            match bincode::deserialize::<crate::progress::ProgressV1>(&raw) {
                Ok(p) => p.into(),
                Err(e) => match bincode::deserialize::<crate::progress::ProgressV0>(&raw) {
                    Ok(p) => p.into(),
                    Err(_) => {
                        return Err(e).with_context(|| format!("decoding {}", legacy.display()));
                    }
                },
            }
        };

        // A missing or truncated boundary file costs count-once precision at
        // the watermark for a single lag window, which the analyses already
        // tolerate. Not worth refusing to start over.
        if let Ok(bytes) = std::fs::read(self.boundary_path(name)) {
            progress.boundary = bytes
                .chunks_exact(32)
                .map(|c| {
                    let mut id = [0u8; 32];
                    id.copy_from_slice(c);
                    crate::types::Hash32(id)
                })
                .collect();
        }

        Ok(Some((state, progress)))
    }

    /// Persist `(state_bytes, progress)` for `name` via write+rename.
    pub fn save(&self, name: &str, state: &[u8], progress: &Progress) -> Result<()> {
        write_atomic(&self.state_path(name), state)?;
        write_atomic(
            &self.progress_path(name),
            &serde_json::to_vec(&ProgressMeta::from(progress))?,
        )?;

        // Raw concatenated keys: no framing, no encoding overhead, and a
        // truncated file simply yields fewer ids rather than failing to parse.
        let mut ids = Vec::with_capacity(progress.boundary.len() * 32);
        for id in &progress.boundary {
            ids.extend_from_slice(&id.0);
        }
        write_atomic(&self.boundary_path(name), &ids)?;

        // The old combined file is now stale and must not be read again.
        let _ = std::fs::remove_file(self.legacy_progress_path(name));
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
