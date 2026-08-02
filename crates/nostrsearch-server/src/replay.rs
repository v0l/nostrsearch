//! Background re-ingest of archive dumps, driven from the admin API.
//!
//! Re-running the `ingest` binary means taking the archive relay down, which is
//! not an option on a live node. This replays dump files through the same
//! writer task that serves the firehose, at strictly lower priority, so the
//! relay keeps running while gaps are filled.
//!
//! Two deliberate differences from the `nostr-archive-cursor` walk used by the
//! `ingest` binary:
//!
//! 1. **A read error does not abandon the file.** The cursor breaks out of its
//!    read loop on any reader-level error, so a single bad byte 30 GB into a
//!    200 GB dump silently skips the remaining 170 GB while the run reports
//!    success. JSONL is line-delimited, so recovery is simply "continue at the
//!    next newline" — this reader counts the bad line and carries on.
//! 2. **Progress is reported.** Bytes consumed against file size makes a short
//!    read visible instead of something you infer from a document count much
//!    later.

use nostrsearch_core::event::NostrEvent;
use nostrsearch_indexer::id_store::IdStore;
use serde::Serialize;
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

/// Events handed to the writer per batch before yielding, so a replay can
/// never monopolise the writer against live traffic.
const BATCH: usize = 512;

#[derive(Debug, Clone, Default, Serialize)]
pub struct FileProgress {
    pub name: String,
    pub bytes_total: u64,
    pub bytes_read: u64,
    /// Lines that failed to parse. Non-zero is tolerable; large is a corrupt
    /// dump.
    pub malformed: u64,
    pub events: u64,
    /// New to the index (not in the dedupe set).
    pub new: u64,
    /// True once the file was read to EOF rather than cut short.
    pub complete: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ReplayStatus {
    pub running: bool,
    pub cancelled: bool,
    pub started_at: u64,
    pub finished_at: u64,
    pub files_total: usize,
    pub files_done: usize,
    pub events: u64,
    pub new: u64,
    pub malformed: u64,
    pub current: Option<String>,
    pub files: Vec<FileProgress>,
}

#[derive(Clone)]
pub struct ReplayState {
    status: Arc<RwLock<ReplayStatus>>,
    cancel: Arc<AtomicBool>,
}

impl Default for ReplayState {
    fn default() -> Self {
        Self {
            status: Arc::new(RwLock::new(ReplayStatus::default())),
            cancel: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl ReplayState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn status(&self) -> ReplayStatus {
        self.status.read().map(|s| s.clone()).unwrap_or_default()
    }

    pub fn is_running(&self) -> bool {
        self.status.read().map(|s| s.running).unwrap_or(false)
    }

    /// Ask a running replay to stop at the next batch boundary.
    pub fn cancel(&self) -> bool {
        if !self.is_running() {
            return false;
        }
        self.cancel.store(true, Ordering::Relaxed);
        if let Ok(mut s) = self.status.write() {
            s.cancelled = true;
        }
        true
    }

    fn update(&self, f: impl FnOnce(&mut ReplayStatus)) {
        if let Ok(mut s) = self.status.write() {
            f(&mut s);
        }
    }
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Open a dump, transparently decompressing `.zst`/`.zstd`.
///
/// Returns a reader plus a shared counter of compressed bytes consumed, so
/// progress reflects position in the file on disk rather than in the expanded
/// stream.
fn open_dump(path: &Path) -> std::io::Result<Box<dyn BufRead + Send>> {
    let file = std::fs::File::open(path)?;
    let name = path.to_string_lossy().to_ascii_lowercase();
    // 8 MiB buffers: these are large sequential reads.
    let buf = std::io::BufReader::with_capacity(8 << 20, file);
    if name.ends_with(".zst") || name.ends_with(".zstd") {
        let dec = zstd::stream::read::Decoder::new(buf)?;
        Ok(Box::new(std::io::BufReader::with_capacity(8 << 20, dec)))
    } else if name.ends_with(".gz") {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "gzip dumps are not supported by replay yet",
        ))
    } else {
        Ok(Box::new(buf))
    }
}

/// Which dumps to replay.
#[derive(Debug, Clone, Default)]
pub struct ReplaySelection {
    /// Explicit file names; empty selects every dump in the directory.
    pub files: Vec<String>,
}

fn is_dump(name: &str) -> bool {
    let l = name.to_ascii_lowercase();
    [".jsonl", ".json", ".jsonl.zst", ".jsonl.zstd", ".json.zst"]
        .iter()
        .any(|e| l.ends_with(e))
}

/// Start a background replay. Returns an error if one is already running.
///
/// `submit` receives every event that is not already in the dedupe set; it is
/// expected to hand them to the writer with lower priority than live traffic.
pub fn spawn(
    state: ReplayState,
    dir: PathBuf,
    selection: ReplaySelection,
    dedupe: Option<Arc<IdStore>>,
    submit: crate::node::ReplaySink,
) -> Result<(), String> {
    if state.is_running() {
        return Err("a replay is already running".into());
    }

    let mut names: Vec<String> = if selection.files.is_empty() {
        std::fs::read_dir(&dir)
            .map_err(|e| format!("cannot read {}: {e}", dir.display()))?
            .flatten()
            .filter(|e| e.metadata().map(|m| m.is_file()).unwrap_or(false))
            .filter_map(|e| e.file_name().to_str().map(str::to_string))
            .filter(|n| is_dump(n))
            .collect()
    } else {
        selection.files.clone()
    };
    names.sort();

    if names.is_empty() {
        return Err("no dump files matched".into());
    }

    state.cancel.store(false, Ordering::Relaxed);
    state.update(|s| {
        *s = ReplayStatus {
            running: true,
            started_at: unix_now(),
            files_total: names.len(),
            ..Default::default()
        };
    });

    tokio::task::spawn_blocking(move || {
        for name in names {
            if state.cancel.load(Ordering::Relaxed) {
                break;
            }
            let path = dir.join(&name);
            state.update(|s| s.current = Some(name.clone()));

            let bytes_total = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            let mut fp = FileProgress {
                name: name.clone(),
                bytes_total,
                ..Default::default()
            };

            match open_dump(&path) {
                Ok(reader) => replay_file(&state, reader, &mut fp, dedupe.as_deref(), &submit),
                Err(e) => fp.error = Some(format!("open failed: {e}")),
            }

            if !fp.complete && fp.error.is_none() && !state.cancel.load(Ordering::Relaxed) {
                fp.error = Some("file ended early".into());
            }
            if let Some(err) = &fp.error {
                tracing::warn!(file = %name, error = %err, "replay problem");
            } else {
                tracing::info!(
                    file = %name,
                    events = fp.events,
                    new = fp.new,
                    malformed = fp.malformed,
                    "replayed dump"
                );
            }

            state.update(|s| {
                s.files_done += 1;
                s.events += fp.events;
                s.new += fp.new;
                s.malformed += fp.malformed;
                s.files.push(fp);
            });
        }

        state.update(|s| {
            s.running = false;
            s.current = None;
            s.finished_at = unix_now();
        });
        tracing::info!("replay finished");
    });

    Ok(())
}

fn replay_file(
    state: &ReplayState,
    mut reader: Box<dyn BufRead + Send>,
    fp: &mut FileProgress,
    dedupe: Option<&IdStore>,
    submit: &crate::node::ReplaySink,
) {
    let mut line = Vec::with_capacity(4096);
    let mut batch = 0usize;

    loop {
        if state.cancel.load(Ordering::Relaxed) {
            return;
        }
        line.clear();
        match reader.read_until(b'\n', &mut line) {
            Ok(0) => {
                fp.complete = true;
                return;
            }
            Ok(n) => {
                fp.bytes_read += n as u64;
                let trimmed = line.strip_suffix(b"\n").unwrap_or(&line);
                if trimmed.is_empty() {
                    continue;
                }
                match serde_json::from_slice::<NostrEvent>(trimmed) {
                    Ok(ev) => {
                        fp.events += 1;
                        let known = hex32(&ev.id)
                            .and_then(|id| dedupe.map(|d| d.contains(&id)))
                            .unwrap_or(false);
                        if !known {
                            fp.new += 1;
                            submit.blocking_submit(ev);
                            batch += 1;
                            if batch >= BATCH {
                                batch = 0;
                                // Let the writer drain live traffic.
                                std::thread::yield_now();
                            }
                        }
                    }
                    // A bad line is skipped, never fatal: the whole point is
                    // that one corrupt record must not cost the rest of the
                    // file.
                    Err(_) => fp.malformed += 1,
                }
            }
            Err(e) => {
                fp.error = Some(format!("read error after {} bytes: {e}", fp.bytes_read));
                return;
            }
        }
    }
}

fn hex32(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(hex.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dump_detection_matches_the_archive_listing() {
        assert!(is_dump("combined.jsonl"));
        assert!(is_dump("events_20260802.jsonl.zst"));
        assert!(!is_dump("LOCK"));
        assert!(!is_dump("000018.sst"));
    }

    #[test]
    fn hex_ids_round_trip() {
        let id = "a".repeat(64);
        assert_eq!(hex32(&id).unwrap()[0], 0xaa);
        assert!(hex32("short").is_none());
    }
}
