use crate::nip98;
use crate::node::{EventSink, to_core};
use axum::{
    Json, Router,
    body::Body,
    extract::{OriginalUri, Path, Query, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use futures::StreamExt;
use nostr_archive_cursor::DefaultJsonFilesDatabase;
use nostr_sdk::prelude::*;
use nostrsearch_indexer::id_store::IdStore;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Read};
use std::sync::atomic::AtomicBool;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

const MAX_LINE_BYTES: u64 = 1 << 20;
const FUTURE_SLACK_SECS: u64 = 86_400;
const CHANNEL_CHUNKS: usize = 8;
const FLOOR_CHECK_EVERY: std::time::Duration = std::time::Duration::from_secs(1);
const STREAMED_FORMATS: [&str; 4] = [".jsonl", ".jsonl.zst", ".jsonl.gz", ".jsonl.bz2"];
const DISK_FORMATS: [&str; 3] = [".mdb", ".mdb.zst", ".mdb.gz"];
const FORMATS: [&str; 7] = [
    ".jsonl",
    ".jsonl.zst",
    ".jsonl.gz",
    ".jsonl.bz2",
    ".mdb",
    ".mdb.zst",
    ".mdb.gz",
];

pub struct SubmitConfig {
    pub dir: PathBuf,
    pub max_concurrent: usize,
    pub min_free_bytes: u64,
    pub free_space: Option<Arc<dyn Fn() -> u64 + Send + Sync>>,
    pub origin: Option<String>,
    pub admins: HashSet<PublicKey>,
}

impl SubmitConfig {
    fn free(&self) -> u64 {
        match &self.free_space {
            Some(f) => f(),
            None => free_bytes(&self.dir).unwrap_or(0),
        }
    }

    pub fn from_env(
        dir: PathBuf,
        admins: HashSet<PublicKey>,
        admin_origin: Option<String>,
    ) -> Self {
        let u = |k: &str, d: u64| {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(d)
        };
        const GB: u64 = 1_000_000_000;
        Self {
            dir: std::env::var("SUBMIT_DIR").map(PathBuf::from).unwrap_or(dir),
            max_concurrent: u("SUBMIT_MAX_CONCURRENT", 2) as usize,
            min_free_bytes: u("SUBMIT_MIN_FREE_BYTES", 100 * GB),
            free_space: None,
            origin: std::env::var("SUBMIT_ORIGIN")
                .ok()
                .map(|s| s.trim_end_matches('/').to_string())
                .or(admin_origin),
            admins,
        }
    }
}

#[derive(Default)]
struct Counts {
    lines: AtomicU64,
    malformed: AtomicU64,
    undecodable: AtomicU64,
    bad_signature: AtomicU64,
    ephemeral: AtomicU64,
    future: AtomicU64,
    valid: AtomicU64,
    duplicate: AtomicU64,
    new: AtomicU64,
    bytes: AtomicU64,
}

#[derive(Clone, Copy, Serialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Uploading,
    Processing,
    Done,
    Failed,
    Interrupted,
}

struct Job {
    id: String,
    pubkey: PublicKey,
    name: String,
    started_at: u64,
    counts: Counts,
    phase: Mutex<(Phase, Option<String>, u64)>,
    complete: AtomicBool,
    abort: Mutex<Option<String>>,
}

#[derive(Serialize)]
pub struct JobView {
    id: String,
    pubkey: String,
    name: String,
    state: Phase,
    error: Option<String>,
    started_at: u64,
    finished_at: u64,
    bytes: u64,
    lines: u64,
    valid: u64,
    malformed: u64,
    undecodable: u64,
    bad_signature: u64,
    ephemeral: u64,
    future: u64,
    duplicate: u64,
    new: u64,
}

impl Job {
    fn set(&self, phase: Phase, error: Option<String>) {
        let finished = if matches!(phase, Phase::Done | Phase::Failed | Phase::Interrupted) {
            nip98::unix_now()
        } else {
            0
        };
        *self.phase.lock().unwrap() = (phase, error, finished);
    }

    fn view(&self) -> JobView {
        let (state, error, finished_at) = self.phase.lock().unwrap().clone();
        let c = &self.counts;
        let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
        JobView {
            id: self.id.clone(),
            pubkey: self.pubkey.to_hex(),
            name: self.name.clone(),
            state,
            error,
            started_at: self.started_at,
            finished_at,
            bytes: g(&c.bytes),
            lines: g(&c.lines),
            valid: g(&c.valid),
            malformed: g(&c.malformed),
            undecodable: g(&c.undecodable),
            bad_signature: g(&c.bad_signature),
            ephemeral: g(&c.ephemeral),
            future: g(&c.future),
            duplicate: g(&c.duplicate),
            new: g(&c.new),
        }
    }
}

#[derive(Clone)]
pub struct SubmitState {
    cfg: Arc<SubmitConfig>,
    db: DefaultJsonFilesDatabase,
    sink: EventSink,
    dedupe: Option<Arc<IdStore>>,
    jobs: Arc<Mutex<Vec<Arc<Job>>>>,
    seen: Arc<Mutex<HashMap<String, u64>>>,
    active: Arc<Mutex<usize>>,
}

struct Slot(Arc<Mutex<usize>>);

impl Drop for Slot {
    fn drop(&mut self) {
        let mut n = self.0.lock().unwrap();
        *n = n.saturating_sub(1);
    }
}

impl SubmitState {
    pub fn new(
        cfg: SubmitConfig,
        db: DefaultJsonFilesDatabase,
        sink: EventSink,
        dedupe: Option<Arc<IdStore>>,
    ) -> Self {
        Self {
            cfg: Arc::new(cfg),
            db,
            sink,
            dedupe,
            jobs: Default::default(),
            seen: Default::default(),
            active: Default::default(),
        }
    }

    fn take_slot(&self) -> Option<Slot> {
        let mut n = self.active.lock().unwrap();
        if *n >= self.cfg.max_concurrent {
            return None;
        }
        *n += 1;
        Some(Slot(self.active.clone()))
    }

    fn accept_once(&self, id: &str, now: u64) -> bool {
        let mut seen = self.seen.lock().unwrap();
        seen.retain(|_, t| now.saturating_sub(*t) <= nip98::MAX_AGE_SECS);
        seen.insert(id.to_string(), now).is_none()
    }

    fn authenticate(
        &self,
        headers: &HeaderMap,
        method: &Method,
        uri: &axum::http::Uri,
    ) -> Result<PublicKey, Response> {
        let event = nip98::verify(headers, method, uri, self.cfg.origin.as_deref())
            .map_err(deny)?;
        if method != Method::GET && !self.accept_once(&event.id.to_hex(), nip98::unix_now()) {
            return Err(deny("auth event already used"));
        }
        Ok(event.pubkey)
    }
}

fn deny(msg: &str) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [("www-authenticate", "Nostr")],
        Json(serde_json::json!({ "error": msg })),
    )
        .into_response()
}

fn fail(code: StatusCode, msg: &str) -> Response {
    (code, Json(serde_json::json!({ "error": msg }))).into_response()
}

pub fn router(state: SubmitState) -> Router {
    Router::new()
        .route("/submit", get(list).post(upload))
        .route("/submit/limits", get(limits))
        .route("/submit/{id}", get(status))
        .with_state(state)
}

fn free_bytes(dir: &std::path::Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(dir.as_os_str().as_bytes()).ok()?;
    let mut s: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c.as_ptr(), &mut s) } != 0 {
        return None;
    }
    Some(s.f_bavail as u64 * s.f_frsize as u64)
}

#[derive(Deserialize)]
struct UploadParams {
    name: Option<String>,
}

async fn list(
    State(st): State<SubmitState>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let pk = match st.authenticate(&headers, &method, &uri) {
        Ok(pk) => pk,
        Err(r) => return r,
    };
    let admin = st.cfg.admins.contains(&pk);
    let jobs = st.jobs.lock().unwrap();
    let views: Vec<JobView> = jobs
        .iter()
        .rev()
        .filter(|j| admin || j.pubkey == pk)
        .map(|j| j.view())
        .collect();
    Json(views).into_response()
}

async fn limits(State(st): State<SubmitState>) -> Response {
    let free = st.cfg.free();
    Json(serde_json::json!({
        "max_concurrent": st.cfg.max_concurrent,
        "accepting": free >= st.cfg.min_free_bytes,
        "min_free_bytes": st.cfg.min_free_bytes,
        "free_bytes": free,
        "formats": FORMATS,
        "streamed_formats": STREAMED_FORMATS,
        "disk_formats": DISK_FORMATS,
        "size_limit": serde_json::Value::Null,
        "disk_note": "jsonl uploads of any size are processed as they arrive and nothing is staged. A strfry .mdb upload must be written to disk first, so it is accepted only when its declared size plus the free-space floor fits in the free space.",
        "resume_note": "If a connection drops, the events already processed stay in the archive and the submission ends as interrupted. Upload the file again to continue: events we already hold are counted as duplicates and skipped.",
    }))
    .into_response()
}

async fn status(State(st): State<SubmitState>, Path(id): Path<String>) -> Response {
    let job = st.jobs.lock().unwrap().iter().find(|j| j.id == id).cloned();
    match job {
        Some(j) => Json(j.view()).into_response(),
        None => fail(StatusCode::NOT_FOUND, "no such submission"),
    }
}

struct ChannelReader {
    rx: mpsc::Receiver<bytes::Bytes>,
    cur: bytes::Bytes,
    job: Arc<Job>,
}

impl Read for ChannelReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        while self.cur.is_empty() {
            match self.rx.blocking_recv() {
                Some(b) => self.cur = b,
                None if self.job.complete.load(Ordering::SeqCst) => return Ok(0),
                None => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "upload interrupted",
                    ));
                }
            }
        }
        let n = out.len().min(self.cur.len());
        out[..n].copy_from_slice(&self.cur[..n]);
        bytes::Buf::advance(&mut self.cur, n);
        Ok(n)
    }
}

async fn upload(
    State(st): State<SubmitState>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
    Query(params): Query<UploadParams>,
    body: Body,
) -> Response {
    let pk = match st.authenticate(&headers, &method, &uri) {
        Ok(pk) => pk,
        Err(r) => return r,
    };
    let now = nip98::unix_now();

    let name = params.name.unwrap_or_else(|| "dump.jsonl".to_string());
    let lower = name.to_lowercase();
    if !FORMATS.iter().any(|f| lower.ends_with(f)) {
        return fail(
            StatusCode::BAD_REQUEST,
            "name must end in .jsonl, .jsonl.zst, .jsonl.gz, .jsonl.bz2, .mdb, .mdb.zst or .mdb.gz",
        );
    }

    let declared = headers
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());

    if std::fs::create_dir_all(&st.cfg.dir).is_err() {
        return fail(StatusCode::INTERNAL_SERVER_ERROR, "cannot create upload dir");
    }
    let floor = st.cfg.min_free_bytes;
    let free = st.cfg.free();
    let needs_disk = DISK_FORMATS.iter().any(|f| lower.ends_with(f));
    if needs_disk {
        let Some(declared) = declared else {
            return fail(
                StatusCode::LENGTH_REQUIRED,
                ".mdb uploads must declare Content-Length, because they are written to disk first",
            );
        };
        if free < floor.saturating_add(declared) {
            return fail(
                StatusCode::INSUFFICIENT_STORAGE,
                "not enough free disk space for a .mdb of this size; send jsonl instead, which is streamed",
            );
        }
    } else if free < floor {
        return fail(
            StatusCode::INSUFFICIENT_STORAGE,
            "not enough free disk space to accept uploads",
        );
    }

    let Some(slot) = st.take_slot() else {
        return fail(StatusCode::TOO_MANY_REQUESTS, "too many uploads in progress");
    };

    let id = {
        let mut b = [0u8; 16];
        use std::io::Read as _;
        match std::fs::File::open("/dev/urandom").and_then(|mut f| f.read_exact(&mut b)) {
            Ok(()) => hex::encode(b),
            Err(_) => return fail(StatusCode::INTERNAL_SERVER_ERROR, "no randomness"),
        }
    };
    let job = Arc::new(Job {
        id: id.clone(),
        pubkey: pk,
        name: lower.clone(),
        started_at: now,
        counts: Counts::default(),
        phase: Mutex::new((Phase::Uploading, None, 0)),
        complete: AtomicBool::new(false),
        abort: Mutex::new(None),
    });
    st.jobs.lock().unwrap().push(job.clone());

    let (tx, rx) = mpsc::channel::<bytes::Bytes>(CHANNEL_CHUNKS);
    {
        let st2 = st.clone();
        let job2 = job.clone();
        let handle = tokio::runtime::Handle::current();
        let lower = lower.clone();
        tokio::task::spawn_blocking(move || {
            let _slot = slot;
            let reader = ChannelReader { rx, cur: bytes::Bytes::new(), job: job2.clone() };
            let outcome = decoded(&lower, reader)
                .and_then(|r| process(&st2, &job2, r, &handle));
            match outcome {
                Ok(()) => job2.set(Phase::Done, None),
                Err(e) => {
                    let abort = job2.abort.lock().unwrap().clone();
                    if let Some(msg) = abort {
                        job2.set(Phase::Failed, Some(msg));
                    } else if !job2.complete.load(Ordering::SeqCst) {
                        job2.set(Phase::Interrupted, Some(e.to_string()));
                    } else {
                        job2.set(Phase::Failed, Some(e.to_string()));
                    }
                }
            }
        });
    }

    let mut stream = body.into_data_stream();
    let mut last_check = std::time::Instant::now();
    let mut worker_gone = false;
    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else {
            return fail(StatusCode::BAD_REQUEST, "upload interrupted");
        };
        job.counts.bytes.fetch_add(chunk.len() as u64, Ordering::Relaxed);
        if last_check.elapsed() >= FLOOR_CHECK_EVERY {
            last_check = std::time::Instant::now();
            if st.cfg.free() < floor {
                let msg = "disk free space floor reached";
                *job.abort.lock().unwrap() = Some(msg.to_string());
                return fail(StatusCode::INSUFFICIENT_STORAGE, msg);
            }
        }
        if tx.send(chunk).await.is_err() {
            worker_gone = true;
            break;
        }
    }
    if !worker_gone {
        job.complete.store(true, Ordering::SeqCst);
        let mut p = job.phase.lock().unwrap();
        if p.0 == Phase::Uploading {
            p.0 = Phase::Processing;
        }
    }
    drop(tx);

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({ "id": id, "status": format!("/submit/{id}") })),
    )
        .into_response()
}

fn decoded(name: &str, reader: impl Read + Send + 'static) -> anyhow::Result<Box<dyn Read + Send>> {
    Ok(if name.ends_with(".zst") {
        Box::new(zstd::stream::read::Decoder::new(reader)?)
    } else if name.ends_with(".gz") {
        Box::new(flate2::read::MultiGzDecoder::new(reader))
    } else if name.ends_with(".bz2") {
        Box::new(bzip2::read::MultiBzDecoder::new(reader))
    } else {
        Box::new(reader)
    })
}

fn process(
    st: &SubmitState,
    job: &Job,
    source: Box<dyn Read + Send>,
    rt: &tokio::runtime::Handle,
) -> anyhow::Result<()> {
    let mut reader = BufReader::with_capacity(1 << 20, source);
    if crate::strfry::has_lmdb_magic(reader.fill_buf()?) {
        return process_lmdb(st, job, reader, rt);
    }
    let c = &job.counts;
    let mut buf = Vec::new();
    loop {
        buf.clear();
        let n = (&mut reader).take(MAX_LINE_BYTES + 1).read_until(b'\n', &mut buf)?;
        if n == 0 {
            break;
        }
        if buf.last() != Some(&b'\n') && n as u64 > MAX_LINE_BYTES {
            let mut sink = Vec::new();
            reader.read_until(b'\n', &mut sink)?;
            c.lines.fetch_add(1, Ordering::Relaxed);
            c.malformed.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        let line = buf.trim_ascii();
        if line.is_empty() {
            continue;
        }
        c.lines.fetch_add(1, Ordering::Relaxed);
        ingest_line(st, job, line, rt)?;
    }
    Ok(())
}

fn spill_to_disk(
    st: &SubmitState,
    mut reader: impl Read,
    dest: &std::path::Path,
) -> anyhow::Result<()> {
    use std::io::Write;
    let mut out = std::fs::File::create(dest)?;
    let mut buf = vec![0u8; 1 << 20];
    let mut since_check = 0u64;
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        since_check += n as u64;
        if since_check >= 64 << 20 {
            since_check = 0;
            if st.cfg.free() < st.cfg.min_free_bytes {
                anyhow::bail!("disk free space floor reached while writing the database");
            }
        }
        out.write_all(&buf[..n])?;
    }
    Ok(())
}

fn process_lmdb(
    st: &SubmitState,
    job: &Job,
    reader: impl Read,
    rt: &tokio::runtime::Handle,
) -> anyhow::Result<()> {
    let path = st.cfg.dir.join(format!("{}.mdb", job.id));
    if let Err(e) = spill_to_disk(st, reader, &path) {
        let _ = std::fs::remove_file(&path);
        return Err(e);
    }
    let c = &job.counts;
    let result = (|| {
        let db = crate::strfry::Lmdb::open(&path)?;
        db.for_each_event(MAX_LINE_BYTES as usize, |payload| {
            c.lines.fetch_add(1, Ordering::Relaxed);
            match payload {
                crate::strfry::Payload::Json(json) => ingest_line(st, job, json.trim_ascii(), rt),
                crate::strfry::Payload::Undecodable => {
                    c.undecodable.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }
            }
        })
    })();
    let _ = std::fs::remove_file(&path);
    result
}

fn ingest_line(
    st: &SubmitState,
    job: &Job,
    line: &[u8],
    rt: &tokio::runtime::Handle,
) -> anyhow::Result<()> {
    let c = &job.counts;
    let Ok(ev) = serde_json::from_slice::<Event>(line) else {
        c.malformed.fetch_add(1, Ordering::Relaxed);
        return Ok(());
    };
    if ev.verify().is_err() {
        c.bad_signature.fetch_add(1, Ordering::Relaxed);
        return Ok(());
    }
    if ev.kind.is_ephemeral() {
        c.ephemeral.fetch_add(1, Ordering::Relaxed);
        return Ok(());
    }
    if ev.created_at.as_secs() > nip98::unix_now() + FUTURE_SLACK_SECS {
        c.future.fetch_add(1, Ordering::Relaxed);
        return Ok(());
    }
    c.valid.fetch_add(1, Ordering::Relaxed);

    if st
        .dedupe
        .as_ref()
        .is_some_and(|d| d.contains(&ev.id.to_bytes()))
    {
        c.duplicate.fetch_add(1, Ordering::Relaxed);
        return Ok(());
    }
    match rt.block_on(st.db.save_event(&ev)) {
        Ok(SaveEventStatus::Success) => {
            rt.block_on(st.sink.send(to_core(&ev)));
            c.new.fetch_add(1, Ordering::Relaxed);
        }
        Ok(_) => {
            c.duplicate.fetch_add(1, Ordering::Relaxed);
        }
        Err(e) => {
            tracing::warn!(error = %e, "archive save failed during submission");
            return Err(anyhow::anyhow!("archive save failed: {e}"));
        }
    }
    Ok(())
}
