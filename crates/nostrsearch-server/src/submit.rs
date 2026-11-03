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
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::sync::Notify;

const MAX_LINE_BYTES: u64 = 1 << 20;
const FUTURE_SLACK_SECS: u64 = 86_400;
const BATCH_LINES: usize = 4096;
const BATCH_BYTES: usize = 32 << 20;
const VERIFY_THREADS: usize = 8;
const SAVE_THREADS: usize = 32;
const FLOOR_CHECK_EVERY: std::time::Duration = std::time::Duration::from_secs(1);
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
    pub process_concurrency: usize,
    pub min_free_bytes: u64,
    pub batch_lines: usize,
    pub batch_pause: std::time::Duration,
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
            process_concurrency: u("SUBMIT_PROCESS_CONCURRENCY", 1) as usize,
            min_free_bytes: u("SUBMIT_MIN_FREE_BYTES", 100 * GB),
            batch_lines: BATCH_LINES,
            batch_pause: std::time::Duration::ZERO,
            free_space: None,
            origin: std::env::var("SUBMIT_ORIGIN")
                .ok()
                .map(|s| s.trim_end_matches('/').to_string())
                .or(admin_origin),
            admins,
        }
    }
}

#[derive(Default, Serialize, Deserialize)]
#[serde(default)]
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
    parse_ns: AtomicU64,
    verify_ns: AtomicU64,
    dedupe_ns: AtomicU64,
    save_ns: AtomicU64,
    sink_ns: AtomicU64,
}

fn timed<T>(slot: &AtomicU64, f: impl FnOnce() -> T) -> T {
    let t = std::time::Instant::now();
    let out = f();
    slot.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
    out
}

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Uploading,
    Queued,
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
    cursor: AtomicU64,
    read_bytes: AtomicU64,
    dir: PathBuf,
}

#[derive(Deserialize)]
struct Meta {
    id: String,
    pubkey: String,
    name: String,
    started_at: u64,
    state: Phase,
    error: Option<String>,
    finished_at: u64,
    cursor: u64,
    counts: Counts,
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
    bytes_read: u64,
    percent: f64,
    lines: u64,
    valid: u64,
    malformed: u64,
    undecodable: u64,
    bad_signature: u64,
    ephemeral: u64,
    future: u64,
    duplicate: u64,
    new: u64,
    stage_ms: StageMs,
}

#[derive(Serialize)]
struct StageMs {
    parse: u64,
    verify: u64,
    dedupe: u64,
    save: u64,
    sink: u64,
}

impl Job {
    fn new(dir: &std::path::Path, id: String, pubkey: PublicKey, name: String, now: u64) -> Self {
        Self {
            id,
            pubkey,
            name,
            started_at: now,
            counts: Counts::default(),
            phase: Mutex::new((Phase::Uploading, None, 0)),
            cursor: AtomicU64::new(0),
            read_bytes: AtomicU64::new(0),
            dir: dir.to_path_buf(),
        }
    }

    fn from_meta(dir: &std::path::Path, m: Meta) -> Option<Self> {
        Some(Self {
            id: m.id,
            pubkey: PublicKey::from_hex(&m.pubkey).ok()?,
            name: m.name,
            started_at: m.started_at,
            counts: m.counts,
            phase: Mutex::new((m.state, m.error, m.finished_at)),
            cursor: AtomicU64::new(m.cursor),
            read_bytes: AtomicU64::new(0),
            dir: dir.to_path_buf(),
        })
    }

    fn data_path(&self) -> PathBuf {
        self.dir.join(format!("{}.upload", self.id))
    }

    fn meta_path(&self) -> PathBuf {
        self.dir.join(format!("{}.job.json", self.id))
    }

    fn persist(&self) {
        let (state, error, finished_at) = self.phase.lock().unwrap().clone();
        let meta = serde_json::json!({
            "id": self.id,
            "pubkey": self.pubkey.to_hex(),
            "name": self.name,
            "started_at": self.started_at,
            "state": state,
            "error": error,
            "finished_at": finished_at,
            "cursor": self.cursor.load(Ordering::SeqCst),
            "counts": &self.counts,
        });
        let tmp = self.dir.join(format!("{}.job.json.tmp", self.id));
        let written = serde_json::to_vec(&meta)
            .map_err(std::io::Error::other)
            .and_then(|b| std::fs::write(&tmp, b))
            .and_then(|_| std::fs::rename(&tmp, self.meta_path()));
        if let Err(e) = written {
            tracing::warn!(error = %e, job = %self.id, "cannot persist submission state");
        }
    }

    fn state(&self) -> Phase {
        self.phase.lock().unwrap().0
    }

    fn set(&self, phase: Phase, error: Option<String>) {
        let finished = if matches!(phase, Phase::Done | Phase::Failed | Phase::Interrupted) {
            nip98::unix_now()
        } else {
            0
        };
        *self.phase.lock().unwrap() = (phase, error, finished);
        self.persist();
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
            bytes_read: self.read_bytes.load(Ordering::Relaxed),
            percent: match state {
                Phase::Done => 100.0,
                _ if g(&c.bytes) == 0 => 0.0,
                _ => (self.read_bytes.load(Ordering::Relaxed) as f64 * 100.0 / g(&c.bytes) as f64).min(100.0),
            },
            lines: g(&c.lines),
            valid: g(&c.valid),
            malformed: g(&c.malformed),
            undecodable: g(&c.undecodable),
            bad_signature: g(&c.bad_signature),
            ephemeral: g(&c.ephemeral),
            future: g(&c.future),
            duplicate: g(&c.duplicate),
            new: g(&c.new),
            stage_ms: StageMs {
                parse: g(&c.parse_ns) / 1_000_000,
                verify: g(&c.verify_ns) / 1_000_000,
                dedupe: g(&c.dedupe_ns) / 1_000_000,
                save: g(&c.save_ns) / 1_000_000,
                sink: g(&c.sink_ns) / 1_000_000,
            },
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
    wake: Arc<Notify>,
    stopping: Arc<AtomicBool>,
    running: Arc<Mutex<usize>>,
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
        let _ = std::fs::create_dir_all(&cfg.dir);
        let jobs = load_jobs(&cfg.dir);
        let workers = cfg.process_concurrency;
        let st = Self {
            cfg: Arc::new(cfg),
            db,
            sink,
            dedupe,
            jobs: Arc::new(Mutex::new(jobs)),
            seen: Default::default(),
            active: Default::default(),
            wake: Default::default(),
            stopping: Default::default(),
            running: Default::default(),
        };
        for _ in 0..workers {
            tokio::spawn(worker(st.clone()));
        }
        st
    }

    pub fn stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        self.wake.notify_waiters();
    }

    pub async fn stop_and_wait(&self) {
        self.stop();
        for _ in 0..600 {
            if *self.running.lock().unwrap() == 0 {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    fn claim(&self) -> Option<Arc<Job>> {
        let jobs = self.jobs.lock().unwrap();
        let job = jobs.iter().find(|j| j.state() == Phase::Queued)?.clone();
        job.set(Phase::Processing, None);
        Some(job)
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
    let floor = st.cfg.min_free_bytes;
    let (queued, queued_bytes) = {
        let jobs = st.jobs.lock().unwrap();
        jobs.iter()
            .filter(|j| matches!(j.state(), Phase::Queued | Phase::Processing))
            .fold((0u64, 0u64), |(n, b), j| (n + 1, b + j.counts.bytes.load(Ordering::Relaxed)))
    };
    Json(serde_json::json!({
        "max_concurrent": st.cfg.max_concurrent,
        "process_concurrency": st.cfg.process_concurrency,
        "accepting": free >= floor,
        "min_free_bytes": floor,
        "free_bytes": free,
        "acceptable_bytes": free.saturating_sub(floor),
        "queued_jobs": queued,
        "queued_bytes": queued_bytes,
        "formats": FORMATS,
        "size_limit": serde_json::Value::Null,
        "disk_note": "Uploads are stored on disk exactly as sent, then ingested in the background one after another. An upload is refused, or stopped, when the free space left would drop below the floor. acceptable_bytes is how much can be accepted right now. A .mdb is also unpacked on disk while it is ingested.",
        "resume_note": "Ingest survives restarts and continues where it stopped. If an upload connection drops, the partial file is discarded; send it again and events we already hold are counted as duplicates and skipped.",
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

struct UploadGuard {
    job: Arc<Job>,
}

impl Drop for UploadGuard {
    fn drop(&mut self) {
        if self.job.state() == Phase::Uploading {
            let _ = std::fs::remove_file(self.job.data_path());
            self.job.set(Phase::Interrupted, Some("upload interrupted".to_string()));
        }
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
    if free < floor.saturating_add(declared.unwrap_or(0)) {
        return fail(
            StatusCode::INSUFFICIENT_STORAGE,
            "not enough free disk space: this upload would take free space below the floor",
        );
    }

    let Some(_slot) = st.take_slot() else {
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
    let job = Arc::new(Job::new(&st.cfg.dir, id.clone(), pk, lower, now));
    st.jobs.lock().unwrap().push(job.clone());
    let guard = UploadGuard { job: job.clone() };

    let file = match tokio::fs::File::create(job.data_path()).await {
        Ok(f) => f,
        Err(e) => {
            job.set(Phase::Failed, Some(format!("cannot store upload: {e}")));
            return fail(StatusCode::INTERNAL_SERVER_ERROR, "cannot store upload");
        }
    };
    let mut out = tokio::io::BufWriter::with_capacity(1 << 20, file);
    let mut stream = body.into_data_stream();
    let mut last_check = std::time::Instant::now();
    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else {
            return fail(StatusCode::BAD_REQUEST, "upload interrupted");
        };
        if last_check.elapsed() >= FLOOR_CHECK_EVERY {
            last_check = std::time::Instant::now();
            if st.cfg.free() < floor {
                let msg = "disk free space floor reached";
                let _ = std::fs::remove_file(job.data_path());
                job.set(Phase::Failed, Some(msg.to_string()));
                return fail(StatusCode::INSUFFICIENT_STORAGE, msg);
            }
        }
        if let Err(e) = out.write_all(&chunk).await {
            let _ = std::fs::remove_file(job.data_path());
            job.set(Phase::Failed, Some(format!("cannot store upload: {e}")));
            return fail(StatusCode::INSUFFICIENT_STORAGE, "cannot store upload");
        }
        job.counts.bytes.fetch_add(chunk.len() as u64, Ordering::Relaxed);
    }
    let synced = async {
        out.flush().await?;
        out.get_ref().sync_all().await
    }
    .await;
    if let Err(e) = synced {
        let _ = std::fs::remove_file(job.data_path());
        job.set(Phase::Failed, Some(format!("cannot store upload: {e}")));
        return fail(StatusCode::INSUFFICIENT_STORAGE, "cannot store upload");
    }
    job.set(Phase::Queued, None);
    drop(guard);
    st.wake.notify_waiters();

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

#[derive(Debug)]
struct Stopped;

impl std::fmt::Display for Stopped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("server stopping")
    }
}

impl std::error::Error for Stopped {}

struct Counting<R> {
    inner: R,
    job: Arc<Job>,
}

impl<R: Read> Read for Counting<R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(out)?;
        self.job.read_bytes.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }
}

fn load_jobs(dir: &std::path::Path) -> Vec<Arc<Job>> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return vec![];
    };
    let mut jobs: Vec<Arc<Job>> = entries
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with(".job.json"))
        .filter_map(|e| {
            let meta: Meta = serde_json::from_slice(&std::fs::read(e.path()).ok()?).ok()?;
            Job::from_meta(dir, meta).map(Arc::new)
        })
        .collect();
    jobs.sort_by_key(|j| j.started_at);
    for job in &jobs {
        let has_file = job.data_path().exists();
        match job.state() {
            Phase::Uploading => {
                let _ = std::fs::remove_file(job.data_path());
                job.set(Phase::Interrupted, Some("server restarted during upload".to_string()));
            }
            Phase::Queued | Phase::Processing if has_file => job.set(Phase::Queued, None),
            Phase::Queued | Phase::Processing => {
                job.set(Phase::Failed, Some("stored upload is missing".to_string()))
            }
            Phase::Done | Phase::Failed | Phase::Interrupted => {
                let _ = std::fs::remove_file(job.data_path());
                let _ = std::fs::remove_file(job.dir.join(format!("{}.mdb", job.id)));
            }
        }
    }
    jobs
}

async fn worker(st: SubmitState) {
    loop {
        if st.stopping.load(Ordering::SeqCst) {
            return;
        }
        let Some(job) = st.claim() else {
            tokio::select! {
                _ = st.wake.notified() => {}
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
            }
            continue;
        };
        *st.running.lock().unwrap() += 1;
        let (st2, job2, rt) = (st.clone(), job.clone(), tokio::runtime::Handle::current());
        let result = tokio::task::spawn_blocking(move || process(&st2, &job2, &rt)).await;
        match result {
            Ok(Ok(())) => {
                job.set(Phase::Done, None);
                remove_files(&job);
            }
            Ok(Err(e)) if e.downcast_ref::<Stopped>().is_some() => job.set(Phase::Queued, None),
            Ok(Err(e)) => {
                job.set(Phase::Failed, Some(e.to_string()));
                remove_files(&job);
            }
            Err(e) => {
                job.set(Phase::Failed, Some(format!("processing crashed: {e}")));
                remove_files(&job);
            }
        }
        *st.running.lock().unwrap() -= 1;
    }
}

fn remove_files(job: &Job) {
    let _ = std::fs::remove_file(job.data_path());
    let _ = std::fs::remove_file(job.dir.join(format!("{}.mdb", job.id)));
    let _ = std::fs::remove_file(job.dir.join(format!("{}.mdb.part", job.id)));
}

enum Next {
    Eof,
    Line,
    TooLong,
}

fn next_line(reader: &mut impl BufRead, buf: &mut Vec<u8>) -> std::io::Result<Next> {
    buf.clear();
    let n = reader.by_ref().take(MAX_LINE_BYTES + 1).read_until(b'\n', buf)?;
    if n == 0 {
        return Ok(Next::Eof);
    }
    if buf.last() != Some(&b'\n') && n as u64 > MAX_LINE_BYTES {
        let mut rest = Vec::new();
        reader.read_until(b'\n', &mut rest)?;
        return Ok(Next::TooLong);
    }
    Ok(Next::Line)
}

fn process(st: &SubmitState, job: &Arc<Job>, rt: &tokio::runtime::Handle) -> anyhow::Result<()> {
    let file = std::fs::File::open(job.data_path())?;
    let source = decoded(&job.name, Counting { inner: file, job: job.clone() })?;
    let mut reader = BufReader::with_capacity(1 << 20, source);
    if crate::strfry::has_lmdb_magic(reader.fill_buf()?) {
        return process_lmdb(st, job, reader, rt);
    }
    let c = &job.counts;
    let mut buf = Vec::new();
    let mut skip = job.cursor.load(Ordering::SeqCst);
    while skip > 0 {
        if matches!(next_line(&mut reader, &mut buf)?, Next::Eof) {
            break;
        }
        skip -= 1;
    }
    let mut pending = Batch::default();
    loop {
        if st.stopping.load(Ordering::SeqCst) {
            pending.flush(st, job, rt)?;
            return Err(Stopped.into());
        }
        let next = match next_line(&mut reader, &mut buf) {
            Ok(n) => n,
            Err(e) => {
                let _ = pending.flush(st, job, rt);
                return Err(e.into());
            }
        };
        match next {
            Next::Eof => break,
            Next::TooLong => {
                job.cursor.fetch_add(1, Ordering::SeqCst);
                c.lines.fetch_add(1, Ordering::Relaxed);
                c.malformed.fetch_add(1, Ordering::Relaxed);
            }
            Next::Line => {
                job.cursor.fetch_add(1, Ordering::SeqCst);
                let line = buf.trim_ascii();
                if line.is_empty() {
                    continue;
                }
                c.lines.fetch_add(1, Ordering::Relaxed);
                pending.push(st, job, line, rt)?;
            }
        }
    }
    pending.flush(st, job, rt)
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
    out.sync_all()?;
    Ok(())
}

fn process_lmdb(
    st: &SubmitState,
    job: &Arc<Job>,
    reader: impl Read,
    rt: &tokio::runtime::Handle,
) -> anyhow::Result<()> {
    let path = job.dir.join(format!("{}.mdb", job.id));
    if !path.exists() {
        let part = job.dir.join(format!("{}.mdb.part", job.id));
        if let Err(e) = spill_to_disk(st, reader, &part) {
            let _ = std::fs::remove_file(&part);
            return Err(e);
        }
        std::fs::rename(&part, &path)?;
    }
    let c = &job.counts;
    let skip = job.cursor.load(Ordering::SeqCst);
    let mut seen = 0u64;
    let db = crate::strfry::Lmdb::open(&path)?;
    let mut batch = Batch::default();
    db.for_each_event(MAX_LINE_BYTES as usize, |payload| {
        if st.stopping.load(Ordering::SeqCst) {
            batch.flush(st, job, rt)?;
            return Err(Stopped.into());
        }
        seen += 1;
        if seen <= skip {
            return Ok(());
        }
        job.cursor.store(seen, Ordering::SeqCst);
        c.lines.fetch_add(1, Ordering::Relaxed);
        match payload {
            crate::strfry::Payload::Json(json) => batch.push(st, job, json.trim_ascii(), rt),
            crate::strfry::Payload::Undecodable => {
                c.undecodable.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        }
    })?;
    batch.flush(st, job, rt)
}

#[derive(Default)]
struct Batch {
    lines: Vec<Vec<u8>>,
    bytes: usize,
}

impl Batch {
    fn push(
        &mut self,
        st: &SubmitState,
        job: &Job,
        line: &[u8],
        rt: &tokio::runtime::Handle,
    ) -> anyhow::Result<()> {
        self.bytes += line.len();
        self.lines.push(line.to_vec());
        if self.lines.len() >= st.cfg.batch_lines || self.bytes >= BATCH_BYTES {
            self.flush(st, job, rt)?;
        }
        Ok(())
    }

    fn flush(
        &mut self,
        st: &SubmitState,
        job: &Job,
        rt: &tokio::runtime::Handle,
    ) -> anyhow::Result<()> {
        let lines = std::mem::take(&mut self.lines);
        self.bytes = 0;
        if !lines.is_empty() {
            ingest_batch(st, job, &lines, rt)?;
            if !st.cfg.batch_pause.is_zero() {
                std::thread::sleep(st.cfg.batch_pause);
            }
        }
        job.persist();
        Ok(())
    }
}

fn screen(job: &Job, line: &[u8]) -> Option<Event> {
    let c = &job.counts;
    let Ok(ev) = timed(&c.parse_ns, || serde_json::from_slice::<Event>(line)) else {
        c.malformed.fetch_add(1, Ordering::Relaxed);
        return None;
    };
    if timed(&c.verify_ns, || ev.verify()).is_err() {
        c.bad_signature.fetch_add(1, Ordering::Relaxed);
        return None;
    }
    if ev.kind.is_ephemeral() {
        c.ephemeral.fetch_add(1, Ordering::Relaxed);
        return None;
    }
    if ev.created_at.as_secs() > nip98::unix_now() + FUTURE_SLACK_SECS {
        c.future.fetch_add(1, Ordering::Relaxed);
        return None;
    }
    c.valid.fetch_add(1, Ordering::Relaxed);
    Some(ev)
}

fn screen_all(job: &Job, lines: &[Vec<u8>]) -> Vec<Event> {
    let threads = VERIFY_THREADS.min(lines.len().div_ceil(256)).max(1);
    let per = lines.len().div_ceil(threads);
    std::thread::scope(|scope| {
        lines
            .chunks(per)
            .map(|part| {
                scope.spawn(move || part.iter().filter_map(|l| screen(job, l)).collect::<Vec<_>>())
            })
            .collect::<Vec<_>>()
            .into_iter()
            .flat_map(|h| h.join().expect("screen thread"))
            .collect()
    })
}

fn save_all(
    job: &Job,
    db: &DefaultJsonFilesDatabase,
    events: &[Event],
    rt: &tokio::runtime::Handle,
) -> Vec<Result<SaveEventStatus, DatabaseError>> {
    let threads = SAVE_THREADS.min(events.len()).max(1);
    let per = events.len().div_ceil(threads).max(1);
    timed(&job.counts.save_ns, || {
        std::thread::scope(|scope| {
            events
                .chunks(per)
                .map(|part| {
                    scope.spawn(move || {
                        part.iter()
                            .map(|ev| rt.block_on(db.save_event(ev)))
                            .collect::<Vec<_>>()
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .flat_map(|h| h.join().expect("save thread"))
                .collect()
        })
    })
}

fn ingest_batch(
    st: &SubmitState,
    job: &Job,
    lines: &[Vec<u8>],
    rt: &tokio::runtime::Handle,
) -> anyhow::Result<()> {
    let c = &job.counts;
    let screened = screen_all(job, lines);

    let mut in_batch = HashSet::with_capacity(screened.len());
    let mut unique = Vec::with_capacity(screened.len());
    for ev in screened {
        if in_batch.insert(ev.id) {
            unique.push(ev);
        } else {
            c.duplicate.fetch_add(1, Ordering::Relaxed);
        }
    }

    let fresh: Vec<Event> = match st.dedupe.as_ref() {
        Some(d) => {
            let ids: Vec<[u8; 32]> = unique.iter().map(|e| e.id.to_bytes()).collect();
            let known = timed(&c.dedupe_ns, || d.contains_batch(&ids));
            let mut keep = Vec::with_capacity(unique.len());
            for (ev, known) in unique.into_iter().zip(known) {
                if known {
                    c.duplicate.fetch_add(1, Ordering::Relaxed);
                } else {
                    keep.push(ev);
                }
            }
            keep
        }
        None => unique,
    };

    let statuses = save_all(job, &st.db, &fresh, rt);
    for (ev, status) in fresh.iter().zip(statuses) {
        match status {
            Ok(SaveEventStatus::Success) => {
                timed(&c.sink_ns, || rt.block_on(st.sink.send(to_core(ev))));
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
    }
    Ok(())
}
