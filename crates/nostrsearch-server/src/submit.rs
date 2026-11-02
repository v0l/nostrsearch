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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;

const MAX_LINE_BYTES: u64 = 1 << 20;
const FUTURE_SLACK_SECS: u64 = 86_400;
const DAY_SECS: u64 = 86_400;
const FORMATS: [&str; 4] = [".jsonl", ".jsonl.zst", ".jsonl.gz", ".jsonl.bz2"];

pub struct SubmitConfig {
    pub dir: PathBuf,
    pub max_bytes: u64,
    pub max_concurrent: usize,
    pub daily_bytes_per_pubkey: u64,
    pub min_free_bytes: u64,
    pub origin: Option<String>,
    pub admins: HashSet<PublicKey>,
}

impl SubmitConfig {
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
            max_bytes: u("SUBMIT_MAX_BYTES", 20 * GB),
            max_concurrent: u("SUBMIT_MAX_CONCURRENT", 2) as usize,
            daily_bytes_per_pubkey: u("SUBMIT_DAILY_BYTES", 40 * GB),
            min_free_bytes: u("SUBMIT_MIN_FREE_BYTES", 100 * GB),
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
}

struct Job {
    id: String,
    pubkey: PublicKey,
    name: String,
    started_at: u64,
    counts: Counts,
    phase: Mutex<(Phase, Option<String>, u64)>,
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
    bad_signature: u64,
    ephemeral: u64,
    future: u64,
    duplicate: u64,
    new: u64,
}

impl Job {
    fn set(&self, phase: Phase, error: Option<String>) {
        let finished = if matches!(phase, Phase::Done | Phase::Failed) {
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
    quota: Arc<Mutex<HashMap<PublicKey, (u64, u64)>>>,
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
            quota: Default::default(),
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

    fn quota_left(&self, pk: &PublicKey, now: u64) -> u64 {
        let day = now / DAY_SECS;
        let q = self.quota.lock().unwrap();
        let used = match q.get(pk) {
            Some((d, b)) if *d == day => *b,
            _ => 0,
        };
        self.cfg.daily_bytes_per_pubkey.saturating_sub(used)
    }

    fn charge(&self, pk: &PublicKey, now: u64, bytes: u64) {
        let day = now / DAY_SECS;
        let mut q = self.quota.lock().unwrap();
        let e = q.entry(*pk).or_insert((day, 0));
        if e.0 != day {
            *e = (day, 0);
        }
        e.1 += bytes;
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

async fn status(State(st): State<SubmitState>, Path(id): Path<String>) -> Response {
    let job = st.jobs.lock().unwrap().iter().find(|j| j.id == id).cloned();
    match job {
        Some(j) => Json(j.view()).into_response(),
        None => fail(StatusCode::NOT_FOUND, "no such submission"),
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
            "name must end in .jsonl, .jsonl.zst, .jsonl.gz or .jsonl.bz2",
        );
    }

    let declared = headers
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    if declared.is_some_and(|n| n > st.cfg.max_bytes) {
        return fail(StatusCode::PAYLOAD_TOO_LARGE, "upload exceeds the size limit");
    }

    let quota_left = st.quota_left(&pk, now);
    if quota_left == 0 || declared.is_some_and(|n| n > quota_left) {
        return fail(StatusCode::TOO_MANY_REQUESTS, "daily upload quota used up");
    }

    if std::fs::create_dir_all(&st.cfg.dir).is_err() {
        return fail(StatusCode::INTERNAL_SERVER_ERROR, "cannot create upload dir");
    }
    let free = free_bytes(&st.cfg.dir).unwrap_or(0);
    if free < st.cfg.min_free_bytes.saturating_add(declared.unwrap_or(0)) {
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
    });
    st.jobs.lock().unwrap().push(job.clone());

    let ext = FORMATS
        .iter()
        .find(|f| lower.ends_with(**f))
        .copied()
        .unwrap_or(".jsonl");
    let path = st.cfg.dir.join(format!("{id}{ext}"));

    let limit = st.cfg.max_bytes.min(quota_left);
    let floor = st.cfg.min_free_bytes;
    let result = write_body(&path, body, &job, limit, floor, &st.cfg.dir).await;
    st.charge(&pk, now, job.counts.bytes.load(Ordering::Relaxed));
    if let Err((code, msg)) = result {
        let _ = tokio::fs::remove_file(&path).await;
        job.set(Phase::Failed, Some(msg.to_string()));
        return fail(code, msg);
    }

    job.set(Phase::Processing, None);
    let view_id = id.clone();
    let st2 = st.clone();
    let handle = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        let _slot = slot;
        let outcome = process(&st2, &job, &path, &handle);
        let _ = std::fs::remove_file(&path);
        match outcome {
            Ok(()) => job.set(Phase::Done, None),
            Err(e) => job.set(Phase::Failed, Some(e.to_string())),
        }
    });

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({ "id": view_id, "status": format!("/submit/{view_id}") })),
    )
        .into_response()
}

async fn write_body(
    path: &std::path::Path,
    body: Body,
    job: &Job,
    limit: u64,
    floor: u64,
    dir: &std::path::Path,
) -> Result<(), (StatusCode, &'static str)> {
    let mut file = tokio::fs::File::create(path)
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "cannot create upload file"))?;
    let mut stream = body.into_data_stream();
    let mut since_check = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| (StatusCode::BAD_REQUEST, "upload interrupted"))?;
        let total = job.counts.bytes.fetch_add(chunk.len() as u64, Ordering::Relaxed)
            + chunk.len() as u64;
        if total > limit {
            return Err((
                StatusCode::PAYLOAD_TOO_LARGE,
                "upload exceeds the size or quota limit",
            ));
        }
        since_check += chunk.len() as u64;
        if since_check >= 64 << 20 {
            since_check = 0;
            if free_bytes(dir).unwrap_or(0) < floor {
                return Err((StatusCode::INSUFFICIENT_STORAGE, "disk free space floor reached"));
            }
        }
        file.write_all(&chunk)
            .await
            .map_err(|_| (StatusCode::INSUFFICIENT_STORAGE, "write failed"))?;
    }
    file.flush()
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "write failed"))?;
    Ok(())
}

fn open_decoded(path: &std::path::Path) -> anyhow::Result<Box<dyn Read + Send>> {
    let f = std::fs::File::open(path)?;
    let name = path.to_string_lossy();
    Ok(if name.ends_with(".zst") {
        Box::new(zstd::stream::read::Decoder::new(f)?)
    } else if name.ends_with(".gz") {
        Box::new(flate2::read::MultiGzDecoder::new(f))
    } else if name.ends_with(".bz2") {
        Box::new(bzip2::read::MultiBzDecoder::new(f))
    } else {
        Box::new(f)
    })
}

fn process(
    st: &SubmitState,
    job: &Job,
    path: &std::path::Path,
    rt: &tokio::runtime::Handle,
) -> anyhow::Result<()> {
    let mut reader = BufReader::with_capacity(1 << 20, open_decoded(path)?);
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
        let Ok(ev) = serde_json::from_slice::<Event>(line) else {
            c.malformed.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        if ev.verify().is_err() {
            c.bad_signature.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        if ev.kind.is_ephemeral() {
            c.ephemeral.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        if ev.created_at.as_secs() > nip98::unix_now() + FUTURE_SLACK_SECS {
            c.future.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        c.valid.fetch_add(1, Ordering::Relaxed);

        if st
            .dedupe
            .as_ref()
            .is_some_and(|d| d.contains(&ev.id.to_bytes()))
        {
            c.duplicate.fetch_add(1, Ordering::Relaxed);
            continue;
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
    }
    Ok(())
}
