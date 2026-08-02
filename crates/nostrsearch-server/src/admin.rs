//! Authenticated admin endpoints.
//!
//! These reset persisted state — an analysis's accumulated results, or the
//! record of which (relay, day) pairs have been scraped — so they are
//! destructive and must not be open to the network.
//!
//! Auth is [NIP-98] HTTP Auth rather than a shared bearer token: the caller
//! signs a kind-27235 event naming the exact URL and method, and the node
//! checks the signature against a configured allowlist of admin pubkeys. That
//! means no secret is ever transmitted, nothing to leak from a proxy log, and
//! the operator authenticates with the nostr key they already have.
//!
//! Fails closed: with no `ADMIN_PUBKEYS` configured the routes are not mounted
//! at all, so a misconfigured deploy exposes nothing rather than everything.
//!
//! [NIP-98]: https://github.com/nostr-protocol/nips/blob/master/98.md

use crate::node::WriterCtl;
use axum::{
    Json, Router,
    extract::{Path, Query, Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::Engine;
use nostr_sdk::prelude::JsonUtil;
use nostr_sdk::{Event, PublicKey};
use nostrsearch_indexer::scrape::ScrapeState;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// NIP-98 auth events older than this are refused.
const MAX_AGE_SECS: u64 = 60;

/// kind for NIP-98 HTTP Auth.
const HTTP_AUTH_KIND: u16 = 27235;

#[derive(Clone)]
pub struct AdminState {
    pub cfg: Arc<AdminConfig>,
    pub ctl: WriterCtl,
    pub scrape: Option<Arc<ScrapeState>>,
    /// Set when this node has an archive directory it can replay from.
    pub replay: Option<ReplayCtx>,
    /// Recently accepted auth event ids, to stop a captured header being
    /// replayed within its freshness window.
    seen: Arc<Mutex<HashMap<String, u64>>>,
}

pub struct AdminConfig {
    /// Pubkeys permitted to call admin endpoints.
    pub pubkeys: HashSet<PublicKey>,
    /// Public origin (`https://archive.v0l.io`) used to check the `u` tag.
    /// When unset only the path and method are enforced, because behind a
    /// proxy the node cannot reliably reconstruct its own external URL.
    pub origin: Option<String>,
}

impl AdminConfig {
    /// Read `ADMIN_PUBKEYS` (comma-separated hex or npub) and `ADMIN_ORIGIN`.
    /// `None` when no admin keys are configured — routes stay unmounted.
    pub fn from_env() -> Option<Self> {
        let raw = std::env::var("ADMIN_PUBKEYS").ok()?;
        let mut pubkeys = HashSet::new();
        for tok in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            match PublicKey::parse(tok) {
                Ok(pk) => {
                    pubkeys.insert(pk);
                }
                Err(e) => {
                    tracing::warn!(key = tok, error = %e, "ignoring unparseable ADMIN_PUBKEYS entry")
                }
            }
        }
        if pubkeys.is_empty() {
            tracing::warn!("ADMIN_PUBKEYS set but no valid keys; admin endpoints disabled");
            return None;
        }
        let origin = std::env::var("ADMIN_ORIGIN")
            .ok()
            .map(|s| s.trim_end_matches('/').to_string());
        tracing::info!(
            admins = pubkeys.len(),
            origin = ?origin,
            "admin endpoints enabled (NIP-98 auth)"
        );
        Some(Self { pubkeys, origin })
    }
}

/// Everything the replay endpoints need to start a background re-ingest.
#[derive(Clone)]
pub struct ReplayCtx {
    pub state: crate::replay::ReplayState,
    pub dir: std::path::PathBuf,
    pub dedupe: Option<Arc<nostrsearch_indexer::id_store::IdStore>>,
    pub sink: crate::node::ReplaySink,
}

impl AdminState {
    pub fn new(cfg: AdminConfig, ctl: WriterCtl, scrape: Option<Arc<ScrapeState>>) -> Self {
        Self {
            cfg: Arc::new(cfg),
            ctl,
            scrape,
            replay: None,
            seen: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn with_replay(mut self, replay: ReplayCtx) -> Self {
        self.replay = Some(replay);
        self
    }

    /// Record an auth event id, rejecting it if already used. Also prunes ids
    /// older than the freshness window, so the set stays bounded.
    fn accept_once(&self, id: &str, now: u64) -> bool {
        let Ok(mut seen) = self.seen.lock() else {
            return false;
        };
        seen.retain(|_, t| now.saturating_sub(*t) <= MAX_AGE_SECS);
        if seen.contains_key(id) {
            return false;
        }
        seen.insert(id.to_string(), now);
        true
    }
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn deny(msg: &str) -> Response {
    // 401 + the NIP-98 scheme so a client knows what to present.
    (
        StatusCode::UNAUTHORIZED,
        [("www-authenticate", "Nostr")],
        Json(serde_json::json!({ "error": msg })),
    )
        .into_response()
}

/// NIP-98 gate for every admin route.
async fn auth(State(st): State<AdminState>, req: Request, next: Next) -> Response {
    let Some(header) = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    else {
        return deny("missing Authorization: Nostr <base64 event>");
    };

    let Some(b64) = header
        .strip_prefix("Nostr ")
        .or_else(|| header.strip_prefix("nostr "))
    else {
        return deny("expected the Nostr auth scheme");
    };

    let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(b64.trim()) else {
        return deny("auth event is not valid base64");
    };
    let Ok(json) = String::from_utf8(bytes) else {
        return deny("auth event is not valid utf-8");
    };
    let event: Event = match Event::from_json(&json) {
        Ok(e) => e,
        Err(_) => return deny("auth event is not a valid nostr event"),
    };
    {}

    // Signature first: everything below trusts the event's contents.
    if event.verify().is_err() {
        return deny("bad signature");
    }
    if event.kind.as_u16() != HTTP_AUTH_KIND {
        return deny("auth event must be kind 27235");
    }
    if !st.cfg.pubkeys.contains(&event.pubkey) {
        return deny("pubkey is not an admin");
    }

    // Freshness, in both directions: a far-future timestamp must not buy an
    // attacker an indefinitely valid header.
    let now = unix_now();
    let created = event.created_at.as_secs();
    if created.abs_diff(now) > MAX_AGE_SECS {
        return deny("auth event timestamp is outside the allowed window");
    }

    let tag = |name: &str| {
        event
            .tags
            .iter()
            .map(|t| t.clone().to_vec())
            .find(|t| t.first().map(String::as_str) == Some(name))
            .and_then(|t| t.get(1).cloned())
    };

    let Some(u) = tag("u") else {
        return deny("auth event is missing the u tag");
    };
    let Some(method) = tag("method") else {
        return deny("auth event is missing the method tag");
    };
    if !method.eq_ignore_ascii_case(req.method().as_str()) {
        return deny("auth event method does not match the request");
    }

    // The signed URL must name this exact request, so a header captured for a
    // read endpoint cannot be replayed against a destructive one.
    //
    // `nest` strips the `/admin` prefix before this middleware runs, so compare
    // against the original URI — otherwise every caller would have to sign the
    // internal post-nesting path, which is not the URL they actually requested.
    let original = req
        .extensions()
        .get::<axum::extract::OriginalUri>()
        .map(|o| o.0.path().to_string());
    let want_path = original.as_deref().unwrap_or_else(|| req.uri().path());
    let ok_url = match url::Url::parse(&u) {
        Ok(parsed) => {
            let path_ok = parsed.path() == want_path;
            match &st.cfg.origin {
                Some(origin) => path_ok && u.starts_with(origin.as_str()),
                None => path_ok,
            }
        }
        Err(_) => false,
    };
    if !ok_url {
        return deny("auth event u tag does not match this URL");
    }

    if !st.accept_once(&event.id.to_hex(), now) {
        return deny("auth event already used");
    }

    tracing::info!(
        admin = %event.pubkey,
        method = %req.method(),
        path = %want_path,
        "authenticated admin request"
    );
    next.run(req).await
}

pub fn router(state: AdminState) -> Router {
    Router::new()
        .route("/analyses", get(analyses))
        .route("/analyses/{name}/reset", post(reset_analysis))
        .route("/ingest", get(ingest_status).post(start_ingest))
        .route("/ingest/cancel", post(cancel_ingest))
        .route("/scrape", get(scrape_state))
        .route("/scrape/reset", post(reset_scrape))
        .route("/scrape/relay/reset", post(reset_relay))
        .layer(middleware::from_fn_with_state(state.clone(), auth))
        .with_state(state)
}

async fn analyses(State(st): State<AdminState>) -> Response {
    match st.ctl.status().await {
        Ok(s) => Json(s).into_response(),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
    }
}

#[derive(Serialize)]
struct ResetResult {
    reset: bool,
    detail: String,
}

async fn reset_analysis(State(st): State<AdminState>, Path(name): Path<String>) -> Response {
    match st.ctl.reset_analysis(&name).await {
        Ok(true) => Json(ResetResult {
            reset: true,
            detail: format!("{name} will re-derive from incoming events"),
        })
        .into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("no analysis named {name}") })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
    }
}

/// `POST /admin/scrape/reset?relay=&from=YYYY-MM-DD&to=YYYY-MM-DD`
///
/// The same filters drive `GET /admin/scrape`, so you can preview a reset
/// before running it.
#[derive(Debug, Deserialize)]
pub struct ScrapeResetQuery {
    pub relay: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
}

/// `POST /admin/ingest[?file=combined.jsonl&file=...]`
///
/// Re-reads archive dumps through the live writer, so gaps are filled without
/// stopping the relay. Already-indexed events are skipped via the dedupe set,
/// and the replay is fed to the writer at lower priority than live traffic.
#[derive(Debug, Deserialize)]
pub struct IngestQuery {
    /// Repeatable. Omitted = every dump in the archive directory.
    #[serde(default)]
    pub file: Vec<String>,
}

async fn start_ingest(State(st): State<AdminState>, Query(q): Query<IngestQuery>) -> Response {
    let Some(rp) = st.replay.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "this node has no archive directory to replay" })),
        )
            .into_response();
    };

    // Reject traversal outright rather than sanitising: these names come from
    // the archive listing, so anything else is a mistake or an attack.
    if q.file.iter().any(|f| f.contains('/') || f.contains("..")) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "file must be a bare name from /archive/files" })),
        )
            .into_response();
    }

    let selection = crate::replay::ReplaySelection {
        files: q.file.clone(),
    };
    match crate::replay::spawn(rp.state.clone(), rp.dir, selection, rp.dedupe, rp.sink) {
        Ok(()) => Json(serde_json::json!({
            "started": true,
            "files": q.file,
            "detail": "replaying at lower priority than live traffic; poll GET /admin/ingest",
        }))
        .into_response(),
        Err(e) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": e })),
        )
            .into_response(),
    }
}

async fn ingest_status(State(st): State<AdminState>) -> Response {
    match st.replay.as_ref() {
        Some(rp) => Json(rp.state.status()).into_response(),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "this node has no archive directory to replay" })),
        )
            .into_response(),
    }
}

async fn cancel_ingest(State(st): State<AdminState>) -> Response {
    match st.replay.as_ref() {
        Some(rp) if rp.state.cancel() => {
            Json(serde_json::json!({ "cancelled": true })).into_response()
        }
        Some(_) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "no replay is running" })),
        )
            .into_response(),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "this node has no archive directory to replay" })),
        )
            .into_response(),
    }
}

/// `GET /admin/scrape[?relay=&from=&to=]`
///
/// The full scrape state: every relay (not the truncated public `/sync` view)
/// plus overall progress. With filters it also reports exactly which
/// (relay, day) records a reset with those same filters would remove.
async fn scrape_state(State(st): State<AdminState>, Query(q): Query<ScrapeResetQuery>) -> Response {
    let Some(scrape) = st.scrape.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "scraper is not running on this node" })),
        )
            .into_response();
    };

    let filtered = q.relay.is_some() || q.from.is_some() || q.to.is_some();
    let out = tokio::task::spawn_blocking(move || {
        let relays: Vec<_> = scrape
            .relays()
            .into_iter()
            .map(|(url, i)| {
                serde_json::json!({
                    "url": url,
                    "sources": i.sources,
                    "negentropy": i.negentropy,
                    "cap": i.cap,
                    "fails": i.fails,
                    "last_ok": i.last_ok,
                    "birthday": i.birthday,
                })
            })
            .collect();
        let progress = scrape.progress(25);
        let matching = filtered.then(|| {
            let (count, sample) =
                scrape.days_matching(q.relay.as_deref(), q.from.as_deref(), q.to.as_deref(), 100);
            serde_json::json!({
                "count": count,
                "sample": sample,
                "detail": "a reset with these same filters would clear exactly these records",
            })
        });
        serde_json::json!({
            "relays": relays,
            "progress": progress,
            "matching_days": matching,
        })
    })
    .await;

    match out {
        Ok(v) => Json(v).into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": "scrape state unavailable" })),
        )
            .into_response(),
    }
}

async fn reset_scrape(State(st): State<AdminState>, Query(q): Query<ScrapeResetQuery>) -> Response {
    let Some(scrape) = st.scrape.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "scraper is not running on this node" })),
        )
            .into_response();
    };

    // RocksDB scans block; keep them off the async worker.
    let removed = tokio::task::spawn_blocking(move || {
        scrape.reset_days(q.relay.as_deref(), q.from.as_deref(), q.to.as_deref())
    })
    .await;

    match removed {
        Ok(n) => Json(serde_json::json!({
            "reset_days": n,
            "detail": "those (relay, day) pairs will be scraped again on the next pass",
        }))
        .into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": "reset failed" })),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
pub struct RelayResetQuery {
    pub relay: String,
}

async fn reset_relay(State(st): State<AdminState>, Query(q): Query<RelayResetQuery>) -> Response {
    let Some(scrape) = st.scrape.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "scraper is not running on this node" })),
        )
            .into_response();
    };
    let url = q.relay.clone();
    let ok = tokio::task::spawn_blocking(move || scrape.reset_relay(&url)).await;
    match ok {
        Ok(true) => Json(serde_json::json!({
            "reset": true,
            "detail": format!("cleared learned state (horizon, failures, caps) for {}", q.relay),
        }))
        .into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown relay" })),
        )
            .into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": "reset failed" })),
        )
            .into_response(),
    }
}
