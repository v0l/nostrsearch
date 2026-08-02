//! Axum REST API over the shard registry.

use crate::registry::{RegistryStats, SearchHit, ShardRegistry};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
};
use nostrsearch_core::query::SearchFilter;
use serde::Deserialize;
use std::sync::{Arc, Mutex};

/// Shared application state. The registry is behind a Mutex because shard
/// readers are lazily opened on first touch; contention is low because reads
/// dominate and the lock is held only for the fan-out, not hydration of
/// already-cached readers. (A RwLock + eager open is a later optimization.)
pub struct AppState {
    pub registry: Mutex<ShardRegistry>,
}

pub type SharedState = Arc<AppState>;

pub fn router(state: SharedState) -> Router {
    router_with_archive(state, None)
}

/// Router with optional archive serving and relay (absorbs nostrhole's HTTP +
/// relay roles): archive files under `/archive`, nostr relay websocket at `/`.
pub fn router_with_archive(
    state: SharedState,
    archive: Option<crate::archive::ArchiveState>,
) -> Router {
    router_full(state, archive, None)
}

/// Full router: search API + optional archive serving + optional nostr relay.
pub fn router_full(
    state: SharedState,
    archive: Option<crate::archive::ArchiveState>,
    relay: Option<crate::relay::RelayState>,
) -> Router {
    router_all(state, archive, relay, None)
}

/// Everything, plus the analysis reports published by the writer task.
pub fn router_all(
    state: SharedState,
    archive: Option<crate::archive::ArchiveState>,
    relay: Option<crate::relay::RelayState>,
    reports: Option<crate::reports::ReportStore>,
) -> Router {
    router_all_sync(state, archive, relay, reports, None)
}

/// Everything, plus scrape/sync progress at `/sync`.
pub fn router_all_sync(
    state: SharedState,
    archive: Option<crate::archive::ArchiveState>,
    relay: Option<crate::relay::RelayState>,
    reports: Option<crate::reports::ReportStore>,
    scrape: Option<std::sync::Arc<nostrsearch_indexer::scrape::ScrapeState>>,
) -> Router {
    router_full_node(state, archive, relay, reports, scrape, None)
}

/// The complete node router, including authenticated admin routes.
pub fn router_full_node(
    state: SharedState,
    archive: Option<crate::archive::ArchiveState>,
    relay: Option<crate::relay::RelayState>,
    reports: Option<crate::reports::ReportStore>,
    scrape: Option<std::sync::Arc<nostrsearch_indexer::scrape::ScrapeState>>,
    admin: Option<crate::admin::AdminState>,
) -> Router {
    let mut app = Router::new()
        .route("/search", get(search_get).post(search_post))
        .route("/event/{id}", get(get_event))
        .route("/stats", get(stats))
        .route("/healthz", get(healthz))
        .with_state(state);

    if let Some(a) = archive {
        app = app.nest("/archive", crate::archive::router(a));
    }

    if let Some(r) = reports {
        app = app.nest("/reports", crate::reports::router(r));
    }

    if let Some(s) = scrape {
        app = app.nest("/sync", crate::reports::sync_router(s));
    }

    if let Some(a) = admin {
        app = app.nest("/admin", crate::admin::router(a));
    }

    // The console is always served: most of what it shows comes from the open
    // endpoints above, and the admin panels gate themselves on a signed key.
    app = app.merge(crate::dashboard::routes());

    if let Some(r) = relay {
        // Nostr relays live at the root path, and so does the console. A
        // websocket upgrade is handed to LocalRelay; a plain GET is a browser,
        // and gets the page.
        app = app.route("/", get(crate::relay::ws_handler).with_state(r));
    } else {
        app = app.route("/", get(crate::dashboard::page));
    }

    app.layer(tower_http::cors::CorsLayer::permissive())
        .layer(tower_http::trace::TraceLayer::new_for_http())
}

async fn healthz() -> &'static str {
    "ok"
}

// ---------------------------------------------------------------------------
// GET /search  (query-string form)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct SearchParams {
    q: Option<String>,
    kind: Option<String>,   // comma-separated
    author: Option<String>, // comma-separated
    tag: Option<String>,    // comma-separated hashtags
    since: Option<u64>,
    until: Option<u64>,
    lang: Option<String>,
    limit: Option<usize>,
    exclude_deleted: Option<bool>,
    exclude_superseded: Option<bool>,
}

fn split_csv(s: &Option<String>) -> Vec<String> {
    s.as_deref()
        .map(|v| {
            v.split(',')
                .map(|x| x.trim().to_string())
                .filter(|x| !x.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

fn split_kinds(s: &Option<String>) -> Vec<u16> {
    split_csv(s).iter().filter_map(|x| x.parse().ok()).collect()
}

async fn search_get(
    State(state): State<SharedState>,
    Query(p): Query<SearchParams>,
) -> Result<Json<Vec<SearchHit>>, ApiError> {
    let filter = SearchFilter {
        search: p.q,
        authors: split_csv(&p.author),
        kinds: split_kinds(&p.kind),
        tag_t: split_csv(&p.tag),
        since: p.since,
        until: p.until,
        lang: p.lang,
        limit: p.limit.unwrap_or(50).min(500),
        exclude_deleted: p.exclude_deleted.unwrap_or(false),
        exclude_superseded: p.exclude_superseded.unwrap_or(false),
        ..Default::default()
    };
    run_search(state, filter)
}

// ---------------------------------------------------------------------------
// POST /search  (full query DSL)
// ---------------------------------------------------------------------------

async fn search_post(
    State(state): State<SharedState>,
    Json(filter): Json<SearchFilterDto>,
) -> Result<Json<Vec<SearchHit>>, ApiError> {
    run_search(state, filter.into())
}

/// The wire form of a search filter (mirrors `SearchFilter`, serde-friendly).
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct SearchFilterDto {
    pub search: Option<String>,
    pub authors: Vec<String>,
    pub kinds: Vec<u16>,
    pub tag_t: Vec<String>,
    pub tag_e: Vec<String>,
    pub tag_p: Vec<String>,
    pub tag_a: Vec<String>,
    pub tag_d: Vec<String>,
    pub tag_g: Vec<String>,
    pub since: Option<u64>,
    pub until: Option<u64>,
    pub lang: Option<String>,
    pub exclude_deleted: bool,
    pub exclude_superseded: bool,
    pub limit: Option<usize>,
}

impl Default for SearchFilterDto {
    fn default() -> Self {
        Self {
            search: None,
            authors: vec![],
            kinds: vec![],
            tag_t: vec![],
            tag_e: vec![],
            tag_p: vec![],
            tag_a: vec![],
            tag_d: vec![],
            tag_g: vec![],
            since: None,
            until: None,
            lang: None,
            exclude_deleted: false,
            exclude_superseded: false,
            limit: None,
        }
    }
}

impl From<SearchFilterDto> for SearchFilter {
    fn from(d: SearchFilterDto) -> Self {
        SearchFilter {
            search: d.search,
            authors: d.authors,
            kinds: d.kinds,
            tag_t: d.tag_t,
            tag_e: d.tag_e,
            tag_p: d.tag_p,
            tag_a: d.tag_a,
            tag_d: d.tag_d,
            tag_g: d.tag_g,
            since: d.since,
            until: d.until,
            lang: d.lang,
            exclude_deleted: d.exclude_deleted,
            exclude_superseded: d.exclude_superseded,
            limit: d.limit.unwrap_or(50).min(500),
        }
    }
}

fn run_search(state: SharedState, filter: SearchFilter) -> Result<Json<Vec<SearchHit>>, ApiError> {
    let mut reg = state.registry.lock().map_err(|_| ApiError::Poisoned)?;
    let hits = reg.search(&filter).map_err(ApiError::Registry)?;
    Ok(Json(hits))
}

// ---------------------------------------------------------------------------
// GET /event/{id}
// ---------------------------------------------------------------------------

async fn get_event(
    State(state): State<SharedState>,
    Path(id): Path<String>,
) -> Result<Json<SearchHit>, ApiError> {
    let mut reg = state.registry.lock().map_err(|_| ApiError::Poisoned)?;
    match reg.get_event(&id).map_err(ApiError::Registry)? {
        Some(hit) => Ok(Json(hit)),
        None => Err(ApiError::NotFound),
    }
}

// ---------------------------------------------------------------------------
// GET /stats
// ---------------------------------------------------------------------------

async fn stats(State(state): State<SharedState>) -> Result<Json<RegistryStats>, ApiError> {
    let mut reg = state.registry.lock().map_err(|_| ApiError::Poisoned)?;
    Ok(Json(reg.stats()))
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum ApiError {
    Registry(crate::registry::RegistryError),
    Poisoned,
    NotFound,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (code, msg) = match self {
            ApiError::Registry(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            ApiError::Poisoned => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "registry lock poisoned".to_string(),
            ),
            ApiError::NotFound => (StatusCode::NOT_FOUND, "event not found".to_string()),
        };
        (code, Json(serde_json::json!({ "error": msg }))).into_response()
    }
}
