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

/// State for the search routes: the index, plus the archive they hydrate
/// complete signed events from.
///
/// Kept separate from [`AppState`] so a node without an archive (a read-only
/// search replica) is a `None` here rather than a different constructor
/// everywhere.
#[derive(Clone)]
pub struct SearchState {
    pub app: SharedState,
    pub archive: Option<crate::archive::ArchiveState>,
}

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
    let search_state = SearchState {
        app: state.clone(),
        archive: archive.clone(),
    };
    let mut app = Router::new()
        .route("/search", get(search_get).post(search_post))
        .route("/event/{id}", get(get_event))
        .with_state(search_state)
        .merge(
            Router::new()
                .route("/stats", get(stats))
                .route("/healthz", get(healthz))
                .with_state(state),
        );

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
    site: Option<String>,  // comma-separated URL hosts
    nip05: Option<String>, // comma-separated NIP-05 identifiers
    geo: Option<String>,   // comma-separated geohash cells (prefix match)
    limit: Option<usize>,
    // `exclude_deleted` / `exclude_superseded` are gone. They read columns
    // that only ever held zero, so the parameters errored rather than
    // filtering; both views are derivable from what is indexed (see the schema
    // module docs). Unknown query parameters are ignored, so an old client
    // sending them now gets results instead of a 500.
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
    State(state): State<SearchState>,
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
        tag_g: split_csv(&p.geo),
        hosts: split_csv(&p.site),
        nip05: split_csv(&p.nip05),
        limit: p.limit.unwrap_or(50).min(500),
        ..Default::default()
    };
    run_search(state, filter).await
}

// ---------------------------------------------------------------------------
// POST /search  (full query DSL)
// ---------------------------------------------------------------------------

async fn search_post(
    State(state): State<SearchState>,
    Json(filter): Json<SearchFilterDto>,
) -> Result<Json<Vec<SearchHit>>, ApiError> {
    run_search(state, filter.into()).await
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
    pub hosts: Vec<String>,
    pub nip05: Vec<String>,
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
            hosts: vec![],
            nip05: vec![],
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
            hosts: d.hosts,
            nip05: d.nip05,
            limit: d.limit.unwrap_or(50).min(500),
        }
    }
}

async fn run_search(
    state: SearchState,
    filter: SearchFilter,
) -> Result<Json<Vec<SearchHit>>, ApiError> {
    let mut hits = {
        // Scoped so the registry lock is released before the (async) archive
        // reads: a MutexGuard must not be held across an await.
        let mut reg = state.app.registry.lock().map_err(|_| ApiError::Poisoned)?;
        reg.search(&filter).map_err(ApiError::Registry)?
    };
    hydrate_events(&state, &mut hits).await;
    Ok(Json(hits))
}

/// Attach the complete signed event to each hit, from the archive.
///
/// One batched lookup for the whole page, after ranking and truncation, so the
/// cost is proportional to what is returned rather than to what matched.
async fn hydrate_events(state: &SearchState, hits: &mut [SearchHit]) {
    let Some(archive) = state.archive.as_ref() else {
        return;
    };
    if hits.is_empty() {
        return;
    }
    let ids: Vec<String> = hits.iter().map(|h| h.event_id.clone()).collect();
    for (hit, ev) in hits.iter_mut().zip(archive.events_by_hex_ids(&ids).await) {
        hit.event = ev;
    }
}

// ---------------------------------------------------------------------------
// GET /event/{id}
// ---------------------------------------------------------------------------

async fn get_event(
    State(state): State<SearchState>,
    Path(id): Path<String>,
) -> Result<Json<SearchHit>, ApiError> {
    // Hex ids are indexed lowercase, so a caller using uppercase must be
    // folded the same way or the lookup misses.
    let id = nostrsearch_core::schema::normalize_hex(id.trim());
    let hit = {
        let mut reg = state.app.registry.lock().map_err(|_| ApiError::Poisoned)?;
        reg.get_event(&id).map_err(ApiError::Registry)?
    };
    match hit {
        Some(mut hit) => {
            hydrate_events(&state, std::slice::from_mut(&mut hit)).await;
            Ok(Json(hit))
        }
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
