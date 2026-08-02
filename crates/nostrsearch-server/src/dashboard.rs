//! The operator dashboard, embedded in the binary.
//!
//! `dashboard/` is a Preact app built by Vite with `vite-plugin-singlefile`, so
//! the entire console — markup, styles, script — is one HTML file with no asset
//! requests. That file is checked in at `assets/dashboard.html` and pulled in
//! here with `include_str!`, which means deploying the console is deploying the
//! binary: no static directory to mount, no CDN, no version skew between the
//! API and the UI talking to it.
//!
//! Rebuild the asset with `scripts/build-dashboard.sh` after changing anything
//! under `dashboard/`.
//!
//! The page itself is served unauthenticated, because a browser cannot attach a
//! NIP-98 header to a top-level navigation. Nothing is exposed by that: the
//! document is a static shell, and every admin call it makes is signed by the
//! operator's key and checked by [`crate::admin`] like any other client's.
//!
//! It is always mounted, on `/` as well as `/dashboard`, because most of what it
//! shows — corpus coverage, relay health, index size and the published analysis
//! reports — comes from open endpoints. A node with no admin keys still has a
//! front page; the panels that need a key say so.

use axum::{
    Router,
    http::header,
    response::{Html, IntoResponse, Response},
    routing::get,
};

const PAGE: &str = include_str!("../assets/dashboard.html");

pub async fn page() -> Response {
    (
        // Tied to the binary, so revalidate rather than cache: an operator who
        // deploys a fix should not be looking at last week's console.
        [(header::CACHE_CONTROL, "no-cache")],
        Html(PAGE),
    )
        .into_response()
}

/// The named routes. `/` is mounted separately in [`crate::http`] because a
/// node running the relay has to share it with the websocket upgrade.
pub fn routes() -> Router {
    Router::new()
        .route("/dashboard", get(page))
        .route("/dashboard/", get(page))
}
