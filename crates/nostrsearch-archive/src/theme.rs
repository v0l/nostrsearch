//! The console's stylesheet, served to the plain HTML pages.
//!
//! The archive listing and the ingest status page are the two pages a visitor
//! sees *before* the console exists — an archive-only node never builds one,
//! and a node mid-backfill has not got there yet. Looking like a different
//! product on those two pages is exactly the wrong first impression.
//!
//! So rather than restating the design in Rust and letting the copy rot, this
//! serves `dashboard/src/styles.css` itself, `include_str!`d at build time.
//! Editing the console's palette or type changes these pages in the same
//! commit. Its `body.page` section covers the chrome a server-rendered page
//! needs; anything specific to one page lives inline in that page's markup.
//!
//! The pages use the console's own classes — `.panel`, `.plate`, `.readouts`,
//! `.wordmark`, `.chip` — so anything defined there is already available here.
//!
//! **Build note:** `include_str!` needs the file present when cargo runs, so
//! the Dockerfile copies `dashboard/src/styles.css` into the build stage. The
//! bundled dashboard is a single inlined HTML file with no separate CSS asset,
//! so the source is the only artifact to point at.

use axum::Router;
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::get;

/// The console stylesheet, verbatim.
pub const CONSOLE_CSS: &str = include_str!("../../../dashboard/src/styles.css");

/// `<head>` for a page: charset, viewport, and a link to `href`.
///
/// The stylesheet is a linked route rather than an inline `<style>` so a
/// browser caches it once across the archive listing and the status page.
pub fn head(title: &str, href: &str) -> String {
    format!(
        r#"<meta charset="utf-8">
<title>{title}</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<link rel="stylesheet" href="{href}">"#
    )
}

/// `GET style.css` — the console stylesheet plus the static-page additions.
///
/// Mount it wherever the pages that link to it live; both the archive router
/// and `ingest --bind` do.
pub fn css_router() -> Router {
    Router::new().route("/style.css", get(stylesheet))
}

async fn stylesheet() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/css; charset=utf-8"),
            // Compiled into the binary, so it can only change with a redeploy.
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        CONSOLE_CSS,
    )
}

/// Bytes as a human-readable size.
///
/// Archive shards run from a few MiB to tens of GiB, and a listing that prints
/// every one of them in GiB is a column of `0.00`.
pub fn human_size(bytes: u64) -> String {
    const MIB: f64 = 1024.0 * 1024.0;
    let mib = bytes as f64 / MIB;
    if mib >= 1024.0 {
        format!("{:.2} GiB", mib / 1024.0)
    } else {
        format!("{mib:.1} MiB")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_switch_unit_at_a_gibibyte() {
        assert_eq!(human_size(0), "0.0 MiB");
        assert_eq!(human_size(1024 * 1024), "1.0 MiB");
        assert_eq!(human_size(1024 * 1024 * 1023), "1023.0 MiB");
        assert_eq!(human_size(1024 * 1024 * 1024), "1.00 GiB");
        assert_eq!(
            human_size(3 * 1024 * 1024 * 1024 + 512 * 1024 * 1024),
            "3.50 GiB"
        );
    }

    /// The whole point of `include_str!` here: if the console's tokens stop
    /// arriving, the pages silently lose the design rather than failing.
    #[test]
    fn the_console_stylesheet_is_embedded() {
        assert!(CONSOLE_CSS.contains("--patina"), "palette missing");
        assert!(CONSOLE_CSS.contains(".panel"), "panel missing");
        assert!(CONSOLE_CSS.contains(".wordmark"), "wordmark missing");
    }
}
