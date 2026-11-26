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
/// The unit is picked by magnitude (MiB, GiB, TiB, PiB), so a 1,488 GiB corpus
/// reads as `1.45 TiB` rather than a four-digit GiB. Precision follows the
/// value: 2 decimals under 10, 1 under 100, none above. A thin space separates
/// number and unit so the unit never reads as a bigger number.
pub fn human_size(bytes: u64) -> String {
    const MIB: f64 = 1024.0 * 1024.0;
    let mib = bytes as f64 / MIB;
    let (value, unit) = if mib >= 1024.0 * 1024.0 * 1024.0 {
        (mib / (1024.0 * 1024.0 * 1024.0), "PiB")
    } else if mib >= 1024.0 * 1024.0 {
        (mib / (1024.0 * 1024.0), "TiB")
    } else if mib >= 1024.0 {
        (mib / 1024.0, "GiB")
    } else {
        (mib, "MiB")
    };
    let decimals = if value < 10.0 { 2 } else if value < 100.0 { 1 } else { 0 };
    format!("{value:.prec$}\u{2009}{unit}", prec = decimals)
}

/// An integer with thousands separators, `en-US` style: `1,332,983,500`.
///
/// Fixed locale so the server-rendered pages and the browser's
/// `Intl.NumberFormat("en-US")` agree, with no hydration flicker.
pub fn format_int(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_pick_unit_by_magnitude() {
        assert_eq!(human_size(0), "0.00\u{2009}MiB");
        assert_eq!(human_size(1), "0.00\u{2009}MiB");
        assert_eq!(human_size(1024 * 1024), "1.00\u{2009}MiB");
        assert_eq!(human_size(1024 * 1024 * 1023), "1023\u{2009}MiB");
        assert_eq!(human_size(1024 * 1024 * 1024), "1.00\u{2009}GiB");
        assert_eq!(
            human_size(3 * 1024 * 1024 * 1024 + 512 * 1024 * 1024),
            "3.50\u{2009}GiB"
        );
        let gib = 1488.35_f64 * 1024.0 * 1024.0 * 1024.0;
        assert_eq!(human_size(gib as u64), "1.45\u{2009}TiB");
    }

    #[test]
    fn sizes_round_before_picking_precision() {
        let gib = |g: f64| (g * 1024.0 * 1024.0 * 1024.0) as u64;
        let mib = |m: f64| (m * 1024.0 * 1024.0) as u64;
        assert_eq!(human_size(mib(999.96)), "1000\u{2009}MiB");
        assert_eq!(human_size(mib(1023.4)), "1023\u{2009}MiB");
        assert_eq!(human_size(mib(1023.9)), "1.00\u{2009}GiB");
        assert_eq!(human_size(gib(9.996)), "10.0\u{2009}GiB");
        assert_eq!(human_size(gib(15.45)), "15.4\u{2009}GiB");
        assert_eq!(human_size(gib(99.96)), "100\u{2009}GiB");
        assert_eq!(human_size(gib(1023.96)), "1.00\u{2009}TiB");
    }

    #[test]
    fn integers_get_thousands_separators() {
        assert_eq!(format_int(0), "0");
        assert_eq!(format_int(1), "1");
        assert_eq!(format_int(999), "999");
        assert_eq!(format_int(1000), "1,000");
        assert_eq!(format_int(1332983500), "1,332,983,500");
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
