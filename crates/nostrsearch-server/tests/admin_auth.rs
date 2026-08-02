//! The admin endpoints reset persisted state, so the NIP-98 gate in front of
//! them is security-critical. These tests exercise the rejection paths, not
//! just the happy path — an auth check that only proves "a valid key works" is
//! the kind that silently accepts everything.

use base64::Engine;
use nostr_sdk::prelude::*;
use nostrsearch_core::scoring::ScoreWeights;
use nostrsearch_indexer::{PipelineConfig, ShardWriterConfig};
use nostrsearch_server::admin::{AdminConfig, AdminState};
use nostrsearch_server::{AppState, ShardRegistry};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

fn tempdir(tag: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "nsadmin-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// Serve the node with `admin` as the only permitted key.
async fn serve(admin: &Keys) -> anyhow::Result<String> {
    let root = tempdir("root");
    let registry = ShardRegistry::open(root.join("index"), ScoreWeights::default())?;
    let state = Arc::new(AppState {
        registry: Mutex::new(registry),
    });

    let (_sink, _handle, ctl, _replay) = nostrsearch_server::node::spawn_writer_with_reports(
        PipelineConfig {
            index_root: root.join("index"),
            shard: ShardWriterConfig::default(),
            state_dir: Some(root.join("stats")),
            wot_refresh_every: u64::MAX,
            min_refresh_interval: std::time::Duration::from_secs(3600),
            persist_interval: std::time::Duration::from_secs(3600),
            wot_out: None,
        },
        128,
        std::time::Duration::from_secs(30),
        None,
    )?;
    // Leak the handles: the server must outlive this helper.
    std::mem::forget(_sink);
    std::mem::forget(_handle);

    let cfg = AdminConfig {
        pubkeys: HashSet::from([admin.public_key()]),
        origin: None,
    };
    let app = nostrsearch_server::http::router_full_node(
        state,
        None,
        None,
        None,
        None,
        Some(AdminState::new(cfg, ctl, None)),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(format!("http://{addr}"))
}

/// Build a NIP-98 header, with knobs for the things that must be rejected.
async fn nip98(
    keys: &Keys,
    url: &str,
    method: &str,
    created_at: Option<u64>,
) -> anyhow::Result<String> {
    let mut builder = EventBuilder::new(Kind::Custom(27235), "")
        .tags([Tag::parse(["u", url])?, Tag::parse(["method", method])?]);
    if let Some(ts) = created_at {
        builder = builder.custom_created_at(Timestamp::from_secs(ts));
    }
    let event = builder.sign(keys).await?;
    Ok(format!(
        "Nostr {}",
        base64::engine::general_purpose::STANDARD.encode(event.as_json())
    ))
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[tokio::test]
async fn admin_key_is_accepted_and_everything_else_is_not() -> anyhow::Result<()> {
    let admin = Keys::generate();
    let stranger = Keys::generate();
    let base = serve(&admin).await?;
    let url = format!("{base}/admin/analyses");
    let http = reqwest::Client::new();

    // No header at all.
    let r = http.get(&url).send().await?;
    assert_eq!(r.status(), 401, "unauthenticated request must be refused");
    assert!(
        r.headers().contains_key("www-authenticate"),
        "should advertise the Nostr scheme"
    );

    // A valid signature from a key that is not an admin.
    let r = http
        .get(&url)
        .header("Authorization", nip98(&stranger, &url, "GET", None).await?)
        .send()
        .await?;
    assert_eq!(r.status(), 401, "non-admin key must be refused");

    // The configured admin.
    let r = http
        .get(&url)
        .header("Authorization", nip98(&admin, &url, "GET", None).await?)
        .send()
        .await?;
    assert_eq!(r.status(), 200, "admin key must be accepted");
    let body: serde_json::Value = r.json().await?;
    assert!(body.is_array(), "expected the analyses list, got {body}");

    Ok(())
}

#[tokio::test]
async fn stale_replayed_and_mismatched_headers_are_refused() -> anyhow::Result<()> {
    let admin = Keys::generate();
    let base = serve(&admin).await?;
    let url = format!("{base}/admin/analyses");
    let http = reqwest::Client::new();

    // Too old.
    let r = http
        .get(&url)
        .header(
            "Authorization",
            nip98(&admin, &url, "GET", Some(now() - 3600)).await?,
        )
        .send()
        .await?;
    assert_eq!(r.status(), 401, "stale auth event must be refused");

    // Far-future timestamps must not buy an indefinitely valid header.
    let r = http
        .get(&url)
        .header(
            "Authorization",
            nip98(&admin, &url, "GET", Some(now() + 86_400)).await?,
        )
        .send()
        .await?;
    assert_eq!(r.status(), 401, "future auth event must be refused");

    // Signed for a different path: must not work on this one.
    let other = format!("{base}/admin/analyses/activity/reset");
    let r = http
        .get(&url)
        .header("Authorization", nip98(&admin, &other, "GET", None).await?)
        .send()
        .await?;
    assert_eq!(r.status(), 401, "u tag for another path must be refused");

    // Signed for a different method.
    let r = http
        .get(&url)
        .header("Authorization", nip98(&admin, &url, "POST", None).await?)
        .send()
        .await?;
    assert_eq!(r.status(), 401, "method mismatch must be refused");

    // Replay: the same header twice.
    let header = nip98(&admin, &url, "GET", None).await?;
    let first = http
        .get(&url)
        .header("Authorization", &header)
        .send()
        .await?;
    assert_eq!(first.status(), 200);
    let second = http
        .get(&url)
        .header("Authorization", &header)
        .send()
        .await?;
    // A repeated GET is allowed. Proxies transparently retry idempotent
    // requests against a slow upstream, and rejecting the retry made the
    // status endpoints 401 exactly when the node was busy enough to need them.
    assert_eq!(
        second.status(),
        200,
        "a retried GET must not be rejected as a replay"
    );

    // State-changing requests keep strict single-use: running a reset twice
    // because something retried it is the outcome worth preventing.
    let post_url = format!("{base}/admin/analyses/activity/reset");
    let post_hdr = nip98(&admin, &post_url, "POST", None).await?;
    let p1 = http
        .post(&post_url)
        .header("Authorization", &post_hdr)
        .send()
        .await?;
    assert_eq!(p1.status(), 200);
    let p2 = http
        .post(&post_url)
        .header("Authorization", &post_hdr)
        .send()
        .await?;
    assert_eq!(p2.status(), 401, "replayed POST must be refused");

    // Garbage.
    for bad in ["Nostr not-base64!!", "Bearer hunter2", "Nostr "] {
        let r = http.get(&url).header("Authorization", bad).send().await?;
        assert_eq!(r.status(), 401, "malformed header accepted: {bad}");
    }

    Ok(())
}

/// Every admin route must sit behind the gate -- including ones added later.
#[tokio::test]
async fn listing_scrape_state_is_authenticated() -> anyhow::Result<()> {
    let admin = Keys::generate();
    let base = serve(&admin).await?;
    let http = reqwest::Client::new();
    let url = format!("{base}/admin/scrape");

    // Unauthenticated.
    assert_eq!(http.get(&url).send().await?.status(), 401);

    // Authenticated: this node has no scraper, so 503 rather than 401 -- which
    // is what proves the request got past the auth layer.
    let r = http
        .get(&url)
        .header("Authorization", nip98(&admin, &url, "GET", None).await?)
        .send()
        .await?;
    assert_eq!(
        r.status(),
        503,
        "expected to pass auth and then report no scraper"
    );
    Ok(())
}

#[tokio::test]
async fn resetting_an_analysis_requires_auth_and_reports_unknown_names() -> anyhow::Result<()> {
    let admin = Keys::generate();
    let base = serve(&admin).await?;
    let http = reqwest::Client::new();

    // Unauthenticated reset must not touch anything.
    let url = format!("{base}/admin/analyses/activity/reset");
    let r = http.post(&url).send().await?;
    assert_eq!(r.status(), 401);

    // Authenticated reset of a real analysis.
    let r = http
        .post(&url)
        .header("Authorization", nip98(&admin, &url, "POST", None).await?)
        .send()
        .await?;
    assert_eq!(r.status(), 200, "admin reset should succeed");
    let body: serde_json::Value = r.json().await?;
    // The response names every analysis that was reset, including the
    // dependents dragged in with it, so the caller can see the blast radius.
    let reset = body["reset"].as_array().expect("reset is a list of names");
    assert!(
        reset.iter().any(|n| n == "activity"),
        "the requested analysis must be in the reset set: {reset:?}"
    );

    // An unknown analysis is a 404, not a silent success.
    let url = format!("{base}/admin/analyses/nonexistent/reset");
    let r = http
        .post(&url)
        .header("Authorization", nip98(&admin, &url, "POST", None).await?)
        .send()
        .await?;
    assert_eq!(r.status(), 404);

    Ok(())
}

/// `reset-all` clears every analysis and reports what it cleared.
#[tokio::test]
async fn reset_all_clears_every_analysis() -> anyhow::Result<()> {
    let admin = Keys::generate();
    let base = serve(&admin).await?;
    let http = reqwest::Client::new();
    let url = format!("{base}/admin/analyses/reset-all");

    // Unauthenticated is refused, like every other admin route.
    let r = http.post(&url).send().await?;
    assert_eq!(r.status(), 401);

    let r = http
        .post(&url)
        .header("Authorization", nip98(&admin, &url, "POST", None).await?)
        .send()
        .await?;

    // Without an archive the node refuses rather than emptying the analyses
    // with no way to refill them.
    if r.status() == 503 {
        let body: serde_json::Value = r.json().await?;
        assert!(
            body["error"].as_str().unwrap_or("").contains("archive"),
            "503 must explain the missing archive: {body}"
        );
        return Ok(());
    }

    assert_eq!(r.status(), 200);
    let body: serde_json::Value = r.json().await?;
    let reset = body["reset"].as_array().expect("names of what was cleared");
    for expected in ["follow_graph", "activity", "active_users", "client_tags"] {
        assert!(
            reset.iter().any(|n| n == expected),
            "reset-all must clear {expected}: {reset:?}"
        );
    }
    Ok(())
}
