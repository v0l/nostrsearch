//! The console is the node's front page, so what is worth pinning down is that
//! it is served everywhere it should be — including on a node with no admin
//! keys, where the open panels still work — and that the served bytes are the
//! built bundle rather than a placeholder someone forgot to rebuild.

use nostr_sdk::Keys;
use nostrsearch_core::scoring::ScoreWeights;
use nostrsearch_indexer::{PipelineConfig, ShardWriterConfig};
use nostrsearch_server::admin::{AdminConfig, AdminState};
use nostrsearch_server::{AppState, ShardRegistry};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

fn tempdir(tag: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "nsdash-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

async fn serve(with_admin: bool) -> anyhow::Result<String> {
    let root = tempdir("root");
    let registry = ShardRegistry::open(root.join("index"), ScoreWeights::default())?;
    let state = Arc::new(AppState {
        registry: Mutex::new(registry),
    });

    let (sink, handle, ctl, _replay) = nostrsearch_server::node::spawn_writer_with_reports(
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
    std::mem::forget(sink);
    std::mem::forget(handle);

    let admin = with_admin.then(|| {
        let cfg = AdminConfig {
            pubkeys: HashSet::from([Keys::generate().public_key()]),
            origin: None,
        };
        AdminState::new(cfg, ctl, None)
    });

    let app = nostrsearch_server::http::router_full_node(state, None, None, None, None, admin);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(format!("http://{addr}"))
}

#[tokio::test]
async fn console_is_served_on_the_root_path_and_by_name() -> anyhow::Result<()> {
    let base = serve(true).await?;
    let http = reqwest::Client::new();

    for path in ["/", "/dashboard", "/dashboard/"] {
        let r = http.get(format!("{base}{path}")).send().await?;
        assert_eq!(r.status(), 200, "{path} should serve the console");
        assert!(
            r.headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .starts_with("text/html"),
            "{path} should be HTML"
        );
        let body = r.text().await?;
        // The bundle is inlined, so a build that dropped the script would still
        // return 200 with a shell. Check for the app root and inline script.
        assert!(body.contains("id=\"app\""), "{path} missing the app root");
        assert!(
            body.contains("<script") && body.len() > 10_000,
            "{path} looks like an unbuilt placeholder ({} bytes)",
            body.len()
        );
    }
    Ok(())
}

#[tokio::test]
async fn console_is_served_without_admin_keys() -> anyhow::Result<()> {
    // Corpus coverage, relay health, the index panel and the reports all come
    // from open endpoints, so a node with no admin keys still has a front page.
    let base = serve(false).await?;
    for path in ["/", "/dashboard"] {
        let r = reqwest::get(format!("{base}{path}")).await?;
        assert_eq!(r.status(), 200, "{path} should serve the console");
    }
    Ok(())
}
