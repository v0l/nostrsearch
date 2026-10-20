use nostrsearch_core::event::NostrEvent;
use nostrsearch_core::scoring::ScoreWeights;
use nostrsearch_indexer::{ShardManager, ShardWriterConfig};
use nostrsearch_server::{AppState, ArchiveState, ShardRegistry};
use std::sync::Arc;

fn event(i: u64, created_at: u64) -> NostrEvent {
    NostrEvent {
        id: format!("{i:064x}"),
        pubkey: "b".repeat(64),
        created_at,
        kind: 1,
        tags: vec![],
        content: format!("note {i} about bitcoin"),
        sig: String::new(),
    }
}

async fn serve(events: &[NostrEvent]) -> anyhow::Result<(String, tempfile::TempDir)> {
    let dir = tempfile::tempdir()?;
    let index_root = dir.path().join("index");
    let archive_dir = dir.path().join("archive");
    std::fs::create_dir_all(&archive_dir)?;
    std::fs::write(archive_dir.join("combined.jsonl"), b"{}\n")?;
    std::fs::write(archive_dir.join("LOCK"), b"")?;

    let mgr = ShardManager::new(&index_root, ShardWriterConfig::default());
    for ev in events {
        mgr.index_event(ev)?;
    }
    mgr.commit_all()?;

    let registry = ShardRegistry::open(&index_root, ScoreWeights::default())?;
    let state = Arc::new(AppState { registry });
    let app = nostrsearch_server::http::router_with_archive(
        state,
        Some(ArchiveState::open(&archive_dir)?),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok((format!("http://{addr}"), dir))
}

#[tokio::test]
async fn every_listed_archive_file_can_be_downloaded() -> anyhow::Result<()> {
    let (base, _dir) = serve(&[]).await?;
    let files: Vec<serde_json::Value> = reqwest::get(format!("{base}/archive/files"))
        .await?
        .json()
        .await?;
    assert_eq!(files.len(), 1);
    for f in files {
        let name = f["name"].as_str().unwrap();
        let r = reqwest::get(format!("{base}/archive/{name}")).await?;
        assert_eq!(r.status(), 200, "{name} is listed but not served");
    }
    let r = reqwest::get(format!("{base}/archive/LOCK")).await?;
    assert_eq!(r.status(), 400);
    Ok(())
}

#[tokio::test]
async fn malformed_query_is_a_client_error() -> anyhow::Result<()> {
    let (base, _dir) = serve(&[event(1, 1_700_000_000)]).await?;
    let r = reqwest::get(format!("{base}/search?q=%22unterminated")).await?;
    assert_eq!(r.status(), 400);
    Ok(())
}

#[tokio::test]
async fn zero_limit_still_answers() -> anyhow::Result<()> {
    let (base, _dir) = serve(&[event(1, 1_700_000_000)]).await?;
    for q in ["limit=0", "q=bitcoin&limit=0"] {
        let hits: Vec<serde_json::Value> = reqwest::get(format!("{base}/search?{q}"))
            .await?
            .json()
            .await?;
        assert_eq!(hits.len(), 1, "{q}");
    }
    Ok(())
}

/// Timestamps a few seconds apart round to the same `f32`, so merging shards
/// by score put events either side of a month boundary in arbitrary order.
#[tokio::test]
async fn metadata_results_are_newest_first_across_shards() -> anyhow::Result<()> {
    let boundary = 1_698_796_800;
    let events: Vec<NostrEvent> = (0..20).map(|i| event(i, boundary - 10 + i)).collect();
    let (base, _dir) = serve(&events).await?;
    let hits: Vec<serde_json::Value> = reqwest::get(format!("{base}/search?kind=1&limit=20"))
        .await?
        .json()
        .await?;
    let got: Vec<u64> = hits
        .iter()
        .map(|h| h["created_at"].as_u64().unwrap())
        .collect();
    let want: Vec<u64> = (0..20).rev().map(|i| boundary - 10 + i).collect();
    assert_eq!(got, want);
    Ok(())
}
