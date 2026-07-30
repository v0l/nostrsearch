//! Verifies the absorbed nostrhole roles: a signed event published to the
//! relay endpoint is persisted into the JSONL archive, and the archive HTTP
//! endpoints serve it back.

use nostr_archive_cursor::DefaultJsonFilesDatabase;
use nostr_sdk::prelude::*;
use nostrsearch_core::scoring::ScoreWeights;
use nostrsearch_server::{AppState, ArchiveState, RelayState, ShardRegistry};
use std::sync::{Arc, Mutex};

fn tempdir(tag: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "nsrelay-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

#[tokio::test]
async fn signed_event_published_to_relay_lands_in_archive() -> anyhow::Result<()> {
    let root = tempdir("root");
    let archive_dir = root.join("archive");
    let index_dir = root.join("index");
    std::fs::create_dir_all(&archive_dir)?;
    std::fs::create_dir_all(&index_dir)?;

    // Search state (empty index is fine; we're exercising relay + archive).
    let registry = ShardRegistry::open(&index_dir, ScoreWeights::default())?;
    let state = Arc::new(AppState {
        registry: Mutex::new(registry),
    });

    // Relay mode: this process owns the archive index lock.
    let archive = ArchiveState::open_with_index(&archive_dir)?;
    let db = archive.db.clone().expect("indexed archive");
    let db_check: DefaultJsonFilesDatabase = (*db).clone();
    let relay = RelayState::new((*db).clone(), None);

    let app = nostrsearch_server::http::router_full(state, Some(archive), Some(relay));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Publish a properly signed event to our relay.
    let keys = Keys::generate();
    let client = Client::builder().signer(keys.clone()).build();
    client.add_relay(format!("ws://{addr}")).await?;
    client.connect().await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let builder = EventBuilder::text_note("archived via the absorbed relay");
    let out = client.send_event_builder(builder).await?;
    let event_id = *out.id();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // The archive database should now know this event id.
    let known = db_check
        .check_id(&event_id)
        .await
        .map(|s| !matches!(s, DatabaseEventStatus::NotExistent))
        .unwrap_or(false);
    assert!(known, "relay-published event should be archived");

    // And the archive HTTP listing should expose a file.
    let body: serde_json::Value =
        reqwest_get_json(&format!("http://{addr}/archive/files")).await?;
    assert!(
        body.as_array().map(|a| !a.is_empty()).unwrap_or(false),
        "archive listing should contain at least one file, got {body}"
    );

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

/// Minimal JSON GET without adding an HTTP client dependency.
async fn reqwest_get_json(url: &str) -> anyhow::Result<serde_json::Value> {
    let url = url.strip_prefix("http://").unwrap();
    let (host, path) = url.split_once('/').unwrap();
    let mut stream = tokio::net::TcpStream::connect(host).await?;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    stream
        .write_all(
            format!("GET /{path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await?;
    let text = String::from_utf8_lossy(&buf);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("");
    Ok(serde_json::from_str(body.trim())?)
}
