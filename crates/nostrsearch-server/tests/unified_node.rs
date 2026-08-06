//! The unified node: one process running relay + search + archive over a
//! shared index. Proves an event published to the relay is archived AND
//! becomes searchable through the same process's search API.

use nostr_sdk::prelude::*;
use nostrsearch_core::scoring::ScoreWeights;
use nostrsearch_indexer::{PipelineConfig, ShardWriterConfig};
use nostrsearch_server::{AppState, ArchiveState, RelayState, ShardRegistry};
use std::sync::{Arc, Mutex};

fn tempdir(tag: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "nsnode-{tag}-{}-{}",
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
async fn relay_write_is_archived_and_searchable_in_one_process() -> anyhow::Result<()> {
    let root = tempdir("root");
    let archive_dir = root.join("archive");
    let index_root = root.join("index");
    std::fs::create_dir_all(&archive_dir)?;
    std::fs::create_dir_all(&index_root)?;

    // One process owns the archive index AND the Tantivy writer.
    let archive = ArchiveState::open_with_index(&archive_dir)?;
    let db = archive.db.clone().expect("indexed archive");

    let (sink, _writer) = nostrsearch_server::spawn_writer(
        PipelineConfig {
            index_root: index_root.clone(),
            shard: ShardWriterConfig::default(),
            state_dir: Some(root.join("stats")),
            wot_refresh_every: 1_000_000,
            min_refresh_interval: std::time::Duration::from_secs(60),
            persist_interval: std::time::Duration::from_secs(300),
            wot_out: None,
        },
        1_000,
        // Commit quickly so the search reader sees writes within the test.
        std::time::Duration::from_millis(200),
    )?;

    // Relay writes go through NodeDb: archive + index + stats.
    let node_db = nostrsearch_server::NodeDb::new((*db).clone(), sink.clone());
    let relay = RelayState::new(node_db, None);

    // Search reader over the same index dir (auto-reloads on commit).
    let registry = ShardRegistry::open(&index_root, ScoreWeights::default())?;
    let state = Arc::new(AppState {
        registry,
    });

    let app = nostrsearch_server::http::router_full(state.clone(), Some(archive), Some(relay));
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

    // Publish a signed note to our own relay.
    let keys = Keys::generate();
    let client = Client::builder().signer(keys.clone()).build();
    client.add_relay(format!("ws://{addr}")).await?;
    client.connect().await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let unique = "zebrafish";
    let out = client
        .send_event_builder(EventBuilder::text_note(format!(
            "unified node test {unique}"
        )))
        .await?;
    let event_id = out.id().to_hex();

    // Wait for the writer task to commit and the reader to reload.
    let mut found = None;
    for _ in 0..40 {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        let mut reg = ShardRegistry::open(&index_root, ScoreWeights::default())?;
        if reg.stats().total_docs > 0 {
            // Now query through the live server state (shared registry).
            let hits = {
                let r = &state.registry;
                r.search(&nostrsearch_core::query::SearchFilter {
                    search: Some(unique.to_string()),
                    limit: 10,
                    ..Default::default()
                })
                .unwrap_or_default()
            };
            if !hits.is_empty() {
                found = Some(hits);
                break;
            }
        }
    }

    let hits = found.expect("relay-published event should become searchable");
    assert!(
        hits.iter().any(|h| h.content.contains(unique)),
        "search should return the relay-published note, got {hits:?}"
    );

    // get_event must also find it (it enumerates shard dirs from disk, so a
    // node started against an empty index still resolves later writes).
    let by_id = {
        let r = &state.registry;
        r.get_event(&event_id)?
    };
    assert!(
        by_id.is_some(),
        "get_event should resolve the relay-published event {event_id}"
    );

    // A search hit must be a *complete signed event*, which a NIP-50 relay has
    // to be able to return. The index stores neither `tags` nor `sig`, so this
    // only works because the hit is hydrated by id from the archive.
    db.flush().await?;
    let body: serde_json::Value =
        get_json(&format!("http://{addr}/search?q={unique}&limit=5")).await?;
    let first = body
        .as_array()
        .and_then(|a| a.first())
        .expect("search over HTTP should return the note");
    let event = first
        .get("event")
        .expect("hit should carry the full signed event");
    assert_eq!(event["id"].as_str(), Some(event_id.as_str()));
    assert_eq!(
        event["pubkey"].as_str(),
        Some(keys.public_key().to_hex()).as_deref()
    );
    assert!(event["tags"].is_array(), "tags must survive hydration");
    assert_eq!(
        event["sig"].as_str().map(str::len),
        Some(128),
        "a client cannot verify a hit without the signature"
    );

    // Same for the single-event endpoint, including uppercase hex (ids are
    // indexed lowercase, so the query side has to be folded).
    let by_id_http: serde_json::Value =
        get_json(&format!("http://{addr}/event/{}", event_id.to_uppercase())).await?;
    assert_eq!(by_id_http["event"]["id"].as_str(), Some(event_id.as_str()));

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

/// Minimal JSON GET without adding an HTTP client dependency.
async fn get_json(url: &str) -> anyhow::Result<serde_json::Value> {
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
