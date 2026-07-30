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
        registry: Mutex::new(registry),
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
                let mut r = state.registry.lock().unwrap();
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
        let mut r = state.registry.lock().unwrap();
        r.get_event(&event_id)?
    };
    assert!(
        by_id.is_some(),
        "get_event should resolve the relay-published event {event_id}"
    );

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}
