use base64::Engine;
use nostr_sdk::prelude::*;
use nostrsearch_core::scoring::ScoreWeights;
use nostrsearch_indexer::{PipelineConfig, ShardWriterConfig};
use nostrsearch_server::submit::{SubmitConfig, SubmitState};
use nostrsearch_server::{AppState, ArchiveState, ShardRegistry};
use std::collections::HashSet;
use std::io::Write;
use std::sync::Arc;

struct Node {
    base: String,
    _root: tempfile::TempDir,
}

async fn serve(max_bytes: u64, min_free: u64, admins: HashSet<PublicKey>) -> anyhow::Result<Node> {
    let root = tempfile::tempdir()?;
    let archive_dir = root.path().join("archive");
    std::fs::create_dir_all(&archive_dir)?;
    let archive = ArchiveState::open_with_index(&archive_dir)?;
    let db = archive.db.clone().expect("indexed archive");
    let index_root = root.path().join("index");
    std::fs::create_dir_all(&index_root)?;
    let registry = ShardRegistry::open(&index_root, ScoreWeights::default())?;
    let state = Arc::new(AppState { registry });
    let (sink, handle) = nostrsearch_server::spawn_writer(
        PipelineConfig {
            index_root: index_root.clone(),
            shard: ShardWriterConfig::default(),
            state_dir: Some(root.path().join("stats")),
            wot_refresh_every: u64::MAX,
            min_refresh_interval: std::time::Duration::from_secs(3600),
            persist_interval: std::time::Duration::from_secs(3600),
            wot_out: None,
        },
        1_000,
        std::time::Duration::from_millis(200),
    )?;
    std::mem::forget(handle);
    let cfg = SubmitConfig {
        dir: root.path().join("submissions"),
        max_bytes,
        max_concurrent: 2,
        daily_bytes_per_pubkey: u64::MAX,
        min_free_bytes: min_free,
        origin: None,
        admins,
    };
    let submit = SubmitState::new(cfg, (*db).clone(), sink, None);
    let app = nostrsearch_server::http::router_full(state, Some(archive), None)
        .merge(nostrsearch_server::submit::router(submit));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(Node {
        base: format!("http://{addr}"),
        _root: root,
    })
}

async fn nip98(keys: &Keys, url: &str, method: &str) -> anyhow::Result<String> {
    let event = EventBuilder::new(Kind::Custom(27235), "")
        .tags([Tag::parse(["u", url])?, Tag::parse(["method", method])?])
        .sign(keys)
        .await?;
    Ok(format!(
        "Nostr {}",
        base64::engine::general_purpose::STANDARD.encode(event.as_json())
    ))
}

async fn note(keys: &Keys, text: &str) -> Event {
    EventBuilder::text_note(text).sign(keys).await.unwrap()
}

fn dump(events: &[String]) -> Vec<u8> {
    events.join("\n").into_bytes()
}

async fn submit(
    node: &Node,
    keys: &Keys,
    name: &str,
    body: Vec<u8>,
) -> anyhow::Result<reqwest::Response> {
    let url = format!("{}/submit?name={name}", node.base);
    Ok(reqwest::Client::new()
        .post(&url)
        .header("authorization", nip98(keys, &url, "POST").await?)
        .body(body)
        .send()
        .await?)
}

async fn finished(node: &Node, keys: &Keys, id: &str) -> anyhow::Result<serde_json::Value> {
    let url = format!("{}/submit/{id}", node.base);
    for _ in 0..100 {
        let v: serde_json::Value = reqwest::get(&url).await?.json().await?;
        if v["state"] == "done" || v["state"] == "failed" {
            return Ok(v);
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let _ = keys;
    anyhow::bail!("submission never finished")
}

#[tokio::test]
async fn unauthenticated_upload_is_refused() -> anyhow::Result<()> {
    let node = serve(1 << 30, 0, HashSet::new()).await?;
    let r = reqwest::Client::new()
        .post(format!("{}/submit?name=a.jsonl", node.base))
        .body("{}")
        .send()
        .await?;
    assert_eq!(r.status(), 401);
    Ok(())
}

#[tokio::test]
async fn oversized_upload_is_refused() -> anyhow::Result<()> {
    let node = serve(100, 0, HashSet::new()).await?;
    let keys = Keys::generate();
    let r = submit(&node, &keys, "a.jsonl", vec![b'x'; 5000]).await?;
    assert_eq!(r.status(), 413);
    Ok(())
}

#[tokio::test]
async fn low_disk_floor_refuses_upload() -> anyhow::Result<()> {
    let node = serve(1 << 30, u64::MAX / 2, HashSet::new()).await?;
    let keys = Keys::generate();
    let r = submit(&node, &keys, "a.jsonl", b"{}".to_vec()).await?;
    assert_eq!(r.status(), 507);
    Ok(())
}

#[tokio::test]
async fn unknown_extension_is_refused() -> anyhow::Result<()> {
    let node = serve(1 << 30, 0, HashSet::new()).await?;
    let keys = Keys::generate();
    let r = submit(&node, &keys, "a.exe", b"{}".to_vec()).await?;
    assert_eq!(r.status(), 400);
    Ok(())
}

#[tokio::test]
async fn forged_events_are_rejected_and_valid_ones_become_fetchable() -> anyhow::Result<()> {
    let node = serve(1 << 30, 0, HashSet::new()).await?;
    let keys = Keys::generate();
    let good = note(&keys, "good one").await;
    let other = note(&keys, "tampered original").await;
    let mut forged: serde_json::Value = serde_json::to_value(&other)?;
    forged["content"] = "tampered after signing".into();
    let future = EventBuilder::text_note("from the future")
        .custom_created_at(Timestamp::from_secs(4_000_000_000_000))
        .sign(&keys)
        .await?;
    let ephemeral = EventBuilder::new(Kind::Custom(20001), "gone").sign(&keys).await?;
    let old = EventBuilder::text_note("backed up tweet")
        .custom_created_at(Timestamp::from_secs(1_200_000_000))
        .sign(&keys)
        .await?;

    let lines = vec![
        good.as_json(),
        forged.to_string(),
        "not json".to_string(),
        future.as_json(),
        ephemeral.as_json(),
        old.as_json(),
        good.as_json(),
    ];
    let r = submit(&node, &keys, "a.jsonl", dump(&lines)).await?;
    assert_eq!(r.status(), 202);
    let id = r.json::<serde_json::Value>().await?["id"]
        .as_str()
        .unwrap()
        .to_string();
    let v = finished(&node, &keys, &id).await?;
    assert_eq!(v["state"], "done", "{v}");
    assert_eq!(v["lines"], 7);
    assert_eq!(v["bad_signature"], 1);
    assert_eq!(v["malformed"], 1);
    assert_eq!(v["future"], 1);
    assert_eq!(v["ephemeral"], 1);
    assert_eq!(v["valid"], 3);
    assert_eq!(v["new"], 2);
    assert_eq!(v["duplicate"], 1);

    let mut found = false;
    for _ in 0..60 {
        let r = reqwest::get(format!("{}/event/{}", node.base, good.id.to_hex())).await?;
        if r.status() == 200 {
            found = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    assert!(found, "valid event should be fetchable through /event/{{id}}");
    let r = reqwest::get(format!("{}/event/{}", node.base, other.id.to_hex())).await?;
    assert_ne!(r.status(), 200, "forged event must not be indexed");
    Ok(())
}

#[tokio::test]
async fn resubmitting_a_dump_counts_duplicates_and_adds_nothing() -> anyhow::Result<()> {
    let node = serve(1 << 30, 0, HashSet::new()).await?;
    let keys = Keys::generate();
    let ev = note(&keys, "once only").await;
    let mut ids = vec![];
    for round in 0..2 {
        if round == 1 {
            tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        }
        let r = submit(&node, &keys, "a.jsonl", dump(&[ev.as_json()])).await?;
        assert_eq!(r.status(), 202);
        let id = r.json::<serde_json::Value>().await?["id"]
            .as_str()
            .unwrap()
            .to_string();
        ids.push(finished(&node, &keys, &id).await?);
    }
    assert_eq!(ids[0]["new"], 1);
    assert_eq!(ids[1]["new"], 0);
    assert_eq!(ids[1]["duplicate"], 1);
    Ok(())
}

#[tokio::test]
async fn compressed_dumps_are_read() -> anyhow::Result<()> {
    let node = serve(1 << 30, 0, HashSet::new()).await?;
    let keys = Keys::generate();
    let text = dump(&[note(&keys, "zst").await.as_json()]);
    let zst = zstd::encode_all(&text[..], 3)?;
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    let text2 = dump(&[note(&keys, "gz").await.as_json()]);
    gz.write_all(&text2)?;
    let gz = gz.finish()?;
    let mut bz = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::fast());
    let text3 = dump(&[note(&keys, "bz").await.as_json()]);
    bz.write_all(&text3)?;
    let bz = bz.finish()?;
    for (name, body) in [("a.jsonl.zst", zst), ("a.jsonl.gz", gz), ("a.jsonl.bz2", bz)] {
        let r = submit(&node, &keys, name, body).await?;
        assert_eq!(r.status(), 202, "{name}");
        let id = r.json::<serde_json::Value>().await?["id"]
            .as_str()
            .unwrap()
            .to_string();
        let v = finished(&node, &keys, &id).await?;
        assert_eq!(v["new"], 1, "{name}: {v}");
    }
    Ok(())
}

#[tokio::test]
async fn listing_is_scoped_to_the_caller_and_open_to_admins() -> anyhow::Result<()> {
    let admin = Keys::generate();
    let node = serve(1 << 30, 0, HashSet::from([admin.public_key()])).await?;
    let alice = Keys::generate();
    let bob = Keys::generate();
    let r = submit(&node, &alice, "a.jsonl", dump(&[note(&alice, "hi").await.as_json()])).await?;
    assert_eq!(r.status(), 202);

    let list = |keys: Keys| {
        let url = format!("{}/submit", node.base);
        async move {
            let r = reqwest::Client::new()
                .get(&url)
                .header("authorization", nip98(&keys, &url, "GET").await.unwrap())
                .send()
                .await
                .unwrap();
            r.json::<Vec<serde_json::Value>>().await.unwrap()
        }
    };
    assert_eq!(list(alice).await.len(), 1);
    assert_eq!(list(bob).await.len(), 0);
    assert_eq!(list(admin).await.len(), 1);
    Ok(())
}

const TABLE: &str = "rasgueadb_defaultDb__";

fn fb_meta(version: u64, endianness: u64) -> Vec<u8> {
    let mut b = vec![0u8; 48];
    b[0..4].copy_from_slice(&16u32.to_le_bytes());
    b[4..6].copy_from_slice(&10u16.to_le_bytes());
    b[6..8].copy_from_slice(&32u16.to_le_bytes());
    b[8..10].copy_from_slice(&8u16.to_le_bytes());
    b[10..12].copy_from_slice(&16u16.to_le_bytes());
    b[12..14].copy_from_slice(&24u16.to_le_bytes());
    b[16..20].copy_from_slice(&12i32.to_le_bytes());
    b[24..32].copy_from_slice(&version.to_le_bytes());
    b[32..40].copy_from_slice(&endianness.to_le_bytes());
    b
}

fn fb_dict(dict: &[u8]) -> Vec<u8> {
    let mut b = vec![0u8; 20];
    b[0..4].copy_from_slice(&12u32.to_le_bytes());
    b[4..6].copy_from_slice(&6u16.to_le_bytes());
    b[6..8].copy_from_slice(&8u16.to_le_bytes());
    b[8..10].copy_from_slice(&4u16.to_le_bytes());
    b[12..16].copy_from_slice(&8i32.to_le_bytes());
    b[16..20].copy_from_slice(&4u32.to_le_bytes());
    b.extend_from_slice(&(dict.len() as u32).to_le_bytes());
    b.extend_from_slice(dict);
    b
}

fn strfry_db(endianness: u64, version: u64, events: &[(String, Option<u32>)], dict: &[u8]) -> Vec<u8> {
    use heed::types::Bytes;
    use heed::{DatabaseFlags, EnvOpenOptions};
    let dir = tempfile::tempdir().unwrap();
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(256 << 20)
            .max_dbs(64)
            .open(dir.path())
            .unwrap()
    };
    let wtxn = env.write_txn().unwrap();
    env.create_databases_and_commit(wtxn, |opener, txn| -> Result<_, heed::Error> {
        let open = |name: &str, txn: &mut heed::RwTxn| {
            let full = format!("{TABLE}{name}");
            opener
                .database_options()
                .types::<Bytes, Bytes>()
                .flags(DatabaseFlags::INTEGER_KEY)
                .name(&full)
                .create(txn)
        };
        let meta = open("Meta", txn)?;
        meta.put(txn, &1u64.to_ne_bytes(), &fb_meta(version, endianness))?;
        let dicts = open("CompressionDictionary", txn)?;
        dicts.put(txn, &7u64.to_ne_bytes(), &fb_dict(dict))?;
        let payloads = open("EventPayload", txn)?;
        let mut comp = zstd::bulk::Compressor::with_dictionary(3, dict).unwrap();
        for (i, (json, dict_id)) in events.iter().enumerate() {
            let mut v = Vec::new();
            match dict_id {
                None => {
                    v.push(0);
                    v.extend_from_slice(json.as_bytes());
                }
                Some(id) => {
                    v.push(1);
                    v.extend_from_slice(&id.to_ne_bytes());
                    v.extend_from_slice(&comp.compress(json.as_bytes()).unwrap());
                }
            }
            payloads.put(txn, &(i as u64 + 1).to_ne_bytes(), &v)?;
        }
        Ok(heed::CommitOrAbort::<(), ()>::Commit(()))
    })
    .unwrap();
    env.prepare_for_closing().wait();
    std::fs::read(dir.path().join("data.mdb")).unwrap()
}

const DICT: &[u8] = br#"{"id":"","pubkey":"","created_at":0,"kind":1,"tags":[],"content":"","sig":""}"#;

async fn run(node: &Node, keys: &Keys, name: &str, body: Vec<u8>) -> serde_json::Value {
    let r = submit(node, keys, name, body).await.unwrap();
    assert_eq!(r.status(), 202);
    let id = r.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    finished(node, keys, &id).await.unwrap()
}

#[tokio::test]
async fn strfry_database_with_plain_and_compressed_payloads_is_ingested() -> anyhow::Result<()> {
    let node = serve(1 << 30, 0, HashSet::new()).await?;
    let keys = Keys::generate();
    let mut events = vec![];
    for i in 0..300 {
        let text = format!("strfry note {i} {}", "padding ".repeat(i % 40));
        let dict_id = (i % 2 == 0).then_some(7u32);
        events.push((note(&keys, &text).await.as_json(), dict_id));
    }
    let tampered = note(&keys, "tampered").await;
    let mut forged = serde_json::to_value(&tampered)?;
    forged["content"] = "changed".into();
    events.push((forged.to_string(), None));
    events.push((note(&keys, "bad dict").await.as_json(), Some(99)));
    let old = EventBuilder::text_note("2007 tweet")
        .custom_created_at(Timestamp::from_secs(1_190_000_000))
        .sign(&keys)
        .await?;
    events.push((old.as_json(), Some(7)));
    let db = strfry_db(1, 3, &events, DICT);
    assert_eq!(&db[16..20], &0xBEEFC0DEu32.to_le_bytes());

    let v = run(&node, &keys, "data.mdb", db.clone()).await;
    assert_eq!(v["state"], "done", "{v}");
    assert_eq!(v["lines"], 303);
    assert_eq!(v["valid"], 301);
    assert_eq!(v["new"], 301);
    assert_eq!(v["bad_signature"], 1);
    assert_eq!(v["undecodable"], 1);

    let zst = zstd::encode_all(&db[..], 3)?;
    let v = run(&node, &keys, "x.mdb.zst", zst).await;
    assert_eq!(v["state"], "done", "{v}");
    assert_eq!(v["valid"], 301);
    assert_eq!(v["new"], 0);
    assert_eq!(v["duplicate"], 301);

    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    gz.write_all(&db)?;
    let v = run(&node, &keys, "x.mdb.gz", gz.finish()?).await;
    assert_eq!(v["state"], "done", "{v}");
    assert_eq!(v["duplicate"], 301);

    let v = run(&node, &keys, "renamed.jsonl", db).await;
    assert_eq!(v["state"], "done", "{v}");
    assert_eq!(v["duplicate"], 301, "magic number, not the name, decides: {v}");

    let r = reqwest::get(format!("{}/event/{}", node.base, old.id.to_hex())).await?;
    let mut ok = r.status() == 200;
    for _ in 0..40 {
        if ok {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        ok = reqwest::get(format!("{}/event/{}", node.base, old.id.to_hex()))
            .await?
            .status()
            == 200;
    }
    assert!(ok, "old compressed event should be fetchable");
    Ok(())
}

#[tokio::test]
async fn strfry_database_with_foreign_endianness_or_version_is_refused() -> anyhow::Result<()> {
    let node = serve(1 << 30, 0, HashSet::new()).await?;
    let keys = Keys::generate();
    let ev = vec![(note(&keys, "x").await.as_json(), None)];
    for (endianness, version) in [(0u64, 3u64), (1, 2), (1, 4)] {
        let name = format!("e{endianness}v{version}.mdb");
        let v = run(&node, &keys, &name, strfry_db(endianness, version, &ev, DICT)).await;
        assert_eq!(v["state"], "failed", "{endianness}/{version}: {v}");
        assert_eq!(v["new"], 0);
    }
    Ok(())
}

#[tokio::test]
async fn corrupt_strfry_database_fails_cleanly() -> anyhow::Result<()> {
    let node = serve(1 << 30, 0, HashSet::new()).await?;
    let keys = Keys::generate();
    let mut events = vec![];
    for i in 0..200 {
        events.push((note(&keys, &format!("c{i}")).await.as_json(), None));
    }
    let db = strfry_db(1, 3, &events, DICT);

    let truncated = db[..db.len() / 2].to_vec();
    let v = run(&node, &keys, "truncated.mdb", truncated).await;
    assert_eq!(v["state"], "failed", "{v}");

    let mut garbage = db.clone();
    for b in garbage[8192..].iter_mut() {
        *b = 0xA5;
    }
    let v = run(&node, &keys, "garbage.mdb", garbage).await;
    assert_eq!(v["state"], "failed", "{v}");

    let mut header_only = db[..4096].to_vec();
    header_only.resize(4096 * 3, 0);
    let v = run(&node, &keys, "header.mdb", header_only).await;
    assert_eq!(v["state"], "failed", "{v}");

    let ok = run(&node, &keys, "ok.jsonl", dump(&[note(&keys, "still alive").await.as_json()])).await;
    assert_eq!(ok["state"], "done", "{ok}");
    Ok(())
}
