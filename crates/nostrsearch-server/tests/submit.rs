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
    writer: Option<nostrsearch_server::WriterHandle>,
    server: tokio::task::JoinHandle<()>,
    submit: SubmitState,
    _root: Arc<tempfile::TempDir>,
}

type Tweak = Box<dyn FnOnce(&mut SubmitConfig)>;

impl Node {
    async fn restart(self, tweak: Tweak) -> anyhow::Result<Node> {
        let root = self._root.clone();
        self.submit.stop_and_wait().await;
        drop(self);
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        serve_at(root, 0, None, HashSet::new(), None, tweak).await
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.submit.stop();
        self.server.abort();
        if let Some(writer) = self.writer.take() {
            std::thread::spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(writer.shutdown());
            })
            .join()
            .unwrap();
        }
    }
}

async fn serve(_unused: u64, min_free: u64, admins: HashSet<PublicKey>) -> anyhow::Result<Node> {
    serve_with(min_free, None, admins).await
}

async fn serve_with(
    min_free: u64,
    free_space: Option<Arc<dyn Fn() -> u64 + Send + Sync>>,
    admins: HashSet<PublicKey>,
) -> anyhow::Result<Node> {
    serve_deduped(min_free, free_space, admins, None).await
}

async fn serve_deduped(
    min_free: u64,
    free_space: Option<Arc<dyn Fn() -> u64 + Send + Sync>>,
    admins: HashSet<PublicKey>,
    dedupe: Option<Arc<nostrsearch_indexer::id_store::IdStore>>,
) -> anyhow::Result<Node> {
    serve_at(
        Arc::new(tempfile::tempdir()?),
        min_free,
        free_space,
        admins,
        dedupe,
        Box::new(|_| {}),
    )
    .await
}

async fn serve_at(
    root: Arc<tempfile::TempDir>,
    min_free: u64,
    free_space: Option<Arc<dyn Fn() -> u64 + Send + Sync>>,
    admins: HashSet<PublicKey>,
    dedupe: Option<Arc<nostrsearch_indexer::id_store::IdStore>>,
    tweak: Tweak,
) -> anyhow::Result<Node> {
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
    let mut cfg = SubmitConfig {
        dir: root.path().join("submissions"),
        max_concurrent: 2,
        process_concurrency: 1,
        min_free_bytes: min_free,
        batch_lines: 4096,
        batch_pause: std::time::Duration::ZERO,
        free_space,
        origin: None,
        admins,
    };
    tweak(&mut cfg);
    let submit = SubmitState::new(cfg, (*db).clone(), sink, dedupe);
    let app = nostrsearch_server::http::router_full(state, Some(archive), None)
        .merge(nostrsearch_server::submit::router(submit.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(Node {
        base: format!("http://{addr}"),
        writer: Some(handle),
        server,
        submit,
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
    for _ in 0..1200 {
        let v: serde_json::Value = reqwest::get(&url).await?.json().await?;
        if v["state"] == "done" || v["state"] == "failed" || v["state"] == "interrupted" {
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
async fn limits_are_public_so_the_upload_page_can_show_them() -> anyhow::Result<()> {
    let node = serve(12_345, 0, HashSet::new()).await?;
    let r = reqwest::get(format!("{}/submit/limits", node.base)).await?;
    assert_eq!(r.status(), 200);
    let v: serde_json::Value = r.json().await?;
    assert!(v.get("max_bytes").is_none());
    assert!(v.get("daily_bytes").is_none());
    assert!(v["size_limit"].is_null());
    assert!(v["disk_note"].as_str().is_some_and(|n| n.contains("stored on disk")));
    assert!(v["resume_note"].as_str().is_some_and(|n| n.contains("again")));
    assert!(v["acceptable_bytes"].as_u64().is_some());
    assert!(v["queued_jobs"].as_u64().is_some());
    assert!(v["accepting"].as_bool().is_some());
    assert!(v["formats"].as_array().is_some_and(|f| f.iter().any(|x| x == ".mdb")));
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

#[test]
fn reads_a_database_written_by_real_strfry() {
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/strfry");
    let dir = tempfile::tempdir().unwrap();
    let mdb = dir.path().join("data.mdb");
    let packed = std::fs::read(fixtures.join("data.mdb.zst")).unwrap();
    std::fs::write(&mdb, zstd::decode_all(&packed[..]).unwrap()).unwrap();
    let expected: std::collections::HashSet<String> =
        String::from_utf8(zstd::decode_all(&std::fs::read(fixtures.join("expected.jsonl.zst")).unwrap()[..]).unwrap())
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap()["id"].as_str().unwrap().to_string())
            .collect();
    assert_eq!(expected.len(), 299);

    let db = nostrsearch_server::strfry::Lmdb::open(&mdb).unwrap();
    db.check_meta().unwrap();
    let mut got = std::collections::HashSet::new();
    let mut undecodable = 0u64;
    db.for_each_event(4 << 20, |p| {
        match p {
            nostrsearch_server::strfry::Payload::Json(j) => {
                let v: serde_json::Value = serde_json::from_slice(&j).unwrap();
                got.insert(v["id"].as_str().unwrap().to_string());
            }
            nostrsearch_server::strfry::Payload::Undecodable => undecodable += 1,
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(undecodable, 0);
    assert_eq!(got, expected);
}

#[tokio::test]
async fn a_real_strfry_database_uploaded_compressed_is_ingested_in_full() -> anyhow::Result<()> {
    let node = serve(1 << 30, 0, HashSet::new()).await?;
    let keys = Keys::generate();
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/strfry");
    let v = run(&node, &keys, "data.mdb.zst", std::fs::read(fixtures.join("data.mdb.zst"))?).await;
    assert_eq!(v["state"], "done", "{v}");
    assert_eq!(v["lines"], 299, "{v}");
    assert_eq!(v["undecodable"], 0, "{v}");
    assert_eq!(v["malformed"], 0, "{v}");
    let accounted = ["new", "duplicate", "bad_signature", "ephemeral", "future"]
        .iter()
        .map(|k| v[*k].as_u64().unwrap_or(0))
        .sum::<u64>();
    assert_eq!(accounted, 299, "{v}");
    assert!(v["new"].as_u64().unwrap() > 250, "{v}");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn a_large_real_strfry_database_is_ingested_in_full() -> anyhow::Result<()> {
    let path = std::env::var("STRFRY_DB").expect("STRFRY_DB points at a data.mdb");
    let expected: usize = std::env::var("STRFRY_EVENTS").expect("STRFRY_EVENTS").parse()?;
    let node = serve(u64::MAX / 2, 0, HashSet::new()).await?;
    let keys = Keys::generate();
    let url = format!("{}/submit?name=data.mdb", node.base);
    let file = tokio::fs::File::open(&path).await?;
    let r = reqwest::Client::new()
        .post(&url)
        .header("authorization", nip98(&keys, &url, "POST").await?)
        .body(reqwest::Body::wrap_stream(tokio_util::io::ReaderStream::new(file)))
        .send()
        .await?;
    assert_eq!(r.status(), 202);
    let id = r.json::<serde_json::Value>().await?["id"].as_str().unwrap().to_string();
    let status_url = format!("{}/submit/{id}", node.base);
    let v = loop {
        let v: serde_json::Value = reqwest::get(&status_url).await?.json().await?;
        if v["state"] == "done" || v["state"] == "failed" || v["state"] == "interrupted" {
            break v;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    };
    assert_eq!(v["state"], "done", "{v}");
    assert_eq!(v["lines"].as_u64().unwrap() as usize, expected, "{v}");
    assert_eq!(v["undecodable"], 0, "{v}");
    assert_eq!(v["malformed"], 0, "{v}");
    let accounted = ["new", "duplicate", "bad_signature", "ephemeral", "future"]
        .iter()
        .map(|k| v[*k].as_u64().unwrap_or(0))
        .sum::<u64>();
    assert_eq!(accounted as usize, expected, "{v}");
    eprintln!("{v}");
    Ok(())
}

#[tokio::test]
async fn shutting_the_node_down_mid_processing_exits_cleanly() -> anyhow::Result<()> {
    let node = serve(1 << 30, 0, HashSet::new()).await?;
    let keys = Keys::generate();
    let mut events = vec![];
    for i in 0..300 {
        events.push(note(&keys, &format!("in flight {i}")).await.as_json());
    }
    let resp = submit(&node, &keys, "dump.jsonl", dump(&events)).await?;
    assert_eq!(resp.status(), 202);
    drop(node);
    Ok(())
}

async fn list_jobs(node: &Node, keys: &Keys) -> Vec<serde_json::Value> {
    let url = format!("{}/submit", node.base);
    reqwest::Client::new()
        .get(&url)
        .header("authorization", nip98(keys, &url, "GET").await.unwrap())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn wait_for_job(
    node: &Node,
    keys: &Keys,
    what: &str,
    ok: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    for _ in 0..200 {
        if let Some(j) = list_jobs(node, keys).await.into_iter().find(|j| ok(j)) {
            return j;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("never saw: {what}: {:?}", list_jobs(node, keys).await);
}

type Chunk = Result<Vec<u8>, std::io::Error>;

fn live_body() -> (tokio::sync::mpsc::Sender<Chunk>, reqwest::Body) {
    let (tx, rx) = tokio::sync::mpsc::channel::<Chunk>(4);
    let stream = futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|c| (c, rx))
    });
    (tx, reqwest::Body::wrap_stream(stream))
}

async fn lines_of(keys: &Keys, prefix: &str, n: usize) -> Vec<u8> {
    let mut out = Vec::new();
    for i in 0..n {
        out.extend_from_slice(note(keys, &format!("{prefix} {i}")).await.as_json().as_bytes());
        out.push(b'\n');
    }
    out
}

fn start_post(node: &Node, keys: &Keys, name: &str, body: reqwest::Body) -> tokio::task::JoinHandle<()> {
    let url = format!("{}/submit?name={name}", node.base);
    let keys = keys.clone();
    tokio::spawn(async move {
        let auth = nip98(&keys, &url, "POST").await.unwrap();
        let _ = reqwest::Client::new()
            .post(&url)
            .header("authorization", auth)
            .body(body)
            .send()
            .await;
    })
}

#[tokio::test]
async fn upload_beyond_the_old_size_cap_is_accepted() -> anyhow::Result<()> {
    let node = serve(100, 0, HashSet::new()).await?;
    let keys = Keys::generate();
    let body = lines_of(&keys, "big", 40).await;
    assert!(body.len() > 100);
    let r = submit(&node, &keys, "a.jsonl", body).await?;
    assert_eq!(r.status(), 202);
    let id = r.json::<serde_json::Value>().await?["id"].as_str().unwrap().to_string();
    let v = finished(&node, &keys, &id).await?;
    assert_eq!(v["state"], "done", "{v}");
    assert_eq!(v["new"], 40, "{v}");
    Ok(())
}

fn stored_files(node: &Node) -> Vec<String> {
    std::fs::read_dir(node._root.path().join("submissions"))
        .map(|d| {
            d.flatten()
                .map(|e| e.file_name().to_string_lossy().to_string())
                .filter(|n| n.ends_with(".upload") || n.ends_with(".mdb") || n.ends_with(".part"))
                .collect()
        })
        .unwrap_or_default()
}

fn slow(lines: usize, pause_ms: u64) -> Tweak {
    Box::new(move |c| {
        c.batch_lines = lines;
        c.batch_pause = std::time::Duration::from_millis(pause_ms);
    })
}

async fn serve_slow(lines: usize, pause_ms: u64) -> anyhow::Result<Node> {
    serve_at(
        Arc::new(tempfile::tempdir()?),
        0,
        None,
        HashSet::new(),
        None,
        slow(lines, pause_ms),
    )
    .await
}

async fn post_id(node: &Node, keys: &Keys, name: &str, body: Vec<u8>) -> anyhow::Result<String> {
    let r = submit(node, keys, name, body).await?;
    assert_eq!(r.status(), 202);
    Ok(r.json::<serde_json::Value>().await?["id"].as_str().unwrap().to_string())
}

async fn state_of(node: &Node, id: &str) -> serde_json::Value {
    reqwest::get(format!("{}/submit/{id}", node.base)).await.unwrap().json().await.unwrap()
}

#[tokio::test]
async fn upload_returns_before_processing_finishes() -> anyhow::Result<()> {
    let node = serve_slow(10, 200).await?;
    let keys = Keys::generate();
    let id = post_id(&node, &keys, "slow.jsonl", lines_of(&keys, "slow", 100).await).await?;
    let v = state_of(&node, &id).await;
    assert!(v["state"] == "queued" || v["state"] == "processing", "{v}");
    assert!(v["new"].as_u64().unwrap() < 100, "{v}");
    assert_eq!(stored_files(&node).len(), 1, "the upload is kept on disk until processed");
    let v = finished(&node, &keys, &id).await?;
    assert_eq!(v["state"], "done", "{v}");
    assert_eq!(v["new"], 100, "{v}");
    Ok(())
}

#[tokio::test]
async fn processing_job_reports_percent_of_bytes_read() -> anyhow::Result<()> {
    let node = serve_slow(10, 200).await?;
    let keys = Keys::generate();
    let id = post_id(&node, &keys, "slow.jsonl", lines_of(&keys, "pct", 100).await).await?;
    let mut seen = false;
    for _ in 0..100 {
        let v = state_of(&node, &id).await;
        if v["state"] == "processing" && v["percent"].as_f64().unwrap() > 0.0 {
            seen = true;
            assert!(v["bytes_read"].as_u64().unwrap() > 0, "{v}");
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(seen, "never saw live progress");
    let v = finished(&node, &keys, &id).await?;
    assert_eq!(v["percent"], 100.0, "{v}");
    Ok(())
}

#[tokio::test]
async fn restart_mid_processing_resumes_from_the_saved_offset_without_re_adding() -> anyhow::Result<()> {
    let node = serve_slow(10, 150).await?;
    let keys = Keys::generate();
    let id = post_id(&node, &keys, "resume.jsonl", lines_of(&keys, "resume", 120).await).await?;
    for _ in 0..200 {
        let v = state_of(&node, &id).await;
        if v["new"].as_u64().unwrap() >= 30 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let node = node.restart(slow(10, 0)).await?;
    let before = state_of(&node, &id).await;
    assert!(before["state"] == "queued" || before["state"] == "processing" || before["state"] == "done", "{before}");
    let new_before = before["new"].as_u64().unwrap();
    assert!(new_before >= 30 && new_before <= 120, "{before}");
    let v = finished(&node, &keys, &id).await?;
    assert_eq!(v["state"], "done", "{v}");
    assert_eq!(v["new"], 120, "{v}");
    assert_eq!(v["duplicate"], 0, "resume must skip what was already ingested: {v}");
    assert_eq!(v["lines"], 120, "{v}");
    assert!(stored_files(&node).is_empty());
    Ok(())
}

#[tokio::test]
async fn restart_resumes_a_strfry_database_without_re_adding() -> anyhow::Result<()> {
    let node = serve_slow(10, 150).await?;
    let keys = Keys::generate();
    let mut events = vec![];
    for i in 0..100 {
        events.push((note(&keys, &format!("mdb resume {i}")).await.as_json(), None));
    }
    let id = post_id(&node, &keys, "data.mdb", strfry_db(1, 3, &events, DICT)).await?;
    for _ in 0..200 {
        if state_of(&node, &id).await["new"].as_u64().unwrap() >= 20 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let node = node.restart(slow(10, 0)).await?;
    let v = finished(&node, &keys, &id).await?;
    assert_eq!(v["state"], "done", "{v}");
    assert_eq!(v["new"], 100, "{v}");
    assert_eq!(v["duplicate"], 0, "{v}");
    assert!(stored_files(&node).is_empty());
    Ok(())
}

#[tokio::test]
async fn completed_job_deletes_its_stored_file() -> anyhow::Result<()> {
    let node = serve(1 << 30, 0, HashSet::new()).await?;
    let keys = Keys::generate();
    let id = post_id(&node, &keys, "a.jsonl.zst", zstd::encode_all(&lines_of(&keys, "del", 20).await[..], 3)?).await?;
    let v = finished(&node, &keys, &id).await?;
    assert_eq!(v["state"], "done", "{v}");
    assert!(stored_files(&node).is_empty(), "{:?}", stored_files(&node));
    Ok(())
}

#[tokio::test]
async fn upload_is_stored_compressed_as_sent() -> anyhow::Result<()> {
    let node = serve_slow(5, 400).await?;
    let keys = Keys::generate();
    let body = zstd::encode_all(&lines_of(&keys, "raw", 60).await[..], 3)?;
    let id = post_id(&node, &keys, "a.jsonl.zst", body.clone()).await?;
    let stored = stored_files(&node);
    assert_eq!(stored.len(), 1, "{stored:?}");
    let path = node._root.path().join("submissions").join(&stored[0]);
    assert_eq!(std::fs::read(path)?, body);
    let _ = id;
    Ok(())
}

#[tokio::test]
async fn upload_that_would_cross_the_floor_is_refused_by_declared_size() -> anyhow::Result<()> {
    let node = serve_with(5_000, Some(Arc::new(|| 10_000)), HashSet::new()).await?;
    let keys = Keys::generate();
    let r = submit(&node, &keys, "big.jsonl", vec![b' '; 6_000]).await?;
    assert_eq!(r.status(), 507);
    assert!(stored_files(&node).is_empty());
    let limits: serde_json::Value = reqwest::get(format!("{}/submit/limits", node.base)).await?.json().await?;
    assert_eq!(limits["acceptable_bytes"], 5_000);
    let r = submit(&node, &keys, "ok.jsonl", lines_of(&keys, "fits", 5).await).await?;
    assert_eq!(r.status(), 202);
    Ok(())
}

#[tokio::test]
async fn dropped_connection_discards_the_partial_upload_and_ends_interrupted() -> anyhow::Result<()> {
    let node = serve(1 << 30, 0, HashSet::new()).await?;
    let keys = Keys::generate();
    let (tx, body) = live_body();
    let post = start_post(&node, &keys, "cut.jsonl", body);
    tx.send(Ok(lines_of(&keys, "kept", 25).await)).await?;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    tx.send(Err(std::io::Error::other("connection reset"))).await?;
    drop(tx);
    post.await?;
    let v = wait_for_job(&node, &keys, "interrupted", |j| j["state"] == "interrupted").await;
    assert_eq!(v["new"], 0, "{v}");
    assert!(stored_files(&node).is_empty());
    Ok(())
}

#[tokio::test]
async fn free_space_floor_stops_an_upload_midway_and_discards_it() -> anyhow::Result<()> {
    let free = Arc::new(std::sync::atomic::AtomicU64::new(10_000));
    let f2 = free.clone();
    let node = serve_with(
        5_000,
        Some(Arc::new(move || f2.load(std::sync::atomic::Ordering::SeqCst))),
        HashSet::new(),
    )
    .await?;
    let keys = Keys::generate();
    let (tx, body) = live_body();
    let post = start_post(&node, &keys, "floor.jsonl", body);
    tx.send(Ok(lines_of(&keys, "before", 10).await)).await?;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    free.store(100, std::sync::atomic::Ordering::SeqCst);
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    let _ = tx.send(Ok(lines_of(&keys, "after", 10).await)).await;
    drop(tx);
    post.await?;
    let v = wait_for_job(&node, &keys, "failed", |j| j["state"] == "failed").await;
    assert!(v["error"].as_str().unwrap().contains("floor"), "{v}");
    assert!(stored_files(&node).is_empty());
    Ok(())
}

fn signed_notes(n: usize) -> Vec<Event> {
    let keys = Keys::generate();
    (0..n)
        .map(|i| {
            EventBuilder::text_note(format!("bench event number {i} with a little body text"))
                .custom_created_at(Timestamp::from_secs(1_700_000_000 + i as u64))
                .sign_with_keys(&keys)
                .unwrap()
        })
        .collect()
}

#[tokio::test]
#[ignore]
async fn bench_stage_costs() -> anyhow::Result<()> {
    let n: usize = std::env::var("BENCH_EVENTS").ok().and_then(|v| v.parse().ok()).unwrap_or(40_000);
    let events = signed_notes(n);
    let store_dir = tempfile::tempdir()?;
    let store = Arc::new(nostrsearch_indexer::id_store::IdStore::open(store_dir.path())?);
    let known: Vec<[u8; 32]> = events.iter().step_by(2).map(|e| e.id.to_bytes()).collect();
    store.flush(known.iter())?;
    let node = serve_deduped(0, None, HashSet::new(), Some(store)).await?;
    let keys = Keys::generate();
    let lines: Vec<String> = events.iter().map(|e| e.as_json()).collect();
    let started = std::time::Instant::now();
    let r = submit(&node, &keys, "bench.jsonl", dump(&lines)).await?;
    let id = r.json::<serde_json::Value>().await?["id"].as_str().unwrap().to_string();
    let url = format!("{}/submit/{id}", node.base);
    let v = loop {
        let v: serde_json::Value = reqwest::get(&url).await?.json().await?;
        if v["state"] == "done" || v["state"] == "failed" {
            break v;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    let secs = started.elapsed().as_secs_f64();
    println!("events {n} in {secs:.2}s = {:.0}/s", n as f64 / secs);
    println!("{}", serde_json::to_string_pretty(&v)?);
    assert_eq!(v["new"], (n / 2) as u64);
    assert_eq!(v["duplicate"], (n / 2) as u64);
    Ok(())
}


async fn run_deduped(
    node: &Node,
    body: Vec<u8>,
) -> anyhow::Result<serde_json::Value> {
    let keys = Keys::generate();
    let r = submit(node, &keys, "batch.jsonl", body).await?;
    let id = r.json::<serde_json::Value>().await?["id"].as_str().unwrap().to_string();
    finished(node, &keys, &id).await
}

#[tokio::test]
async fn known_ids_are_never_re_added_and_new_ones_never_dropped_across_batches() -> anyhow::Result<()> {
    let events = signed_notes(6_000);
    let store_dir = tempfile::tempdir()?;
    let store = Arc::new(nostrsearch_indexer::id_store::IdStore::open(store_dir.path())?);
    let known: Vec<[u8; 32]> = events.iter().step_by(3).map(|e| e.id.to_bytes()).collect();
    store.flush(known.iter())?;
    let node = serve_deduped(0, None, HashSet::new(), Some(store.clone())).await?;
    let lines: Vec<String> = events.iter().map(|e| e.as_json()).collect();
    let v = run_deduped(&node, dump(&lines)).await?;
    assert_eq!(v["state"], "done", "{v}");
    assert_eq!(v["valid"], 6_000);
    assert_eq!(v["duplicate"], known.len() as u64);
    assert_eq!(v["new"], (6_000 - known.len()) as u64);
    for (i, e) in events.iter().enumerate() {
        let want = i % 3 != 0;
        let mut found = false;
        for _ in 0..40 {
            let r = reqwest::get(format!("{}/event/{}", node.base, e.id.to_hex())).await?;
            found = r.status() == 200;
            if found || !want {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        if i % 997 == 0 || i % 3 == 0 && i < 30 {
            assert_eq!(found, want, "event {i}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn an_event_repeated_across_a_batch_boundary_is_added_once() -> anyhow::Result<()> {
    let events = signed_notes(3);
    let node = serve_deduped(0, None, HashSet::new(), None).await?;
    let mut lines = Vec::new();
    for i in 0..9_000 {
        lines.push(events[i % 3].as_json());
    }
    let v = run_deduped(&node, dump(&lines)).await?;
    assert_eq!(v["state"], "done", "{v}");
    assert_eq!(v["valid"], 9_000);
    assert_eq!(v["new"], 3);
    assert_eq!(v["duplicate"], 8_997);
    Ok(())
}

#[tokio::test]
async fn dedupe_is_asked_in_batches_not_one_id_at_a_time() -> anyhow::Result<()> {
    let events = signed_notes(5_000);
    let store_dir = tempfile::tempdir()?;
    let store = Arc::new(nostrsearch_indexer::id_store::IdStore::open(store_dir.path())?);
    let node = serve_deduped(0, None, HashSet::new(), Some(store.clone())).await?;
    let lines: Vec<String> = events.iter().map(|e| e.as_json()).collect();
    let v = run_deduped(&node, dump(&lines)).await?;
    assert_eq!(v["new"], 5_000);
    let lookups = store.lookups();
    assert_eq!(lookups.single, 0, "per-event reads are the cost being removed");
    assert_eq!(lookups.batched_ids, 5_000);
    assert!(lookups.batches >= 1 && lookups.batches <= 5, "{lookups:?}");
    Ok(())
}
