//! The status service `ingest --bind` runs during a backfill.
//!
//! The point of these is that the service works *without* the archive index:
//! the ingest process holds that lock for hours, and a status page that could
//! only be served after the ingest finished would be useless.

use std::io::Write;

/// Bind the router on an ephemeral port and return its address.
async fn serve(dir: Option<&std::path::Path>) -> std::net::SocketAddr {
    let app = nostrsearch_indexer::serve::router(dir).expect("router");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });
    addr
}

async fn get(addr: std::net::SocketAddr, path: &str) -> (u16, String) {
    let (status, body) = get_bytes(addr, path).await;
    (status, String::from_utf8_lossy(&body).into_owned())
}

/// Bytes, not text: an archive file is compressed, and a lossy UTF-8
/// conversion silently rewrites invalid sequences and changes the length.
async fn get_bytes(addr: std::net::SocketAddr, path: &str) -> (u16, Vec<u8>) {
    let url = format!("http://{addr}{path}");
    let resp = reqwest::get(url).await.expect("request");
    let status = resp.status().as_u16();
    (
        status,
        resp.bytes().await.map(|b| b.to_vec()).unwrap_or_default(),
    )
}

#[tokio::test]
async fn landing_page_serves_without_an_archive() {
    let addr = serve(None).await;

    let (status, body) = get(addr, "/").await;
    assert_eq!(status, 200);
    assert!(body.contains("ingest in progress"), "body: {body}");

    let (status, body) = get(addr, "/healthz").await;
    assert_eq!(status, 200);
    assert_eq!(body, "ok");
}

#[tokio::test]
async fn archive_files_are_listed_and_downloadable_without_the_index() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let mut f = std::fs::File::create(dir.join("events_2024-01.jsonl.zst")).unwrap();
    let body = zstd::encode_all(&b"{\"id\":\"aa\"}\n"[..], 0).unwrap();
    f.write_all(&body).unwrap();
    drop(f);

    let addr = serve(Some(dir)).await;

    // The HTML listing names the file.
    let (status, page) = get(addr, "/archive").await;
    assert_eq!(status, 200);
    assert!(page.contains("events_2024-01.jsonl.zst"), "page: {page}");

    // The JSON listing reports its real size.
    let (status, json) = get(addr, "/archive/files").await;
    assert_eq!(status, 200);
    assert!(json.contains("events_2024-01.jsonl.zst"), "json: {json}");
    assert!(json.contains(&format!("\"size\":{}", body.len())), "{json}");

    // And the bytes come back intact, still decompressing to the original.
    let (status, raw) = get_bytes(addr, "/archive/events_2024-01.jsonl.zst").await;
    assert_eq!(status, 200);
    assert_eq!(raw, body);
    assert_eq!(zstd::decode_all(&raw[..]).unwrap(), b"{\"id\":\"aa\"}\n");
}

/// Without the index lock there is no id->offset map, so a by-id lookup must
/// say "not here" rather than "no such event" — the event is very likely in
/// the corpus, this process just cannot look it up.
#[tokio::test]
async fn event_lookup_reports_unavailable_rather_than_missing() {
    let tmp = tempfile::tempdir().unwrap();
    let addr = serve(Some(tmp.path())).await;

    let (status, _) = get(addr, &format!("/archive/event/{}", "aa".repeat(32))).await;
    assert_eq!(status, 503);
}
