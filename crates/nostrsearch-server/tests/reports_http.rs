//! The report HTTP surface: full snapshots at `/reports/{name}`, an index at
//! `/reports`, and the realtime delta stream at `/reports/stream` that a
//! dashboard uses to animate numbers without refetching whole reports.

use futures::StreamExt;
use nostrsearch_core::scoring::ScoreWeights;
use nostrsearch_server::reports::ReportStore;
use nostrsearch_server::{AppState, ShardRegistry};
use nostrsearch_stats::ReportDelta;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, BufReader};

fn tempdir(tag: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "nsreports-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// Serve the router on an ephemeral port; returns the base URL.
async fn serve(store: ReportStore) -> anyhow::Result<String> {
    let index_root = tempdir("idx");
    let registry = ShardRegistry::open(&index_root, ScoreWeights::default())?;
    let state = Arc::new(AppState {
        registry: Mutex::new(registry),
    });
    let app = nostrsearch_server::http::router_all(state, None, None, Some(store));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(format!("http://{addr}"))
}

#[tokio::test]
async fn serves_report_index_and_named_reports() -> anyhow::Result<()> {
    let store = ReportStore::new();
    store.publish(
        1_700_000_000,
        vec![
            (
                "activity",
                serde_json::json!({"1700000000": {"zap_count": 3}}),
            ),
            ("client_tags", serde_json::json!({"snort": {"sum": 7}})),
        ],
    );
    let base = serve(store).await?;

    // index lists what is available
    let idx: serde_json::Value = reqwest::get(format!("{base}/reports"))
        .await?
        .json()
        .await?;
    assert_eq!(idx["generated_at"], 1_700_000_000);
    let names: Vec<&str> = idx["reports"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["activity", "client_tags"]);

    // a named report returns its snapshot verbatim
    let activity: serde_json::Value = reqwest::get(format!("{base}/reports/activity"))
        .await?
        .json()
        .await?;
    assert_eq!(activity["1700000000"]["zap_count"], 3);

    // unknown report is a 404 that says what *is* available
    let resp = reqwest::get(format!("{base}/reports/nope")).await?;
    assert_eq!(resp.status(), 404);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["error"], "unknown report");
    assert!(
        body["available"]
            .as_array()
            .unwrap()
            .contains(&"activity".into())
    );

    Ok(())
}

/// The realtime path a dashboard actually uses: seed from the full report,
/// then apply streamed patches.
#[tokio::test]
async fn streams_deltas_and_converges_on_the_snapshot() -> anyhow::Result<()> {
    let store = ReportStore::new();
    store.publish(
        1_700_000_000,
        vec![("client_tags", serde_json::json!({"snort": {"sum": 1}}))],
    );
    let base = serve(store.clone()).await?;

    // 1. seed: fetch the full report
    let mut held: serde_json::Value = reqwest::get(format!("{base}/reports/client_tags"))
        .await?
        .json()
        .await?;

    // 2. subscribe to the delta stream
    let resp = reqwest::get(format!("{base}/reports/stream")).await?;
    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/event-stream")
    );

    let stream = resp
        .bytes_stream()
        .map(|r| r.map_err(std::io::Error::other));
    let mut lines = BufReader::new(tokio_util::io::StreamReader::new(stream)).lines();

    // 3. the pipeline reports incremental changes
    let publisher = store.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        publisher.apply_deltas(
            1_700_000_005,
            vec![ReportDelta {
                name: "client_tags".into(),
                patch: serde_json::json!({"snort": {"sum": 2}, "damus": {"sum": 9}}),
            }],
        );
    });

    // 4. read one SSE frame and merge it
    let frame = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while let Ok(Some(line)) = lines.next_line().await {
            if let Some(json) = line.strip_prefix("data: ") {
                return Some(json.to_string());
            }
        }
        None
    })
    .await?
    .expect("received a delta frame");

    let delta: ReportDelta = serde_json::from_str(&frame)?;
    assert_eq!(delta.name, "client_tags");
    nostrsearch_stats::merge_patch(&mut held, &delta.patch);

    // The merged client state equals what a fresh full fetch would return.
    let fresh: serde_json::Value = reqwest::get(format!("{base}/reports/client_tags"))
        .await?
        .json()
        .await?;
    assert_eq!(held, fresh);
    assert_eq!(held["snort"]["sum"], 2);
    assert_eq!(held["damus"]["sum"], 9);

    Ok(())
}
