//! nostrsearch-server binary.

use nostrsearch_core::scoring::ScoreWeights;
use nostrsearch_server::{AppState, ShardRegistry, router};
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "nostrsearch=info,tower_http=info".into()),
        )
        .init();

    let index_root = std::env::var("INDEX_ROOT").unwrap_or_else(|_| "./data/index".into());
    let bind = std::env::var("BIND").unwrap_or_else(|_| "0.0.0.0:8080".into());

    let registry = ShardRegistry::open(&index_root, ScoreWeights::default())?;
    let stats_docs = {
        let mut r = ShardRegistry::open(&index_root, ScoreWeights::default())?;
        r.stats().total_docs
    };
    tracing::info!(index_root = %index_root, docs = stats_docs, "opened index root");

    let state = Arc::new(AppState {
        registry: Mutex::new(registry),
    });

    let app = router(state);
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(bind = %bind, "nostrsearch-server listening");
    axum::serve(listener, app).await?;
    Ok(())
}
