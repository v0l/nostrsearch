//! nostrsearch-server
//!
//! Read side of the engine: opens the time-sharded indices and serves the
//! REST search API. Fan-out across pruned shards, top-k merge, hydration.

pub mod admin;
/// Archive serving, shared with `ingest` (see `nostrsearch-archive`).
pub use nostrsearch_archive as archive;
pub mod dashboard;
pub mod http;
pub mod ingest_job;
pub mod node;
pub mod registry;
pub mod relay;
pub mod reports;
pub mod scraper;

pub use archive::ArchiveState;
pub use http::{AppState, SharedState, router};
pub use node::{EventSink, NodeDb, spawn_firehose, spawn_writer};
pub use registry::{RegistryStats, SearchHit, ShardRegistry};
pub use relay::RelayState;
