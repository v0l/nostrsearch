//! nostrsearch-server
//!
//! Read side of the engine: opens the time-sharded indices and serves the
//! REST search API. Fan-out across pruned shards, top-k merge, hydration.

pub mod admin;
pub mod archive;
pub mod dashboard;
pub mod http;
pub mod node;
pub mod registry;
pub mod relay;
pub mod replay;
pub mod reports;
pub mod scraper;

pub use archive::ArchiveState;
pub use http::{AppState, SharedState, router};
pub use node::{EventSink, NodeDb, spawn_firehose, spawn_writer};
pub use registry::{RegistryStats, SearchHit, ShardRegistry};
pub use relay::RelayState;
