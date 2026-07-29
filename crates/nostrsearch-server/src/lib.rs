//! nostrsearch-server
//!
//! Read side of the engine: opens the time-sharded indices and serves the
//! REST search API. Fan-out across pruned shards, top-k merge, hydration.

pub mod http;
pub mod registry;

pub use http::{AppState, SharedState, router};
pub use registry::{RegistryStats, SearchHit, ShardRegistry};
