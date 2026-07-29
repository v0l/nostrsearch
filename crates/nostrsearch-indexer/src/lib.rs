//! nostrsearch-indexer
//!
//! Turns hole.v0l.io JSONL dumps into time-sharded Tantivy indices.
//!
//! Pipeline:
//!
//! ```text
//! .jsonl(.zst) ──► parse (parallel) ──► route by created_at ──► ShardWriter
//!                                                            (one per month,
//!                                                             own IndexWriter,
//!                                                             scheduled commit)
//! ```
//!
//! Key properties vs. the naive single-index approach:
//!
//! - **No global writer lock.** Each monthly shard owns its `IndexWriter`;
//!   events route to shards by `created_at`, so writers never contend.
//! - **Bounded, immutable shards.** A finished month stops growing → its
//!   segments can be finalized and pushed to object storage.
//! - **Backpressure-aware.** Parsing outruns indexing; a bounded channel
//!   between them keeps memory flat over a 763 GiB corpus.

pub mod shard_writer;
pub mod source;

pub use shard_writer::{ShardManager, ShardWriterConfig};
pub use source::{EventStream, JsonlSource};
