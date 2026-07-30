//! nostrsearch-indexer
//!
//! Turns hole.v0l.io JSONL dumps into time-sharded Tantivy indices.
//!
//! Event reading is delegated to `nostr-archive-cursor` (`NostrCursor`), which
//! walks a directory of `.jsonl`/`.json`/`.zst`/`.gz`/`.bz2` dumps with
//! parallel chunked reads and event-id dedup. This crate owns the write side:
//!
//! ```text
//! NostrCursor ──► route by created_at ──► ShardWriter
//!                                        (one per month, own IndexWriter,
//!                                         scheduled commit)
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

pub mod firehose;
pub mod pipeline;
pub mod shard_writer;

pub use pipeline::{Pipeline, PipelineConfig};
pub use shard_writer::{ShardManager, ShardWriterConfig};
