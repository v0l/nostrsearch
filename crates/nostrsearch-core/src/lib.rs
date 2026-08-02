//! nostrsearch-core
//!
//! Core building blocks for a distributed Nostr search engine:
//!
//! - [`event`]      — the canonical, serde-deserializable Nostr event.
//! - [`schema`]     — the Tantivy index schema tuned for Nostr (tags, fast
//!   fields, scoring signals).
//! - [`shard`]      — time-based shard layout over `created_at`.
//! - [`query`]      — NIP-50 filter → Tantivy query translation.
//! - [`scoring`]    — composite BM25 × (web-of-trust + recency) scoring.

pub mod event;
pub mod query;
pub mod relay;
pub mod schema;
pub mod scoring;
pub mod shard;
