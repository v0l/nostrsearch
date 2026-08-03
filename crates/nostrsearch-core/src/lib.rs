//! nostrsearch-core
//!
//! Core building blocks for a distributed Nostr search engine:
//!
//! - [`event`]      — the canonical, serde-deserializable Nostr event.
//! - [`bech32`]     — NIP-19 `npub`/`note` decoding for the query grammar.
//! - [`tokenizer`]  — script-aware content tokenizer (CJK/Thai bigrams).
//! - [`lang`]       — language detection that populates the `lang` field.
//! - [`schema`]     — the Tantivy index schema tuned for Nostr (tags, fast
//!   fields, scoring signals).
//! - [`shard`]      — time-based shard layout over `created_at`.
//! - [`query`]      — NIP-50 filter → Tantivy query translation.
//! - [`scoring`]    — composite BM25 × (web-of-trust + recency) scoring.

pub mod bech32;
pub mod event;
pub mod lang;
pub mod query;
pub mod relay;
pub mod schema;
pub mod scoring;
pub mod shard;
pub mod tokenizer;
