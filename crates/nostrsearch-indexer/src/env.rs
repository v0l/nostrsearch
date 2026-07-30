//! Shared environment configuration.
//!
//! Every binary (server node, `ingest`, `stats`) reads the **same** variables so
//! a container image can set them once and have all entry points agree. CLI
//! flags, where a binary has them, override these.
//!
//! | Variable | Meaning | Default |
//! |---|---|---|
//! | `INDEX_ROOT` | Tantivy shard root | `./data/index` |
//! | `STATE_DIR` | stats/analysis state store | `./data/stats` |
//! | `WOT_OUT` | web-of-trust snapshot path | `./data/wot.bin` |
//! | `ARCHIVE_DIR` | `.jsonl.zst` corpus + id index | unset |
//! | `RELAYS` | comma-separated upstream relays | unset |
//! | `MAX_OPEN_SHARDS` | shard writers held open | `8` |
//! | `WOT_REFRESH_EVERY` | events between WoT rebuilds | `100000` |
//! | `WOT_MIN_REFRESH_SECS` | wall-clock floor between rebuilds | `60` |
//! | `STATS_PERSIST_SECS` | analysis-state persist cadence | `300` |
//!
//! Defaults are CWD-relative so the binaries work from a checkout; the
//! container image overrides them to absolute paths under the data volume,
//! because a non-root process cannot create `./data` inside the image.

use std::path::PathBuf;

/// A path from `key`, falling back to `default`.
pub fn path(key: &str, default: &str) -> PathBuf {
    PathBuf::from(non_empty(key).unwrap_or_else(|| default.to_string()))
}

/// An optional path — `None` when unset or empty.
pub fn opt_path(key: &str) -> Option<PathBuf> {
    non_empty(key).map(PathBuf::from)
}

/// A truthy flag (`1`, `true`, `yes`).
pub fn flag(key: &str) -> bool {
    matches!(
        non_empty(key).as_deref(),
        Some("1") | Some("true") | Some("yes")
    )
}

/// A `u64`, falling back to `default` when unset or unparseable.
pub fn u64_or(key: &str, default: u64) -> u64 {
    non_empty(key)
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// A comma-separated list, trimmed, empties dropped.
pub fn list(key: &str) -> Vec<String> {
    non_empty(key)
        .map(|v| {
            v.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

fn non_empty(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.is_empty())
}

// ── The shared contract, one definition ────────────────────────────────────

pub fn index_root() -> PathBuf {
    path("INDEX_ROOT", "./data/index")
}

pub fn state_dir() -> PathBuf {
    path("STATE_DIR", "./data/stats")
}

pub fn wot_out() -> PathBuf {
    path("WOT_OUT", "./data/wot.bin")
}

pub fn archive_dir() -> Option<PathBuf> {
    opt_path("ARCHIVE_DIR")
}

pub fn relays() -> Vec<String> {
    list("RELAYS")
}

/// Shard writers held open at once (`MAX_OPEN_SHARDS`, 8). Total writer heap
/// is this times the per-shard heap, so it bounds memory over a corpus that
/// spans many months.
pub fn max_open_shards() -> usize {
    u64_or("MAX_OPEN_SHARDS", 8) as usize
}

/// Indexing threads per shard (`WRITER_THREADS`, 1). Total indexing threads is
/// this times the number of open shards.
pub fn writer_threads() -> usize {
    u64_or("WRITER_THREADS", 1) as usize
}

pub fn wot_refresh_every() -> u64 {
    u64_or("WOT_REFRESH_EVERY", 100_000)
}

/// Wall-clock floor between WoT refreshes (`WOT_MIN_REFRESH_SECS`, 60s).
pub fn min_refresh_interval() -> std::time::Duration {
    std::time::Duration::from_secs(u64_or("WOT_MIN_REFRESH_SECS", 60))
}

/// Cadence for persisting analysis state (`STATS_PERSIST_SECS`, 300s).
pub fn persist_interval() -> std::time::Duration {
    std::time::Duration::from_secs(u64_or("STATS_PERSIST_SECS", 300))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_env_is_treated_as_unset() {
        unsafe { std::env::set_var("NS_TEST_EMPTY", "") };
        assert_eq!(path("NS_TEST_EMPTY", "fallback"), PathBuf::from("fallback"));
        assert!(opt_path("NS_TEST_EMPTY").is_none());
        unsafe { std::env::remove_var("NS_TEST_EMPTY") };
    }

    #[test]
    fn list_trims_and_drops_empties() {
        unsafe { std::env::set_var("NS_TEST_LIST", " wss://a , ,wss://b ") };
        assert_eq!(list("NS_TEST_LIST"), vec!["wss://a", "wss://b"]);
        unsafe { std::env::remove_var("NS_TEST_LIST") };
    }

    #[test]
    fn flag_and_u64_parse() {
        unsafe { std::env::set_var("NS_TEST_FLAG", "true") };
        unsafe { std::env::set_var("NS_TEST_NUM", "42") };
        assert!(flag("NS_TEST_FLAG"));
        assert_eq!(u64_or("NS_TEST_NUM", 7), 42);
        assert_eq!(u64_or("NS_TEST_MISSING", 7), 7);
        unsafe { std::env::remove_var("NS_TEST_FLAG") };
        unsafe { std::env::remove_var("NS_TEST_NUM") };
    }
}
