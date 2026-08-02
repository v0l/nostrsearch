//! Time-based shard layout.
//!
//! The corpus is append-mostly and overwhelmingly queried with a time bound
//! (Nostr clients almost always send `since`/`until`). We therefore shard by
//! `created_at` month. Benefits:
//!
//! - **Shard pruning**: a query over `[since, until)` only opens the shards
//!   that intersect the range — most historical shards are never touched.
//! - **Independent writers**: each shard has its own `IndexWriter`, so ingest
//!   parallelism is `num_active_shards × writer_threads` with no global lock
//!   (the single global `Mutex<IndexWriter>` is what kills moar at scale).
//! - **Cold-tier offload**: old shards are immutable; their segments can be
//!   pushed to S3 and served by stateless searchers (the distributed design).
//! - **Bounded merges**: a month shard reaches a fixed size and stops growing,
//!   so merge cost is predictable.

use chrono::{Datelike, NaiveDate, TimeZone, Utc};

/// Identifier for one monthly shard, e.g. `2026-07`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShardId {
    pub year: i32,
    pub month: u32, // 1-12
}

impl ShardId {
    /// The shard that owns a given `created_at` (unix seconds).
    pub fn from_timestamp(ts: u64) -> Self {
        let dt = Utc
            .timestamp_opt(ts as i64, 0)
            .single()
            .unwrap_or_else(Utc::now);
        Self {
            year: dt.year(),
            month: dt.month(),
        }
    }

    /// Shard for a year/month.
    pub fn new(year: i32, month: u32) -> Self {
        debug_assert!((1..=12).contains(&month));
        Self { year, month }
    }

    /// First unix second of this shard (inclusive).
    pub fn start_ts(&self) -> u64 {
        self.start_date()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp() as u64
    }

    /// First unix second of the *next* shard (exclusive upper bound).
    pub fn end_ts(&self) -> u64 {
        let (y, m) = if self.month == 12 {
            (self.year + 1, 1)
        } else {
            (self.year, self.month + 1)
        };
        ShardId::new(y, m).start_ts()
    }

    fn start_date(&self) -> NaiveDate {
        NaiveDate::from_ymd_opt(self.year, self.month, 1).unwrap()
    }

    /// The next shard in sequence.
    pub fn next(&self) -> Self {
        if self.month == 12 {
            Self::new(self.year + 1, 1)
        } else {
            Self::new(self.year, self.month + 1)
        }
    }

    /// Directory / object-key name for this shard.
    pub fn name(&self) -> String {
        format!("{:04}-{:02}", self.year, self.month)
    }

    /// Parse from a `YYYY-MM` name.
    pub fn parse(name: &str) -> Option<Self> {
        let (y, m) = name.split_once('-')?;
        Some(Self::new(y.parse().ok()?, m.parse().ok()?))
    }
}

impl std::fmt::Display for ShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:04}-{:02}", self.year, self.month)
    }
}

/// Compute the set of shards intersecting `[since, until)`.
///
/// `None` bounds are open-ended. An open `since` starts at the earliest shard
/// the caller knows about (`earliest`), an open `until` ends at the current
/// month. This is the shard-pruning entry point.
pub fn shards_in_range(since: Option<u64>, until: Option<u64>, earliest: ShardId) -> Vec<ShardId> {
    let now = Utc::now();
    let current = ShardId::new(now.year(), now.month());

    let start = since.map(ShardId::from_timestamp).unwrap_or(earliest);
    // until is exclusive; subtract a second so an until exactly at a shard
    // boundary does not pull in the following (empty) shard.
    let end = until
        .map(|u| ShardId::from_timestamp(u.saturating_sub(1)))
        .unwrap_or(current);

    if start > end {
        return Vec::new();
    }

    let mut out = Vec::new();
    let mut cur = start;
    loop {
        out.push(cur);
        if cur == end {
            break;
        }
        cur = cur.next();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_roundtrip_and_bounds() {
        let s = ShardId::from_timestamp(1_700_000_000); // 2023-11-14
        assert_eq!((s.year, s.month), (2023, 11));
        assert_eq!(ShardId::parse(&s.name()), Some(s));
        assert!(s.start_ts() <= 1_700_000_000);
        assert!(s.end_ts() > 1_700_000_000);
        assert_eq!(s.end_ts(), s.next().start_ts());
    }

    #[test]
    fn range_pruning() {
        let earliest = ShardId::new(2023, 1);
        // nov 2023 -> jan 2024 = 3 shards
        let since = ShardId::new(2023, 11).start_ts();
        let until = ShardId::new(2024, 2).start_ts(); // exclusive
        let shards = shards_in_range(Some(since), Some(until), earliest);
        assert_eq!(
            shards,
            vec![
                ShardId::new(2023, 11),
                ShardId::new(2023, 12),
                ShardId::new(2024, 1)
            ]
        );
    }

    #[test]
    fn empty_range() {
        let earliest = ShardId::new(2023, 1);
        let s = shards_in_range(
            Some(ShardId::new(2024, 5).start_ts()),
            Some(ShardId::new(2024, 3).start_ts()),
            earliest,
        );
        assert!(s.is_empty());
    }
}
