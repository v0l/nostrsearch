//! Daily / weekly active users (DAU / WAU), split trusted vs untrusted.
//!
//! Ported from nostr-dashboard's `reports/active_users.rs`. Differences:
//!
//! - Unique publishers are held as [`Pubkey`] (32 bytes) instead of 64-char hex
//!   `String`s — half the memory and no allocation per event.
//! - Trust is captured **at observe time** into a parallel trusted set, so the
//!   result survives checkpoint/restore. Upstream recomputed trust from the
//!   `PreCursor` at save time and its `load` silently dropped every pubkey,
//!   making resumed runs under-count.
//!
//! Scale note: exact distinct-counting keeps one set per bucket. At full-corpus
//! scale this is the analysis to swap for a HyperLogLog sketch (the trait
//! contract stays identical — `merge` is already a set union).

use super::counter::TrustedCount;
use crate::types::Pubkey;
use crate::{Analysis, AnalysisCtx};
use nostrsearch_core::event::NostrEvent;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

const DAY: u64 = 60 * 60 * 24;
const WEEK: u64 = DAY * 7;

/// Active-user counts for one bucket.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct ActiveUsersBucket {
    /// Bucket start (unix seconds, day- or week-aligned).
    pub start: u64,
    pub users: TrustedCount,
}

/// DAU + WAU report.
///
/// Buckets are keyed maps rather than arrays so a partial update has the same
/// shape as the full snapshot and can be applied as a plain merge patch (a
/// JSON array would have to be replaced wholesale, defeating the point).
/// [`BTreeMap`] keeps them in chronological order for charting.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ActiveUsersReport {
    pub daily: BTreeMap<u64, ActiveUsersBucket>,
    pub weekly: BTreeMap<u64, ActiveUsersBucket>,
}

/// Unique publishers per day and per week.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ActiveUsers {
    daily: HashMap<u64, HashSet<Pubkey>>,
    daily_trusted: HashMap<u64, HashSet<Pubkey>>,
    weekly: HashMap<u64, HashSet<Pubkey>>,
    weekly_trusted: HashMap<u64, HashSet<Pubkey>>,
    /// Buckets whose counts changed since the last drain. Realtime-only, so
    /// skipped during (de)serialization: a restored analysis starts with
    /// nothing pending.
    #[serde(skip)]
    dirty_daily: HashSet<u64>,
    #[serde(skip)]
    dirty_weekly: HashSet<u64>,
}

fn union_into(dst: &mut HashMap<u64, HashSet<Pubkey>>, src: HashMap<u64, HashSet<Pubkey>>) {
    for (bucket, keys) in src {
        dst.entry(bucket).or_default().extend(keys);
    }
}

/// Count one bucket, splitting by its trusted subset.
fn bucket_at(
    all: &HashMap<u64, HashSet<Pubkey>>,
    trusted: &HashMap<u64, HashSet<Pubkey>>,
    start: u64,
) -> Option<ActiveUsersBucket> {
    let total = all.get(&start)?.len() as u64;
    let t = trusted.get(&start).map(HashSet::len).unwrap_or(0) as u64;
    Some(ActiveUsersBucket {
        start,
        users: TrustedCount {
            trusted: t,
            // A key counted trusted is always in `all`, so this cannot wrap.
            untrusted: total.saturating_sub(t),
        },
    })
}

fn buckets(
    all: &HashMap<u64, HashSet<Pubkey>>,
    trusted: &HashMap<u64, HashSet<Pubkey>>,
) -> BTreeMap<u64, ActiveUsersBucket> {
    all.keys()
        .filter_map(|&start| Some((start, bucket_at(all, trusted, start)?)))
        .collect()
}

impl ActiveUsers {
    /// Number of distinct publishers in the day bucket containing `ts`.
    pub fn dau(&self, ts: u64) -> usize {
        self.daily
            .get(&(ts - (ts % DAY)))
            .map(HashSet::len)
            .unwrap_or(0)
    }

    /// Number of distinct publishers in the week bucket containing `ts`.
    pub fn wau(&self, ts: u64) -> usize {
        self.weekly
            .get(&(ts - (ts % WEEK)))
            .map(HashSet::len)
            .unwrap_or(0)
    }
}

impl Analysis for ActiveUsers {
    type Output = ActiveUsersReport;

    fn name(&self) -> &'static str {
        "active_users"
    }

    fn deps(&self) -> &'static [&'static str] {
        &["follow_graph"]
    }

    fn observe(&mut self, ev: &NostrEvent, ctx: &AnalysisCtx) -> bool {
        let day = ev.created_at - (ev.created_at % DAY);
        let week = ev.created_at - (ev.created_at % WEEK);

        let fresh_day = self.daily.entry(day).or_default().insert(ctx.author);
        let fresh_week = self.weekly.entry(week).or_default().insert(ctx.author);

        let mut new_trust = false;
        if ctx.author_trusted() {
            new_trust |= self
                .daily_trusted
                .entry(day)
                .or_default()
                .insert(ctx.author);
            new_trust |= self
                .weekly_trusted
                .entry(week)
                .or_default()
                .insert(ctx.author);
        }

        // Only mark dirty when a count actually moved, so a busy relay of
        // already-seen publishers produces no dashboard traffic.
        if fresh_day || new_trust {
            self.dirty_daily.insert(day);
        }
        if fresh_week || new_trust {
            self.dirty_weekly.insert(week);
        }

        // "Folded" = this event contributed a new unique user somewhere.
        fresh_day || fresh_week
    }

    fn merge(&mut self, other: Self) {
        self.dirty_daily.extend(other.daily.keys().copied());
        self.dirty_weekly.extend(other.weekly.keys().copied());
        union_into(&mut self.daily, other.daily);
        union_into(&mut self.daily_trusted, other.daily_trusted);
        union_into(&mut self.weekly, other.weekly);
        union_into(&mut self.weekly_trusted, other.weekly_trusted);
    }

    fn snapshot(&self) -> Self::Output {
        ActiveUsersReport {
            daily: buckets(&self.daily, &self.daily_trusted),
            weekly: buckets(&self.weekly, &self.weekly_trusted),
        }
    }

    /// Emits only the day/week buckets whose counts moved. Both sides are keyed
    /// by bucket start, so this is a plain merge patch over the snapshot.
    fn drain_delta(&mut self) -> Option<serde_json::Value> {
        if self.dirty_daily.is_empty() && self.dirty_weekly.is_empty() {
            return None;
        }
        let daily: serde_json::Map<String, serde_json::Value> = self
            .dirty_daily
            .drain()
            .filter_map(|s| {
                let b = bucket_at(&self.daily, &self.daily_trusted, s)?;
                Some((s.to_string(), serde_json::to_value(b).ok()?))
            })
            .collect();
        let weekly: serde_json::Map<String, serde_json::Value> = self
            .dirty_weekly
            .drain()
            .filter_map(|s| {
                let b = bucket_at(&self.weekly, &self.weekly_trusted, s)?;
                Some((s.to_string(), serde_json::to_value(b).ok()?))
            })
            .collect();

        let mut patch = serde_json::Map::new();
        if !daily.is_empty() {
            patch.insert("daily".into(), serde_json::Value::Object(daily));
        }
        if !weekly.is_empty() {
            patch.insert("weekly".into(), serde_json::Value::Object(weekly));
        }
        Some(serde_json::Value::Object(patch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(created_at: u64, pubkey: &str) -> NostrEvent {
        NostrEvent {
            id: "a".repeat(64),
            pubkey: pubkey.into(),
            created_at,
            kind: 1,
            tags: vec![],
            content: String::new(),
            sig: "c".repeat(128),
        }
    }

    fn ctx_for<'a>(now: u64, pk: &str, world: &'a crate::World) -> AnalysisCtx<'a> {
        AnalysisCtx::new(now, Pubkey::from_hex(pk).unwrap(), Pubkey::ZERO, world)
    }

    #[test]
    fn dedupes_users_splits_trust_and_merges() {
        let trusted = "b".repeat(64);
        let other = "f".repeat(64);
        let mut world = crate::World::new();
        world.set_wot_tier(Pubkey::from_hex(&trusted).unwrap(), 3);

        let t0 = 1_700_000_000;
        let day = t0 - (t0 % DAY);

        let mut a = ActiveUsers::default();
        // same user twice in the day -> counted once
        assert!(a.observe(&ev(t0, &trusted), &ctx_for(t0, &trusted, &world)));
        assert!(!a.observe(&ev(t0 + 5, &trusted), &ctx_for(t0, &trusted, &world)));

        // a different shard/thread saw the untrusted user
        let mut b = ActiveUsers::default();
        b.observe(&ev(t0 + 10, &other), &ctx_for(t0, &other, &world));

        a.merge(b);
        let out = a.snapshot();
        let d = out.daily.get(&day).unwrap();
        assert_eq!(d.users.trusted, 1);
        assert_eq!(d.users.untrusted, 1);
        assert_eq!(a.dau(t0), 2);
        assert_eq!(a.wau(t0), 2);
    }

    #[test]
    fn delta_reports_only_changed_buckets_and_resets() {
        use crate::Analysis as _;
        let world = crate::World::new();
        let pk = "b".repeat(64);
        let t0 = 1_700_000_000;
        let day = t0 - (t0 % DAY);

        let mut a = ActiveUsers::default();
        a.observe(&ev(t0, &pk), &ctx_for(t0, &pk, &world));

        let patch = a.drain_delta().expect("first fold is a change");
        assert_eq!(patch["daily"][day.to_string()]["users"]["untrusted"], 1);

        // Draining twice with no new unique publisher yields nothing.
        assert!(a.drain_delta().is_none());
        a.observe(&ev(t0 + 1, &pk), &ctx_for(t0, &pk, &world));
        assert!(
            a.drain_delta().is_none(),
            "a repeat publisher does not move any count"
        );

        // A genuinely new publisher does.
        let other = "f".repeat(64);
        a.observe(&ev(t0 + 2, &other), &ctx_for(t0, &other, &world));
        let patch = a.drain_delta().expect("new unique user is a change");
        assert_eq!(patch["daily"][day.to_string()]["users"]["untrusted"], 2);
    }

    /// The property the streaming design depends on: applying a delta to a held
    /// snapshot yields exactly the next full snapshot.
    #[test]
    fn delta_application_matches_the_next_snapshot() {
        use crate::Analysis as _;
        let world = crate::World::new();
        let t0 = 1_700_000_000;

        let mut a = ActiveUsers::default();
        a.observe(
            &ev(t0, &"b".repeat(64)),
            &ctx_for(t0, &"b".repeat(64), &world),
        );
        let mut held = serde_json::to_value(a.snapshot()).unwrap();
        a.drain_delta();

        // more activity, including a new day
        for (i, seed) in ["c", "d", "e"].iter().enumerate() {
            let pk = seed.repeat(64);
            a.observe(&ev(t0 + DAY * i as u64, &pk), &ctx_for(t0, &pk, &world));
        }

        let patch = a.drain_delta().unwrap();
        crate::merge_patch(&mut held, &patch);
        assert_eq!(held, serde_json::to_value(a.snapshot()).unwrap());
    }

    #[test]
    fn separate_days_are_separate_buckets() {
        let world = crate::World::new();
        let pk = "b".repeat(64);
        let t0 = 1_700_000_000;
        let mut a = ActiveUsers::default();
        a.observe(&ev(t0, &pk), &ctx_for(t0, &pk, &world));
        a.observe(&ev(t0 + DAY, &pk), &ctx_for(t0, &pk, &world));

        assert_eq!(a.snapshot().daily.len(), 2);
        // ...but the same week bucket
        assert_eq!(a.snapshot().weekly.len(), 1);
    }
}
