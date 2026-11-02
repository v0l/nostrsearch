//! Trending hashtags: what is being written about now.
//!
//! Mentions are kept in hourly buckets over a fixed window and decayed to the
//! moment the snapshot is taken, so a tag that was big last week fades out on
//! its own. Each author counts at most once per tag per hour, so one account
//! repeating a tag cannot make it trend, and authors inside the web of trust
//! weigh more.

use crate::{Analysis, AnalysisCtx};
use nostrsearch_core::event::NostrEvent;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

const HOUR: u64 = 3600;
const WINDOW_HOURS: u64 = 7 * 24;
const HALF_LIFE_HOURS: f64 = 24.0;
const MAX_TAG_LEN: usize = 64;
const TOP: usize = 100;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendingTag {
    pub tag: String,
    pub score: f64,
    pub mentions: u64,
    pub authors_24h: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct HourBucket {
    weight: HashMap<String, f64>,
    mentions: HashMap<String, u64>,
    #[serde(skip)]
    seen: HashSet<(String, crate::types::Pubkey)>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TrendingHashtags {
    hours: BTreeMap<u64, HourBucket>,
    #[serde(skip)]
    now: u64,
}

const TEXT_KINDS: &[u16] = &[1, 20, 21, 22, 1111, 9802, 30023];

fn normalize(tag: &str) -> Option<String> {
    let t = tag.trim().trim_start_matches('#').to_lowercase();
    (!t.is_empty() && t.len() <= MAX_TAG_LEN).then_some(t)
}

impl TrendingHashtags {
    fn prune(&mut self, now: u64) {
        let floor = (now / HOUR).saturating_sub(WINDOW_HOURS) * HOUR;
        self.hours = self.hours.split_off(&floor);
    }

    fn ranked(&self, now: u64) -> Vec<TrendingTag> {
        let now_hour = now / HOUR;
        let mut score: HashMap<&str, f64> = HashMap::new();
        let mut mentions: HashMap<&str, u64> = HashMap::new();
        let mut recent: HashMap<&str, u64> = HashMap::new();
        for (&start, b) in &self.hours {
            let age = now_hour.saturating_sub(start / HOUR) as f64;
            let decay = 0.5_f64.powf(age / HALF_LIFE_HOURS);
            for (tag, w) in &b.weight {
                *score.entry(tag).or_default() += w * decay;
            }
            for (tag, n) in &b.mentions {
                *mentions.entry(tag).or_default() += n;
                if age < 24.0 {
                    *recent.entry(tag).or_default() += n;
                }
            }
        }
        let mut out: Vec<TrendingTag> = score
            .into_iter()
            .map(|(tag, score)| TrendingTag {
                tag: tag.to_string(),
                score,
                mentions: mentions.get(tag).copied().unwrap_or(0),
                authors_24h: recent.get(tag).copied().unwrap_or(0),
            })
            .collect();
        out.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.tag.cmp(&b.tag))
        });
        out.truncate(TOP);
        out
    }
}

impl Analysis for TrendingHashtags {
    type Output = Vec<TrendingTag>;

    fn name(&self) -> &'static str {
        "trending_hashtags"
    }

    fn epoch(&self) -> u32 {
        1
    }

    fn deps(&self) -> &'static [&'static str] {
        &["follow_graph"]
    }

    fn kinds(&self) -> Option<&[u16]> {
        Some(TEXT_KINDS)
    }

    fn refresh_interval(&self) -> Option<std::time::Duration> {
        Some(std::time::Duration::from_secs(HOUR))
    }

    fn refresh(&mut self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(self.now);
        self.prune(now);
    }

    fn observe(&mut self, ev: &NostrEvent, ctx: &AnalysisCtx) -> bool {
        self.now = self.now.max(ctx.now);
        let floor = (ctx.now / HOUR).saturating_sub(WINDOW_HOURS) * HOUR;
        if ev.created_at < floor || ev.created_at > ctx.now + HOUR {
            return false;
        }
        let hour = ev.created_at - (ev.created_at % HOUR);
        let weight = 1.0 + 0.5 * ctx.author_tier() as f64;
        let bucket = self.hours.entry(hour).or_default();
        let mut folded = false;
        let tags: HashSet<String> = ev.tag_values("t").filter_map(normalize).collect();
        for tag in tags {
            if !bucket.seen.insert((tag.clone(), ctx.author)) {
                continue;
            }
            *bucket.weight.entry(tag.clone()).or_default() += weight;
            *bucket.mentions.entry(tag).or_default() += 1;
            folded = true;
        }
        if self.hours.len() as u64 > WINDOW_HOURS + 2 {
            self.prune(ctx.now);
        }
        folded
    }

    fn merge(&mut self, other: Self) {
        for (start, b) in other.hours {
            let slot = self.hours.entry(start).or_default();
            for (k, v) in b.weight {
                *slot.weight.entry(k).or_default() += v;
            }
            for (k, v) in b.mentions {
                *slot.mentions.entry(k).or_default() += v;
            }
            slot.seen.extend(b.seen);
        }
        self.now = self.now.max(other.now);
    }

    fn snapshot(&self) -> Self::Output {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(self.now);
        self.ranked(now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Hash32, Pubkey};

    fn ev(created_at: u64, tags: &[&str]) -> NostrEvent {
        NostrEvent {
            id: "a".repeat(64),
            pubkey: "b".repeat(64),
            created_at,
            kind: 1,
            tags: tags
                .iter()
                .map(|t| vec!["t".to_string(), t.to_string()])
                .collect(),
            content: String::new(),
            sig: "c".repeat(128),
        }
    }

    fn ctx(world: &crate::World, now: u64, seed: u8) -> AnalysisCtx<'_> {
        AnalysisCtx::new(now, Hash32([seed; 32]), Pubkey::ZERO, world)
    }

    #[test]
    fn many_authors_beat_one_author_repeating_a_tag() {
        let now = 1_793_600_000;
        let world = crate::World::new();
        let mut t = TrendingHashtags::default();
        for _ in 0..500 {
            t.observe(&ev(now - 60, &["spam"]), &ctx(&world, now, 1));
        }
        for seed in 10..20 {
            t.observe(&ev(now - 60, &["nostr"]), &ctx(&world, now, seed));
        }
        let out = t.ranked(now);
        assert_eq!(out[0].tag, "nostr");
        assert_eq!(out.iter().find(|x| x.tag == "spam").unwrap().mentions, 1);
    }

    #[test]
    fn last_weeks_big_tag_fades_below_todays() {
        let now = 1_793_600_000;
        let world = crate::World::new();
        let mut t = TrendingHashtags::default();
        for seed in 0..50 {
            t.observe(&ev(now - 6 * 24 * HOUR, &["olympics"]), &ctx(&world, now, seed));
        }
        for seed in 100..110 {
            t.observe(&ev(now - HOUR, &["election"]), &ctx(&world, now, seed));
        }
        let out = t.ranked(now);
        assert_eq!(out[0].tag, "election", "{out:?}");
    }

    #[test]
    fn events_outside_the_window_are_ignored_and_pruned() {
        let now = 1_793_600_000;
        let world = crate::World::new();
        let mut t = TrendingHashtags::default();
        assert!(!t.observe(&ev(now - 30 * 24 * HOUR, &["old"]), &ctx(&world, now, 1)));
        assert!(t.hours.is_empty());
        for h in 0..(WINDOW_HOURS + 10) {
            t.observe(&ev(now - h * HOUR, &["x"]), &ctx(&world, now, 1));
        }
        assert!(t.hours.len() as u64 <= WINDOW_HOURS + 3);
    }

    #[test]
    fn tags_are_normalized_and_counted_once_per_event() {
        let now = 1_793_600_000;
        let world = crate::World::new();
        let mut t = TrendingHashtags::default();
        t.observe(&ev(now, &["Bitcoin", "#bitcoin", "BITCOIN"]), &ctx(&world, now, 1));
        let out = t.ranked(now);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].tag, "bitcoin");
        assert_eq!(out[0].mentions, 1);
    }
}
