//! Trending hashtags via a time-decayed score — an example of a "trending
//! algo" the framework is meant to host. Each `t` tag gains weight that decays
//! with the event's age relative to `ctx.now`, optionally boosted by author
//! WoT tier so a handful of bots can't dominate a trend.

use crate::{Analysis, AnalysisCtx};
use nostrsearch_core::event::NostrEvent;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Half-life (seconds) of a hashtag mention's contribution to the trend score.
const HALF_LIFE_SECS: f64 = 6.0 * 3600.0;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendingTag {
    pub tag: String,
    pub score: f64,
    pub mentions: u64,
}

/// Time-decayed trending hashtag scores. Kind-pre-filtered to text kinds.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TrendingHashtags {
    scores: HashMap<String, f64>,
    mentions: HashMap<String, u64>,
}

const TEXT_KINDS: &[u16] = &[1, 1111, 9802, 30023];

impl Analysis for TrendingHashtags {
    type Output = Vec<TrendingTag>;

    fn name(&self) -> &'static str {
        "trending_hashtags"
    }

    fn kinds(&self) -> Option<&[u16]> {
        Some(TEXT_KINDS)
    }

    fn observe(&mut self, ev: &NostrEvent, ctx: &AnalysisCtx) -> bool {
        // Age-based decay: weight = 0.5 ^ (age / half_life). Future-dated events
        // are clamped to weight 1.
        let age = ctx.now.saturating_sub(ev.created_at) as f64;
        let decay = 0.5_f64.powf(age / HALF_LIFE_SECS);
        // WoT boost: tier 0 → ×1, each tier adds 50%.
        let boost = 1.0 + 0.5 * ctx.author_tier() as f64;
        let weight = decay * boost;

        let mut folded = false;
        for tag in ev.tag_values("t") {
            let key = tag.to_lowercase();
            *self.scores.entry(key.clone()).or_default() += weight;
            *self.mentions.entry(key).or_default() += 1;
            folded = true;
        }
        folded
    }

    fn merge(&mut self, other: Self) {
        for (k, v) in other.scores {
            *self.scores.entry(k).or_default() += v;
        }
        for (k, v) in other.mentions {
            *self.mentions.entry(k).or_default() += v;
        }
    }

    fn snapshot(&self) -> Self::Output {
        let mut out: Vec<TrendingTag> = self
            .scores
            .iter()
            .map(|(tag, &score)| TrendingTag {
                tag: tag.clone(),
                score,
                mentions: self.mentions.get(tag).copied().unwrap_or(0),
            })
            .collect();
        out.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        out.truncate(100);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn recent_tags_outrank_old_and_only_text_kinds() {
        let now = 1_700_000_000;
        let ctx = AnalysisCtx::bare(now);
        let mut t = TrendingHashtags::default();

        // recent #nostr, old #bitcoin (2 half-lives ago)
        t.observe(&ev(now, &["nostr"]), &ctx);
        t.observe(&ev(now - (12 * 3600), &["bitcoin"]), &ctx);

        let out = t.snapshot();
        assert_eq!(out[0].tag, "nostr");
        assert!(out[0].score > out[1].score);

        // a non-text kind is filtered out
        let mut e = ev(now, &["reaction"]);
        e.kind = 7;
        assert!(!t.wants(&e));
    }
}
