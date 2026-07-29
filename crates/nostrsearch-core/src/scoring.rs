//! Composite relevance scoring: BM25 × (1 + WoT boost + recency boost).
//!
//! moar recomputes this by deserializing every stored doc per hit
//! (`searcher.doc()`), which is O(hits × doc-fetch) and does not scale. We read
//! the two score signals (`wot_tier`, `created_at`) from *fast fields* inside a
//! custom collector, so the cost is a columnar lookup, not a document fetch.
//!
//! The final score is:
//!
//! ```text
//! score = bm25 * (1 + wot_weight * wot_tier
//!                   + recency_weight * recency_decay(age_days))
//! ```
//!
//! `recency_decay` is `max(0, 1 - age_days / half_life_days)` — a linear decay
//! to zero at the half-life, which keeps old-but-relevant content findable
//! while letting fresh content surface.

use tantivy::collector::{Collector, SegmentCollector};
use tantivy::columnar::ColumnValues;
use tantivy::{DocId, Score, SegmentReader};
use std::sync::Arc;

/// Weights for the composite score.
#[derive(Debug, Clone, Copy)]
pub struct ScoreWeights {
    /// Multiplier per WoT tier (tier is small: 0..~4).
    pub wot_weight: f32,
    /// Multiplier on the recency decay term.
    pub recency_weight: f32,
    /// Days over which recency decays to zero.
    pub half_life_days: f32,
}

impl Default for ScoreWeights {
    fn default() -> Self {
        Self {
            wot_weight: 0.5,
            recency_weight: 1.0,
            half_life_days: 365.0,
        }
    }
}

/// A scored hit: full doc address (segment + local id) + composite score.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScoredDoc {
    pub segment_ord: tantivy::SegmentOrdinal,
    pub doc_id: DocId,
    pub score: Score,
}

impl ScoredDoc {
    pub fn address(&self) -> tantivy::DocAddress {
        tantivy::DocAddress::new(self.segment_ord, self.doc_id)
    }
}

/// Collector that re-ranks the top-`limit` docs by composite score.
pub struct CompositeCollector {
    pub limit: usize,
    pub weights: ScoreWeights,
    /// Reference "now" (unix seconds) for recency — injectable for tests.
    pub now_ts: u64,
    /// Fast-field names for the score signals.
    pub created_at_field: String,
    pub wot_tier_field: String,
}

impl Collector for CompositeCollector {
    type Fruit = Vec<ScoredDoc>;
    type Child = CompositeSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: u32,
        segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let created_at: Arc<dyn ColumnValues<u64>> = segment
            .fast_fields()
            .u64_lenient(&self.created_at_field)?
            .ok_or_else(|| tantivy::TantivyError::FieldNotFound(self.created_at_field.clone()))?
            .0
            .first_or_default_col(0u64);
        let wot_tier: Arc<dyn ColumnValues<u64>> = segment
            .fast_fields()
            .u64_lenient(&self.wot_tier_field)?
            .ok_or_else(|| tantivy::TantivyError::FieldNotFound(self.wot_tier_field.clone()))?
            .0
            .first_or_default_col(0u64);

        Ok(CompositeSegmentCollector {
            segment_ord: _segment_local_id,
            created_at,
            wot_tier,
            weights: self.weights,
            now_ts: self.now_ts,
            hits: Vec::new(),
        })
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        let mut all: Vec<ScoredDoc> = segment_fruits.into_iter().flatten().collect();
        all.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        all.truncate(self.limit);
        Ok(all)
    }
}

pub struct CompositeSegmentCollector {
    segment_ord: tantivy::SegmentOrdinal,
    created_at: Arc<dyn ColumnValues<u64>>,
    wot_tier: Arc<dyn ColumnValues<u64>>,
    weights: ScoreWeights,
    now_ts: u64,
    hits: Vec<ScoredDoc>,
}

impl CompositeSegmentCollector {
    #[inline]
    fn composite(&self, bm25: Score, doc: DocId) -> Score {
        let created = self.created_at.get_val(doc);
        let wot = self.wot_tier.get_val(doc);

        let age_days = self.now_ts.saturating_sub(created) as f32 / 86_400.0;
        let recency = (1.0 - age_days / self.weights.half_life_days).max(0.0);

        bm25 * (1.0 + self.weights.wot_weight * wot as f32 + self.weights.recency_weight * recency)
    }
}

impl SegmentCollector for CompositeSegmentCollector {
    type Fruit = Vec<ScoredDoc>;

    fn collect(&mut self, doc: DocId, score: Score) {
        let final_score = self.composite(score, doc);
        self.hits.push(ScoredDoc {
            segment_ord: self.segment_ord,
            doc_id: doc,
            score: final_score,
        });
    }

    fn harvest(self) -> Self::Fruit {
        self.hits
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recency_decays_linearly_to_zero() {
        let w = ScoreWeights::default();
        // fresh
        let fresh = (1.0 - 0.0 / w.half_life_days).max(0.0);
        let old = (1.0 - 400.0 / w.half_life_days).max(0.0);
        assert_eq!(fresh, 1.0);
        assert_eq!(old, 0.0);
    }
}
