//! Built-in analyses. Add new collectors here and register them on the
//! [`Registry`](crate::Registry) — the batch and live runners pick them up
//! automatically.

mod follow_graph;
mod kind_breakdown;
mod pagerank;
mod trending_hashtags;

pub use follow_graph::FollowGraph;
pub use kind_breakdown::KindBreakdown;
pub use pagerank::Pagerank;
pub use trending_hashtags::TrendingHashtags;
