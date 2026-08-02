//! Built-in analyses. Add new collectors here and register them on the
//! [`Registry`](crate::Registry) — the batch and live runners pick them up
//! automatically.

mod active_users;
mod activity;
mod clients;
mod counter;
mod follow_graph;
mod kind_breakdown;
mod pagerank;
mod relays;
mod trending_hashtags;

pub use active_users::{ActiveUsers, ActiveUsersBucket, ActiveUsersReport};
pub use activity::{Activity, DailyActivity, parse_invoice_msats};
pub use clients::{ClientStats, Clients, NO_CLIENT};
pub use counter::TrustedCount;
pub use follow_graph::FollowGraph;
pub use kind_breakdown::KindBreakdown;
pub use pagerank::Pagerank;
pub use relays::{MAX_RELAYS, RelayStats, Relays};
pub use trending_hashtags::TrendingHashtags;
