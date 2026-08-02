//! Shared trusted/untrusted split counter.
//!
//! Ports nostr-dashboard's `CounterTrusted`. Upstream serialized this as a
//! `"trusted/not_trusted"` *string*; we keep a plain struct so it round-trips
//! cleanly under both JSON (dashboard) and bincode (checkpoints) and needs no
//! custom parser.

use serde::{Deserialize, Serialize};

/// A count split by whether the publisher is inside the web of trust.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustedCount {
    pub trusted: u64,
    pub untrusted: u64,
}

impl TrustedCount {
    /// Add `amount` to the trusted or untrusted side.
    pub fn incr(&mut self, is_trusted: bool, amount: u64) {
        if is_trusted {
            self.trusted += amount;
        } else {
            self.untrusted += amount;
        }
    }

    pub fn total(&self) -> u64 {
        self.trusted + self.untrusted
    }

    /// Map-reduce merge (associative + commutative).
    pub fn merge(&mut self, other: Self) {
        self.trusted += other.trusted;
        self.untrusted += other.untrusted;
    }
}
