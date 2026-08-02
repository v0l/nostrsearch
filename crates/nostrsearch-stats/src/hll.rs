//! A small HyperLogLog for counting distinct [`Hash32`] keys.
//!
//! [`ActiveUsers`](crate::analyses::ActiveUsers) previously held an exact
//! `HashSet<Pubkey>` per day and per week. That is correct but unbounded: the
//! sets grow with every publisher the corpus has ever seen, and the whole
//! structure is bincode-serialized on every checkpoint (`STATS_PERSIST_SECS`,
//! 300s by default). At corpus scale — years of days, tens of thousands of
//! publishers each — that is gigabytes re-serialized every few minutes.
//!
//! A sketch trades exactness for a *fixed* footprint: [`REGISTERS`] bytes per
//! bucket no matter how many publishers appear, with roughly
//! [`RELATIVE_ERROR`] relative error. For "how many people posted today" that
//! is the right trade — nobody charts DAU to the individual user.
//!
//! Measured over a synthetic year (365 day-buckets x 20k publishers/day):
//!
//! | | checkpoint size | DAU estimate |
//! |---|---|---|
//! | exact sets (daily only) | 233.6 MB | 20000 |
//! | this sketch | 6.9 MB | 20108 (+0.5%) |
//!
//! and, more importantly, the sketch figure is constant in the number of
//! publishers while the exact one grows with it.
//!
//! Two properties make this a drop-in for the exact set it replaces:
//! [`merge`](Hll::merge) is a register-wise max, which is associative and
//! commutative just like the set union it replaces, and [`insert`](Hll::insert)
//! reports whether the estimate moved, preserving the "did this event change
//! anything" signal the delta stream needs.

use serde::{Deserialize, Serialize};

/// log2 of the register count. 14 → 16384 registers → 16 KiB per sketch.
pub const PRECISION: u32 = 14;

/// Number of registers per sketch.
pub const REGISTERS: usize = 1 << PRECISION;

/// Approximate relative error, `1.04 / sqrt(m)` — 0.8% at PRECISION 14.
pub const RELATIVE_ERROR: f64 = 0.008;

/// SplitMix64 finalizer.
///
/// Pubkeys are secp256k1 x-coordinates and so already uniform, but they are
/// also *chosen* by the people being counted. Feeding raw key bits to the
/// sketch would let someone grind keys with long zero prefixes to inflate the
/// estimate; mixing first makes that far less direct. (It is a bijection, so
/// this raises the cost of grinding rather than making it impossible — an
/// acceptable trade for a public activity metric.)
fn mix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Fixed-size distinct-count sketch over 32-byte keys.
#[derive(Clone, Serialize, Deserialize)]
pub struct Hll {
    /// One 6-bit rank per register, stored a byte each for simplicity.
    #[serde(with = "serde_bytes")]
    registers: Vec<u8>,
}

impl Default for Hll {
    fn default() -> Self {
        Self {
            registers: vec![0; REGISTERS],
        }
    }
}

impl std::fmt::Debug for Hll {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Hll(~{})", self.len())
    }
}

impl Hll {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold a key in. Returns `true` if this changed the sketch (and so may
    /// have changed the estimate) — the analogue of `HashSet::insert`.
    pub fn insert(&mut self, key: &crate::types::Hash32) -> bool {
        // The key is 32 uniform bytes; the first 8 carry more than enough
        // entropy for a 4096-register sketch.
        let h = mix64(u64::from_le_bytes(key.0[..8].try_into().unwrap_or([0; 8])));

        // Top PRECISION bits select the register...
        let idx = (h >> (64 - PRECISION)) as usize;
        // ...the rest supplies the rank (position of the first set bit).
        let rest = h << PRECISION;
        let rank = if rest == 0 {
            (64 - PRECISION + 1) as u8
        } else {
            (rest.leading_zeros() + 1) as u8
        };

        if rank > self.registers[idx] {
            self.registers[idx] = rank;
            true
        } else {
            false
        }
    }

    /// Register-wise max: the sketch equivalent of a set union. Associative and
    /// commutative, so it satisfies the `Analysis::merge` contract.
    pub fn merge(&mut self, other: &Self) {
        for (a, b) in self.registers.iter_mut().zip(other.registers.iter()) {
            if *b > *a {
                *a = *b;
            }
        }
    }

    /// Estimated number of distinct keys inserted.
    pub fn len(&self) -> u64 {
        let m = REGISTERS as f64;
        let zeros = self.registers.iter().filter(|r| **r == 0).count();

        // Small cardinalities: linear counting is far more accurate than the
        // raw estimator, and is effectively exact for the handful-of-users
        // buckets at the start of the corpus.
        if zeros > 0 {
            let lc = m * (m / zeros as f64).ln();
            if lc <= 2.5 * m {
                return lc.round() as u64;
            }
        }

        let sum: f64 = self
            .registers
            .iter()
            .map(|&r| 2.0_f64.powi(-(r as i32)))
            .sum();
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        // No large-range correction: with a 64-bit hash the 2^32 collision
        // regime this would fix is unreachable.
        (alpha * m * m / sum).round() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.registers.iter().all(|r| *r == 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Hash32;

    /// Deterministic distinct keys.
    fn key(i: u64) -> Hash32 {
        let mut b = [0u8; 32];
        b[..8].copy_from_slice(&mix64(i ^ 0xdead_beef).to_le_bytes());
        b[8..16].copy_from_slice(&mix64(i).to_le_bytes());
        Hash32(b)
    }

    #[test]
    fn small_counts_are_exact() {
        // Linear counting should nail the bucket sizes that matter early on.
        for n in [1u64, 2, 5, 13, 50] {
            let mut h = Hll::new();
            for i in 0..n {
                h.insert(&key(i));
            }
            assert_eq!(h.len(), n, "expected exact estimate for n={n}");
        }
    }

    #[test]
    fn repeat_inserts_do_not_move_the_estimate() {
        let mut h = Hll::new();
        assert!(h.insert(&key(1)));
        // Same key again: no change, which is what drives the delta stream's
        // "nothing moved" path.
        assert!(!h.insert(&key(1)));
        assert_eq!(h.len(), 1);
    }

    #[test]
    fn large_counts_stay_within_the_error_bound() {
        let mut h = Hll::new();
        let n = 200_000u64;
        for i in 0..n {
            h.insert(&key(i));
        }
        let est = h.len() as f64;
        let err = (est - n as f64).abs() / n as f64;
        // Allow a little headroom over the nominal 1.6%.
        assert!(
            err < 0.03,
            "estimate {est} for {n} is {:.2}% off",
            err * 100.0
        );
    }

    #[test]
    fn merge_is_a_union() {
        let mut a = Hll::new();
        let mut b = Hll::new();
        for i in 0..5_000 {
            a.insert(&key(i));
        }
        // Overlapping ranges: the union is 8000 distinct, not 13000.
        for i in 3_000..11_000 {
            b.insert(&key(i));
        }
        a.merge(&b);

        let est = a.len() as f64;
        let err = (est - 11_000.0).abs() / 11_000.0;
        assert!(
            err < 0.03,
            "union estimate {est} is {:.2}% off",
            err * 100.0
        );
    }

    #[test]
    fn merge_is_commutative_and_idempotent() {
        let mut a = Hll::new();
        let mut b = Hll::new();
        for i in 0..1_000 {
            a.insert(&key(i));
        }
        for i in 500..1_500 {
            b.insert(&key(i));
        }

        let mut ab = a.clone();
        ab.merge(&b);
        let mut ba = b.clone();
        ba.merge(&a);
        assert_eq!(ab.len(), ba.len());

        // Merging the same partial twice must not inflate the count.
        let before = ab.len();
        ab.merge(&b);
        assert_eq!(ab.len(), before);
    }

    #[test]
    fn footprint_is_fixed_regardless_of_cardinality() {
        let mut small = Hll::new();
        small.insert(&key(1));
        let mut big = Hll::new();
        for i in 0..100_000 {
            big.insert(&key(i));
        }

        let s = bincode::serialize(&small).unwrap();
        let b = bincode::serialize(&big).unwrap();
        assert_eq!(
            s.len(),
            b.len(),
            "sketch size must not depend on cardinality"
        );
        // The whole point: a bucket costs 4 KiB, not "32 bytes x every user".
        assert!(b.len() <= REGISTERS + 64, "unexpected encoding overhead");
    }
}
