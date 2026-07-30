//! Compact 32-byte key type for pubkeys / event ids.
//!
//! Nostr pubkeys and event ids are 32 bytes. Storing them as 64-char hex
//! `String`s (as the wire format does) roughly triples memory and forces a heap
//! allocation per key — untenable at 1B+ events. [`Hash32`] is a `Copy`,
//! `Hash`-able `[u8; 32]` newtype.
//!
//! Serde is **format-aware**: human-readable formats (JSON, for the dashboard)
//! emit hex strings; binary formats (bincode, for on-disk checkpoints) emit the
//! raw 32 bytes with no length prefix.

use serde::de::{self, Deserialize, Deserializer};
use serde::ser::{Serialize, Serializer};

/// A 32-byte nostr key (pubkey or event id).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Hash32(pub [u8; 32]);

/// Alias for intent at call sites.
pub type Pubkey = Hash32;
/// Alias for intent at call sites.
pub type EventId = Hash32;

impl Hash32 {
    pub const ZERO: Hash32 = Hash32([0u8; 32]);

    /// Parse from 64-char lowercase/uppercase hex. Returns `None` on bad input.
    #[inline]
    pub fn from_hex(s: &str) -> Option<Self> {
        if s.len() != 64 {
            return None;
        }
        let mut out = [0u8; 32];
        hex::decode_to_slice(s, &mut out).ok()?;
        Some(Hash32(out))
    }

    #[inline]
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Debug for Hash32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl std::fmt::Display for Hash32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl Serialize for Hash32 {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        if s.is_human_readable() {
            s.serialize_str(&self.to_hex())
        } else {
            // serde's [u8; 32] impl writes exactly 32 bytes under bincode
            // (fixed tuple, no length prefix).
            self.0.serialize(s)
        }
    }
}

impl<'de> Deserialize<'de> for Hash32 {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        if d.is_human_readable() {
            let s = String::deserialize(d)?;
            Hash32::from_hex(&s).ok_or_else(|| de::Error::custom("invalid 32-byte hex"))
        } else {
            let bytes = <[u8; 32]>::deserialize(d)?;
            Ok(Hash32(bytes))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_roundtrip() {
        let h = Hash32::from_hex(&"ab".repeat(32)).unwrap();
        assert_eq!(h.to_hex(), "ab".repeat(32));
        assert!(Hash32::from_hex("xyz").is_none());
    }

    #[test]
    fn bincode_is_32_bytes_json_is_hex() {
        let h = Hash32::from_hex(&"cd".repeat(32)).unwrap();
        let bin = bincode::serialize(&h).unwrap();
        assert_eq!(bin.len(), 32, "bincode must be exactly 32 bytes");
        let json = serde_json::to_string(&h).unwrap();
        assert_eq!(json, format!("\"{}\"", "cd".repeat(32)));
        assert_eq!(bincode::deserialize::<Hash32>(&bin).unwrap(), h);
        assert_eq!(serde_json::from_str::<Hash32>(&json).unwrap(), h);
    }
}
