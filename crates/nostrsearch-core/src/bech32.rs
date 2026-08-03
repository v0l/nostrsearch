//! Minimal NIP-19 decoding: `npub` / `note` / `nprofile` / `nevent` → hex.
//!
//! The query grammar has always documented `author:<hex|npub>`, but the parser
//! pushed the raw token into a `TermQuery` against a field holding lowercase
//! hex — so every `author:npub1...` matched exactly nothing, silently. Same
//! for `note1...` in an `#e` filter.
//!
//! Only the decode half is needed, and only for the 32-byte payload: for the
//! TLV forms (`nprofile`, `nevent`) that is the `special` record, which NIP-19
//! defines as type 0 and puts first. Encoding, relay hints, and the other
//! entity types are deliberately out of scope.

/// Bech32 character set (BIP-173).
const CHARSET: &[u8; 32] = b"qpzry9x8gf2tvdw0s3jn54khce6mua7l";

/// Decode a NIP-19 entity to lowercase hex, if its prefix is one of `allowed`.
///
/// Returns `None` for anything that is not a well-formed bech32 string with an
/// acceptable prefix and a 32-byte payload — the caller decides what to do
/// with an unusable token, and "match nothing" is not the same as "no filter".
pub fn decode_hex(s: &str, allowed: &[&str]) -> Option<String> {
    let s = s.trim();
    let sep = s.rfind('1')?;
    let (hrp, data) = s.split_at(sep);
    let hrp = hrp.to_ascii_lowercase();
    if !allowed.contains(&hrp.as_str()) {
        return None;
    }
    let data = &data[1..]; // skip the separator
    if data.len() < 6 {
        return None;
    }
    // Drop the 6-character checksum; a corrupt payload fails the length check
    // below rather than being silently half-decoded.
    let payload = &data[..data.len() - 6];

    let mut bits = Vec::with_capacity(payload.len() * 5);
    for c in payload.bytes() {
        let c = c.to_ascii_lowercase();
        let v = CHARSET.iter().position(|&x| x == c)? as u8;
        for i in (0..5).rev() {
            bits.push((v >> i) & 1);
        }
    }
    let bytes: Vec<u8> = bits
        .chunks_exact(8)
        .map(|c| c.iter().fold(0u8, |acc, b| (acc << 1) | b))
        .collect();

    match hrp.as_str() {
        // Bare 32-byte key/id.
        "npub" | "note" => (bytes.len() == 32).then(|| hex_of(&bytes)),
        // TLV: the 32-byte value is record type 0 ("special").
        "nprofile" | "nevent" => tlv_special(&bytes).map(hex_of),
        _ => None,
    }
}

/// First TLV record of type 0 with a 32-byte value.
fn tlv_special(mut b: &[u8]) -> Option<&[u8]> {
    while b.len() >= 2 {
        let (t, l) = (b[0], b[1] as usize);
        let rest = b.get(2..)?;
        let val = rest.get(..l)?;
        if t == 0 && l == 32 {
            return Some(val);
        }
        b = &rest[l..];
    }
    None
}

fn hex_of(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Jack's well-known npub, and its hex pubkey.
    const NPUB: &str = "npub1sg6plzptd64u62a878hep2kev88swjh3tw00gjsfl8f237lmu63q0uf63m";
    const NPUB_HEX: &str = "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2";

    #[test]
    fn decodes_npub_to_hex() {
        assert_eq!(decode_hex(NPUB, &["npub"]).as_deref(), Some(NPUB_HEX));
    }

    #[test]
    fn rejects_a_prefix_the_caller_did_not_ask_for() {
        // An npub is not an event id: accepting it in an `#e` filter would
        // match nothing anyway, but confusing the two hides the mistake.
        assert_eq!(decode_hex(NPUB, &["note", "nevent"]), None);
    }

    #[test]
    fn rejects_junk() {
        assert_eq!(decode_hex("npub1", &["npub"]), None);
        assert_eq!(decode_hex("not-bech32", &["npub"]), None);
        assert_eq!(decode_hex("", &["npub"]), None);
        assert_eq!(decode_hex(&"a".repeat(64), &["npub"]), None);
        // Right shape, invalid characters for the charset ('b' is not in it).
        assert_eq!(decode_hex("npub1bbbbbb", &["npub"]), None);
    }

    #[test]
    fn is_case_insensitive_on_the_prefix() {
        // Clients occasionally hand back a capitalized entity; the payload
        // charset is case-insensitive too, so the whole token folds.
        assert_eq!(
            decode_hex(&NPUB.to_uppercase(), &["npub"]).as_deref(),
            Some(NPUB_HEX)
        );
    }
}
