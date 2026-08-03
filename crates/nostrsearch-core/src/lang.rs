//! Language detection for the `lang` field.
//!
//! The `lang` field existed in the schema, was exposed as `lang:` in the query
//! grammar and as `lang=` over HTTP, and was written as `None` on every single
//! document — so it matched zero events while looking like a working filter.
//!
//! Detection runs on the ingest hot path (900M events), so the policy is
//! deliberately conservative:
//!
//! - Only text kinds are offered to it at all (the caller's job).
//! - Text shorter than [`MIN_CHARS`] is skipped: trigram detection on "gm" is
//!   a coin flip, and a wrong label is worse than no label because it makes
//!   `lang:en` *exclude* real English notes.
//! - Only a confident verdict is recorded; `whatlang`'s own reliability check
//!   plus a confidence floor.
//! - Only the first [`SAMPLE_BYTES`] are inspected. Detection quality plateaus
//!   quickly, and long-form posts would otherwise dominate the cost.
//!
//! The result is an ISO 639-1 code (`en`, `ja`, `de`), which is what a client
//! sending `lang=en` expects — not whatlang's own three-letter codes.

/// Minimum characters before detection is attempted.
pub const MIN_CHARS: usize = 24;

/// Bytes of content inspected.
pub const SAMPLE_BYTES: usize = 512;

/// Minimum confidence to record a verdict.
pub const MIN_CONFIDENCE: f64 = 0.85;

/// Detect the language of `content` as an ISO 639-1 code.
///
/// `None` means "not confident", and callers must treat it as unknown rather
/// than as a default language.
pub fn detect(content: &str) -> Option<&'static str> {
    if content.chars().take(MIN_CHARS).count() < MIN_CHARS {
        return None;
    }
    // Truncate on a char boundary; content is arbitrary UTF-8.
    let sample = match content.char_indices().find(|(i, _)| *i > SAMPLE_BYTES) {
        Some((i, _)) => &content[..i],
        None => content,
    };

    let info = whatlang::detect(sample)?;
    if !info.is_reliable() || info.confidence() < MIN_CONFIDENCE {
        return None;
    }
    iso_639_1(info.lang())
}

/// Map whatlang's ISO 639-3 enum to the two-letter code clients use.
///
/// Languages with no 639-1 code (Esperanto has one, Cebuano does not) return
/// `None`: emitting a three-letter code into a field documented as two-letter
/// would just move the mismatch somewhere harder to see.
fn iso_639_1(lang: whatlang::Lang) -> Option<&'static str> {
    use whatlang::Lang::*;
    Some(match lang {
        Eng => "en",
        Rus => "ru",
        Cmn => "zh",
        Spa => "es",
        Por => "pt",
        Ita => "it",
        Ben => "bn",
        Fra => "fr",
        Deu => "de",
        Ukr => "uk",
        Kat => "ka",
        Ara => "ar",
        Hin => "hi",
        Jpn => "ja",
        Heb => "he",
        Yid => "yi",
        Pol => "pl",
        Amh => "am",
        Jav => "jv",
        Kor => "ko",
        Nob => "nb",
        Dan => "da",
        Swe => "sv",
        Fin => "fi",
        Tur => "tr",
        Nld => "nl",
        Hun => "hu",
        Ces => "cs",
        Ell => "el",
        Bul => "bg",
        Bel => "be",
        Mar => "mr",
        Kan => "kn",
        Ron => "ro",
        Slv => "sl",
        Hrv => "hr",
        Srp => "sr",
        Mkd => "mk",
        Lit => "lt",
        Lav => "lv",
        Est => "et",
        Tam => "ta",
        Vie => "vi",
        Urd => "ur",
        Tha => "th",
        Guj => "gu",
        Uzb => "uz",
        Pan => "pa",
        Aze => "az",
        Ind => "id",
        Tel => "te",
        Pes => "fa",
        Mal => "ml",
        Ori => "or",
        Mya => "my",
        Nep => "ne",
        Sin => "si",
        Khm => "km",
        Tuk => "tk",
        Aka => "ak",
        Zul => "zu",
        Sna => "sn",
        Afr => "af",
        Lat => "la",
        Slk => "sk",
        Cat => "ca",
        Tgl => "tl",
        Hye => "hy",
        Epo => "eo",
        // No ISO 639-1 code exists (Cebuano, Hausa variants, ...).
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_common_languages_as_two_letter_codes() {
        assert_eq!(
            detect("just bought some sats on the exchange, feeling good about the price today"),
            Some("en")
        );
        assert_eq!(
            detect("Ich habe heute einen langen Spaziergang im Park gemacht und Kaffee getrunken."),
            Some("de")
        );
        assert_eq!(
            detect("Bonjour à tous, je suis très heureux de vous annoncer une nouvelle important."),
            Some("fr")
        );
        // Two-letter codes, not whatlang's three-letter enum: `Jpn` -> `ja`.
        assert_eq!(
            detect("今日はビットコインの会議です。とても楽しかったです。また行きたいです。"),
            Some("ja")
        );
    }

    #[test]
    fn short_text_is_left_unlabelled() {
        // "gm" is not evidence of anything, and a wrong label would make
        // `lang:en` exclude real English notes.
        assert_eq!(detect("gm"), None);
        assert_eq!(detect("gm nostr"), None);
        assert_eq!(detect(""), None);
    }

    #[test]
    fn ambiguous_text_is_left_unlabelled_rather_than_guessed() {
        // whatlang is bimodal on real content: confident verdicts sit at ~1.0,
        // and the rest are coin flips it marks unreliable. This one is Spanish
        // and comes back at ~0.5, so it is recorded as unknown. Unlabelled
        // costs recall on `lang:es`; a wrong label would corrupt `lang:` for
        // every language at once.
        assert_eq!(
            detect("Hoy fue un día muy bonito, fui a la playa con mi familia."),
            None
        );
    }

    #[test]
    fn handles_multibyte_content_without_panicking() {
        // Truncation must land on a char boundary.
        let long = "今日はビットコインの会議です。".repeat(200);
        let _ = detect(&long);
        let emoji = "🚀".repeat(300);
        let _ = detect(&emoji);
    }
}
