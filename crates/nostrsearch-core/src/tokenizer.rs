//! Content tokenizer for Nostr text.
//!
//! Tantivy's `SimpleTokenizer` splits on non-alphanumeric boundaries. That is
//! correct for space-separated scripts and *catastrophic* for the ones that do
//! not use spaces: `今日はビットコインの会議です` contains no boundary, so it
//! became a single token and searching `ビットコイン` matched nothing. On a
//! network as multilingual as Nostr that silently dropped Japanese, Chinese,
//! Korean and Thai content entirely.
//!
//! [`NostrTokenizer`] therefore picks its rule **per script run** rather than
//! per document (a document routinely mixes scripts, and the `lang` field is a
//! guess made after the fact):
//!
//! - Runs of scriptio-continua characters (Han, Hiragana, Katakana, Hangul,
//!   Thai, Lao, Khmer, Myanmar) are emitted as **overlapping bigrams** at
//!   consecutive positions. Query-side tokenization produces the same bigrams,
//!   and Tantivy turns a multi-token query term into a phrase query, so
//!   `ビットコイン` matches as a contiguous substring rather than as a bag of
//!   characters. This is the same strategy as Lucene's `CJKBigramFilter`.
//! - Everything else is split on non-alphanumeric boundaries as before.
//! - A single character between separators (a lone 円, a one-letter word) is
//!   emitted as a unigram rather than dropped.
//!
//! Bigrams cost roughly 2 postings per character versus 1 per word, which is
//! the accepted price of making the content searchable at all.

use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

/// Longest token kept, in bytes.
///
/// The previous limit was 256, which is not a word in any language: a data-URI
/// image in a kind-0 profile (`data:image/png;base64,iVBOR...`) is emitted as a
/// long run of 256-byte tokens, each a unique term nobody will ever search for,
/// and each one costing a term-dictionary entry across a 763 GiB corpus. 40
/// bytes covers essentially every real word (the longest word in most
/// dictionaries is under 30) and the longest CJK bigram is 12.
pub const MAX_TOKEN_BYTES: usize = 40;

/// Whether `c` belongs to a script written without word separators, so a
/// bigram window is the only way to index it usefully.
fn is_scriptio_continua(c: char) -> bool {
    matches!(c as u32,
        // CJK ideographs (Han), incl. the common extensions.
        0x3400..=0x4DBF | 0x4E00..=0x9FFF | 0xF900..=0xFAFF | 0x20000..=0x2FA1F
        // Hiragana + Katakana (incl. phonetic extensions and halfwidth forms).
        | 0x3040..=0x30FF | 0x31F0..=0x31FF | 0xFF66..=0xFF9D
        // Hangul syllables + Jamo.
        | 0x1100..=0x11FF | 0xA960..=0xA97F | 0xAC00..=0xD7FF
        // Thai, Lao, Khmer, Myanmar.
        | 0x0E00..=0x0E7F | 0x0E80..=0x0EFF | 0x1780..=0x17FF | 0x1000..=0x109F
    )
}

/// Nostr content tokenizer: word-split for spaced scripts, bigrams for the
/// rest. See the module docs.
#[derive(Clone, Default)]
pub struct NostrTokenizer;

impl Tokenizer for NostrTokenizer {
    type TokenStream<'a> = VecTokenStream;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        VecTokenStream::new(tokenize(text))
    }
}

/// Split `text` into tokens, choosing the rule per script run.
fn tokenize(text: &str) -> Vec<Token> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    // Current run of ordinary (spaced-script) word characters.
    let mut word_start: Option<usize> = None;
    // Current run of scriptio-continua characters, as (byte offset, char).
    let mut cont: Vec<(usize, char)> = Vec::new();

    let flush_word = |out: &mut Vec<Token>, pos: &mut usize, start: usize, end: usize| {
        out.push(Token {
            offset_from: start,
            offset_to: end,
            position: *pos,
            text: text[start..end].to_string(),
            position_length: 1,
        });
        *pos += 1;
    };

    let flush_cont = |out: &mut Vec<Token>, pos: &mut usize, run: &mut Vec<(usize, char)>| {
        match run.len() {
            0 => {}
            // A lone character still has to be findable on its own.
            1 => {
                let (off, c) = run[0];
                out.push(Token {
                    offset_from: off,
                    offset_to: off + c.len_utf8(),
                    position: *pos,
                    text: c.to_string(),
                    position_length: 1,
                });
                *pos += 1;
            }
            _ => {
                for w in run.windows(2) {
                    let (off, a) = w[0];
                    let (_, b) = w[1];
                    let mut text = String::with_capacity(a.len_utf8() + b.len_utf8());
                    text.push(a);
                    text.push(b);
                    out.push(Token {
                        offset_from: off,
                        offset_to: off + a.len_utf8() + b.len_utf8(),
                        position: *pos,
                        text,
                        position_length: 1,
                    });
                    *pos += 1;
                }
            }
        }
        run.clear();
    };

    for (i, c) in text.char_indices() {
        if is_scriptio_continua(c) {
            if let Some(start) = word_start.take() {
                flush_word(&mut out, &mut pos, start, i);
            }
            cont.push((i, c));
        } else if c.is_alphanumeric() {
            if !cont.is_empty() {
                flush_cont(&mut out, &mut pos, &mut cont);
            }
            if word_start.is_none() {
                word_start = Some(i);
            }
        } else {
            if let Some(start) = word_start.take() {
                flush_word(&mut out, &mut pos, start, i);
            }
            if !cont.is_empty() {
                flush_cont(&mut out, &mut pos, &mut cont);
            }
        }
    }
    if let Some(start) = word_start.take() {
        flush_word(&mut out, &mut pos, start, text.len());
    }
    if !cont.is_empty() {
        flush_cont(&mut out, &mut pos, &mut cont);
    }

    out
}

/// Token stream over a precomputed vector.
pub struct VecTokenStream {
    tokens: Vec<Token>,
    /// Index of the *current* token, offset by one so 0 means "before start".
    cursor: usize,
}

impl VecTokenStream {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, cursor: 0 }
    }
}

impl TokenStream for VecTokenStream {
    fn advance(&mut self) -> bool {
        self.cursor += 1;
        self.cursor <= self.tokens.len()
    }

    fn token(&self) -> &Token {
        &self.tokens[self.cursor - 1]
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.tokens[self.cursor - 1]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn texts(s: &str) -> Vec<String> {
        tokenize(s).into_iter().map(|t| t.text).collect()
    }

    #[test]
    fn latin_text_splits_on_word_boundaries() {
        assert_eq!(texts("gm nostr, bitcoin!"), vec!["gm", "nostr", "bitcoin"]);
    }

    #[test]
    fn japanese_is_bigrammed_so_it_can_be_found() {
        // The bug: SimpleTokenizer emitted this as one token, so no query
        // shorter than the whole sentence could match it.
        let toks = texts("ビットコイン");
        assert_eq!(toks, vec!["ビッ", "ット", "トコ", "コイ", "イン"]);
        // A sentence containing it produces the same bigram sequence, which is
        // what lets a phrase query match.
        let sentence = texts("今日はビットコインの会議です");
        for bigram in &toks {
            assert!(sentence.contains(bigram), "sentence lost {bigram}");
        }
    }

    #[test]
    fn mixed_scripts_switch_rule_mid_string() {
        assert_eq!(
            texts("bitcoin ビット coin"),
            vec!["bitcoin", "ビッ", "ット", "coin"]
        );
    }

    #[test]
    fn single_cjk_character_is_kept_as_a_unigram() {
        assert_eq!(texts("値段は円"), vec!["値段", "段は", "は円"]);
        assert_eq!(texts("a 円 b"), vec!["a", "円", "b"]);
    }

    #[test]
    fn positions_are_consecutive_so_phrases_match() {
        let toks = tokenize("今日はビットコイン");
        let positions: Vec<usize> = toks.iter().map(|t| t.position).collect();
        assert_eq!(positions, (0..toks.len()).collect::<Vec<_>>());
    }

    #[test]
    fn offsets_point_back_into_the_source() {
        for t in tokenize("gm 今日は nostr") {
            assert_eq!(&"gm 今日は nostr"[t.offset_from..t.offset_to], t.text);
        }
    }

    #[test]
    fn korean_and_thai_are_bigrammed_too() {
        assert_eq!(texts("비트코인").len(), 3);
        assert!(!texts("บิตคอยน์").is_empty());
    }
}
