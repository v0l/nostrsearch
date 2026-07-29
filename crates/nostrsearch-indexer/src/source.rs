//! Streaming source for hole.v0l.io JSONL dumps (plain or zstd-compressed).

use nostrsearch_core::event::NostrEvent;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SourceError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

/// A line-oriented source of raw event JSON.
pub enum JsonlSource {
    /// Uncompressed `.jsonl`.
    Plain(BufReader<Box<dyn Read + Send>>),
    /// Zstd-compressed `.jsonl.zst`. BufReader around the decoder for read_line.
    Zstd(BufReader<zstd::stream::read::Decoder<'static, BufReader<Box<dyn Read + Send>>>>),
}

impl JsonlSource {
    /// Open a local file, choosing the decoder from the extension.
    pub fn open(path: &Path) -> Result<Self, SourceError> {
        let file = std::fs::File::open(path)?;
        let reader: Box<dyn Read + Send> = Box::new(file);
        Self::from_reader(reader, path.extension().and_then(|e| e.to_str()))
    }

    /// Wrap an arbitrary reader. `ext` decides the decoder ("zst" => zstd).
    pub fn from_reader(reader: Box<dyn Read + Send>, ext: Option<&str>) -> Result<Self, SourceError> {
        if ext == Some("zst") {
            // Decoder's inner reader must be BufReader<Box<dyn Read + Send>>;
            // we buffer the *output* of the decoder for read_line.
            let dec = zstd::stream::read::Decoder::new(reader)?;
            Ok(Self::Zstd(BufReader::with_capacity(1 << 20, dec)))
        } else {
            Ok(Self::Plain(BufReader::with_capacity(1 << 20, reader)))
        }
    }
}

impl Iterator for JsonlSource {
    type Item = Result<String, SourceError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut line = String::new();
        let res = match self {
            Self::Plain(r) => r.read_line(&mut line),
            Self::Zstd(r) => r.read_line(&mut line),
        };
        match res {
            Ok(0) => None,
            Ok(_) => {
                if line.trim().is_empty() {
                    // skip blank lines without recursing deeply
                    return self.next();
                }
                Some(Ok(line))
            }
            Err(e) => Some(Err(SourceError::Io(e))),
        }
    }
}

/// A fallible stream of parsed events.
pub struct EventStream {
    source: JsonlSource,
    /// Whether to tolerate and skip malformed lines (archival dumps have some).
    pub skip_bad: bool,
    /// Count of lines that failed to parse.
    pub bad_lines: u64,
}

impl EventStream {
    pub fn new(source: JsonlSource) -> Self {
        Self {
            source,
            skip_bad: true,
            bad_lines: 0,
        }
    }
}

impl Iterator for EventStream {
    type Item = NostrEvent;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.source.next()? {
                Ok(line) => match serde_json::from_str::<NostrEvent>(&line) {
                    Ok(ev) => return Some(ev),
                    Err(e) => {
                        self.bad_lines += 1;
                        if !self.skip_bad {
                            tracing::warn!(error = %e, "malformed event line");
                        }
                        continue;
                    }
                },
                Err(e) => {
                    tracing::warn!(error = %e, "source read error");
                    self.bad_lines += 1;
                    continue;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn sample_line() -> String {
        serde_json::json!({
            "id": "a".repeat(64),
            "pubkey": "b".repeat(64),
            "created_at": 1_700_000_000u64,
            "kind": 1,
            "tags": [["t","nostr"]],
            "content": "gm",
            "sig": "c".repeat(128),
        })
        .to_string()
    }

    #[test]
    fn reads_plain_jsonl() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(tmp, "{}", sample_line()).unwrap();
        writeln!(tmp, "{}", sample_line()).unwrap();
        tmp.flush().unwrap();

        let src = JsonlSource::open(tmp.path()).unwrap();
        let stream = EventStream::new(src);
        assert_eq!(stream.count(), 2);
    }

    #[test]
    fn reads_zstd_jsonl() {
        let mut tmp = tempfile::Builder::new().suffix(".zst").tempfile().unwrap();
        let data = format!("{}\n{}\n", sample_line(), sample_line());
        let compressed = zstd::encode_all(data.as_bytes(), 3).unwrap();
        tmp.write_all(&compressed).unwrap();
        tmp.flush().unwrap();

        let src = JsonlSource::open(tmp.path()).unwrap();
        let stream = EventStream::new(src);
        assert_eq!(stream.count(), 2);
    }

    #[test]
    fn skips_bad_lines() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(tmp, "{}", sample_line()).unwrap();
        writeln!(tmp, "{{not json").unwrap();
        writeln!(tmp, "{}", sample_line()).unwrap();
        tmp.flush().unwrap();

        let src = JsonlSource::open(tmp.path()).unwrap();
        let mut stream = EventStream::new(src);
        let events: Vec<_> = stream.by_ref().collect();
        assert_eq!(events.len(), 2);
        assert_eq!(stream.bad_lines, 1);
    }
}
