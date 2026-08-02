//! NIP-50 search filter → Tantivy query translation + shard pruning.
//!
//! NIP-50 extends a standard Nostr filter with a `search` string. We support
//! the common extension operators inside that string, then AND the resulting
//! full-text query with the structured filter clauses (authors, kinds, tags,
//! time range).
//!
//! Supported `search` extensions (on top of bare terms / phrases / AND-OR):
//!   - `author:<hex|npub>`   restrict to an author
//!   - `kind:<n>`            restrict to a kind
//!   - `since:<unix|YYYY-MM-DD>` / `until:<...>`  time bound
//!   - `#tag`  or `tag:<x>`  hashtag (`t`) lookup
//!   - `lang:<code>`         language filter
//!
//! Bare terms are parsed by Tantivy's `QueryParser` against `content`, so the
//! full Tantivy grammar (`"phrase"`, `AND`/`OR`, `-negation`, `*`) works.

use crate::schema::NostrSchema;
use crate::shard::{ShardId, shards_in_range};
use tantivy::query::*;
use tantivy::schema::Field;
use tantivy::{Index, Term};

/// Error type returned by the planner.
pub use tantivy::query::QueryParserError as QueryError;

/// A parsed, planner-ready search request.
#[derive(Debug, Clone, Default)]
pub struct SearchFilter {
    /// Free-text search string (may contain extension operators).
    pub search: Option<String>,
    /// Author pubkeys (hex).
    pub authors: Vec<String>,
    /// Kinds.
    pub kinds: Vec<u16>,
    /// `#t` hashtag values (lowercased).
    pub tag_t: Vec<String>,
    /// `e` referenced event ids.
    pub tag_e: Vec<String>,
    /// `p` referenced pubkeys.
    pub tag_p: Vec<String>,
    /// `a` address coordinates.
    pub tag_a: Vec<String>,
    /// `d` identifiers.
    pub tag_d: Vec<String>,
    /// `g` geohash prefixes.
    pub tag_g: Vec<String>,
    /// Inclusive lower time bound (unix seconds).
    pub since: Option<u64>,
    /// Exclusive upper time bound (unix seconds).
    pub until: Option<u64>,
    /// Restrict to a language.
    pub lang: Option<String>,
    /// Exclude events flagged deleted (default: keep — user chose "filter
    /// nothing", but the option exists).
    pub exclude_deleted: bool,
    /// Exclude superseded replaceable/addressable events.
    pub exclude_superseded: bool,
    /// Max results.
    pub limit: usize,
}

/// Output of planning: a Tantivy query plus the shards to run it against.
pub struct PlannedQuery {
    pub query: Box<dyn Query>,
    pub shards: Vec<ShardId>,
}

/// The query planner. Holds a reference to the schema and the index (for the
/// content `QueryParser`).
pub struct QueryPlanner<'a> {
    pub schema: &'a NostrSchema,
    pub index: &'a Index,
    /// Earliest shard present in the cluster (pruning lower bound).
    pub earliest_shard: ShardId,
}

impl<'a> QueryPlanner<'a> {
    pub fn new(schema: &'a NostrSchema, index: &'a Index, earliest_shard: ShardId) -> Self {
        Self {
            schema,
            index,
            earliest_shard,
        }
    }

    /// Plan a filter into a Tantivy query + pruned shard list.
    pub fn plan(&self, filter: &SearchFilter) -> Result<PlannedQuery, QueryError> {
        let s = self.schema;
        let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();

        // --- 1. full-text `search` string (with extension operators) ---
        let mut since = filter.since;
        let mut until = filter.until;
        if let Some(raw) = &filter.search {
            let (text_query, ext) = self.parse_search_string(raw)?;

            if let Some(tq) = text_query {
                clauses.push((Occur::Must, tq));
            }
            // extension operators fold into the structured filter
            for a in ext.authors {
                clauses.push((Occur::Must, Self::term_query(s.pubkey, &a)));
            }
            for k in ext.kinds {
                clauses.push((Occur::Must, Self::u64_term(s.kind, k as u64)));
            }
            for t in ext.tag_t {
                clauses.push((Occur::Must, Self::term_query(s.tag_t, &t.to_lowercase())));
            }
            if ext.since.is_some() {
                since = ext.since.or(since);
            }
            if ext.until.is_some() {
                until = ext.until.or(until);
            }
            if let Some(l) = ext.lang {
                clauses.push((Occur::Must, Self::term_query(s.lang, &l)));
            }
        }

        // --- 2. structured clauses ---
        if !filter.authors.is_empty() {
            clauses.push((Occur::Must, Self::any_term(s.pubkey, &filter.authors)));
        }
        if !filter.kinds.is_empty() {
            let subs: Vec<(Occur, Box<dyn Query>)> = filter
                .kinds
                .iter()
                .map(|&k| (Occur::Should, Self::u64_term(s.kind, k as u64)))
                .collect();
            clauses.push((Occur::Must, Box::new(BooleanQuery::new(subs))));
        }
        Self::push_tag(&mut clauses, s.tag_t, &filter.tag_t, true);
        Self::push_tag(&mut clauses, s.tag_e, &filter.tag_e, false);
        Self::push_tag(&mut clauses, s.tag_p, &filter.tag_p, false);
        Self::push_tag(&mut clauses, s.tag_a, &filter.tag_a, false);
        Self::push_tag(&mut clauses, s.tag_d, &filter.tag_d, false);
        Self::push_tag(&mut clauses, s.tag_g, &filter.tag_g, false);
        if let Some(l) = &filter.lang {
            clauses.push((Occur::Must, Self::term_query(s.lang, l)));
        }

        // --- 3. time range (also drives shard pruning) ---
        if since.is_some() || until.is_some() {
            let lower = since
                .map(|v| std::ops::Bound::Included(Term::from_field_u64(s.created_at, v)))
                .unwrap_or(std::ops::Bound::Unbounded);
            let upper = until
                .map(|v| std::ops::Bound::Excluded(Term::from_field_u64(s.created_at, v)))
                .unwrap_or(std::ops::Bound::Unbounded);
            clauses.push((Occur::Must, Box::new(RangeQuery::new(lower, upper))));
        }

        // --- 4. soft flags (off by default per "index everything, filter
        //        nothing", but available) ---
        if filter.exclude_deleted {
            clauses.push((Occur::Must, Self::u64_term(s.deleted, 0)));
        }
        if filter.exclude_superseded {
            clauses.push((Occur::Must, Self::u64_term(s.superseded, 0)));
        }

        let query: Box<dyn Query> = match clauses.len() {
            0 => Box::new(AllQuery),
            1 => clauses.pop().unwrap().1,
            _ => Box::new(BooleanQuery::new(clauses)),
        };

        let shards = shards_in_range(since, until, self.earliest_shard);

        Ok(PlannedQuery { query, shards })
    }

    /// Parse the `search` string, splitting out extension operators from the
    /// free-text remainder (which is handed to Tantivy's QueryParser).
    ///
    /// Returns `(text_query, extensions)`.
    fn parse_search_string(
        &self,
        raw: &str,
    ) -> Result<(Option<Box<dyn Query>>, SearchExtensions), QueryError> {
        let mut ext = SearchExtensions::default();
        let mut free_terms: Vec<String> = Vec::new();

        for tok in raw.split_whitespace() {
            if let Some(rest) = tok.strip_prefix("author:") {
                ext.authors.push(rest.to_string());
            } else if let Some(rest) = tok.strip_prefix("kind:") {
                if let Ok(k) = rest.parse::<u16>() {
                    ext.kinds.push(k);
                }
            } else if let Some(rest) = tok.strip_prefix("since:") {
                ext.since = Self::parse_time(rest);
            } else if let Some(rest) = tok.strip_prefix("until:") {
                ext.until = Self::parse_time(rest);
            } else if let Some(rest) = tok.strip_prefix("lang:") {
                ext.lang = Some(rest.to_string());
            } else if let Some(rest) = tok.strip_prefix('#') {
                ext.tag_t.push(rest.to_string());
            } else if let Some(rest) = tok.strip_prefix("tag:") {
                ext.tag_t.push(rest.to_string());
            } else {
                free_terms.push(tok.to_string());
            }
        }

        let text_query: Option<Box<dyn Query>> = if free_terms.is_empty() {
            None
        } else {
            let qp = QueryParser::for_index(self.index, vec![self.schema.content]);
            let joined = free_terms.join(" ");
            Some(qp.parse_query(&joined)?)
        };

        Ok((text_query, ext))
    }

    /// Accept unix seconds or `YYYY-MM-DD`.
    fn parse_time(s: &str) -> Option<u64> {
        if let Ok(v) = s.parse::<u64>() {
            return Some(v);
        }
        chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .ok()
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .map(|dt| dt.and_utc().timestamp() as u64)
    }

    fn term_query(field: Field, value: &str) -> Box<dyn Query> {
        Box::new(TermQuery::new(
            Term::from_field_text(field, value),
            tantivy::schema::IndexRecordOption::Basic,
        ))
    }

    fn u64_term(field: Field, value: u64) -> Box<dyn Query> {
        Box::new(TermQuery::new(
            Term::from_field_u64(field, value),
            tantivy::schema::IndexRecordOption::Basic,
        ))
    }

    /// `any of these values` on a field → OR of term queries.
    fn any_term(field: Field, values: &[String]) -> Box<dyn Query> {
        let subs: Vec<(Occur, Box<dyn Query>)> = values
            .iter()
            .map(|v| (Occur::Should, Self::term_query(field, v)))
            .collect();
        Box::new(BooleanQuery::new(subs))
    }

    /// Multi-valued tag clause: any of the values matches (Nostr `#t` filter
    /// semantics are OR within a tag name).
    fn push_tag(
        clauses: &mut Vec<(Occur, Box<dyn Query>)>,
        field: Field,
        values: &[String],
        lowercase: bool,
    ) {
        if values.is_empty() {
            return;
        }
        let vals: Vec<String> = if lowercase {
            values.iter().map(|v| v.to_lowercase()).collect()
        } else {
            values.to_vec()
        };
        clauses.push((Occur::Must, Self::any_term(field, &vals)));
    }
}

#[derive(Default)]
struct SearchExtensions {
    authors: Vec<String>,
    kinds: Vec<u16>,
    tag_t: Vec<String>,
    since: Option<u64>,
    until: Option<u64>,
    lang: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::NostrSchema;

    fn setup() -> (Index, NostrSchema) {
        let (schema, ns) = NostrSchema::build();
        let index = Index::create_in_ram(schema);
        NostrSchema::register_tokenizers(&index);
        (index, ns)
    }

    #[test]
    fn parses_time_unix_and_date() {
        assert_eq!(QueryPlanner::parse_time("1700000000"), Some(1_700_000_000));
        assert!(QueryPlanner::parse_time("2023-11-14").is_some());
    }

    #[test]
    fn empty_filter_plans_all_query() {
        let (index, ns) = setup();
        let p = QueryPlanner::new(&ns, &index, ShardId::new(2023, 1));
        let f = SearchFilter {
            limit: 10,
            ..Default::default()
        };
        let planned = p.plan(&f).unwrap();
        // open time range prunes to [earliest..now]
        assert!(!planned.shards.is_empty());
    }

    #[test]
    fn search_extensions_extracted() {
        let (index, ns) = setup();
        let p = QueryPlanner::new(&ns, &index, ShardId::new(2023, 1));
        let (tq, ext) = p
            .parse_search_string("bitcoin author:abc kind:1 #nostr since:2024-01-01")
            .unwrap();
        assert!(tq.is_some()); // "bitcoin" remains as free text
        assert_eq!(ext.authors, vec!["abc"]);
        assert_eq!(ext.kinds, vec![1]);
        assert_eq!(ext.tag_t, vec!["nostr"]);
        assert!(ext.since.is_some());
    }
}
