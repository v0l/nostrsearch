//! Full-network historical scrape: fill index gaps by walking day-by-day
//! backwards from yesterday across every relay the network advertises.
//!
//! - **Targets** come from kind-10002 (NIP-65) relay lists already in the
//!   index, weighted by how many distinct authors advertise each relay.
//! - **Negentropy first** (NIP-77): reconcile a day against a relay to learn
//!   which event ids it holds, subtract everything `.dedupe` already has, and
//!   fetch only the missing ids. Bandwidth scales with the gap, not the day.
//! - **Windowed fallback** for relays without negentropy: since/until REQs
//!   with adaptive bisection. Relays cap result counts silently, so a window
//!   whose result count reaches the relay's observed ceiling is split in half,
//!   down to a floor (default 10 minutes).
//! - **Resumable**: per-(relay, day) completion is persisted; restarts skip
//!   finished work instead of re-scraping.

use chrono::{Datelike, TimeZone, Utc};
pub use nostrsearch_core::relay::normalize_relay_url;
use rocksdb::{DB, Options};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;

/// A relay we intend to scrape.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RelayInfo {
    /// Distinct authors whose kind-10002 advertised this relay.
    pub sources: u32,
    /// `Some(true)` once a negentropy sync succeeded, `Some(false)` once the
    /// relay rejected it; `None` = not yet probed.
    pub negentropy: Option<bool>,
    /// Largest result count ever returned by a single REQ — the working
    /// estimate of the relay's silent result cap.
    pub cap: u32,
    /// Consecutive day-level failures (reset on success).
    pub fails: u32,
    /// Unix secs of the last successful day.
    pub last_ok: u64,
    /// Oldest day-start (unix secs) this relay has actually returned events
    /// for.
    ///
    /// Informational only. It must **not** bound which days are drawn: it
    /// records where data was *found*, not where data *ends*. Using it as a
    /// floor meant the first successful draw pinned the relay's history at
    /// that day and everything older was never sampled again.
    ///
    /// Walking backwards contiguously, the old scraper could infer a real
    /// horizon from consecutive empty days. Sampling at random cannot: an
    /// empty day is one absent day, not a boundary. Empty days are recorded
    /// like any other, so they are drawn at most once each and the cost of
    /// probing below a relay's real start is bounded and paid once.
    #[serde(default)]
    pub birthday: Option<u64>,
    /// Unix secs until which this relay is considered dead and not probed.
    ///
    /// A relay that refuses connections fails every draw it appears in, and
    /// with work sampled at random it keeps reappearing -- burning a slot in
    /// batch after batch on a host that is not there. Once it has failed
    /// enough times in a row it is set aside for a day rather than retried
    /// immediately.
    #[serde(default)]
    pub dead_until: Option<u64>,
}

/// One completed (relay, day), flattened for the status page.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DayEntry {
    pub date: String,
    pub relay: String,
    pub seen: u64,
    pub new: u64,
    pub at: u64,
}

/// Aggregate scrape progress across every relay and day.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScrapeProgress {
    /// Distinct dates with at least one relay completed.
    pub days: u64,
    /// Completed (relay, day) pairs.
    pub relay_days: u64,
    /// Events relays returned, and how many were new to the index.
    pub events_seen: u64,
    pub events_new: u64,
    pub oldest_day: Option<String>,
    pub newest_day: Option<String>,
    /// Most recently completed (relay, day) results, newest first.
    pub recent: Vec<DayEntry>,
    /// Per-relay totals, keyed by url.
    ///
    /// Accumulated during the scan this struct already does, so it costs
    /// nothing extra. Not serialized: `/sync` folds it into each relay row
    /// rather than shipping a second copy of the same numbers.
    #[serde(skip)]
    pub by_relay: std::collections::HashMap<String, RelayTotals>,
}

/// What one relay has produced across every day scraped from it.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct RelayTotals {
    /// Days completed for this relay.
    pub days: u64,
    /// Events it returned.
    pub seen: u64,
    /// Of those, events new to the index.
    pub new: u64,
}

/// Outcome of one fully-scraped (relay, day).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DayDone {
    /// Events the relay returned for this day.
    pub seen: u64,
    /// Events that were new to the index.
    pub new: u64,
    /// Unix secs when completed.
    pub at: u64,
}

/// Persistent scrape state: relay targets and per-(relay, day) completion.
///
/// Layout (single RocksDB, prefix-keyed):
/// - `r|<url>`              → bincode [`RelayInfo`]
/// - `d|<YYYY-MM-DD>|<url>` → bincode [`DayDone`]
pub struct ScrapeState {
    db: DB,
}

impl ScrapeState {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        // Small store, but it holds a record per (relay, day) -- 8000+ relays
        // times years of days -- so it grows like the others and gets the same
        // treatment: index and filter blocks under a bounded cache rather than
        // resident per open SST.
        let mut bb = rocksdb::BlockBasedOptions::default();
        bb.set_block_cache(&rocksdb::Cache::new_lru_cache(64 * 1024 * 1024));
        bb.set_cache_index_and_filter_blocks(true);
        bb.set_pin_l0_filter_and_index_blocks_in_cache(true);

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_block_based_table_factory(&bb);
        opts.set_write_buffer_size(32 * 1024 * 1024);
        // Bounded descriptor use; the default (-1) keeps an fd per SST.
        opts.set_max_open_files(256);
        Ok(Self {
            db: DB::open(&opts, path)?,
        })
    }

    pub fn relays(&self) -> Vec<(String, RelayInfo)> {
        let mut out = Vec::new();
        for kv in self.db.prefix_iterator(b"r|") {
            let Ok((k, v)) = kv else { break };
            if !k.starts_with(b"r|") {
                break;
            }
            let url = String::from_utf8_lossy(&k[2..]).into_owned();
            if let Ok(info) = bincode::deserialize::<RelayInfo>(&v) {
                out.push((url, info));
            }
        }
        out
    }

    /// Unix seconds when relay discovery last completed, if ever.
    ///
    /// Persisted because discovery is the single most expensive thing the
    /// scraper does -- it opens every shard and fetches a stored document per
    /// kind-10002 hit -- and an in-process timer cannot survive the restart
    /// that a deploy causes. Without this, discovery ran in full on every boot
    /// however recently it had last finished.
    pub fn last_discovery(&self) -> Option<u64> {
        self.db
            .get(b"meta|last_discovery")
            .ok()
            .flatten()
            .and_then(|v| v.try_into().ok())
            .map(u64::from_be_bytes)
    }

    pub fn set_last_discovery(&self, unix: u64) {
        let _ = self.db.put(b"meta|last_discovery", unix.to_be_bytes());
    }

    pub fn put_relay(&self, url: &str, info: &RelayInfo) {
        let mut k = b"r|".to_vec();
        k.extend_from_slice(url.as_bytes());
        if let Ok(v) = bincode::serialize(info) {
            let _ = self.db.put(k, v);
        }
    }

    pub fn day_done(&self, date: &str, url: &str) -> bool {
        self.db
            .get_pinned(Self::day_key(date, url))
            .map(|v| v.is_some())
            .unwrap_or(false)
    }

    pub fn put_day(&self, date: &str, url: &str, done: &DayDone) {
        if let Ok(v) = bincode::serialize(done) {
            let _ = self.db.put(Self::day_key(date, url), v);
        }
    }

    /// Aggregate view of everything scraped so far, for the sync status page.
    ///
    /// One pass over the `d|` prefix. Completion is tracked per (relay, day),
    /// so `days` counts distinct dates while `relay_days` counts the pairs.
    pub fn progress(&self, recent_limit: usize) -> ScrapeProgress {
        let mut p = ScrapeProgress::default();
        let mut recent: Vec<DayEntry> = Vec::new();
        let mut dates: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

        for kv in self.db.prefix_iterator(b"d|") {
            let Ok((k, v)) = kv else { break };
            if !k.starts_with(b"d|") {
                break;
            }
            let rest = String::from_utf8_lossy(&k[2..]).into_owned();
            let Some((date, url)) = rest.split_once('|') else {
                continue;
            };
            let Ok(done) = bincode::deserialize::<DayDone>(&v) else {
                continue;
            };

            p.relay_days += 1;
            p.events_seen += done.seen;
            p.events_new += done.new;
            dates.insert(date.to_string());

            let t = p.by_relay.entry(url.to_string()).or_default();
            t.days += 1;
            t.seen += done.seen;
            t.new += done.new;

            recent.push(DayEntry {
                date: date.to_string(),
                relay: url.to_string(),
                seen: done.seen,
                new: done.new,
                at: done.at,
            });
        }

        p.days = dates.len() as u64;
        p.oldest_day = dates.iter().next().cloned();
        p.newest_day = dates.iter().next_back().cloned();

        // Most recently completed first.
        recent.sort_by(|a, b| b.at.cmp(&a.at));
        recent.truncate(recent_limit);
        p.recent = recent;
        p
    }

    /// Forget completion records so those (relay, day) pairs get scraped again.
    ///
    /// `relay` / `from` / `to` are all optional filters; `from` and `to` are
    /// inclusive `YYYY-MM-DD` bounds compared lexically, which is exactly
    /// chronological for that format. Returns how many records were dropped.
    ///
    /// This is the operational escape hatch for "that day was scraped against a
    /// relay that was broken/empty at the time" — without it the only remedy is
    /// deleting the whole state database and re-walking the entire network.
    pub fn reset_days(&self, relay: Option<&str>, from: Option<&str>, to: Option<&str>) -> u64 {
        let mut keys: Vec<Vec<u8>> = Vec::new();
        self.for_each_day(relay, from, to, |key, _, _, _| {
            keys.push(key.to_vec());
        });
        let n = keys.len() as u64;
        for k in keys {
            let _ = self.db.delete(k);
        }
        n
    }

    /// Days matching the same filters [`reset_days`](Self::reset_days) uses:
    /// the total count plus up to `limit` entries.
    ///
    /// Shares the matcher with the reset path deliberately — a preview that
    /// could disagree with what the reset actually deletes would be worse than
    /// no preview at all.
    pub fn days_matching(
        &self,
        relay: Option<&str>,
        from: Option<&str>,
        to: Option<&str>,
        limit: usize,
    ) -> (u64, Vec<DayEntry>) {
        let mut count = 0u64;
        let mut sample = Vec::new();
        self.for_each_day(relay, from, to, |_, date, url, done| {
            count += 1;
            if sample.len() < limit {
                sample.push(DayEntry {
                    date: date.to_string(),
                    relay: url.to_string(),
                    seen: done.seen,
                    new: done.new,
                    at: done.at,
                });
            }
        });
        sample.sort_by(|a, b| b.date.cmp(&a.date));
        (count, sample)
    }

    /// Walk completed (relay, day) records matching the optional filters.
    /// `from`/`to` are inclusive `YYYY-MM-DD`, compared lexically — which is
    /// chronological for that format.
    fn for_each_day(
        &self,
        relay: Option<&str>,
        from: Option<&str>,
        to: Option<&str>,
        mut f: impl FnMut(&[u8], &str, &str, &DayDone),
    ) {
        for kv in self.db.prefix_iterator(b"d|") {
            let Ok((k, v)) = kv else { break };
            if !k.starts_with(b"d|") {
                break;
            }
            let rest = String::from_utf8_lossy(&k[2..]).into_owned();
            let Some((date, url)) = rest.split_once('|') else {
                continue;
            };
            if let Some(r) = relay
                && r != url
            {
                continue;
            }
            if let Some(fr) = from
                && date < fr
            {
                continue;
            }
            if let Some(t) = to
                && date > t
            {
                continue;
            }
            let Ok(done) = bincode::deserialize::<DayDone>(&v) else {
                continue;
            };
            f(&k, date, url, &done);
        }
    }

    /// Clear a relay's *learned* state: the detected data horizon, failure
    /// count, observed result cap and negentropy probe result.
    ///
    /// `sources` (how many authors advertise it) is discovery data, not
    /// learned behaviour, so it is preserved. Mainly for a relay that was down
    /// or lying when first probed and has since been fixed — otherwise its
    /// `birthday` permanently stops us walking earlier history.
    pub fn reset_relay(&self, url: &str) -> bool {
        let mut k = b"r|".to_vec();
        k.extend_from_slice(url.as_bytes());
        let Ok(Some(v)) = self.db.get(&k) else {
            return false;
        };
        let Ok(old) = bincode::deserialize::<RelayInfo>(&v) else {
            return false;
        };
        let fresh = RelayInfo {
            sources: old.sources,
            ..Default::default()
        };
        if let Ok(v) = bincode::serialize(&fresh) {
            let _ = self.db.put(k, v);
            return true;
        }
        false
    }

    fn day_key(date: &str, url: &str) -> Vec<u8> {
        let mut k = b"d|".to_vec();
        k.extend_from_slice(date.as_bytes());
        k.push(b'|');
        k.extend_from_slice(url.as_bytes());
        k
    }
}

/// Normalize a relay URL for use as a stable target key. Returns `None` for
/// anything we don't want to scrape (onions, localhost, non-websocket).

/// Scan the Tantivy index for kind-10002 relay lists and count distinct
/// authors per relay. Returns `(url, distinct_authors)` sorted descending.
pub fn discover_relays(index_root: &Path) -> anyhow::Result<Vec<(String, u32)>> {
    use std::collections::HashMap;
    use tantivy::collector::DocSetCollector;
    use tantivy::query::TermQuery;
    use tantivy::schema::IndexRecordOption;
    use tantivy::schema::Value;
    use tantivy::{Index, Term};

    let (_ts, schema) = nostrsearch_core::schema::NostrSchema::build();
    let mut authors: HashMap<String, HashSet<u64>> = HashMap::new();

    if !index_root.exists() {
        return Ok(Vec::new());
    }
    for entry in std::fs::read_dir(index_root)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if nostrsearch_core::shard::ShardId::parse(&name).is_none() {
            continue;
        }
        let Ok(index) = Index::open_in_dir(entry.path()) else {
            continue;
        };
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let query = TermQuery::new(
            Term::from_field_u64(schema.kind, 10_002),
            IndexRecordOption::Basic,
        );
        let docs = searcher.search(&query, &DocSetCollector)?;
        for addr in docs {
            let doc: tantivy::TantivyDocument = searcher.doc(addr)?;
            let pk_hash = doc
                .get_first(schema.pubkey)
                .and_then(|v| v.as_str())
                .map(|s: &str| {
                    use std::hash::{Hash, Hasher};
                    let mut h = std::collections::hash_map::DefaultHasher::new();
                    s.hash(&mut h);
                    h.finish()
                })
                .unwrap_or(0);
            for v in doc.get_all(schema.tag_url) {
                if let Some(u) = v.as_str().and_then(|s| normalize_relay_url(s)) {
                    authors.entry(u).or_default().insert(pk_hash);
                }
            }
        }
    }

    let mut out: Vec<(String, u32)> = authors
        .into_iter()
        .map(|(u, s)| (u, s.len() as u32))
        .collect();
    out.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    Ok(out)
}

/// UTC date string (`YYYY-MM-DD`) and `[start, end)` unix bounds for the day
/// `days_back` days before today.
pub fn day_bounds(days_back: u64) -> (String, u64, u64) {
    let now = Utc::now().timestamp() as u64;
    let today_start = now - (now % 86_400);
    let start = today_start - days_back * 86_400;
    let dt = Utc.timestamp_opt(start as i64, 0).single().unwrap();
    (
        format!("{:04}-{:02}-{:02}", dt.year(), dt.month(), dt.day()),
        start,
        start + 86_400,
    )
}

/// Parse `YYYY-MM-DD` into a unix day-start timestamp.
pub fn parse_date(s: &str) -> Option<u64> {
    let mut it = s.split('-');
    let (y, m, d) = (
        it.next()?.parse().ok()?,
        it.next()?.parse().ok()?,
        it.next()?.parse().ok()?,
    );
    Utc.with_ymd_and_hms(y, m, d, 0, 0, 0)
        .single()
        .map(|dt| dt.timestamp() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_normalization() {
        assert_eq!(
            normalize_relay_url("wss://Relay.Damus.io/"),
            Some("wss://relay.damus.io".into())
        );
        assert_eq!(
            normalize_relay_url("wss://r.example.com/path?x=1"),
            Some("wss://r.example.com/path".into())
        );
        assert_eq!(normalize_relay_url("https://not-ws.example"), None);
        assert_eq!(normalize_relay_url("wss://abc.onion"), None);
        assert_eq!(normalize_relay_url("wss://127.0.0.1:8080"), None);
        assert_eq!(normalize_relay_url("wss://localhost"), None);
    }

    #[test]
    fn state_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let st = ScrapeState::open(dir.path()).unwrap();
        st.put_relay(
            "wss://a.example",
            &RelayInfo {
                sources: 5,
                ..Default::default()
            },
        );
        st.put_relay("wss://b.example", &RelayInfo::default());
        assert_eq!(st.relays().len(), 2);
        assert!(!st.day_done("2026-07-30", "wss://a.example"));
        st.put_day(
            "2026-07-30",
            "wss://a.example",
            &DayDone {
                seen: 10,
                new: 3,
                at: 0,
            },
        );
        assert!(st.day_done("2026-07-30", "wss://a.example"));
        assert!(!st.day_done("2026-07-30", "wss://b.example"));
    }

    #[test]
    fn day_bounds_align() {
        let (d, start, end) = day_bounds(1);
        assert_eq!(end - start, 86_400);
        assert_eq!(start % 86_400, 0);
        assert_eq!(d.len(), 10);
    }
}

// ---------------------------------------------------------------------------
// Scrape engine — generic over where accepted events go.
// ---------------------------------------------------------------------------

/// Destination for scraped events. The engine asks which ids are still needed
/// (so negentropy fetches only gaps) and hands over fetched events; the sink
/// is responsible for final dedupe and for feeding index/stats/archive.
pub trait Sink: Send + Sync + 'static {
    /// Filter to the ids we do not yet have.
    fn missing(
        &self,
        ids: Vec<[u8; 32]>,
    ) -> impl std::future::Future<Output = Vec<[u8; 32]>> + Send;
    /// Our local (event id, created_at) set for `since..=until`, fed to
    /// negentropy as the reconciliation baseline. An empty set degrades to
    /// full id enumeration from the relay (correct, just more id traffic).
    fn local_items(
        &self,
        since: u64,
        until: u64,
    ) -> impl std::future::Future<Output = Vec<(nostr_sdk::EventId, nostr_sdk::Timestamp)>> + Send
    {
        let _ = (since, until);
        async { Vec::new() }
    }
    /// Store a batch of fetched events; returns how many were genuinely new.
    fn process(
        &self,
        events: Vec<nostr_sdk::Event>,
    ) -> impl std::future::Future<Output = u64> + Send;
}

/// Tunables for one scrape pass.
#[derive(Debug, Clone)]
pub struct ScrapeConfig {
    /// Stop walking backwards at this unix day-start.
    pub min_date: u64,
    /// Smallest bisection window in seconds.
    pub floor_secs: u64,
    /// Relays scraped concurrently.
    /// Concurrent relay/day queries.
    ///
    /// Each relay walks its days serially, so this is the number of relays
    /// being talked to at once *and* the number of outstanding queries: one
    /// per relay. It is the only thing bounding network load now that relay
    /// discovery is uncapped.
    pub concurrency: usize,
    /// Consecutive empty days before concluding we've walked past the relay's
    /// data horizon ("birthday") and stopping.
    pub empty_days_limit: u32,
    /// Consecutive failures before a relay is set aside as dead.
    pub dead_after_fails: u32,
    /// How long a dead relay is left alone, in seconds.
    pub dead_for_secs: u64,
    /// Hard cap on one relay-day, in seconds. A relay that connects and then
    /// stalls would otherwise hold a worker slot for the rest of the pass.
    pub unit_timeout_secs: u64,
    /// Percentage of relays to scrape, most-advertised first.
    ///
    /// Discovery is uncapped and finds ~8000 relays, but advertisement is
    /// heavily skewed: the tail is largely dead hosts, personal relays and
    /// one-off test instances that cost a connection, a sync and a timeout
    /// each to learn nothing. Scraping the top half spends the same worker
    /// slots on the relays people actually publish to.
    ///
    /// 100 disables the cut.
    pub top_percent: u32,
}

impl Default for ScrapeConfig {
    fn default() -> Self {
        Self {
            min_date: parse_date("2022-01-01").unwrap_or(0),
            floor_secs: 600,
            concurrency: 50,
            empty_days_limit: 14,
            dead_after_fails: 3,
            dead_for_secs: 24 * 3600,
            unit_timeout_secs: 180,
            top_percent: 50,
        }
    }
}

/// Run one pass over every target relay, walking each backwards from
/// yesterday to `min_date`. Finished (relay, day) pairs are skipped via state,
/// so repeated passes only do new work (yesterday moves forward daily).
pub async fn run_pass<S: Sink>(
    state: std::sync::Arc<ScrapeState>,
    sink: std::sync::Arc<S>,
    cfg: ScrapeConfig,
) {
    let all_relays = state.relays().len();
    let scraped_relays = top_by_sources(state.relays(), cfg.top_percent).len();
    tracing::info!(
        relays = scraped_relays,
        discovered = all_relays,
        top_percent = cfg.top_percent,
        "scrape pass starting"
    );

    // Work is a (relay, day) pair drawn at random, run in batches of at most
    // `concurrency`.
    //
    // The old shape was one task per relay walking days backwards from
    // yesterday. That made the most-advertised relays monopolise the workers
    // for as long as their history took, so a relay far down the list waited
    // out everything above it, and an interrupted pass always left the same
    // tail unscraped. Sampling relay-days spreads progress evenly: every relay
    // advances a little on every pass, and coverage fills in uniformly rather
    // than front to back.
    // StdRng, not thread_rng: the rng is held across await points here, and
    // ThreadRng is !Send, which would make this whole future !Send and break
    // every caller that spawns it.
    let mut rng = <rand::rngs::StdRng as rand::SeedableRng>::from_entropy();
    let batch_size = cfg.concurrency.max(1);
    let mut scraped = 0u64;

    // Slots are held per unit, not per batch.
    //
    // Awaiting a whole batch before drawing the next meant every batch ran at
    // the speed of its slowest member, and among 8000+ relays there is always
    // one that accepts a connection and then stalls. 49 workers idled waiting
    // for it, so throughput collapsed to a few relay-days a minute with fifty
    // workers configured. A slot now frees the moment its own unit finishes.
    let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(batch_size));
    // Units handed out but not yet recorded as done. Without this the planner
    // would redraw them -- day_done is only true after completion.
    let in_flight: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>> =
        Default::default();
    let unit_timeout = std::time::Duration::from_secs(cfg.unit_timeout_secs);

    // Fill newest-first, one fixed block of days at a time.
    //
    // Within a month days are still drawn at random, which is what keeps every
    // relay advancing together instead of one relay monopolising the workers.
    // Across blocks the order is deliberate: a window is finished before the
    // next one back is opened, so recent history -- the part anyone is
    // actually searching -- completes first and coverage has a definite edge
    // rather than being uniformly sparse over four years.
    // One client for the pass. `Client` owns a relay pool and manages its
    // connections internally, so building one per relay-day meant constructing
    // and tearing down a pool 50 at a time, continuously, for the whole run --
    // and gave up connection reuse across the days of the same relay.
    //
    // Cloned into each task rather than wrapped: Client is already a handle
    // around shared state, so a clone is a refcount bump and every clone
    // drives the same pool.
    let client = nostr_sdk::Client::default();

    let now0 = chrono::Utc::now().timestamp() as u64;
    let today_start = now0 - (now0 % 86_400);
    let mut block = 0u64;
    let mut window = block_window(today_start, block);

    loop {
        // Re-read between batches rather than working from one snapshot taken
        // at pass start.
        //
        // Each completed day writes the relay's info back. Against a stale
        // snapshot those writes carry pre-probe values, so a negentropy result
        // recorded by one batch was overwritten by the next batch's copy and
        // the relay reverted to unprobed. It also meant `dead_until` was never
        // visible to later batches, so a dead relay kept being drawn for the
        // whole pass -- the retirement did nothing.
        let targets = top_by_sources(state.relays(), cfg.top_percent);
        let mut batch = plan_batch(
            &targets, &state, &cfg, batch_size, &mut rng, window, &in_flight,
        );

        // This month is done for every relay: step back and keep going, until
        // the window falls off the configured floor.
        while batch.is_empty() {
            if window.0 <= cfg.min_date {
                break;
            }
            block += 1;
            window = block_window(today_start, block);
            tracing::info!(
                block,
                window_start = window.0,
                window_end = window.1,
                scraped,
                "scrape window complete; moving back a block"
            );
            batch = plan_batch(
                &targets, &state, &cfg, batch_size, &mut rng, window, &in_flight,
            );
        }
        if batch.is_empty() {
            break;
        }

        for (url, info, date, start, end) in batch {
            // Take a slot first, so no more than `concurrency` units are ever
            // in flight. This blocks until some *individual* unit finishes --
            // not until a whole batch does.
            let Ok(permit) = sem.clone().acquire_owned().await else {
                break;
            };
            let key = format!("{url}|{date}");
            in_flight.lock().unwrap().insert(key.clone());

            let state = state.clone();
            let sink = sink.clone();
            let cfg = cfg.clone();
            let client = client.clone();
            let in_flight_t = in_flight.clone();
            tokio::spawn(async move {
                let _permit = permit;
                // A relay that accepts a connection and then never answers
                // would otherwise hold its slot for the rest of the pass.
                let work =
                    scrape_relay_day(&client, &url, info, &date, start, end, state, sink, &cfg);
                if tokio::time::timeout(unit_timeout, work).await.is_err() {
                    tracing::warn!(relay = %url, date = %date, "relay-day timed out");
                }
                in_flight_t.lock().unwrap().remove(&key);
            });
            scraped += 1;
        }
        if scraped % (batch_size as u64 * 20) == 0 {
            tracing::info!(scraped, "scrape pass progress");
        }
    }
    tracing::info!(scraped, "scrape pass complete");
}

/// Days in one backfill window.
pub const BLOCK_DAYS: u64 = 30;

/// Window `k` counting back from yesterday, as day-aligned `[start, end)`.
///
/// Fixed-width blocks anchored on yesterday, not calendar months: every window
/// is the same size regardless of where it lands, so progress through one says
/// the same thing as progress through any other, and there is no month
/// arithmetic to get wrong. Yesterday is the anchor because today is still
/// being written -- scraping a partial day records it as done and the rest of
/// it is never fetched.
pub fn block_window(today_start: u64, k: u64) -> (u64, u64) {
    // Exclusive end of window 0 is the start of today, i.e. yesterday's end.
    let end = today_start.saturating_sub(k * BLOCK_DAYS * 86_400);
    let start = end.saturating_sub(BLOCK_DAYS * 86_400);
    (start, end)
}

/// Keep the most-advertised `percent` of relays.
///
/// Sorted by advertiser count, ties broken on url so the cut is deterministic
/// -- an unstable boundary would move relays in and out of scope between
/// passes and leave their history permanently half-scraped.
///
/// Never returns empty for a non-empty input: a rounding-down cut on a small
/// relay set would otherwise stop the scraper entirely.
pub fn top_by_sources(
    mut targets: Vec<(String, RelayInfo)>,
    percent: u32,
) -> Vec<(String, RelayInfo)> {
    if percent >= 100 || targets.is_empty() {
        return targets;
    }
    targets.sort_by(|a, b| b.1.sources.cmp(&a.1.sources).then_with(|| a.0.cmp(&b.0)));
    let keep = ((targets.len() * percent as usize) / 100).max(1);
    targets.truncate(keep);
    targets
}

/// One unit of work: a relay and the day to fetch from it.
type RelayDay = (String, RelayInfo, String, u64, u64);

/// Draw up to `limit` relay-days at random.
///
/// Days already recorded and days before `min_date` are not candidates.
/// Relays are sampled without replacement within a batch so one relay cannot
/// take every slot and open several connections to itself.
fn plan_batch(
    targets: &[(String, RelayInfo)],
    state: &std::sync::Arc<ScrapeState>,
    cfg: &ScrapeConfig,
    limit: usize,
    rng: &mut impl rand::Rng,
    window: (u64, u64),
    in_flight: &std::sync::Mutex<std::collections::HashSet<String>>,
) -> Vec<RelayDay> {
    use rand::seq::SliceRandom;

    let now = chrono::Utc::now().timestamp() as u64;
    let today_start = now - (now % 86_400);

    let mut order: Vec<usize> = (0..targets.len()).collect();
    order.shuffle(rng);

    let mut out = Vec::with_capacity(limit);
    for idx in order {
        if out.len() >= limit {
            break;
        }
        let (url, info) = &targets[idx];

        // Set aside as dead, and not yet due for a retry.
        if info.dead_until.is_some_and(|t| now < t) {
            continue;
        }

        // Days are drawn from the current window only, newest window first.
        // Complete randomness across the whole corpus spread every relay
        // thinly over four years, so nothing was ever finished and recent
        // history -- the part anyone is actually searching -- filled at the
        // same glacial rate as 2022.
        let lo = window.0.max(cfg.min_date);
        let hi = window.1.min(today_start);
        if lo >= hi {
            continue;
        }
        let span_days = (hi - lo) / 86_400;
        if span_days == 0 {
            continue;
        }

        // Enumerate the window's unfinished days, then pick one at random.
        //
        // This used to take 8 random draws and give up. Once a window was
        // mostly complete those draws kept landing on finished days, the relay
        // yielded nothing, and if that held for every relay the batch came
        // back empty -- which the caller reads as "window exhausted" and steps
        // past, permanently, with days still unscraped. Enough windows falsely
        // exhausted and the pass ends and the scraper goes idle.
        //
        // A window is BLOCK_DAYS wide, so this is at most 30 `day_done`
        // lookups against RocksDB: cheaper than the relay query it schedules,
        // and it cannot report exhaustion that is not real.
        let mut open_days: Vec<(String, u64, u64)> = Vec::new();
        for off in 0..span_days {
            let start = lo + off * 86_400;
            let back = (today_start - start) / 86_400;
            let (date, start, end) = day_bounds(back);
            if start < lo || start >= hi {
                continue;
            }
            // Skip both finished days and ones already handed to a worker:
            // day_done only becomes true once a unit completes.
            if !state.day_done(&date, url)
                && !in_flight.lock().unwrap().contains(&format!("{url}|{date}"))
            {
                open_days.push((date, start, end));
            }
        }
        // Random *within* the window still, so relays stay interleaved rather
        // than every one of them marching the same day in lockstep.
        if let Some((date, start, end)) = open_days.choose(rng).cloned() {
            out.push((url.clone(), info.clone(), date, start, end));
        }
    }
    out
}

/// Fetch one day from one relay and record the outcome.
#[allow(clippy::too_many_arguments)]
async fn scrape_relay_day<S: Sink>(
    client: &nostr_sdk::Client,
    url: &str,
    mut info: RelayInfo,
    date: &str,
    start: u64,
    end: u64,
    state: std::sync::Arc<ScrapeState>,
    sink: std::sync::Arc<S>,
    cfg: &ScrapeConfig,
) {
    use nostr_sdk::prelude::*;

    // Added to the shared pool on demand. add_relay is idempotent, so a relay
    // touched again in a later batch reuses the existing connection instead of
    // handshaking afresh.
    if client.add_relay(url).await.is_err() {
        return;
    }
    if client.connect_relay(url).await.is_err() {
        return;
    }

    match scrape_day(client, url, &mut info, &sink, start, end, cfg.floor_secs).await {
        Ok((seen, new)) => {
            // Alive: clear both the failure streak and any death sentence.
            info.fails = 0;
            info.dead_until = None;
            info.last_ok = chrono::Utc::now().timestamp() as u64;
            state.put_day(
                date,
                url,
                &DayDone {
                    seen,
                    new,
                    at: info.last_ok,
                },
            );
            // An empty day at or below the relay's oldest known data is
            // evidence of its horizon. Days arrive out of order now, so the
            // old "N consecutive empties" rule cannot apply; instead the
            // birthday only ever moves forward to the oldest day that did
            // return something.
            if seen > 0 {
                info.birthday = Some(match info.birthday {
                    Some(b) => b.min(start),
                    None => start,
                });
            }
        }
        Err(_) => {
            info.fails = info.fails.saturating_add(1);
            if info.fails >= cfg.dead_after_fails {
                let now = chrono::Utc::now().timestamp() as u64;
                info.dead_until = Some(now + cfg.dead_for_secs);
                tracing::info!(
                    relay = url,
                    fails = info.fails,
                    retry_in_secs = cfg.dead_for_secs,
                    "relay marked dead"
                );
            }
        }
    }
    state.put_relay(url, &info);
    // Drop the connection but keep the client: leaving every relay attached
    // would accumulate thousands of open sockets across a pass over 8000+
    // relays. The pool keeps whatever the next batch re-adds.
    let _ = client.remove_relay(url).await;
}


/// Scrape one (relay, day): negentropy id-reconciliation with gap fetch when
/// supported, adaptive since/until bisection otherwise.
async fn scrape_day<S: Sink>(
    client: &nostr_sdk::Client,
    url: &str,
    info: &mut RelayInfo,
    sink: &std::sync::Arc<S>,
    start: u64,
    end: u64,
    floor_secs: u64,
) -> anyhow::Result<(u64, u64)> {
    use nostr_sdk::prelude::*;

    let mut seen = 0u64;
    let mut new = 0u64;

    // 1. Negentropy: enumerate the relay's ids for the day, fetch only gaps.
    if info.negentropy != Some(false) {
        let filter = Filter::new()
            .since(Timestamp::from(start))
            .until(Timestamp::from(end - 1));
        let opts = SyncOptions::new().dry_run();
        // Reconcile against our real local set for the day, so steady-state
        // traffic is the difference rather than the relay's full id list.
        let items = sink.local_items(start, end - 1).await;
        let sync = async {
            let relay = client.relay(url).await?;
            relay
                .sync_with_items(filter.clone(), items, &opts)
                .await
                .map_err(anyhow::Error::from)
        };
        match sync.await {
            Ok(out) => {
                info.negentropy = Some(true);
                let remote = &out.remote;
                seen = remote.len() as u64;
                let ids: Vec<[u8; 32]> = remote.iter().map(|id| id.to_bytes()).collect();
                let missing = sink.missing(ids).await;
                for chunk in missing.chunks(500) {
                    let ids: Vec<EventId> =
                        chunk.iter().map(|b| EventId::from_byte_array(*b)).collect();
                    let events = client
                        .fetch_events_from(
                            [url],
                            Filter::new().ids(ids),
                            std::time::Duration::from_secs(30),
                        )
                        .await?;
                    new += sink.process(events.into_iter().collect()).await;
                }
                return Ok((seen, new));
            }
            Err(e) => {
                let msg = e.to_string().to_lowercase();
                if msg.contains("negentropy") || msg.contains("not supported") {
                    // Remember, so we don't retry it every day.
                    info.negentropy = Some(false);
                } else {
                    return Err(e.into());
                }
            }
        }
    }

    // 2. Windowed since/until with adaptive bisection. Relays cap result
    // counts silently, so a window that returns >= the relay's observed
    // ceiling is assumed truncated and split, down to the floor window.
    let mut stack = vec![(start, end)];
    while let Some((s, e)) = stack.pop() {
        let filter = Filter::new()
            .since(Timestamp::from(s))
            .until(Timestamp::from(e - 1))
            .limit(10_000);
        let events = client
            .fetch_events_from([url], filter, std::time::Duration::from_secs(30))
            .await?;
        let n = events.len() as u64;
        info.cap = info.cap.max(n as u32);
        let ceiling = (info.cap.max(500)) as u64;
        if n >= ceiling && (e - s) / 2 >= floor_secs {
            let mid = s + (e - s) / 2;
            stack.push((s, mid));
            stack.push((mid, e));
            continue;
        }
        seen += n;
        new += sink.process(events.into_iter().collect()).await;
    }
    Ok((seen, new))
}

#[cfg(test)]
mod scrape_concurrency_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn state(tag: &str) -> (std::sync::Arc<super::ScrapeState>, std::path::PathBuf) {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "nsscrape-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let st = std::sync::Arc::new(super::ScrapeState::open(&p).unwrap());
        (st, p)
    }

    fn targets(n: usize) -> Vec<(String, super::RelayInfo)> {
        (0..n)
            .map(|i| (format!("wss://r{i}.example"), super::RelayInfo::default()))
            .collect()
    }

    /// A batch is the concurrency limit, and no more.
    #[test]
    fn batch_is_bounded_and_spread_across_relays() {
        let (st, dir) = state("bounded");
        let t = targets(200);
        let cfg = super::ScrapeConfig::default();
        let mut rng = rand::thread_rng();
        let win = (0u64, u64::MAX); // whole corpus, for tests about other rules
        let inflight = std::sync::Mutex::new(std::collections::HashSet::new());

        let batch = super::plan_batch(&t, &st, &cfg, 50, &mut rng, win, &inflight);
        assert!(batch.len() <= 50, "batch must not exceed the limit");
        assert!(batch.len() > 40, "should fill a batch from 200 fresh relays");

        // One relay must not take several slots: that would open several
        // connections to the same host in one batch.
        let mut urls: Vec<&str> = batch.iter().map(|(u, ..)| u.as_str()).collect();
        urls.sort_unstable();
        let before = urls.len();
        urls.dedup();
        assert_eq!(before, urls.len(), "a relay may appear at most once per batch");
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Days already recorded are not candidates, or a pass would redo work
    /// forever and never reach the days it has not seen.
    #[test]
    fn finished_days_are_never_redrawn() {
        let (st, dir) = state("done");
        let t = targets(1);
        let cfg = super::ScrapeConfig::default();
        let mut rng = rand::thread_rng();
        let win = (0u64, u64::MAX); // whole corpus, for tests about other rules
        let inflight = std::sync::Mutex::new(std::collections::HashSet::new());

        // Record every day this relay could be asked for.
        let now = chrono::Utc::now().timestamp() as u64;
        let today = now - (now % 86_400);
        let span = (today - cfg.min_date) / 86_400;
        for back in 1..=span {
            let (date, ..) = super::day_bounds(back);
            st.put_day(&date, &t[0].0, &super::DayDone { seen: 1, new: 0, at: now });
        }

        for _ in 0..20 {
            assert!(
                super::plan_batch(&t, &st, &cfg, 50, &mut rng, win, &inflight).is_empty(),
                "a fully-scraped relay must yield no work"
            );
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Finding data must not truncate the history still to be scraped.
    ///
    /// `birthday` records the oldest day a relay has *returned* events for.
    /// It was also used as the floor for drawing days, so the first
    /// successful draw pinned the relay's history there and nothing older was
    /// ever sampled again -- a relay whose first random hit landed in 2026
    /// would never be scraped for 2022.
    #[test]
    fn days_older_than_the_oldest_known_data_are_still_drawn() {
        let (st, dir) = state("bday");
        let now = chrono::Utc::now().timestamp() as u64;
        let today = now - (now % 86_400);
        let birthday = today - 3 * 86_400;

        let mut info = super::RelayInfo::default();
        info.birthday = Some(birthday);
        let t = vec![("wss://r.example".to_string(), info)];
        let cfg = super::ScrapeConfig::default();
        let mut rng = rand::thread_rng();
        let win = (0u64, u64::MAX); // whole corpus, for tests about other rules
        let inflight = std::sync::Mutex::new(std::collections::HashSet::new());

        let mut saw_older = false;
        for _ in 0..200 {
            for (_, _, _, start, _) in super::plan_batch(&t, &st, &cfg, 10, &mut rng, win, &inflight) {
                assert!(start >= cfg.min_date, "drew below the configured floor");
                if start < birthday {
                    saw_older = true;
                }
            }
        }
        assert!(
            saw_older,
            "days older than the oldest known data must still be drawn, or a \
             relay's history is truncated at whichever day happened to be hit \
             first"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A dead relay is not drawn until its retry time passes.
    ///
    /// With work sampled at random a dead relay is not walked past once, it
    /// reappears in batch after batch, burning a slot each time on a host that
    /// is not answering.
    #[test]
    fn dead_relays_are_not_drawn_until_the_retry_time() {
        let (st, dir) = state("dead");
        let now = chrono::Utc::now().timestamp() as u64;

        let mut dead = super::RelayInfo::default();
        dead.dead_until = Some(now + 3600);
        let mut alive = super::RelayInfo::default();
        alive.dead_until = Some(now - 1); // sentence expired

        let t = vec![
            ("wss://dead.example".to_string(), dead),
            ("wss://alive.example".to_string(), alive),
        ];
        let cfg = super::ScrapeConfig::default();
        let mut rng = rand::thread_rng();
        let win = (0u64, u64::MAX); // whole corpus, for tests about other rules
        let inflight = std::sync::Mutex::new(std::collections::HashSet::new());

        let mut saw_alive = false;
        for _ in 0..100 {
            for (url, ..) in super::plan_batch(&t, &st, &cfg, 10, &mut rng, win, &inflight) {
                assert_ne!(url, "wss://dead.example", "a dead relay must not be drawn");
                if url == "wss://alive.example" {
                    saw_alive = true;
                }
            }
        }
        assert!(saw_alive, "an expired sentence must let the relay back in");
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Draws stay inside the window they are given.
    ///
    /// Filling newest-first is the whole point: complete randomness across
    /// four years spread every relay thinly over the corpus, so nothing
    /// finished and recent history filled no faster than 2022.
    #[test]
    fn draws_are_confined_to_the_window() {
        let (st, dir) = state("window");
        let t = targets(40);
        let cfg = super::ScrapeConfig::default();
        let mut rng = rand::thread_rng();

        let now = chrono::Utc::now().timestamp() as u64;
        let win = super::block_window(now - (now % 86_400), 0);
        let inflight = std::sync::Mutex::new(std::collections::HashSet::new());
        for _ in 0..40 {
            for (_, _, _, start, _) in super::plan_batch(&t, &st, &cfg, 20, &mut rng, win, &inflight) {
                assert!(
                    start >= win.0 && start < win.1,
                    "drew {start} outside the window {win:?}"
                );
            }
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Blocks are contiguous, uniform, and walk strictly backwards.
    #[test]
    fn blocks_tile_backwards_from_yesterday_without_gaps() {
        let now = chrono::Utc::now().timestamp() as u64;
        let today = now - (now % 86_400);

        let (s0, e0) = super::block_window(today, 0);
        // Today is still being written; window 0 must end where today begins.
        assert_eq!(e0, today, "window 0 must end at yesterday, not include today");
        assert_eq!(
            (e0 - s0) / 86_400,
            super::BLOCK_DAYS,
            "every window is the same width"
        );

        let mut prev_start = s0;
        for k in 1..24 {
            let (s, e) = super::block_window(today, k);
            assert_eq!(e, prev_start, "block {k} must abut the previous, no gap");
            assert_eq!((e - s) / 86_400, super::BLOCK_DAYS, "uniform width");
            assert!(s < prev_start, "each block must reach strictly older days");
            prev_start = s;
        }
    }

    /// A window with one day left must still yield it.
    ///
    /// The sampler took 8 random draws and gave up. On a nearly-complete
    /// window those draws almost always land on finished days, so the relay
    /// yields nothing, the batch comes back empty, and the caller reads that
    /// as "window exhausted" and steps past it -- permanently, with days still
    /// unscraped. This is the case that must never report exhaustion falsely.
    #[test]
    fn a_single_remaining_day_is_always_found() {
        let (st, dir) = state("lastday");
        let t = targets(1);
        let cfg = super::ScrapeConfig::default();
        let mut rng = rand::thread_rng();

        let now = chrono::Utc::now().timestamp() as u64;
        let today = now - (now % 86_400);
        let win = super::block_window(today, 0);
        let inflight = std::sync::Mutex::new(std::collections::HashSet::new());

        // Finish every day in the window except one.
        let mut keep_open: Option<String> = None;
        for off in 0..super::BLOCK_DAYS {
            let start = win.0 + off * 86_400;
            if start >= win.1 {
                break;
            }
            let back = (today - start) / 86_400;
            let (date, ..) = super::day_bounds(back);
            if keep_open.is_none() {
                keep_open = Some(date);
                continue;
            }
            st.put_day(&date, &t[0].0, &super::DayDone { seen: 1, new: 0, at: now });
        }
        let open = keep_open.expect("the window must contain at least one day");

        // Every attempt must find it. A sampler that gives up would miss it
        // most of the time.
        for _ in 0..25 {
            let b = super::plan_batch(&t, &st, &cfg, 50, &mut rng, win, &inflight);
            assert_eq!(b.len(), 1, "the one unfinished day must be scheduled");
            assert_eq!(b[0].2, open, "and it must be that day");
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    /// The cut keeps the most-advertised relays and is stable across passes.
    #[test]
    fn the_relay_cut_keeps_the_most_advertised_and_is_deterministic() {
        let mk = |url: &str, n: u32| {
            let mut i = super::RelayInfo::default();
            i.sources = n;
            (url.to_string(), i)
        };
        let all = vec![
            mk("wss://d.example", 1),
            mk("wss://a.example", 900),
            mk("wss://c.example", 5),
            mk("wss://b.example", 100),
        ];

        let kept = super::top_by_sources(all.clone(), 50);
        let urls: Vec<&str> = kept.iter().map(|(u, _)| u.as_str()).collect();
        assert_eq!(
            urls,
            vec!["wss://a.example", "wss://b.example"],
            "the cut must keep the most-advertised half"
        );

        // Stability matters: a boundary that moves between passes leaves the
        // relays either side of it permanently half-scraped.
        for _ in 0..20 {
            let again = super::top_by_sources(all.clone(), 50);
            assert_eq!(
                again.iter().map(|(u, _)| u.clone()).collect::<Vec<_>>(),
                kept.iter().map(|(u, _)| u.clone()).collect::<Vec<_>>(),
                "the same input must always yield the same cut"
            );
        }

        // 100 disables it; a tiny set must never cut to nothing.
        assert_eq!(super::top_by_sources(all.clone(), 100).len(), 4);
        assert_eq!(
            super::top_by_sources(vec![mk("wss://only.example", 3)], 50).len(),
            1,
            "rounding down must not stop the scraper on a small relay set"
        );
        assert!(super::top_by_sources(vec![], 50).is_empty());
    }

    /// A unit already handed to a worker must not be drawn again.
    ///
    /// day_done only becomes true once a unit completes, so without an
    /// in-flight guard the planner reissues work that is already running --
    /// two connections to the same relay for the same day.
    #[test]
    fn work_in_flight_is_not_redrawn() {
        let (st, dir) = state("inflight");
        let t = targets(1);
        let cfg = super::ScrapeConfig::default();
        let mut rng = rand::thread_rng();
        let now = chrono::Utc::now().timestamp() as u64;
        let win = super::block_window(now - (now % 86_400), 0);
        let inflight = std::sync::Mutex::new(std::collections::HashSet::new());

        let first = super::plan_batch(&t, &st, &cfg, 1, &mut rng, win, &inflight);
        assert_eq!(first.len(), 1, "a fresh relay must yield work");
        let (url, _, date, ..) = first[0].clone();
        inflight.lock().unwrap().insert(format!("{url}|{date}"));

        for _ in 0..50 {
            for (u, _, d, ..) in super::plan_batch(&t, &st, &cfg, 5, &mut rng, win, &inflight) {
                assert!(
                    !(u == url && d == date),
                    "{u} {d} is already running and must not be reissued"
                );
            }
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    /// One stalled unit must not stop the others finishing.
    ///
    /// The pass used to await a whole batch before drawing the next, so every
    /// batch ran at the speed of its slowest member. Among 8000+ relays there
    /// is always one that connects and then stalls, and 49 workers idled
    /// waiting for it -- a few relay-days a minute with fifty configured.
    #[tokio::test]
    async fn a_stalled_unit_does_not_block_the_others() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let limit = 4usize;
        let sem = Arc::new(tokio::sync::Semaphore::new(limit));
        let done = Arc::new(AtomicUsize::new(0));

        // One unit stalls far longer than the rest. With a batch barrier
        // nothing after it could start until it finished.
        for i in 0..40usize {
            let permit = sem.clone().acquire_owned().await.unwrap();
            let done = done.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let ms = if i == 0 { 60_000 } else { 5 };
                tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                done.fetch_add(1, Ordering::SeqCst);
            });
        }
        tokio::time::sleep(std::time::Duration::from_millis(800)).await;

        let n = done.load(Ordering::SeqCst);
        assert!(
            n >= 39,
            "every unit but the stalled one should have finished; only {n} did"
        );
    }

    /// Permits must be taken before spawning, not inside the task.
    ///
    /// Spawning first parks one live task per known relay on the semaphore.
    /// With discovery uncapped that is thousands of tasks to run `limit` of
    /// them, each holding its own clones of the state, sink and config. This
    /// reproduces the shape of the pass loop and asserts that the number of
    /// tasks in flight never exceeds the limit.
    #[tokio::test]
    async fn live_tasks_never_exceed_the_concurrency_limit() {
        let limit = 4usize;
        let targets = 200usize;
        let sem = Arc::new(tokio::sync::Semaphore::new(limit));
        let live = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..targets {
            let permit = sem.clone().acquire_owned().await.unwrap();
            let (live, peak) = (live.clone(), peak.clone());
            handles.push(tokio::spawn(async move {
                let _permit = permit;
                let n = live.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(n, Ordering::SeqCst);
                tokio::task::yield_now().await;
                live.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for h in handles {
            let _ = h.await;
        }

        let peak = peak.load(Ordering::SeqCst);
        assert!(
            peak <= limit,
            "at most {limit} relay/day queries may be in flight, saw {peak}"
        );
    }
}
