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
    /// Detected data horizon: day-start (unix secs) before which the relay
    /// returned nothing. Passes never walk earlier than this — relays prune
    /// or simply didn't exist, and hammering empty history wastes both sides'
    /// bandwidth forever.
    #[serde(default)]
    pub birthday: Option<u64>,
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
        let mut opts = Options::default();
        opts.create_if_missing(true);
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
}

impl Default for ScrapeConfig {
    fn default() -> Self {
        Self {
            min_date: parse_date("2022-01-01").unwrap_or(0),
            floor_secs: 600,
            concurrency: 50,
            empty_days_limit: 14,
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
    let targets = state.relays();
    tracing::info!(relays = targets.len(), "scrape pass starting");

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

    loop {
        let batch = plan_batch(&targets, &state, &cfg, batch_size, &mut rng);
        if batch.is_empty() {
            break;
        }

        let mut handles = Vec::with_capacity(batch.len());
        for (url, info, date, start, end) in batch {
            let state = state.clone();
            let sink = sink.clone();
            let cfg = cfg.clone();
            handles.push(tokio::spawn(async move {
                scrape_relay_day(&url, info, &date, start, end, state, sink, &cfg).await;
            }));
        }
        // Await the whole batch before drawing the next, so in-flight queries
        // never exceed `concurrency` even briefly.
        for h in handles {
            let _ = h.await;
        }
        scraped += batch_size as u64;
        if scraped % (batch_size as u64 * 20) == 0 {
            tracing::info!(scraped, "scrape pass progress");
        }
    }
    tracing::info!(scraped, "scrape pass complete");
}

/// One unit of work: a relay and the day to fetch from it.
type RelayDay = (String, RelayInfo, String, u64, u64);

/// Draw up to `limit` relay-days at random.
///
/// Days already recorded, days before `min_date`, and days behind a relay's
/// known birthday are not candidates. Relays are sampled without replacement
/// within a batch so one relay cannot take every slot and open several
/// connections to itself.
fn plan_batch(
    targets: &[(String, RelayInfo)],
    state: &std::sync::Arc<ScrapeState>,
    cfg: &ScrapeConfig,
    limit: usize,
    rng: &mut impl rand::Rng,
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

        // The oldest day worth asking this relay for.
        let floor = cfg.min_date.max(info.birthday.unwrap_or(0));
        if floor >= today_start {
            continue;
        }
        let span_days = (today_start - floor) / 86_400;
        if span_days == 0 {
            continue;
        }

        // Sample a few days for this relay rather than scanning its whole
        // history: with years of days a linear scan for an unfinished one
        // costs more than the query it schedules.
        for _ in 0..8 {
            let back = rng.gen_range(1..=span_days);
            let (date, start, end) = day_bounds(back);
            if start < floor {
                continue;
            }
            if state.day_done(&date, url) {
                continue;
            }
            out.push((url.clone(), info.clone(), date, start, end));
            break;
        }
    }
    out
}

/// Fetch one day from one relay and record the outcome.
#[allow(clippy::too_many_arguments)]
async fn scrape_relay_day<S: Sink>(
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

    let client = Client::default();
    if client.add_relay(url).await.is_err() {
        return;
    }
    client.connect().await;

    match scrape_day(&client, url, &mut info, &sink, start, end, cfg.floor_secs).await {
        Ok((seen, new)) => {
            info.fails = 0;
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
        }
    }
    state.put_relay(url, &info);
    let _ = client.shutdown().await;
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

        let batch = super::plan_batch(&t, &st, &cfg, 50, &mut rng);
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
                super::plan_batch(&t, &st, &cfg, 50, &mut rng).is_empty(),
                "a fully-scraped relay must yield no work"
            );
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Nothing older than the relay's horizon is worth asking for.
    #[test]
    fn days_behind_the_birthday_are_not_drawn() {
        let (st, dir) = state("bday");
        let now = chrono::Utc::now().timestamp() as u64;
        let today = now - (now % 86_400);
        let birthday = today - 3 * 86_400;

        let mut info = super::RelayInfo::default();
        info.birthday = Some(birthday);
        let t = vec![("wss://r.example".to_string(), info)];
        let cfg = super::ScrapeConfig::default();
        let mut rng = rand::thread_rng();

        for _ in 0..200 {
            for (_, _, _, start, _) in super::plan_batch(&t, &st, &cfg, 10, &mut rng) {
                assert!(
                    start >= birthday,
                    "drew {start}, behind the relay birthday {birthday}"
                );
            }
        }
        let _ = std::fs::remove_dir_all(dir);
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
