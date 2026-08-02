# nostrsearch — Unified Merge Plan

nostrsearch becomes **the master source for all Nostr data**. Two sibling
projects are absorbed as *consumers/enrichers* of the corpus rather than
independent event collectors:

| Repo | Role today | Role after merge |
|---|---|---|
| **nostrsearch** (this) | Tantivy full-text corpus (~900M events), REST API, planned NIP-50 relay | **Master data plane** — single ingest, single event store, WoT-scored search |
| **nostr-profiles / nostr-classify** | Live relay collector + LLM profile classification, SQLite/FTS5, dashboard | **Profile enrichment plane** — reads events *from* nostrsearch, writes classifications to SQLite |
| **nostr-dashboard / nostr-report** | Batch stats (pagerank, followers, activity, clients) from the archive, JSON reports, React dashboard | **Analytics plane** — computes stats from the corpus, **feeds pagerank/follower WoT back into search scoring** |

The unifying fact: **all three already read the same hole.v0l.io archive via
`nostr-archive-cursor`** (nostrsearch's indexer switched to it in `be60f16`;
nostr-dashboard depends on it directly). classify is the only one still doing
its own live relay collection — that becomes the one live-ingest path feeding
the master corpus.

---

## Target workspace layout

```
nostrsearch/
├── crates/
│   ├── nostrsearch-core         # event model, schema, query, scoring, shard   (unchanged, +profile/stat signals)
│   ├── nostrsearch-indexer      # archive/live ingest → Tantivy shards         (+live relay collector from classify)
│   ├── nostrsearch-server       # REST + NIP-50; fans out over shards          (unchanged core)
│   ├── nostrsearch-stats        # NEW — from nostr-dashboard `shared`+`precompute`: reports + pagerank/WoT precompute
│   └── nostrsearch-classify     # NEW — from nostr-classify: LLM classification, SQLite/FTS5, image/video/OG tooling
├── dashboards/
│   ├── profiles/                # from nostr-classify/dashboard (Preact/Vite)
│   └── reports/                 # from nostr-dashboard/nostr-report-app (React/Vite)
└── docs/MERGE_PLAN.md
```

One Cargo workspace, one `edition = "2024"`, shared `[workspace.dependencies]`
(pin `nostr-sdk`, `nostr-archive-cursor`, `tantivy`, `sqlx`, `axum` once).

---

## Data-flow after merge

```
                 hole.v0l.io archive (.jsonl.zst)      live relays (wss)
                          │                                   │
                          │  nostr-archive-cursor             │ nostr-sdk subscription
                          ▼                                   ▼
                 ┌─────────────────────────── nostrsearch-indexer ───────────────────────────┐
                 │  parse → route by created_at → per-month ShardWriter (Tantivy)             │
                 │  wot_lookup ◄──────────────── pagerank/follower tier (from stats)          │
                 └───────────────┬───────────────────────────────────────────────────────────┘
                                 │  <root>/<YYYY-MM>/  (master event store)
             ┌───────────────────┼───────────────────────────────┐
             ▼                   ▼                                ▼
   nostrsearch-server    nostrsearch-stats                nostrsearch-classify
   REST + NIP-50         pagerank / followers /           reads a pubkey's events from
   search over corpus    activity / clients / pubkey      the corpus (core registry or REST),
                         → JSON reports + WoT tiers        LLM-classifies → SQLite/FTS5
                                 │                                │
                                 ▼                                ▼
                         dashboards/reports              dashboards/profiles
```

Key: **classify stops caching its own events**. Its `nostr_collector` +
SQLite `events` table are removed; a `CorpusEventSource` fetches a pubkey's
events from the master store on demand. Only `profiles` + `classifications`
(+ FTS5) remain in SQLite.

---

## Phased execution

### Phase 0 — Repo prep (no behavior change)
- Vendor both sibling repos under `crates/` and `dashboards/` (git history via
  `git subtree`/`read-tree` if we want provenance, else plain copy).
- Unify to `edition = "2024"`, single workspace `Cargo.toml`, dedupe deps.
- CI: one workflow builds all crates + both dashboards.
- **Gate:** `cargo build --workspace` + both `bun run build` green.

### Phase 1 — Stats crate + WoT wiring (highest leverage, lowest risk)
- Import nostr-dashboard `shared` (StatObject, PreCursor, reports) +
  `precompute` as `nostrsearch-stats`. Reuse its `nostr-archive-cursor` reader.
- Expose a `WotProvider` from stats: `pubkey → tier` derived from pagerank
  percentile / follower count buckets (map to the existing 0..~4 `wot_tier`).
- Wire `ShardWriter::with_wot_lookup` to it in the indexer. This lights up the
  already-built-but-dormant WoT scoring path — immediate search-quality win.
- Serve report JSON from `nostrsearch-server` (or keep a thin stats API).
- **Gate:** pagerank/follower reports produced from a day dump; search results
  show non-zero `wot_tier`; reports dashboard renders.

### Phase 2 — Classify crate reading from the corpus
- Import nostr-classify as `nostrsearch-classify`; keep classifier.rs,
  image_cache, video, opengraph, job_queue, db (profiles + classifications +
  FTS5), search_relay, dashboard.
- Replace `nostr_collector` + SQLite `events` with a `CorpusEventSource` trait:
  - in-process impl over `nostrsearch-core` shard registry (preferred), or
  - HTTP impl over `nostrsearch-server` `/search`/`/event`.
- `get_profile_events` / follower counts now hit the corpus (+ stats follower
  graph) instead of live relays.
- Drop the `events`, retention-sweep, and `cache_days` machinery; migrate the
  SQLite schema (remove events table, keep profiles/classifications/FTS5).
- **Gate:** classify a known pubkey end-to-end pulling events only from the
  corpus; profiles dashboard + FTS5 search relay work; e2e_test ported.

### Phase 3 — Live ingest unification (optional but on-mission)
- Promote classify's relay subscription into `nostrsearch-indexer` as a *live
  source* that writes to the same shards (open-shard for current month).
- Now nostrsearch has **both** ingest paths (archive backfill + live tail),
  making it the true master. classify + stats both consume one store.
- **Gate:** live events appear in search within the commit interval; no
  duplicate event stores anywhere.

### Phase 4 — Config, ops, docs
- One `config.yaml` schema: `[index]`, `[relays]`, `[llm]`, `[stats]`,
  `[classify]`, `[server]`. Env-overridable.
- One Dockerfile (multi-stage: Bun builds both dashboards → Rust workspace →
  slim runtime with ffmpeg for classify's video path). Compose profiles:
  `ingest`, `server`, `stats`, `classify`.
- k8s manifests updated; README rewritten to describe the three planes.

---

## Pluggable analysis framework (`nostrsearch-stats`)

> Note: the previously-recalled `/core/old_kieran` work does **not** exist on
> disk or in any git branch/stash/history of nostrsearch, nostr-dashboard,
> nostr-classify, nostrhole, or nostr-archive-cursor. The real prior art is
> nostr-dashboard's `StatObject` trait; we generalize it below.

The stats crate is built around a generic, pluggable analysis trait so new
collectors (trending algos, per-kind stats, zap flows, …) drop in without
touching the pipeline. It generalizes `StatObject` with **map-reduce merge**
(parallel over time-shards) and **kind pre-filtering**, and folds over the
canonical `nostrsearch_core::event::NostrEvent` so a single parse feeds both
indexing and analysis (the shared batch+live stream).

```rust
pub trait Analysis: Send + Sync {
    type Output: Serialize + DeserializeOwned + Send;
    fn name(&self) -> &'static str;
    fn kinds(&self) -> Option<&[u16]> { None }          // None = all kinds
    fn observe(&mut self, ev: &NostrEvent, ctx: &AnalysisCtx);
    fn merge(&mut self, other: Self) where Self: Sized;  // combine shard partials
    fn snapshot(&self) -> Self::Output;                  // typed result for API/persist
}
```

- `AnalysisCtx` exposes precomputed context (WoT tier, pagerank, follower
  count, `now`) from the PreCursor so analyses stay pure folds.
- An object-safe `DynAnalysis` wrapper (erasing `Output` to `serde_json::Value`)
  + a `Registry` let collectors register in one line and run automatically over
  **both** the archive backfill and the live tail, sharing one event stream.
- Existing dashboard reports port mechanically: `incr` → `observe`, add a
  `merge`, `Display`/JSON → `snapshot`.
- The pagerank/follower analyses additionally expose a `WotProvider`
  (`pubkey → tier`) consumed by `ShardWriter::with_wot_lookup`.

**Run drivers** (same trait, two callers):
- *Batch*: parallel pass over archive files → per-file partials → `merge`.
- *Live*: the indexer's live tail calls `observe` on each new event; periodic
  `snapshot` persists/serves.

### Additive + resumable (realtime)

Each analysis carries its own [`Progress`] (epoch + `created_at` watermark +
boundary-id dedup set). This makes the framework **additive**: registering a
brand-new analysis (watermark 0) triggers a full backfill scan of the corpus
*for that analysis only*, while already-computed analyses resume from their
saved watermark and just tail live events. State is persisted via `StatStore`
(full checkpoint + progress per analysis); bumping an analysis's `epoch()`
discards its saved state and re-scrapes from scratch. After backfill completes,
the same `observe` path folds live events in realtime with count-once
semantics across the backfill→live boundary.

### Inter-dependent analyses (staged execution)

Analyses declare `deps()` (by name). The registry topologically sorts them into
**stages**; each producer stage `contribute`s its results into a shared owned
`World` (follower counts, WoT tiers, pagerank), and downstream stages read that
`World` through `AnalysisCtx`. A reusable `PublisherFilter { min_followers,
min_wot_tier }` lets a consumer skip publishers below a threshold (e.g. "don't
count stats for users with < 10 followers"). This mirrors nostr-dashboard's
`PreCursor` → report flow: e.g. `follow_graph` (producer) → WoT/pagerank →
follower/WoT-filtered activity & trending (consumers). Backfill runs one
ascending pass per stage so each stage sees the prior stage's `World`; the
staging/`World` protocol is identical for a Tantivy/archive-streamed runner.

### Efficiency, refresh scheduling & observability (implemented)

- **Compact keys** — `types::Hash32` (`[u8;32]`, `Copy`, `Hash`) replaces
  64-char hex strings for all pubkeys/ids. Format-aware serde: hex in JSON (for
  the dashboard), raw 32 bytes under bincode (verified in a test).
- **Binary checkpoints** — `StatStore` persists each analysis as bincode
  (`<name>.state.bin` + `.progress.bin`); no giant `serde_json::Value` tree in
  RAM at corpus scale.
- **Single per-pubkey `World`** — one `HashMap<Pubkey, PubkeyStat{followers,
  wot_tier, pagerank}>` instead of three parallel maps.
- **Per-analysis refresh interval** — `refresh_interval()` + `refresh()`.
  Cheap analyses fold every event (`None`); expensive ones (pagerank) accumulate
  the graph in `observe` and run the costly recompute only on a schedule
  (`Some(24h)`). The runner gates `refresh()` by wall-clock and then
  `contribute`s the cached result. `FollowGraph` keeps follower counts fully
  incremental (diff on kind-3 replace) so it needs no refresh at all.
- **Realtime metrics** — the `Registry` emits `MetricsEvent`s to a
  transport-agnostic `MetricsObserver` (server bridges to WS/SSE): an initial
  `Snapshot` of full pipeline state, periodic `Tick` frames (EWMA
  events/sec + per-analysis observed/consumed/filtered/watermark/backfill/
  refresh timing), plus `Refreshed` and `BackfillComplete` lifecycle events.
  `BufferObserver` keeps the latest snapshot + a bounded ring for a pull
  endpoint.

**Status:** `nostrsearch-stats` implements all of the above —
`Analysis` trait (observe→bool/merge/snapshot + epoch/deps/refresh/contribute/
kinds), `Hash32`, `World`/`PubkeyStat`/`PublisherFilter`, `Progress` (watermark
+ boundary dedup), binary `StatStore`, staged `Registry` with metrics +
refresh scheduling, `backfill_in_memory` runner, `metrics` (events + observers),
and example analyses (`FollowGraph`, `Pagerank` 24h-refresh producer,
`KindBreakdown` w/ publisher filter, `TrendingHashtags`). **8 tests pass**,
covering dependency-staging, publisher-filtering, additive binary resume,
pagerank scheduled refresh, and realtime metrics emission.

### WoT → search scoring + streaming runner (implemented)

The producer→scoring loop is wired end to end:

- **`wot::WotIndex`** — compact `pubkey→tier` map (non-zero tiers only), binary
  save/load; built from the materialized `World`.
- **`wot::SharedWot`** — `Arc<RwLock<WotIndex>>`, cloneable and hot-swappable;
  `lookup()` yields the `Fn(&str)->u8` closure for
  `ShardManager::with_wot_lookup`, so ingest picks up fresh trust without a
  restart (verified by a hot-swap test).
- **`stats` binary** (`nostrsearch-indexer/src/bin/stats.rs`) — streams the
  archive via `nostr-archive-cursor` through `FollowGraph` + `Pagerank`
  (`observe_backfill` handles the archive's unordered delivery), materializes
  the `World`, persists resumable analysis state to a `StatStore`, and writes a
  `WotIndex` snapshot.
- **`ingest --wot <file>`** loads that snapshot (tolerant if absent) so every
  indexed doc carries a real `wot_tier`, lighting up the `CompositeCollector`
  boost.

Pipeline: `stats` (archive → WoT snapshot) → `ingest --wot` (snapshot →
`wot_tier` fast field) → search ranks by BM25×(1+wot·tier+recency).

### Unified ingest: archive + firehose, one pipeline (implemented)

Static JSONL and the live relay firehose are now two *sources* feeding a single
[`Pipeline`] that fans each event out to **both** the Tantivy index and the
stats/WoT engine — no separate stats pass required.

```text
  JSONL dumps ──┐                    ┌─► stats Registry (follow-graph, pagerank, …)
 (archive cursor)├─► Pipeline::process┤
  relay firehose ┘   (one event)      └─► ShardManager  (writes wot_tier)
 (nostr-sdk WS)                              ▲
                                             └── SharedWot hot-swapped every
                                                 --wot-refresh-every events
```

- **`pipeline.rs`** — owns `ShardManager` + `Registry` + `World` + `SharedWot`;
  `process()` folds stats then indexes with the current tier; `refresh_wot()`
  re-materializes, rebuilds the `WotIndex`, **hot-swaps** it into the live
  lookup, persists state, emits a metrics tick; `go_live()` flips from
  backfill to watermark-gated live folding.
- **`firehose.rs`** — nostr-sdk relay tail (`since(now)`, reconnect loop),
  converting sdk events to the canonical `NostrEvent`.
- **`ingest` CLI** — `--input-dir` (backfill), `--relays` (live, repeatable),
  or **both** (backfill then tail). `--wot-refresh-every`, `--state-dir`,
  `--wot-out`.
- **Warm start** — on startup, if analysis state was restored, the pipeline
  materializes WoT immediately so documents are indexed with real tiers from
  event #1 (without this, everything before the first refresh got tier 0).

**Verified end to end** on a synthetic corpus (12 followers → one trusted
author + an untrusted spammer): before the warm-start fix the spam outranked
the trusted author (0.6775 vs 0.5550, all tiers 0); after, the trusted author
scores **1.6649 vs 0.6775** — exactly the `1 + 0.5×tier(4)` = 3× boost.

**Deploy wiring:** Dockerfile builds `ingest` + `stats` + `archive` + server.
Because the server is now the unified node, the per-role deploy variants
collapsed to one of each: compose has `server` (the node) plus an opt-in
`ingest` profile for bulk backfill, and k8s is a single `k8s/nostrsearch.yaml`
(Namespace + PVC + Service + Deployment) with the optional roles as commented
env vars and the backfill Job commented at the bottom.

### nostrhole absorbed (implemented)

nostrhole was the *upstream producer* of the corpus nostrsearch consumed, so the
data made a full round trip: relays → nostrhole → JSONL → hole.v0l.io HTTP →
download → nostrsearch ingest. Plus **two independent relay subscriptions**.
All four of its roles now live in nostrsearch:

| nostrhole role | Where it lives now |
|---|---|
| Ingest (firehose → JSONL archive) | `firehose.rs` — `JsonFilesDatabase` attached to the *same* client that feeds the pipeline |
| HTTP archive serving | `nostrsearch-server::archive` — `/archive`, `/archive/files`, `/archive/{file}` |
| Nostr relay (inbound writes) | `nostrsearch-server::relay` — WS upgrade at `/` → `LocalRelay` |
| Maintenance | `archive` binary — `--stats`, `--rebuild-index` |

One subscription now writes **three** sinks:

```text
                          ┌─► JsonFilesDatabase (.jsonl.zst archive + id index)
relays ──► ONE client ────┼─► ShardManager      (Tantivy search index)
                          └─► stats Registry    (WoT / trending)
```

This works because `JsonFilesDatabase` implements nostr-sdk's `NostrDatabase`:
the client persists each verified event *before* emitting the notification the
pipeline consumes, so dedup + signature verification come free.

**Deployment constraint discovered:** the archive's RocksDB id index is an
**exclusive per-process lock**. nostrhole was a single process sharing one DB
handle, but nostrsearch splits ingest and server. Fix: archive *file serving*
needs no index (it's filesystem work), so `ArchiveState::open` is **index-free**
by default and `open_with_index` (required only by the relay, which must persist
events) takes the lock. Verified: two processes serve the same archive
concurrently — one holding the lock with the relay enabled, one lock-free.

So exactly one process may own the archive index:
- firehose with `--archive-dir` (normal production), **or**
- server with `ENABLE_RELAY=1` — not both.

Verified end to end by `tests/relay_archive.rs`: a signed event published to the
relay is persisted to the archive and exposed by the HTTP listing.

**Remaining nostrhole gap:** `prune_ephemeral` (rewrites completed archives to
drop kinds 20000-29999) is not ported — the firehose already excludes ephemeral
kinds at subscribe time, so it only matters for cleaning pre-existing archives.
`repair_count` was dropped as redundant (`--rebuild-index` recomputes it) and
isn't in nostrsearch's pinned cursor rev.

### 1B+ scale caveats (documented, not yet addressed)

The aggregate/trending analyses scale to 1B+ on this design. The remaining
per-pubkey/graph producers (`FollowGraph`, `Pagerank`) still hold the full
adjacency in RAM — fine for the example, but at 1B they need an on-disk graph
(RocksDB/CSR) and the offline-pagerank path from nostr-dashboard. The trait /
staging / metrics / refresh contract stays identical; only the producer's
storage backend changes.

### Dashboard reports ported (implemented)

The reports from nostr-dashboard's `shared/reports` now run as first-class
`Analysis` impls inside the same single pass as indexing:

| Upstream report | Here | Notes |
|---|---|---|
| `activity` | `analyses::Activity` | per-day kind counts + zap volume |
| `active_users` | `analyses::ActiveUsers` | DAU/WAU, exact distinct sets |
| `clients` (`client_tags`) | `analyses::Clients` | client market share |
| `followers` | `analyses::FollowGraph` | already present (WoT producer) |
| `pagerank` | `analyses::Pagerank` | already present |
| `pubkey_stats` | **not ported** | per-pubkey x day x kind timelines are a
  profile-page feature, not a search-engine one: O(pubkeys x days x kinds) for
  something nothing in nostrsearch consumes. Revisit only if a profile view needs it. |

Correctness fixes applied on the way in (upstream bugs, not ports of them):

- **bolt11 parsing** uses the `lightning-invoice` crate instead of a hand-rolled
  HRP splitter. Upstream's multipliers were each 1000x too small *and* the
  result was divided by 1000 again; `lnbc` was stripped before `lnbcrt`; and
  splitting the HRP on `p` confused the pico multiplier with the bech32
  separator. Verified against the BOLT-11 spec vectors.
- **Zap attribution.** A kind-9735 receipt is signed by the recipient's LNURL
  server, so upstream's trusted/untrusted split on `ev.pubkey` measured zapper
  *services*. Value is now attributed to the `P`/zap-request sender and the `p`
  recipient (`zaps_sent_sats` / `zaps_received_sats`), and the amount is taken
  from the receipt's `bolt11` (what was paid) ahead of the request's `amount`
  (what was asked).
- **Client key space is bounded.** `client` is attacker-controlled freeform
  text; the map is now normalized (case, `" - "`/`@` version suffixes, length)
  and capped at `MAX_CLIENTS`, overflowing into `(other)`.

### Staged streaming backfill (implemented)

`Analysis::deps()` was only honoured by the in-memory staged runner. The
streaming `Pipeline` fed every analysis in one pass, so on a cold corpus the
reports folded against an empty `World`: every author read as untrusted and any
follower-filtered analysis dropped everything.

`Pipeline` is now stage-aware. `backfill_passes()` reports one pass per
dependency stage, `process()` folds only the current stage, and `advance_pass()`
materializes the finished stage into the `World` before the next begins.
**Only pass 0 indexes** — later passes replay the archive purely to feed
dependent analyses (and the id-store dedupe gate is bypassed for them). With the
default set this is 2 passes: WoT producers + client stats, then the reports.

The cost is a second read of the corpus. That is the price of a correct trust
split; an analysis that does not need `World` (like `Clients`) stays in stage 0
and is unaffected.

### Realtime partial updates (implemented)

Full snapshots cannot show a number *moving*, and the activity report carries
every day the corpus has seen. So `Analysis::drain_delta()` lets each impl emit
**its own** partial change since the last drain, as a JSON merge patch (RFC
7386) over that analysis's snapshot shape:

- `Activity` emits only the day buckets it touched (in practice, today's).
- `ActiveUsers` emits only buckets whose counts actually moved — a repeat
  publisher produces no traffic at all.
- `Clients` emits only the clients that published.
- Default is `None`: an analysis without an incremental view is simply polled.

`ActiveUsersReport` uses key->bucket maps rather than arrays specifically so a
patch and a snapshot have the same shape (a JSON array would have to be replaced
wholesale). Dirty sets are `#[serde(skip)]` — realtime-only, never checkpointed.

Serving: the writer task owns the `Pipeline`, so it *publishes* rather than
exposing it. Full snapshots go out on the commit tick; deltas are drained every
second and fanned out over a broadcast channel.

| Endpoint | Purpose |
|---|---|
| `GET /reports` | available reports + `generated_at` |
| `GET /reports/{name}` | full snapshot (dashboard seeds from this) |
| `GET /reports/stream` | SSE of `{name, patch}` frames; `lagged` tells a slow client to re-sync |

The invariant under test: seed from `/reports/{name}`, merge-patch each streamed
frame, and the result equals the next full snapshot — verified both through the
real `Pipeline` and over real HTTP.

## Decisions still open

1. **Stats storage** — nostr-dashboard writes JSON report files + in-memory
   PreCursor. Keep file-based reports, or persist pagerank/followers into
   SQLite/Tantivy for queryability? (Default: keep JSON reports for the
   dashboard, expose `WotProvider` in-memory/rebuilt on ingest.)
2. **Corpus access from classify** — in-process (link `nostrsearch-core`, one
   binary/host, fastest) vs. HTTP (decoupled, network cost). Default:
   in-process, with the HTTP impl behind the same trait for the distributed
   deployment.
3. **Dashboards** — keep the two SPAs separate (`/reports`, `/profiles`) or
   merge into one shell later? Default: keep separate now, unify shell later.
4. **Signature verification** — corpus is currently trust-the-archive (no sig
   check). Live ingest (Phase 3) from relays should verify sigs before write.

---

## Why this order

Phase 1 first because it's **pure additive value with no destructive change**
and it activates scoring infrastructure nostrsearch already built. Phase 2 is
the real "merge" and is where SQLite/event-cache removal happens, so it goes
after the corpus is proven as a read source. Phase 3 makes nostrsearch the
authoritative *live* source, which is the endgame of "master source for all
data." Phases can ship independently.
