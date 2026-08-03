# nostrsearch

Full-text search over the entire Nostr event corpus, in Rust on
[Tantivy](https://github.com/quickwit-oss/tantivy).

One process is a search API, a NIP-50-shaped query planner, an archive server,
an archival relay, a live firehose ingest and a stats/web-of-trust engine —
over a single time-sharded index, a single archive, and a single writer.

Target corpus: the **hole.v0l.io** archive, ~900M events / ~763 GiB of JSONL
dumps.

## Status

Single-node engine works end to end. The scale-out layer is designed, not
built.

| | |
|---|---|
| Ingest | archive backfill + live firehose, both feeding index and stats |
| Search | BM25 × (WoT + recency), NIP-50 search string + structured filters |
| Hydration | hits carry the complete signed event, fetched by id from the archive |
| Archive | serves the corpus, indexes it by id, accepts relay writes |
| API | `/search`, `/event/{id}`, `/stats`, `/archive/*`, `/reports/*`, `/admin/*` |
| Console | built into the binary, live-patched from `/reports/stream` |
| Tests | 152 passing |

Throughput measured on real hole.v0l.io data at ~122k events/sec (48 cores),
and ~2.5 GB of index per 2.8 GB day dump. Both predate the current schema:
documents now carry more fields and CJK text is bigrammed, while `content` is
no longer stored twice. Neither number has been re-measured since — treat them
as the right order of magnitude, not a benchmark.

## Quick start

```bash
./scripts/build-dashboard.sh          # console is compiled into the binary
docker compose up --build             # node on :8080, console at /
```

Or from a checkout:

```bash
./scripts/build-dashboard.sh
cargo build --release

# backfill an archive directory, then serve it
./target/release/ingest --index-root ./data/index --input-dir ./dumps --exit-when-done
INDEX_ROOT=./data/index BIND=0.0.0.0:8080 ./target/release/nostrsearch-server
```

The console is compiled in from `dashboard/dist/index.html`, a build artifact
rather than a committed file, so build it before `cargo build` and again after
changing anything under `dashboard/` (needs [bun](https://bun.sh)). Docker and
CI do it themselves.

## Architecture

```
   dumps (.jsonl.zst)          upstream relays            inbound writes
          │                          │                          │
          ▼                          ▼                          ▼
    ┌─────────────┐            ┌───────────┐             ┌────────────┐
    │ ingest CLI  │            │ firehose  │             │   relay    │
    └──────┬──────┘            └─────┬─────┘             └──────┬─────┘
           │                         │                          │
           │                         ▼                          │
           │                  ┌──────────────┐                  │
           └─────────────────►│ archive (id  │◄─────────────────┘
                              │  index +     │   .jsonl.zst corpus, RocksDB
                              │  .jsonl.zst) │   index: id → shard + offset
                              └──────┬───────┘
                                     │
                              ┌──────▼───────┐
                              │ writer task  │  single owner of the pipeline
                              └──────┬───────┘
                                     │
                     ┌───────────────┴───────────────┐
                     ▼                               ▼
            ┌─────────────────┐            ┌──────────────────┐
            │ Tantivy shards  │            │ stats / WoT      │
            │ <root>/YYYY-MM/ │            │ analyses+reports │
            └────────┬────────┘            └────────┬─────────┘
                     │                              │
                     ▼                              ▼
            ShardRegistry: prune → fan out → merge → hydrate
                     │
                     ▼
              /search, /event/{id}   ──(by id)──►  archive
```

Two rules hold the design together:

- **One writer.** Tantivy shard writers, the RocksDB id index and the dedupe
  store all take exclusive locks, so exactly one process may write at a time.
  A node that is not writing serves search and the archive read-only, with no
  locks, beside a writer.
- **Time-sharded index.** One Tantivy index per month under
  `<root>/<YYYY-MM>/`, each with its own writer and commit policy. Time
  filters prune whole shards, merges stay bounded, cold shards can be closed,
  and ingest parallelism scales with cores instead of queueing on one
  `Mutex<IndexWriter>`.

## Crates

- **`nostrsearch-core`** — event model, Tantivy schema, script-aware
  tokenizer, language detection, NIP-19 decoding, time-shard layout, query
  planner, scoring. No I/O.
- **`nostrsearch-indexer`** — archive ingest, live firehose, `ShardManager`
  (per-shard writers, scheduled commits), dedupe store, network scrape, and
  the `ingest` / `archive` / `stats` / `scrape` binaries.
- **`nostrsearch-archive`** — the archive HTTP routes (listing, download,
  event-by-id), shared by the server node and by `ingest --bind`. Its own
  crate because the server depends on the indexer, so the shared half cannot
  live in either without a cycle.
- **`nostrsearch-stats`** — the analysis engine: follow graph, pagerank,
  activity, clients, relays, and the reports the console renders.
- **`nostrsearch-server`** — `ShardRegistry` (fan-out, merge, hydrate), axum
  API, archive HTTP, relay endpoint, NIP-98 admin, embedded console.
- **`dashboard/`** — the console (Preact + Vite), see
  [dashboard/README.md](dashboard/README.md).

## Running a node

Every binary reads the same environment, so one container image configures all
of them; flags override.

| Variable | Meaning | Default |
|---|---|---|
| `INDEX_ROOT` | Tantivy shard root | `./data/index` |
| `BIND` | listen address | `0.0.0.0:8080` |
| `ARCHIVE_DIR` | `.jsonl.zst` corpus + id index; enables `/archive` | unset |
| `ENABLE_RELAY` | accept inbound writes at `/` (needs `ARCHIVE_DIR`) | off |
| `RELAY_KINDS` | kind whitelist for the relay | all |
| `RELAYS` | comma-separated upstreams; enables the firehose | unset |
| `STATE_DIR` | analysis state store | `./data/stats` |
| `WOT_OUT` | web-of-trust snapshot | `./data/wot.bin` |
| `WOT_REFRESH_EVERY` | events between WoT rebuilds | `100000` |
| `MAX_OPEN_SHARDS` | shard writers held open | `64` |
| `MAX_OPEN_SHARD_READERS` | shard readers held open | `48` |
| `ADMIN_PUBKEYS` | comma-separated hex/npub; enables the admin API | unset |

Any write role (relay or firehose) makes the process the writer. Without one
it opens the index and archive read-only.

## Ingest

```bash
# backfill a directory of dumps
ingest --index-root ./data/index --input-dir ./dumps

# backfill, then tail the live firehose into the same index
ingest --index-root ./data/index --input-dir ./dumps \
       --relays wss://relay.damus.io --relays wss://nos.lol

# firehose only, also archiving what it sees
ingest --index-root ./data/index --relays wss://relay.damus.io \
       --archive-dir ./data/archive

# backfill, serving a status page + the corpus while it works
ingest --index-root ./data/index --input-dir ./dumps --bind 0.0.0.0:8080
```

### Serving during a backfill

A full backfill runs for hours, and for that whole time the node has nothing on
its port. `--bind <addr>` gives it three routes while it works:

| Route | |
|---|---|
| `GET /` | static "ingest in progress" page |
| `GET /healthz` | `ok`, so a liveness probe passes during the run |
| `GET /archive` | the corpus — listing, JSON index, file downloads |

Search is deliberately **not** served: the index is half-built, and answering
queries from a partial corpus looks like data loss rather than progress. The
archive handle is opened index-free (no RocksDB lock, which the ingest itself
holds), so file listing and downloads work throughout; `/archive/event/{id}`
answers `503` rather than pretending the event is missing.

Backfill is resumable: indexed ids are recorded in `<index-root>/.dedupe` and
checkpointed after Tantivy commits, so a killed run re-processes at most one
checkpoint window rather than duplicating documents. Wiping the index wipes the
dedupe store with it, keeping the two in sync.

When it finishes, ingest **idles instead of exiting** — anything with a restart
policy of `Always` treats a clean exit as a reason to run the whole backfill
again. Pass `--exit-when-done` for batch use (a Job, or a local run). `SIGTERM`
always flushes and exits promptly.

### Rebuilding

```bash
ingest --index-root ./data/index --input-dir ./dumps --rebuild
```

`--rebuild` runs both migrations in the only order that works, then a normal
backfill:

1. rebuild the **archive** id index — frame sidecars, then each event's
   location (shard + frame offset + length), reframing single-frame dumps so a
   lookup decodes one frame instead of a whole file;
2. wipe the **Tantivy** index and the dedupe store;
3. re-ingest.

It is O(corpus) twice and deletes the serving index before it starts, so scale
the node down first and run it deliberately. The parts are also available
separately (`--rebuild-archive-index`, `--reindex`, `--compact-archive-index`).

### Other binaries

| Binary | For |
|---|---|
| `ingest` | archive backfill + firehose (the main one) |
| `archive` | archive maintenance: `--stats`, `--index-new`, `--rebuild-index`, `--compact`, `--locate <id>` |
| `stats` | stats/WoT backfill, emits a WoT snapshot for scoring |
| `scrape` | dedicated full-network historical scrape session |

All of them write, so none may run while a writing node is up.

## HTTP API

| Route | |
|---|---|
| `GET /search?q=…` | search (query-string form) |
| `POST /search` | search (full filter DSL) |
| `GET /event/{id}` | one event by hex id |
| `GET /stats` | index/cluster stats |
| `GET /healthz` | liveness |
| `GET /archive` `…/files` `…/stats` | corpus listing |
| `GET /archive/{file}` | stream one dump |
| `GET /archive/event/{id}` | one event straight from the archive index |
| `GET /reports` `…/{name}` `…/stream` | analyses, and an SSE patch stream |
| `GET /sync` | scrape/backfill coverage |
| `POST /admin/*` | replay control and resets (NIP-98 signed) |
| `GET /` | console, or a relay websocket on upgrade |

```bash
curl 'localhost:8080/search?q=bitcoin&kind=1&limit=20'
curl 'localhost:8080/search?tag=nostr&since=1784000000'
curl 'localhost:8080/search?q=lightning+author:npub1…+since:2026-01-01'

curl -XPOST localhost:8080/search -H 'content-type: application/json' -d '{
  "search": "bitcoin AND lightning",
  "kinds": [1, 30023],
  "tag_t": ["nostr"],
  "since": 1780000000,
  "limit": 50
}'
```

Admin calls are signed in the browser with a NIP-07 extension — no shared
secret, no token to leak. `scripts/nsadmin` does the same from a shell.

## Search

Inside the `search`/`q` string, on top of Tantivy's grammar (`"phrase"`,
`AND`/`OR`, `-negation`, `*`):

| Operator | Meaning |
|---|---|
| `author:<hex\|npub>` | restrict to an author |
| `kind:<n>` | restrict to a kind |
| `since:<unix\|YYYY-MM-DD>` / `until:<…>` | time bound (drives shard pruning) |
| `#tag` or `tag:<x>` | hashtag (`t`) lookup |
| `lang:<code>` | language, detected at index time |
| `geo:<geohash>` | everything inside that cell, at any precision |
| `site:<domain>` | events linking to that host |
| `nip05:<id>` | profile by NIP-05 identifier |

Semantics worth knowing:

- **Bare terms are ANDed**, and matched against `title`, `summary` and
  `content` with titles boosted. `bitcoin conference` means both words
  anywhere in the event's searchable text; explicit `OR` still works.
- **Scripts written without spaces** (Chinese, Japanese, Korean, Thai) are
  indexed as overlapping bigrams, so substring search works there too.
- **Hex is case-folded** on both sides, and `npub`/`note`/`nprofile`/`nevent`
  are decoded, so an identifier matches however it was written.
- **Hits carry the complete signed event** when the node has an archive: the
  index stores no `tags` and no `sig`, so the event is fetched by id from the
  archive, which records where each one lives. Without an archive a hit is
  metadata plus content.

## Scoring

```
score = BM25 × (1 + wot_weight·wot_tier + recency_weight·recency_decay)
recency_decay = max(0, 1 − age_days / half_life_days)   (default half-life 365d)
```

WoT tier comes from the stats engine's follow graph and is hot-swapped into the
writer every `WOT_REFRESH_EVERY` events; it defaults to 0, which makes scoring
BM25 + recency. A live tail alone sees too few kind-3 contact lists to
bootstrap it, so run a backfill before serving if ranking matters.

## Design notes

**Tags are first-class.** Nostr "advanced search" is mostly tag lookup, so
`t/e/p/a/d/g/l` and URL hosts are dedicated exact-match term fields, not
something recovered from a text blob.

**Two content paths.** Only human-text kinds are tokenized for BM25. The rest
of the corpus — encrypted DMs, gift wraps, app blobs — is still fully indexed
for *metadata*, but tokenizing ciphertext buys nothing and costs terabytes of
term dictionary.

**Fast fields for scoring.** `created_at`, `kind` and `wot_tier` are columnar
and read by a custom collector, rather than deserializing a stored document per
hit.

**Stored once.** Exactly one copy of the content lives in the index; complete
events come from the archive by id. Duplicating 763 GiB of JSON inside the
index to avoid an O(1) lookup is not a trade worth making.

**No `deleted` / `superseded` columns.** This is a full archive, and both are
derived views over what is already indexed rather than properties of an event.
The version history of a replaceable event is the query `authors + kind + #d`
ordered by `created_at` — newest is live, the rest are superseded by
definition. A deletion is a kind-5 event naming its target in an `e` tag, which
stays searchable as the ordinary event it is. Recording either as a column
would freeze one answer at index time and be wrong as soon as the next version
arrived; enacting them is the caller's policy, not the archive's.

## Roadmap

- [ ] **NIP-50 websocket relay** — serve `REQ` from the same planner (the relay
      endpoint currently accepts writes and rejects queries)
- [ ] **S3 segment offload** — finalized monthly shards pushed to object storage
- [ ] **Stateless searchers** — fetch cold shards on demand, the
      Quickwit-style decoupled compute/storage model

## License

MIT
