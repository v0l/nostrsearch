# nostrsearch

**The mother of all Nostr search relays.** A distributed, full-text search
engine over the entire Nostr event corpus, built in Rust on
[Tantivy](https://github.com/quickwit-oss/tantivy).

Target: index the **hole.v0l.io** archive — **~900M events / ~763 GiB** of
JSONL dumps — and serve advanced search (NIP-50 + rich filters) at scale.

## Status: working POC

Single-node engine is functional and benchmarked against real hole.v0l.io data:

| Metric | Value |
|---|---|
| Ingest throughput | **~122,000 events/sec** (single node, 48 cores) |
| 1.34M-event day dump | indexed in **~11s** |
| Index size | ~2.5 GB per 2.8 GB raw day dump |
| Full-text search | BM25 × (WoT + recency) scoring, working |
| NIP-50 search | `search` string + extension operators, working |
| REST API | `/search` `/event/{id}` `/stats` `/healthz`, working |
| Tests | 17 passing |

## Architecture

```
                ┌────────────────────────────────────────────┐
 hole.v0l.io    │  nostrsearch-indexer                       │
 .jsonl(.zst) ─►│  parse ─► route by created_at ─► ShardWriter│
                │                          (one per month,    │
                │                           own IndexWriter,  │
                │                           no global lock)   │
                └───────────────┬────────────────────────────┘
                                │  <root>/<YYYY-MM>/  (Tantivy index)
                ┌───────────────▼────────────────────────────┐
                │  nostrsearch-server                        │
                │  ShardRegistry: discover + fan-out + merge │
                │  QueryPlanner: NIP-50 filter ─► Tantivy    │
                │  REST API (axum)                           │
                └────────────────────────────────────────────┘
```

### Why this design (vs. moar / nostrarchives)

Built after studying [barrydeen/moar](https://github.com/barrydeen/moar)
(Tantivy secondary index) and
[barrydeen/nostrarchives-api](https://github.com/barrydeen/nostrarchives-api)
(Postgres FTS). Those are single-node and hit a wall well before 1B events.
This engine keeps moar's correct secondary-index idea but fixes what doesn't
scale:

- **Time-sharded indices** (one per month) instead of one monolithic index →
  shard pruning, bounded merges, cold-shard offload.
- **Shard-per-writer, no global `Mutex<IndexWriter>`** → ingest parallelism
  scales with cores.
- **Tags are first-class** (`t/e/p/a/d/g/l/url` as exact-match term fields) —
  moar only indexes content/pubkey/kind.
- **Fast-field scoring** — WoT tier + recency read from columnar fast fields in
  a custom collector, not per-hit stored-doc deserialization.
- **Content split** — only human-text kinds are tokenized for BM25; the other
  ~90% of the corpus (encrypted DMs, gift-wraps, app blobs) is fully indexed
  for *metadata* but doesn't pollute the term dictionary with ciphertext.

## Crates

- **`nostrsearch-core`** — event model, Tantivy schema, time-shard layout,
  NIP-50 query planner, composite scoring. No I/O.
- **`nostrsearch-indexer`** — JSONL/zstd source, `ShardManager` (per-shard
  writers, scheduled commits), `ingest` CLI.
- **`nostrsearch-server`** — `ShardRegistry` (fan-out + merge + hydrate), axum
  REST API.

## Usage

### Ingest the corpus

```bash
# a single dump
cargo run --release --bin ingest -- \
  --index-root ./data/index --input events_20260715.jsonl

# a whole directory of dumps (date order)
cargo run --release --bin ingest -- \
  --index-root ./data/index --input-dir ./dumps/

# straight from hole.v0l.io
cargo run --release --bin ingest -- \
  --index-root ./data/index --url https://hole.v0l.io/events_20260714.jsonl.zst
```

### Run the search API

```bash
INDEX_ROOT=./data/index BIND=0.0.0.0:8080 \
  cargo run --release --bin nostrsearch-server
```

### Query

```bash
# full-text, kind-1 notes about bitcoin
curl 'localhost:8080/search?q=bitcoin&kind=1&limit=20'

# hashtag + time range
curl 'localhost:8080/search?tag=nostr&since=1784000000&limit=20'

# NIP-50 extension operators inside the search string
curl 'localhost:8080/search?q=lightning author:<hex> kind:1 since:2026-01-01'

# full DSL
curl -XPOST localhost:8080/search -H 'content-type: application/json' -d '{
  "search": "bitcoin AND lightning",
  "kinds": [1, 30023],
  "tag_t": ["nostr"],
  "since": 1780000000,
  "limit": 50
}'

# fetch one event, cluster stats
curl localhost:8080/event/<hex-id>
curl localhost:8080/stats
```

## NIP-50 search extensions

Inside the `search`/`q` string, on top of Tantivy's grammar (`"phrase"`,
`AND`/`OR`, `-negation`, `*`):

| Operator | Meaning |
|---|---|
| `author:<hex>` | restrict to an author |
| `kind:<n>` | restrict to a kind |
| `since:<unix\|YYYY-MM-DD>` / `until:<...>` | time bound (drives shard pruning) |
| `#tag` or `tag:<x>` | hashtag (`t`) lookup |
| `lang:<code>` | language filter |

## Scoring

```
score = BM25 × (1 + wot_weight·wot_tier + recency_weight·recency_decay)
recency_decay = max(0, 1 − age_days / half_life_days)   (default half-life 365d)
```

WoT tier is injectable at ingest (`ShardManager::with_wot_lookup`); default 0.

## Roadmap to distributed ("enterprise")

The single-node core is done. The scale-out layer is designed but not yet built:

- [ ] **NIP-50 websocket relay** — second frontend over the same `QueryPlanner`
- [ ] **S3 segment offload** — finalized monthly shards pushed to object storage
- [ ] **Stateless searchers** — fetch cold shards from S3 on demand (the
      Quickwit-style decoupled-compute/storage model)
- [ ] **Parallel multi-file ingest** + open-shard eviction + event-id dedup
- [ ] **Mutability flags** — `deleted`/`superseded` already in schema; wire the
      kind-5 / replaceable-event tracking to populate them

## License

MIT
