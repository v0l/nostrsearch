# Operator console

The node's front page, served at `/` and `/dashboard`. Preact + TypeScript,
built by Vite with `vite-plugin-singlefile` so the whole thing — markup, styles,
script — is one HTML file that `nostrsearch-server` pulls in with `include_str!`.
Shipping the console is shipping the binary.

It is always mounted. Corpus coverage, the analysis reports, relay health and
index memory all come from open endpoints; only replay, analyses and the scrape
resets need a key. On a node running the relay, `/` serves the console to a
browser and the relay to a websocket upgrade.

```bash
bun install
bun run dev          # http://localhost:5173, proxying to a node on :8080
NODE_URL=https://archive.example bun run dev

bun mock.ts          # http://localhost:5199, built console + fake payloads
DENY=1 bun mock.ts   # same, but every admin call returns 401

../scripts/build-dashboard.sh   # build and copy into the server crate
```

The built asset lives at `crates/nostrsearch-server/assets/dashboard.html` and
is committed, so `cargo build` never needs bun. Rebuild it whenever you change
anything here.

## Signing in

There is no session. Every admin request carries its own NIP-98 event (kind
27235) naming that exact URL and method, signed in the browser by a NIP-07
extension. Nothing secret is stored or transmitted, and the node refuses a
header it has already seen, so each request is signed fresh.

Signing in does one real admin call to check the key against the node's
`ADMIN_PUBKEYS`, and says so plainly when the key is not on the list — rather
than showing a working console that fails on the first action.

Public panels (corpus, relays, index) render without a key. Replay, analyses
and the scrape resets stay locked until one is presented.

## Endpoints it talks to

| Endpoint | Auth | Used for |
| --- | --- | --- |
| `GET /stats` | — | index size, shards, readers, memory |
| `GET /sync/` | — | relay health, backfill coverage |
| `GET /archive/files` | — | dumps available to replay |
| `GET /reports/` | — | which reports exist |
| `GET /reports/{name}` | — | full snapshot, seeds each report panel |
| `GET /reports/stream` | — | merge patches over those snapshots (SSE) |
| `GET /admin/analyses` | NIP-98 | analysis state; also the sign-in check |
| `POST /admin/analyses/{name}/reset` | NIP-98 | re-derive one analysis |
| `GET|POST /admin/ingest`, `POST /admin/ingest/cancel` | NIP-98 | replay control |
| `GET /admin/scrape`, `POST /admin/scrape/reset` | NIP-98 | count and clear day records |
| `POST /admin/scrape/relay/reset` | NIP-98 | forget what a relay taught the scraper |

## Reports

Each report panel seeds from `GET /reports/{name}` and then applies every frame
from the delta stream as an RFC 7386 merge patch — the same operation the node
performs on its own copy, so the two stay in step without refetching a
multi-year report every tick. A `lagged` frame means the node dropped us as a
slow consumer, and the client re-seeds rather than keep patching a gapped state.

Rendered: activity and zap volume per day, unique publishers per day and week,
trending hashtags, the kind breakdown, and the client-tag table. Each is drawn
from that analysis's own `snapshot()` shape in `nostrsearch-stats`.

## Design

Mono-first instrument panel on cold pressed paper: everything on screen is a
hash, a URL, a count or a timestamp, so IBM Plex Mono carries the interface and
Archivo appears only twice — the wordmark and the four corpus readouts. Colour
is oxidised copper (`--patina`) on grey-green stock; rust and brass are reserved
for state and never used as decoration.

The signature element is the backfill ribbon at the top: one bar per completed
calendar day, height for events the relays returned, solid fill for the share
that was new to the index. A tall hollow bar is a day that was re-read for
nothing. The same instrument carries the report charts, where the fill is the
share from inside the web of trust — so a tall hollow bar reads the same way
there: volume that arrived without trusted people behind it.

Destructive controls arm on first click and fire on the second, so there is no
modal between an operator and the thing they were looking at.
