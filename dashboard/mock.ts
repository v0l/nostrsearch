/**
 * Fake node for designing against: `bun mock.ts` serves the built dashboard
 * plus plausible payloads on every endpoint it talks to.
 */
const html = await Bun.file(new URL("./dist/index.html", import.meta.url)).text();

const days = Array.from({ length: 25 }, (_, i) => {
  const d = new Date(Date.UTC(2024, 4, 1 + i));
  const seen = Math.round(40_000 + Math.sin(i / 2.2) * 26_000 + Math.random() * 12_000);
  return {
    date: d.toISOString().slice(0, 10),
    relay: `wss://relay${(i % 4) + 1}.example`,
    seen,
    new: Math.round(seen * (0.15 + Math.random() * 0.6)),
    at: Math.floor(Date.now() / 1000) - i * 900,
  };
});

const relays = Array.from({ length: 12 }, (_, i) => ({
  url: `wss://relay${i + 1}.damus.example.social`,
  sources: 4200 - i * 260,
  negentropy: i % 3 === 0 ? true : i % 3 === 1 ? false : null,
  cap: 500 - i * 20,
  fails: i === 4 ? 7 : 0,
  last_ok: Math.floor(Date.now() / 1000) - i * 3600,
  birthday: null,
}));

// --- reports ---------------------------------------------------------------

const DAY = 86400;
const today = Math.floor(Date.now() / 1000 / DAY) * DAY;
const tc = (t: number, u: number) => ({ trusted: t, untrusted: u });

const activity: Record<string, unknown> = {};
for (let i = 59; i >= 0; i--) {
  const start = today - i * DAY;
  const base = 900_000 + Math.sin(i / 4) * 300_000 + Math.random() * 120_000;
  activity[start] = {
    zaps_sent_sats: tc(Math.round(base * 0.4), Math.round(base * 0.1)),
    zaps_received_sats: tc(Math.round(base * 0.35), Math.round(base * 0.15)),
    zap_count: Math.round(base / 900),
    kinds: {
      "1": tc(Math.round(base * 0.22), Math.round(base * 0.3)),
      "7": tc(Math.round(base * 0.4), Math.round(base * 0.5)),
      "6": tc(Math.round(base * 0.05), Math.round(base * 0.06)),
      "9735": tc(Math.round(base * 0.03), Math.round(base * 0.02)),
      "30023": tc(Math.round(base * 0.004), Math.round(base * 0.002)),
    },
  };
}

const activeUsers = {
  daily: Object.fromEntries(
    Array.from({ length: 60 }, (_, i) => {
      const start = today - (59 - i) * DAY;
      const n = 18_000 + Math.round(Math.sin(i / 5) * 5_000 + Math.random() * 2_000);
      return [start, { start, users: tc(Math.round(n * 0.42), Math.round(n * 0.58)) }];
    }),
  ),
  weekly: Object.fromEntries(
    Array.from({ length: 12 }, (_, i) => {
      const start = today - (11 - i) * DAY * 7;
      const n = 74_000 + Math.round(Math.random() * 9_000);
      return [start, { start, users: tc(Math.round(n * 0.39), Math.round(n * 0.61)) }];
    }),
  ),
};

const trending = [
  "bitcoin", "nostr", "grownostr", "art", "photography", "zapathon", "lightning",
  "plebchain", "foodstr", "running", "coffeechain", "devstr", "music", "garden",
].map((tag, i) => ({
  tag,
  score: Math.round((1400 - i * 88) * (0.85 + Math.random() * 0.3)),
  mentions: Math.round((9200 - i * 540) * (0.8 + Math.random() * 0.4)),
}));

const clientTags: Record<string, unknown> = Object.fromEntries(
  [
    ["damus", 41e6], ["N/A", 33e6], ["amethyst", 28e6], ["primal", 19e6],
    ["snort", 8.4e6], ["iris", 3.1e6], ["coracle", 2.2e6], ["nostur", 1.4e6],
    ["gossip", 900e3], ["yakihonne", 640e3], ["0xchat", 410e3], ["other", 280e3],
  ].map(([name, sum]) => [
    name,
    {
      sum,
      last_note: Math.floor(Date.now() / 1000) - Math.round(Math.random() * 7200),
      kinds: { "1": Math.round((sum as number) * 0.3), "7": Math.round((sum as number) * 0.5) },
    },
  ]),
);

const kindBreakdown: Record<string, unknown> = {
  "7": tc(310e6, 402e6),
  "1": tc(122e6, 168e6),
  "6": tc(38e6, 41e6),
  "9735": tc(21e6, 9e6),
  "3": tc(11e6, 14e6),
  "0": tc(4e6, 9e6),
  "30023": tc(900e3, 400e3),
  "10002": tc(700e3, 1.2e6),
};

const reports: Record<string, unknown> = {
  activity,
  active_users: activeUsers,
  trending_hashtags: trending,
  client_tags: clientTags,
  kind_breakdown: kindBreakdown,
  follow_graph: {},
  pagerank: [],
};

const replayStart = Date.now() - 900_000;

const json = (v: unknown) =>
  new Response(JSON.stringify(v), { headers: { "content-type": "application/json" } });

// `LIVE=https://archive.v0l.io bun mock.ts` serves the local build against a
// real node's data, which is the only way to catch a shape the fixtures got
// wrong. Admin routes are proxied too, but unsigned, so they will 401.
const LIVE = process.env.LIVE;

Bun.serve({
  port: 5199,
  async fetch(req) {
    const p = new URL(req.url).pathname;

    if (LIVE && p !== "/") {
      const upstream = new URL(p + new URL(req.url).search, LIVE);
      const r = await fetch(upstream, { headers: { accept: req.headers.get("accept") ?? "*/*" } });
      return new Response(r.body, { status: r.status, headers: r.headers });
    }

    if (p === "/stats")
      return json({
        total_docs: 812_449_301,
        shard_count: 14,
        open_readers: 9,
        max_open_readers: 12,
        open_fds: 1840,
        nofile_soft: 4096,
        memory: {
          rss_mb: 24_100,
          peak_rss_mb: 28_400,
          cgroup_current_mb: 30_100,
          cgroup_anon_mb: 6_200,
          cgroup_file_mb: 23_900,
          cgroup_limit_mb: 32_768,
        },
        shards: Array.from({ length: 14 }, (_, i) => ({
          shard: `2024-${String(i + 1).padStart(2, "0")}`,
          docs: Math.round(80_000_000 / (i + 1)),
        })),
      });
    if (p === "/sync/")
      return json({
        relays: { total: 1284, negentropy: 311, no_negentropy: 640, unprobed: 333, failing: 27, top: relays },
        scrape: {
          days: 1904,
          relay_days: 41_233,
          events_seen: 2_113_000_000,
          events_new: 780_400_000,
          oldest_day: "2019-11-02",
          newest_day: "2025-08-02",
          recent: days,
        },
      });
    if (p === "/archive/files")
      return json(
        Array.from({ length: 9 }, (_, i) => ({
          name: i === 0 ? "combined.jsonl" : `events_2024-0${i}.jsonl.zst`,
          size: Math.round(i === 0 ? 214e9 : 3.1e9),
          timestamp: Math.floor(Date.now() / 1000) - i * 86400,
        })),
      );
    if (p.startsWith("/admin/") && process.env.DENY)
      return new Response(JSON.stringify({ error: "pubkey is not an admin" }), {
        status: 401,
        headers: { "content-type": "application/json", "www-authenticate": "Nostr" },
      });
    if (p === "/admin/analyses")
      return json(
        ["activity", "active_users", "client_tags", "follow_graph", "pagerank", "trending_hashtags"].map(
          (name, i) => ({
            name,
            epoch: 3,
            backfilled: i % 4 !== 2,
            watermark: Math.floor(Date.now() / 1000) - i * 640,
            events: 90_000_000,
            observed: 88_000_000 - i * 1e6,
            consumed: 61_000_000 - i * 1e6,
            filtered: 27_000_000,
            deps: name === "pagerank" ? ["follow_graph"] : [],
          }),
        ),
      );
    if (p === "/admin/ingest") {
      // Advance the read head so the panel's rate and ETA have something real
      // to measure between polls.
      // Loops through the file so the rate readout always has movement to
      // measure; set STALL=1 to exercise the stalled state instead.
      const elapsed = (Date.now() - replayStart) / 1000;
      const read = process.env.STALL ? 96e9 : 40e9 + ((elapsed * 180e6) % 170e9);
      return json({
        running: true,
        cancelled: false,
        started_at: Math.floor(replayStart / 1000),
        finished_at: 0,
        files_total: 9,
        files_done: 2,
        events: 21_400_000,
        new: 900_000,
        malformed: 4,
        current: "combined.jsonl",
        current_progress: {
          name: "combined.jsonl",
          bytes_total: 214e9,
          bytes_read: Math.round(read),
          malformed: 812,
          events: Math.round(read / 520),
          new: Math.round(read / 11_000),
          complete: false,
          error: null,
        },
        files: [
          { name: "events_2024-07.jsonl.zst", bytes_total: 3.1e9, bytes_read: 3.1e9, malformed: 0, events: 21e6, new: 400e3, complete: true, error: null },
          { name: "events_2024-06.jsonl.zst", bytes_total: 2.9e9, bytes_read: 2.9e9, malformed: 4, events: 400e3, new: 500e3, complete: true, error: null },
          { name: "events_2024-05.jsonl.zst", bytes_total: 2.8e9, bytes_read: 1.2e9, malformed: 0, events: 90e3, new: 1e3, complete: false, error: "unexpected end of zstd frame" },
        ],
      });
    }
    if (p === "/reports/" || p === "/reports")
      return json({ generated_at: Math.floor(Date.now() / 1000) - 40, reports: Object.keys(reports) });
    if (p.startsWith("/reports/")) {
      const name = decodeURIComponent(p.slice("/reports/".length));
      if (name === "stream") {
        // Nudge one hashtag and today's activity every 2s so the live patching
        // is actually visible while designing.
        const stream = new ReadableStream({
          // Async start keeps the controller open for the life of the promise;
          // returning synchronously makes Bun close the stream immediately.
          async start(c) {
            const enc = new TextEncoder();
            let closed = false;
            const send = (event: string, data: unknown) => {
              if (closed) return;
              try {
                c.enqueue(enc.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`));
              } catch {
                // The client went away; stop writing to a dead controller.
                closed = true;
              }
            };
            send("delta", { name: "activity", patch: {} });
            const h = setInterval(() => {
              const t = trending[Math.floor(Math.random() * trending.length)];
              t.score += Math.round(Math.random() * 120);
              t.mentions += Math.round(Math.random() * 40);
              trending.sort((a, b) => b.score - a.score);
              send("delta", { name: "trending_hashtags", patch: trending });

              const bucket = activity[today] as { zap_count: number };
              bucket.zap_count += Math.round(Math.random() * 20);
              send("delta", { name: "activity", patch: { [today]: bucket } });
            }, 2000);
            while (!closed) await new Promise((r) => setTimeout(r, 500));
            clearInterval(h);
          },
        });
        return new Response(stream, {
          headers: { "content-type": "text/event-stream", "cache-control": "no-cache" },
        });
      }
      return name in reports
        ? json(reports[name])
        : new Response(JSON.stringify({ error: "unknown report" }), { status: 404 });
    }
    return new Response(html, { headers: { "content-type": "text/html" } });
  },
});

console.log("mock node on http://127.0.0.1:5199");
