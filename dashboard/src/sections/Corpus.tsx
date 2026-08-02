import type { RegistryStats, SyncStatus } from "../types";
import { Bars, Chip, Readout, compact, num } from "../ui";

/**
 * The signature element: the corpus ribbon.
 *
 * Each bar is one calendar day the scraper has finished. Height is how many
 * events relays returned for that day; the solid fill rising from the baseline
 * is the share that was new to the index. A tall hollow bar means the day was
 * re-read for nothing — the shape of the backfill, not a decorative chart.
 */
function Ribbon({ sync }: { sync: SyncStatus | null }) {
  const recent = sync?.scrape.recent ?? [];

  const byDay = new Map<string, { seen: number; fresh: number; relays: number }>();
  for (const d of recent) {
    const e = byDay.get(d.date) ?? { seen: 0, fresh: 0, relays: 0 };
    e.seen += d.seen;
    e.fresh += d.new;
    e.relays += 1;
    byDay.set(d.date, e);
  }
  const days = [...byDay.entries()].sort((a, b) => a[0].localeCompare(b[0]));
  const peak = Math.max(1, ...days.map(([, v]) => v.seen));

  return (
    <div class="ribbon">
      <div class="ribbon-head">
        <span>Backfill · events returned per day, new events filled</span>
        <b>{days.length ? `${days.length} days on screen` : "no completed days yet"}</b>
      </div>

      <Bars
        items={days.map(([date, v]) => ({
          key: date,
          height: v.seen / peak,
          fill: v.seen > 0 ? v.fresh / v.seen : 0,
          title: `${date} — ${num(v.seen)} returned, ${num(v.fresh)} new, ${v.relays} relay${v.relays === 1 ? "" : "s"}`,
        }))}
        left={days.length ? days[0][0] : undefined}
        center={days.length ? `peak ${compact(peak)} events/day` : undefined}
        right={days.length ? days[days.length - 1][0] : undefined}
        empty="The scraper has not finished a relay-day yet. Bars appear as days complete."
      />
    </div>
  );
}

export function Corpus({
  stats,
  sync,
  live,
}: {
  stats: RegistryStats | null;
  sync: SyncStatus | null;
  live: boolean;
}) {
  const p = sync?.scrape;
  const span =
    p?.oldest_day && p?.newest_day ? `${p.oldest_day} → ${p.newest_day}` : "range unknown";
  const hitRate =
    p && p.events_seen > 0 ? `${((p.events_new / p.events_seen) * 100).toFixed(1)}% were new` : undefined;

  return (
    <section id="corpus" class="stack">
      <div class="row tight" style={{ justifyContent: "space-between" }}>
        <div class="row tight">
          <Chip tone={live ? "ok" : "mute"} dot={live}>
            {live ? "Live" : "Offline"}
          </Chip>
          <Chip tone="mute">{span}</Chip>
        </div>
        <span class="mono-key">{p ? `${num(p.relay_days)} relay-days recorded` : ""}</span>
      </div>

      <Ribbon sync={sync} />

      <div class="readouts">
        <Readout
          value={compact(stats?.total_docs)}
          label="Documents indexed"
          sub={`across ${num(stats?.shard_count)} shards`}
        />
        <Readout
          value={compact(p?.relay_days)}
          label="Relay-days covered"
          sub={`${num(p?.days)} distinct dates`}
        />
        <Readout value={compact(p?.events_new)} label="Events kept" sub={hitRate} />
        <Readout
          value={compact(sync?.relays.total)}
          label="Relays known"
          sub={`${num(sync?.relays.negentropy)} speak negentropy`}
        />
      </div>
    </section>
  );
}
