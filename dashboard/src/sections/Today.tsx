import {
  asRecord,
  total,
  type ActiveUsersReport,
  type ClientStats,
  type DailyActivity,
  type Reports as ReportsState,
  type TrustedCount,
} from "../reports";
import { Chip, Readout, compact, num, plural, isoDay } from "../ui";

/** Kinds worth naming; anything else is shown as its number. */
export const KIND_NAMES: Record<string, string> = {
  "0": "profiles",
  "1": "notes",
  "3": "contacts",
  "4": "DMs",
  "5": "deletions",
  "6": "reposts",
  "7": "reactions",
  "1059": "gift wraps",
  "1111": "comments",
  "9735": "zap receipts",
  "9802": "highlights",
  "10002": "relay lists",
  "17375": "wallet",
  "30023": "articles",
  "30078": "app data",
  "30311": "live events",
};

export const kindLabel = (k: string) => KIND_NAMES[k] ?? `kind ${k}`;

/** Latest day bucket in an `activity` report, with its start. */
export function latestDay(
  raw: unknown,
): { start: number; day: DailyActivity } | null {
  const byDay = asRecord<DailyActivity>(raw);
  let best: { start: number; day: DailyActivity } | null = null;
  for (const [k, v] of Object.entries(byDay)) {
    const start = Number(k);
    if (!Number.isFinite(start) || !v) continue;
    if (!best || start > best.start) best = { start, day: v };
  }
  return best;
}

export const dayEvents = (d: DailyActivity): number =>
  Object.values(d.kinds ?? {}).reduce((n, c) => n + total(c), 0);

export const dayTrusted = (d: DailyActivity): number =>
  Object.values(d.kinds ?? {}).reduce((n, c) => n + (c.trusted ?? 0), 0);

/**
 * The signature element: what the corpus took in today, by kind.
 *
 * This is the one thing a nostr index is for — the composition of the stream,
 * not its volume — and it is the report that moves while you watch it, so the
 * page opens on the live thing rather than on a total. One bar, segmented by
 * kind, widths proportional to the day's counts; the legend carries the names
 * because "kind 30078" tells nobody anything.
 */
function Composition({ kinds }: { kinds: Record<string, TrustedCount> }) {
  const rows = Object.entries(kinds)
    .map(([kind, c]) => ({ kind, count: total(c), trusted: c.trusted ?? 0 }))
    .filter((r) => r.count > 0)
    .sort((a, b) => b.count - a.count);

  const grand = rows.reduce((n, r) => n + r.count, 0);
  if (grand === 0) {
    return (
      <div class="ribbon-empty">
        Nothing recorded for today yet. This fills in as events arrive.
      </div>
    );
  }

  const top = rows.slice(0, 7);
  const rest = rows.slice(7);
  const restCount = rest.reduce((n, r) => n + r.count, 0);
  const segments = restCount > 0 ? [...top, { kind: "rest", count: restCount, trusted: 0 }] : top;

  // Tints of the one accent, darkest first: the palette stays closed and the
  // order of the bar is readable without a colour key.
  // Floors at 34% so the last segment is still legible on the panel.
  const tint = (i: number) =>
    `color-mix(in srgb, var(--patina) ${Math.max(34, 100 - i * 9)}%, var(--panel))`;

  return (
    <>
      <div class="composition" role="img" aria-label="Events today by kind">
        {segments.map((s, i) => (
          <div
            key={s.kind}
            class="seg"
            style={{ width: `${(s.count / grand) * 100}%`, background: tint(i) }}
            title={`${s.kind === "rest" ? `${rest.length} other kinds` : kindLabel(s.kind)} — ${num(s.count)} events, ${((s.count / grand) * 100).toFixed(1)}%`}
          />
        ))}
      </div>

      <ul class="legend">
        {segments.map((s, i) => (
          <li key={s.kind}>
            <i style={{ background: tint(i) }} />
            <span class="nm">
              {s.kind === "rest" ? `${rest.length} other kinds` : kindLabel(s.kind)}
            </span>
            <b>{compact(s.count)}</b>
            <span class="pc">{((s.count / grand) * 100).toFixed(1)}%</span>
          </li>
        ))}
      </ul>
    </>
  );
}

export function Today({ reports }: { reports: ReportsState }) {
  const latest = latestDay(reports.data.activity);
  const day = latest?.day;
  const events = day ? dayEvents(day) : 0;
  const trusted = day ? dayTrusted(day) : 0;
  const zapSats = day ? total(day.zaps_sent_sats) : 0;

  const users = (reports.data.active_users ?? {}) as Partial<ActiveUsersReport>;
  const dailyUsers = Object.values(asRecord<ActiveUsersReport["daily"][string]>(users.daily))
    .filter((b) => b && Number.isFinite(b.start))
    .sort((a, b) => b.start - a.start)[0];

  const clients = Object.entries(asRecord<ClientStats>(reports.data.client_tags));
  const activeClients = clients.filter(
    ([, s]) => latest && s?.last_note >= latest.start,
  ).length;

  const date = latest
    ? isoDay(latest.start)
    : "no day recorded";

  return (
    <section id="today" class="stack">
      <div class="row tight" style={{ justifyContent: "space-between" }}>
        <div class="row tight">
          <Chip tone={reports.live ? "ok" : "mute"} dot={reports.live}>
            {reports.live ? "Patching live" : "Snapshot only"}
          </Chip>
          <Chip tone="mute">{date}</Chip>
        </div>
        <span class="mono-key">
          {reports.names.length ? `${reports.names.length} reports published` : "no reports yet"}
        </span>
      </div>

      <div class="ribbon">
        <div class="ribbon-head">
          <span>What the index took in today, by kind</span>
          <b>{events > 0 ? `${num(events)} events` : "waiting"}</b>
        </div>
        <div style={{ paddingBottom: "18px" }}>
          <Composition kinds={day?.kinds ?? {}} />
        </div>
      </div>

      <div class="readouts">
        <Readout
          value={compact(events)}
          label="Events today"
          sub={events > 0 ? `${((trusted / events) * 100).toFixed(1)}% from trusted keys` : undefined}
        />
        <Readout
          value={compact(dailyUsers ? total(dailyUsers.users) : 0)}
          label="Publishers today"
          sub={dailyUsers ? `${num(dailyUsers.users.trusted)} inside the web of trust` : undefined}
        />
        <Readout
          value={compact(zapSats)}
          label="Sats zapped today"
          sub={day?.zap_count ? plural(day.zap_count, "receipt") : undefined}
        />
        <Readout
          value={compact(activeClients || clients.length)}
          label={activeClients ? "Clients publishing today" : "Clients seen"}
          sub={activeClients ? `${num(clients.length)} known in total` : undefined}
        />
      </div>
    </section>
  );
}
