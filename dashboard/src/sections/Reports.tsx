import { useEffect, useState } from "preact/hooks";
import { api } from "../api";
import type { Profile } from "../types";
import {
  asArray,
  asRecord,
  total,
  type ActiveUsersBucket,
  type ActiveUsersReport,
  type ClientStats,
  type DailyActivity,
  type NewUsersReport,
  type Reports as ReportsState,
  type TrendingTag,
  type TrustedCount,
} from "../reports";
import { dayEvents, dayTrusted, kindLabel } from "./Today";
import {
  Bars,
  COVERAGE_GAPS,
  Meter,
  Panel,
  ago,
  compact,
  coverageGap,
  isoDay,
  num,
  plural,
  plausibleDay,
  shortKey,
} from "../ui";

// Junk buckets are filtered out before display; isoDay still guards the rest.
const day = isoDay;

/** Trusted share of a count, as a percentage. */
function trustPct(c: TrustedCount): string {
  const t = total(c);
  return t > 0 ? `${((c.trusted / t) * 100).toFixed(0)}%` : "—";
}

const trustShare = (c: TrustedCount): string =>
  total(c) > 0 ? `${trustPct(c)} trusted` : "—";

/** A readout at panel scale rather than hero scale. */
function ReadoutSmall(props: { value: string; label: string; sub?: string }) {
  return (
    <div class="readout compact">
      <div class="v">{props.value}</div>
      <div class="k">{props.label}</div>
      {props.sub ? <div class="s">{props.sub}</div> : null}
    </div>
  );
}

// --- activity --------------------------------------------------------------

type Range = "90d" | "all";

function RangeToggle(props: { value: Range; onChange: (r: Range) => void }) {
  return (
    <span class="range">
      {(["90d", "all"] as Range[]).map((r) => (
        <button
          key={r}
          class="tiny"
          aria-pressed={props.value === r}
          onClick={() => props.onChange(r)}
        >
          {r === "90d" ? "90 days" : "all time"}
        </button>
      ))}
    </span>
  );
}

const monthLabel = (unix: number): string => isoDay(unix).slice(0, 7);

function monthOf(unix: number): number {
  const d = new Date(unix * 1000);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1) / 1000;
}

function nextMonth(unix: number): number {
  const d = new Date(unix * 1000);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 1) / 1000;
}

function GapNote() {
  return (
    <p class="panel-note gap-note">
      {`Striped bars fall in periods the archive did not record (${COVERAGE_GAPS.map((g) => `${g.from} to ${g.to}`).join(", ")}). Counts there are only what relays still held when they were scraped, so read them as a floor.`}
    </p>
  );
}

function Activity({ raw }: { raw: unknown }) {
  const [range, setRange] = useState<Range>("90d");
  const all = Object.entries(asRecord<DailyActivity>(raw))
    .map(([start, d]) => ({
      start: Number(start),
      events: dayEvents(d),
      trusted: dayTrusted(d),
      zaps: total(d.zaps_sent_sats ?? { trusted: 0, untrusted: 0 }),
      zapCount: d.zap_count ?? 0,
    }))
    .filter((d) => plausibleDay(d.start))
    .sort((a, b) => a.start - b.start);

  let rows = all;
  let unit = "day";
  if (range === "all") {
    const by = new Map<number, (typeof all)[number]>();
    for (const d of all) {
      const m = monthOf(d.start);
      const slot = by.get(m) ?? { start: m, events: 0, trusted: 0, zaps: 0, zapCount: 0 };
      slot.events += d.events;
      slot.trusted += d.trusted;
      slot.zaps += d.zaps;
      slot.zapCount += d.zapCount;
      by.set(m, slot);
    }
    rows = [...by.values()].sort((a, b) => a.start - b.start);
    unit = "month";
  } else {
    rows = all.slice(-90);
  }
  const label = range === "all" ? monthLabel : day;
  const end = (start: number) => (range === "all" ? nextMonth(start) : start + 86_400);

  const peak = Math.max(1, ...rows.map((d) => d.events));
  const zapSats = rows.reduce((n, d) => n + d.zaps, 0);
  const zapCount = rows.reduce((n, d) => n + d.zapCount, 0);
  const events = rows.reduce((n, d) => n + d.events, 0);

  return (
    <Panel
      id="activity"
      label="Activity"
      aside={<RangeToggle value={range} onChange={setRange} />}
      note={`Events published per ${unit}, with the share from inside the web of trust filled in.`}
    >
      <Bars
        items={rows.map((d) => {
          const gap = coverageGap(d.start, end(d.start));
          return {
            key: String(d.start),
            height: d.events / peak,
            fill: d.events > 0 ? d.trusted / d.events : 0,
            gap: !!gap,
            title: `${label(d.start)}: ${num(d.events)} events, ${num(d.trusted)} trusted, ${num(d.zaps)} sats zapped${gap ? ` (${gap})` : ""}`,
          };
        })}
        height={112}
        left={rows.length ? label(rows[0].start) : undefined}
        center={rows.length ? `peak ${compact(peak)} events/${unit}` : undefined}
        right={rows.length ? label(rows[rows.length - 1].start) : undefined}
        empty="No activity recorded yet. Days appear as the analysis consumes events."
      />
      <GapNote />

      <div class="readouts" style={{ marginTop: "18px" }}>
        <ReadoutSmall value={compact(events)} label="Events recorded" sub={`over ${plural(rows.length, unit)}`} />
        <ReadoutSmall value={compact(zapSats)} label="Sats zapped" sub={plural(zapCount, "receipt")} />
      </div>
    </Panel>
  );
}

// --- active users ----------------------------------------------------------

function sorted(rec: Record<string, ActiveUsersBucket> | undefined): ActiveUsersBucket[] {
  return Object.values(asRecord<ActiveUsersBucket>(rec))
    .filter((b) => b && plausibleDay(b.start))
    .sort((a, b) => a.start - b.start);
}

function ActiveUsers({ raw, fresh }: { raw: unknown; fresh: unknown }) {
  const [range, setRange] = useState<Range>("90d");
  const rep = (raw ?? {}) as Partial<ActiveUsersReport>;
  const newRep = (fresh ?? {}) as Partial<NewUsersReport>;
  const newDaily = asRecord<number>(newRep.daily);
  const newMonthly = asRecord<number>(newRep.monthly);

  const daily = sorted(rep.daily);
  const weekly = sorted(rep.weekly);
  const monthly = sorted(rep.monthly);

  const rows = range === "all" ? monthly : daily.slice(-90);
  const newOf = (start: number) => (range === "all" ? newMonthly[String(start)] : newDaily[String(start)]) ?? 0;
  const label = range === "all" ? monthLabel : day;
  const unit = range === "all" ? "month" : "day";
  const end = (start: number) => (range === "all" ? nextMonth(start) : start + 86_400);
  const peak = Math.max(1, ...rows.map((b) => total(b.users)));
  const newPeak = Math.max(1, ...rows.map((b) => newOf(b.start)));

  const lastDay = daily[daily.length - 1];
  const lastWeek = weekly[weekly.length - 1];
  const lastMonth = monthly[monthly.length - 1];
  const newLast30 = daily.slice(-30).reduce((n, b) => n + (newDaily[String(b.start)] ?? 0), 0);

  return (
    <Panel
      id="publishers"
      label="Active users"
      aside={<RangeToggle value={range} onChange={setRange} />}
      note="Distinct pubkeys that published anything signed by their own key. Gift wraps, MLS messages, bunker requests and ephemeral events use one-off keys and are left out. Counted with HyperLogLog sketches, so within about 2%."
    >
      {range === "all" && monthly.length === 0 ? (
        <p class="empty">Monthly counts appear once the active_users analysis has replayed the archive.</p>
      ) : (
        <Bars
          items={rows.map((b) => {
            const gap = coverageGap(b.start, end(b.start));
            return {
              key: String(b.start),
              height: total(b.users) / peak,
              fill: total(b.users) > 0 ? b.users.trusted / total(b.users) : 0,
              gap: !!gap,
              title: `${label(b.start)}: ${num(total(b.users))} active, ${num(b.users.trusted)} trusted, ${num(newOf(b.start))} new${gap ? ` (${gap})` : ""}`,
            };
          })}
          height={96}
          left={rows.length ? label(rows[0].start) : undefined}
          center={rows.length ? `peak ${compact(peak)} per ${unit}` : undefined}
          right={rows.length ? label(rows[rows.length - 1].start) : undefined}
          empty="No publisher counts yet."
        />
      )}

      <div class="readouts" style={{ marginTop: "18px" }}>
        <ReadoutSmall value={compact(lastDay ? total(lastDay.users) : undefined)} label="Daily active" sub={lastDay ? trustShare(lastDay.users) : undefined} />
        <ReadoutSmall value={compact(lastWeek ? total(lastWeek.users) : undefined)} label="Weekly active" sub={lastWeek ? trustShare(lastWeek.users) : undefined} />
        <ReadoutSmall value={compact(lastMonth ? total(lastMonth.users) : undefined)} label="Monthly active" sub={lastMonth ? `${monthLabel(lastMonth.start)}, ${trustShare(lastMonth.users)}` : undefined} />
        <ReadoutSmall value={compact(newLast30)} label="New users" sub="first seen in the last 30 days" />
      </div>

      <h3 class="subhead">New users per {unit}</h3>
      <p class="panel-note">A pubkey counts as new on the day of the oldest event the archive holds from it.</p>
      <Bars
        items={rows.map((b) => {
          const gap = coverageGap(b.start, end(b.start));
          return {
            key: `n${b.start}`,
            height: newOf(b.start) / newPeak,
            fill: 1,
            gap: !!gap,
            title: `${label(b.start)}: ${num(newOf(b.start))} new pubkeys${gap ? ` (${gap})` : ""}`,
          };
        })}
        height={64}
        left={rows.length ? label(rows[0].start) : undefined}
        right={rows.length ? label(rows[rows.length - 1].start) : undefined}
        empty="No new-user counts yet."
      />
    </Panel>
  );
}

// --- trending --------------------------------------------------------------

function Trending({ raw }: { raw: unknown }) {
  const tags = asArray<TrendingTag>(raw).slice(0, 14);
  const peak = Math.max(1, ...tags.map((t) => t.score));

  return (
    <Panel
      id="trending"
      label="Trending"
      aside={tags.length ? `top ${tags.length}` : undefined}
      note="Hashtags from the last seven days, scored by how many different people used them and decayed with a one-day half-life. One account repeating a tag counts once per hour. The number is distinct authors in the last 24 hours."
    >
      {tags.length === 0 ? (
        <p class="empty">No hashtag scores yet.</p>
      ) : (
        <ol class="ranks">
          {tags.map((t, i) => (
            <li key={t.tag}>
              <span class="rank">{String(i + 1).padStart(2, "0")}</span>
              <span class="tag">#{t.tag}</span>
              <span class="track">
                <Meter value={t.score} max={peak} />
              </span>
              <span class="count" title={`${num(t.mentions)} authors over 7 days`}>
                {num(t.authors_24h ?? t.mentions)}
              </span>
            </li>
          ))}
        </ol>
      )}
    </Panel>
  );
}

// --- kinds -----------------------------------------------------------------

/**
 * Kinds, from `kind_breakdown` when the node registers it and summed out of
 * `activity` when it does not — the day buckets carry the same per-kind
 * trusted/untrusted counts, so the panel works either way rather than sitting
 * empty on a node that never registered the standalone analysis.
 */
function Kinds({ activity }: { activity: unknown }) {
  const days = Object.entries(asRecord<DailyActivity>(activity))
    .map(([k, d]) => [Number(k), d] as const)
    .filter(([k, d]) => plausibleDay(k) && d)
    .sort((a, b) => a[0] - b[0]);
  const lastDay = days.length ? days[days.length - 1][0] : 0;
  const recentFrom = lastDay - 29 * 86_400;
  const priorFrom = recentFrom - 30 * 86_400;

  const recent: Record<string, TrustedCount> = {};
  const prior: Record<string, number> = {};
  for (const [start, d] of days) {
    if (start < priorFrom) continue;
    for (const [kind, c] of Object.entries(d.kinds ?? {})) {
      if (start >= recentFrom) {
        const slot = (recent[kind] ??= { trusted: 0, untrusted: 0 });
        slot.trusted += c.trusted ?? 0;
        slot.untrusted += c.untrusted ?? 0;
      } else {
        prior[kind] = (prior[kind] ?? 0) + total(c);
      }
    }
  }
  const grand = Object.values(recent).reduce((n, c) => n + total(c), 0);
  const entries = Object.entries(recent)
    .filter(([, c]) => total(c) > 0)
    .sort((a, b) => total(b[1]) - total(a[1]))
    .slice(0, 20);
  const peak = entries.length ? total(entries[0][1]) : 1;
  const priorGap = coverageGap(priorFrom, recentFrom) ?? coverageGap(recentFrom, lastDay + 86_400);
  const change = (kind: string, now: number): string => {
    if (priorGap) return "—";
    const before = prior[kind] ?? 0;
    if (before === 0) return now > 0 ? "new" : "—";
    const pct = ((now - before) / before) * 100;
    return `${pct >= 0 ? "+" : ""}${pct.toFixed(0)}%`;
  };

  return (
    <Panel
      id="kinds"
      label="Kinds"
      aside="last 30 days"
      note={`What was published in the last 30 days by kind, against the 30 days before. Trusted is the share from authors inside the web of trust.${priorGap ? ` The comparison is hidden because the earlier window falls in a coverage gap (${priorGap}).` : ""}`}
    >
      {entries.length === 0 ? (
        <p class="empty">No kind counts yet.</p>
      ) : (
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Kind</th>
                <th class="num">Events</th>
                <th class="num">Share</th>
                <th class="num">vs prior 30d</th>
                <th class="num">Trusted</th>
                <th>Volume</th>
              </tr>
            </thead>
            <tbody>
              {entries.map(([kind, c]) => (
                <tr key={kind}>
                  <td>
                    {kind} · {kindLabel(kind)}
                  </td>
                  <td class="num">{num(total(c))}</td>
                  <td class="num">{grand > 0 ? `${((total(c) / grand) * 100).toFixed(1)}%` : "—"}</td>
                  <td class="num">{change(kind, total(c))}</td>
                  <td class="num">{trustPct(c)}</td>
                  <td style={{ width: "28%" }}>
                    <Meter value={total(c)} max={peak} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  );
}

// --- clients ---------------------------------------------------------------

function Clients({ raw }: { raw: unknown }) {
  const all = Object.entries(asRecord<ClientStats>(raw)).filter(([, s]) => s && typeof s.sum === "number");
  const untagged = all.find(([name]) => name === "N/A")?.[1].sum ?? 0;
  const entries = all.filter(([name]) => name !== "N/A").sort((a, b) => b[1].sum - a[1].sum);
  const grand = entries.reduce((n, [, s]) => n + s.sum, 0);
  const top = entries.slice(0, 14);
  const taggedPct = grand + untagged > 0 ? ((grand / (grand + untagged)) * 100).toFixed(1) : "0";

  return (
    <Panel
      id="clients"
      label="Clients"
      aside={entries.length ? `${num(entries.length)} seen` : undefined}
      note={`From the client tag, which is self-reported and optional. Only ${taggedPct}% of events carry one, so shares are of tagged events only.`}
    >
      {top.length === 0 ? (
        <p class="empty">No client tags yet.</p>
      ) : (
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Client</th>
                <th class="num">Events</th>
                <th class="num">Share</th>
                <th>Spread</th>
                <th>Last event</th>
              </tr>
            </thead>
            <tbody>
              {top.map(([name, s]) => (
                <tr key={name}>
                  <td style={{ fontWeight: 600 }}>{name}</td>
                  <td class="num">{num(s.sum)}</td>
                  <td class="num">{grand > 0 ? `${((s.sum / grand) * 100).toFixed(1)}%` : "—"}</td>
                  <td style={{ width: "30%" }}>
                    <Meter value={s.sum} max={top[0][1].sum} />
                  </td>
                  <td>{ago(s.last_note)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  );
}

// --- pagerank --------------------------------------------------------------

/**
 * An avatar, or the initial of a name over a colour derived from the pubkey.
 *
 * Profile pictures are arbitrary URLs from arbitrary hosts: plenty 404, hang,
 * or are not images at all. A broken one must degrade to the fallback rather
 * than leave a torn image in the list.
 */
function Avatar({ src, seed, label }: { src?: string; seed: string; label: string }) {
  const [failed, setFailed] = useState(false);
  const hue = parseInt(seed.slice(0, 6) || "0", 16) % 360;

  if (src && !failed) {
    return (
      <img
        class="avatar"
        src={src}
        alt=""
        loading="lazy"
        referrerpolicy="no-referrer"
        onError={() => setFailed(true)}
      />
    );
  }
  return (
    <span class="avatar avatar-fb" style={{ background: `hsl(${hue} 40% 30%)` }}>
      {(label[0] ?? "?").toUpperCase()}
    </span>
  );
}

/** `pagerank`: `[[pubkey, score], …]`, already sorted, top 1000. */
function Rank({ raw }: { raw: unknown }) {
  const rows = asArray<[string, number]>(raw)
    .filter((r) => Array.isArray(r) && typeof r[0] === "string")
    .slice(0, 12);
  const peak = rows.length ? rows[0][1] : 1;

  // Resolve names for exactly the keys on screen, refetched when they change.
  const keys = rows.map(([pk]) => pk);
  const sig = keys.join(",");
  const [people, setPeople] = useState<Record<string, Profile>>({});
  useEffect(() => {
    if (!keys.length) return;
    let alive = true;
    api
      .profiles(keys)
      .then((list) => {
        if (!alive) return;
        const by: Record<string, Profile> = {};
        for (const p of list) by[p.pubkey] = p;
        setPeople(by);
      })
      // A missing name is not worth surfacing: the row still renders with the
      // key it already has.
      .catch(() => {});
    return () => {
      alive = false;
    };
  }, [sig]);

  return (
    <Panel
      id="rank"
      label="Rank"
      aside={rows.length ? `top ${rows.length}` : undefined}
      note="PageRank over the follow graph, recomputed daily. Trust everywhere else on this page comes from follower counts; this list is who the graph says carries the most weight."
    >
      {rows.length === 0 ? (
        <p class="empty">No ranks computed yet.</p>
      ) : (
        <ol class="ranks">
          {rows.map(([pubkey, score], i) => {
            const p = people[pubkey];
            const name = p?.display_name || p?.name;
            return (
              <li key={pubkey}>
                <span class="rank">{String(i + 1).padStart(2, "0")}</span>
                <Avatar src={p?.picture} seed={pubkey} label={name || pubkey} />
                <span class="who" title={pubkey}>
                  <span class="who-name">{name || shortKey(pubkey)}</span>
                  {p?.nip05 ? <span class="who-sub">{p.nip05}</span> : null}
                </span>
                <span class="track">
                  <Meter value={score} max={peak} />
                </span>
                <span class="count">{score.toFixed(4)}</span>
              </li>
            );
          })}
        </ol>
      )}
    </Panel>
  );
}

// --- section ---------------------------------------------------------------

export function Reports({ reports }: { reports: ReportsState }) {
  const { data, names, loading, error } = reports;
  const has = (n: string) => names.includes(n);

  if (loading) {
    return (
      <section id="reports" class="stack">
        <p class="empty">Loading reports…</p>
      </section>
    );
  }

  if (names.length === 0) {
    return (
      <section id="reports" class="stack">
        <Panel
          label="Reports"
          note={
            error
              ? "The node did not answer. Reports reload on their own once it does."
              : "Analyses publish here once the writer has committed a batch."
          }
        >
          <p class="empty">{error ?? "Nothing published yet."}</p>
        </Panel>
      </section>
    );
  }

  // Only render what this node actually publishes: a panel for an analysis
  // that was never registered is a dead widget, not missing data.
  return (
    <section id="reports" class="stack" style={{ gap: "40px" }}>
      {has("activity") ? <Activity raw={data.activity} /> : null}
      {has("active_users") ? <ActiveUsers raw={data.active_users} fresh={data.new_users} /> : null}
      {has("trending_hashtags") ? <Trending raw={data.trending_hashtags} /> : null}
      {has("activity") ? <Kinds activity={data.activity} /> : null}
      {has("client_tags") ? <Clients raw={data.client_tags} /> : null}
      {has("pagerank") ? <Rank raw={data.pagerank} /> : null}
    </section>
  );
}
