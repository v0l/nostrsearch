import {
  asArray,
  asRecord,
  total,
  type ActiveUsersReport,
  type ClientStats,
  type DailyActivity,
  type Reports as ReportsState,
  type TrendingTag,
  type TrustedCount,
} from "../reports";
import { Bars, Chip, Meter, Panel, ago, compact, num } from "../ui";

/** Kinds worth naming; anything else is shown as its number. */
const KIND_NAMES: Record<string, string> = {
  "0": "profile",
  "1": "note",
  "3": "contacts",
  "4": "DM",
  "5": "deletion",
  "6": "repost",
  "7": "reaction",
  "1059": "gift wrap",
  "1111": "comment",
  "9735": "zap receipt",
  "9802": "highlight",
  "10002": "relay list",
  "30023": "article",
  "30311": "live event",
};

const kindLabel = (k: string) => (KIND_NAMES[k] ? `${k} · ${KIND_NAMES[k]}` : `kind ${k}`);

const day = (unix: number) => new Date(unix * 1000).toISOString().slice(0, 10);

/** Trusted share of a count, as a percentage. */
function trustPct(c: TrustedCount): string {
  const t = total(c);
  return t > 0 ? `${((c.trusted / t) * 100).toFixed(0)}%` : "—";
}

const trustShare = (c: TrustedCount): string =>
  total(c) > 0 ? `${trustPct(c)} trusted` : "—";

// --- activity --------------------------------------------------------------

function Activity({ raw }: { raw: unknown }) {
  const byDay = asRecord<DailyActivity>(raw);
  const days = Object.entries(byDay)
    .map(([start, d]) => ({
      start: Number(start),
      events: Object.values(d.kinds ?? {}).reduce((n, c) => n + total(c), 0),
      trusted: Object.values(d.kinds ?? {}).reduce((n, c) => n + (c.trusted ?? 0), 0),
      zaps: d.zaps_sent_sats,
      zapCount: d.zap_count ?? 0,
    }))
    .filter((d) => Number.isFinite(d.start))
    .sort((a, b) => a.start - b.start)
    .slice(-60);

  const peak = Math.max(1, ...days.map((d) => d.events));
  const latest = days[days.length - 1];
  const zapSats = days.reduce((n, d) => n + total(d.zaps), 0);
  const zapCount = days.reduce((n, d) => n + d.zapCount, 0);

  return (
    <Panel
      id="activity"
      label="Activity"
      aside={days.length ? `${days.length} days` : undefined}
      note="Events published per day, with the share from inside the web of trust filled in. A tall hollow bar is a day the corpus grew without trusted people writing."
    >
      <Bars
        items={days.map((d) => ({
          key: String(d.start),
          height: d.events / peak,
          fill: d.events > 0 ? d.trusted / d.events : 0,
          title: `${day(d.start)} — ${num(d.events)} events, ${num(d.trusted)} trusted, ${num(total(d.zaps))} sats zapped`,
        }))}
        height={112}
        left={days.length ? day(days[0].start) : undefined}
        center={days.length ? `peak ${compact(peak)} events/day` : undefined}
        right={latest ? day(latest.start) : undefined}
        empty="No activity published yet. The writer fills this in on its first commit."
      />

      <div class="readouts" style={{ marginTop: "18px" }}>
        <ReadoutSmall
          value={compact(latest?.events)}
          label="Events on the last day"
          sub={latest ? trustShare({ trusted: latest.trusted, untrusted: latest.events - latest.trusted }) : undefined}
        />
        <ReadoutSmall
          value={compact(zapSats)}
          label="Sats zapped"
          sub={`over ${days.length} days`}
        />
        <ReadoutSmall value={compact(zapCount)} label="Zap receipts" sub="with a readable amount" />
      </div>
    </Panel>
  );
}

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

// --- active users ----------------------------------------------------------

function ActiveUsers({ raw }: { raw: unknown }) {
  const rep = (raw ?? {}) as Partial<ActiveUsersReport>;
  const daily = Object.values(asRecord<ActiveUsersReport["daily"][string]>(rep.daily))
    .filter((b) => b && Number.isFinite(b.start))
    .sort((a, b) => a.start - b.start)
    .slice(-60);
  const weekly = Object.values(asRecord<ActiveUsersReport["weekly"][string]>(rep.weekly))
    .filter((b) => b && Number.isFinite(b.start))
    .sort((a, b) => a.start - b.start);

  const peak = Math.max(1, ...daily.map((b) => total(b.users)));
  const last = daily[daily.length - 1];
  const lastWeek = weekly[weekly.length - 1];

  return (
    <Panel
      id="active-users"
      label="Publishers"
      aside={last ? `${compact(total(last.users))} today` : undefined}
      note="Distinct pubkeys that published, counted with HyperLogLog sketches — close, not exact, and cheap enough to keep per day forever."
    >
      <Bars
        items={daily.map((b) => ({
          key: String(b.start),
          height: total(b.users) / peak,
          fill: total(b.users) > 0 ? b.users.trusted / total(b.users) : 0,
          title: `${day(b.start)} — ${num(total(b.users))} publishers, ${num(b.users.trusted)} trusted`,
        }))}
        // Deliberately shorter than the activity chart above it: same
        // instrument, secondary series, so the two do not compete.
        height={64}
        left={daily.length ? day(daily[0].start) : undefined}
        center={daily.length ? `peak ${compact(peak)}/day` : undefined}
        right={last ? day(last.start) : undefined}
        empty="No publisher counts yet."
      />

      <div class="readouts" style={{ marginTop: "18px" }}>
        <ReadoutSmall
          value={compact(last ? total(last.users) : undefined)}
          label="Publishers, last day"
          sub={last ? trustShare(last.users) : undefined}
        />
        <ReadoutSmall
          value={compact(lastWeek ? total(lastWeek.users) : undefined)}
          label="Publishers, last week"
          sub={lastWeek ? trustShare(lastWeek.users) : undefined}
        />
      </div>
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
      note="Hashtag scores decay with a six-hour half-life, so this is what is being written about now rather than what has been written most."
    >
      {tags.length === 0 ? (
        <p class="empty">No hashtag scores published yet.</p>
      ) : (
        <ol class="ranks">
          {tags.map((t, i) => (
            <li key={t.tag}>
              <span class="rank">{String(i + 1).padStart(2, "0")}</span>
              <span class="tag">#{t.tag}</span>
              <span class="track">
                <Meter value={t.score} max={peak} />
              </span>
              <span class="count">{num(t.mentions)}</span>
            </li>
          ))}
        </ol>
      )}
    </Panel>
  );
}

// --- clients ---------------------------------------------------------------

function Clients({ raw }: { raw: unknown }) {
  const entries = Object.entries(asRecord<ClientStats>(raw))
    .filter(([, s]) => s && typeof s.sum === "number")
    .sort((a, b) => b[1].sum - a[1].sum);
  const grand = entries.reduce((n, [, s]) => n + s.sum, 0);
  const top = entries.slice(0, 12);

  return (
    <Panel
      id="clients"
      label="Clients"
      aside={entries.length ? `${num(entries.length)} seen` : undefined}
      note="Taken from the client tag, which is self-reported and optional — events without one are counted as N/A rather than guessed at."
    >
      {top.length === 0 ? (
        <p class="empty">No client tags published yet.</p>
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
                  <td style={{ width: "34%" }}>
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

// --- kinds -----------------------------------------------------------------

function Kinds({ raw }: { raw: unknown }) {
  const entries = Object.entries(asRecord<TrustedCount>(raw))
    .filter(([, c]) => c && typeof c.trusted === "number")
    .sort((a, b) => total(b[1]) - total(a[1]))
    .slice(0, 12);
  const peak = entries.length ? total(entries[0][1]) : 1;

  return (
    <Panel
      id="kinds"
      label="Kinds"
      aside={entries.length ? `${entries.length} of the busiest` : undefined}
      note="What the corpus is actually made of, split by whether the author is inside the web of trust."
    >
      {entries.length === 0 ? (
        <p class="empty">No kind breakdown published yet.</p>
      ) : (
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Kind</th>
                <th class="num">Events</th>
                <th class="num">Trusted</th>
                <th>Split</th>
              </tr>
            </thead>
            <tbody>
              {entries.map(([kind, c]) => (
                <tr key={kind}>
                  <td>{kindLabel(kind)}</td>
                  <td class="num">{num(total(c))}</td>
                  <td class="num">{trustPct(c)}</td>
                  <td style={{ width: "40%" }}>
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

// --- section ---------------------------------------------------------------

export function Reports({ reports }: { reports: ReportsState }) {
  const { data, names, live, loading, generatedAt, updatedAt } = reports;

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
        <Panel label="Reports" note="Analyses publish here once the writer has committed a batch.">
          <p class="empty">Nothing published yet.</p>
        </Panel>
      </section>
    );
  }

  return (
    <section id="reports" class="stack" style={{ gap: "40px" }}>
      <div class="row tight" style={{ justifyContent: "space-between" }}>
        <div class="row tight">
          <Chip tone={live ? "ok" : "mute"} dot={live}>
            {live ? "Patching live" : "Snapshot only"}
          </Chip>
          <Chip tone="mute">{names.length} reports</Chip>
        </div>
        <span class="mono-key">
          {updatedAt
            ? `last change ${ago(updatedAt)}`
            : generatedAt
              ? `published ${ago(generatedAt)}`
              : ""}
        </span>
      </div>

      <Activity raw={data.activity} />
      <ActiveUsers raw={data.active_users} />
      <Trending raw={data.trending_hashtags} />
      <Kinds raw={data.kind_breakdown} />
      <Clients raw={data.client_tags} />
    </section>
  );
}
