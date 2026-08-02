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
import { dayEvents, dayTrusted, kindLabel } from "./Today";
import { Bars, Meter, Panel, ago, compact, num, plural, shortKey } from "../ui";

const day = (unix: number) => new Date(unix * 1000).toISOString().slice(0, 10);

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

function Activity({ raw }: { raw: unknown }) {
  const days = Object.entries(asRecord<DailyActivity>(raw))
    .map(([start, d]) => ({
      start: Number(start),
      events: dayEvents(d),
      trusted: dayTrusted(d),
      zaps: total(d.zaps_sent_sats ?? { trusted: 0, untrusted: 0 }),
      zapCount: d.zap_count ?? 0,
    }))
    .filter((d) => Number.isFinite(d.start))
    .sort((a, b) => a.start - b.start)
    .slice(-90);

  const peak = Math.max(1, ...days.map((d) => d.events));
  const zapSats = days.reduce((n, d) => n + d.zaps, 0);
  const zapCount = days.reduce((n, d) => n + d.zapCount, 0);
  const events = days.reduce((n, d) => n + d.events, 0);

  return (
    <Panel
      id="activity"
      label="Activity"
      aside={days.length ? plural(days.length, "day") : undefined}
      note="Events published per day, with the share from inside the web of trust filled in. A tall hollow bar is a day the corpus grew without trusted people writing."
    >
      <Bars
        items={days.map((d) => ({
          key: String(d.start),
          height: d.events / peak,
          fill: d.events > 0 ? d.trusted / d.events : 0,
          title: `${day(d.start)} — ${num(d.events)} events, ${num(d.trusted)} trusted, ${num(d.zaps)} sats zapped`,
        }))}
        height={112}
        left={days.length ? day(days[0].start) : undefined}
        center={days.length ? `peak ${compact(peak)} events/day` : undefined}
        right={days.length ? day(days[days.length - 1].start) : undefined}
        empty="No activity recorded yet. Days appear as the analysis consumes events."
      />

      <div class="readouts" style={{ marginTop: "18px" }}>
        <ReadoutSmall
          value={compact(events)}
          label="Events recorded"
          sub={`over ${plural(days.length, "day")}`}
        />
        <ReadoutSmall
          value={compact(zapSats)}
          label="Sats zapped"
          sub={plural(zapCount, "receipt")}
        />
      </div>
    </Panel>
  );
}

// --- active users ----------------------------------------------------------

function ActiveUsers({ raw }: { raw: unknown }) {
  const rep = (raw ?? {}) as Partial<ActiveUsersReport>;
  const daily = Object.values(asRecord<ActiveUsersReport["daily"][string]>(rep.daily))
    .filter((b) => b && Number.isFinite(b.start))
    .sort((a, b) => a.start - b.start)
    .slice(-90);
  const weekly = Object.values(asRecord<ActiveUsersReport["weekly"][string]>(rep.weekly))
    .filter((b) => b && Number.isFinite(b.start))
    .sort((a, b) => a.start - b.start);

  const peak = Math.max(1, ...daily.map((b) => total(b.users)));
  const last = daily[daily.length - 1];
  const lastWeek = weekly[weekly.length - 1];

  return (
    <Panel
      id="publishers"
      label="Publishers"
      aside={last ? `${compact(total(last.users))} on the last day` : undefined}
      note="Distinct pubkeys that published, counted with HyperLogLog sketches — close, not exact, and cheap enough to keep per day forever."
    >
      <Bars
        items={daily.map((b) => ({
          key: String(b.start),
          height: total(b.users) / peak,
          fill: total(b.users) > 0 ? b.users.trusted / total(b.users) : 0,
          title: `${day(b.start)} — ${num(total(b.users))} publishers, ${num(b.users.trusted)} trusted`,
        }))}
        // Deliberately shorter than the activity chart: same instrument,
        // secondary series, so the two do not compete.
        height={64}
        left={daily.length ? day(daily[0].start) : undefined}
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
              <span class="count">{num(t.mentions)}</span>
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
function Kinds({ breakdown, activity }: { breakdown: unknown; activity: unknown }) {
  const direct = asRecord<TrustedCount>(breakdown);
  const derived: Record<string, TrustedCount> = {};
  if (Object.keys(direct).length === 0) {
    for (const d of Object.values(asRecord<DailyActivity>(activity))) {
      for (const [kind, c] of Object.entries(d.kinds ?? {})) {
        const slot = (derived[kind] ??= { trusted: 0, untrusted: 0 });
        slot.trusted += c.trusted ?? 0;
        slot.untrusted += c.untrusted ?? 0;
      }
    }
  }
  const fromActivity = Object.keys(direct).length === 0;
  const entries = Object.entries(fromActivity ? derived : direct)
    .filter(([, c]) => c && total(c) > 0)
    .sort((a, b) => total(b[1]) - total(a[1]))
    .slice(0, 14);
  const peak = entries.length ? total(entries[0][1]) : 1;

  return (
    <Panel
      id="kinds"
      label="Kinds"
      aside={fromActivity && entries.length ? "from activity" : undefined}
      note="What the corpus is made of, split by whether the author is inside the web of trust."
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
                <th class="num">Trusted</th>
                <th>Split</th>
              </tr>
            </thead>
            <tbody>
              {entries.map(([kind, c]) => (
                <tr key={kind}>
                  <td>
                    {kind} · {kindLabel(kind)}
                  </td>
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

// --- clients ---------------------------------------------------------------

function Clients({ raw }: { raw: unknown }) {
  const entries = Object.entries(asRecord<ClientStats>(raw))
    .filter(([, s]) => s && typeof s.sum === "number")
    .sort((a, b) => b[1].sum - a[1].sum);
  const grand = entries.reduce((n, [, s]) => n + s.sum, 0);
  const top = entries.slice(0, 14);

  return (
    <Panel
      id="clients"
      label="Clients"
      aside={entries.length ? `${num(entries.length)} seen` : undefined}
      note="Taken from the client tag, which is self-reported and optional — events without one are counted as N/A rather than guessed at."
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

/** `pagerank`: `[[pubkey, score], …]`, already sorted, top 1000. */
function Rank({ raw }: { raw: unknown }) {
  const rows = asArray<[string, number]>(raw)
    .filter((r) => Array.isArray(r) && typeof r[0] === "string")
    .slice(0, 12);
  const peak = rows.length ? rows[0][1] : 1;

  return (
    <Panel
      id="rank"
      label="Rank"
      aside={rows.length ? `top ${rows.length}` : undefined}
      note="PageRank over the follow graph, recomputed on a schedule rather than per event. This is the signal that decides whose events count as trusted everywhere else on this page."
    >
      {rows.length === 0 ? (
        <p class="empty">No ranks computed yet.</p>
      ) : (
        <ol class="ranks">
          {rows.map(([pubkey, score], i) => (
            <li key={pubkey}>
              <span class="rank">{String(i + 1).padStart(2, "0")}</span>
              <span class="tag" title={pubkey}>
                {shortKey(pubkey)}
              </span>
              <span class="track">
                <Meter value={score} max={peak} />
              </span>
              <span class="count">{score.toFixed(4)}</span>
            </li>
          ))}
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
      {has("active_users") ? <ActiveUsers raw={data.active_users} /> : null}
      {has("trending_hashtags") ? <Trending raw={data.trending_hashtags} /> : null}
      {has("kind_breakdown") || has("activity") ? (
        <Kinds breakdown={data.kind_breakdown} activity={data.activity} />
      ) : null}
      {has("client_tags") ? <Clients raw={data.client_tags} /> : null}
      {has("pagerank") ? <Rank raw={data.pagerank} /> : null}
    </section>
  );
}
