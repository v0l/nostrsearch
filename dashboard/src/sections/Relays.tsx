import { useState } from "preact/hooks";
import { api } from "../api";
import type { SyncStatus } from "../types";
import { Bars, Chip, ConfirmButton, Panel, ago, compact, num, useNotify } from "../ui";

/**
 * Backfill coverage: one bar per calendar day the scraper has finished, height
 * for the events relays returned and a solid fill for the share that was new to
 * the index. A tall hollow bar is a day that was re-read for nothing.
 *
 * Secondary to everything the reports say — this is how the corpus is being
 * filled, not what is in it.
 */
/**
 * How much scraping is left, and how wide the net currently is.
 *
 * The scraper does not sweep every discovered relay: it ranks them by how
 * many people advertise them and covers a share of that weight, starting
 * narrow so the most-used relays actually finish, then widening as the
 * backlog drains. Without showing the cut, "relays: 40 of 8072" reads as a
 * fault rather than the intended behaviour.
 */
function HorizonPanel({ sync }: { sync: SyncStatus | null }) {
  const h = sync?.horizon;
  if (!h) return null;
  const pct = Math.max(0, Math.min(100, h.percent_complete));

  return (
    <>
      <div class="row tight" style={{ marginBottom: "10px" }}>
        <Chip tone={h.relay_days_remaining > 0 ? "mute" : "ok"}>
          {compact(h.relay_days_remaining)} relay-days left
        </Chip>
        <Chip tone="mute">
          {compact(h.relay_days_done)} / {compact(h.relay_days_total)} done
        </Chip>
        <Chip tone="mute">
          {num(h.relays)} of {num(h.relays_discovered)} relays &middot; top{" "}
          {h.usage_percentile}% by usage
        </Chip>
        <Chip tone="mute">
          {h.oldest_day} &rarr; now &middot; {num(h.days)} days
        </Chip>
      </div>
      <div
        class="hbar"
        title={`${pct}% of the current horizon scraped`}
        aria-label={`${pct}% complete`}
      >
        <span style={{ width: `${pct}%` }} />
      </div>
      <p class="panel-note" style={{ marginTop: "6px" }}>
        Relays are ranked by how many people advertise them; the cut starts
        narrow so the most-used finish first, then widens as coverage fills in.
        The total moves when it widens.
      </p>
    </>
  );
}

function Backfill({ sync }: { sync: SyncStatus | null }) {
  const byDay = new Map<string, { seen: number; fresh: number; relays: number }>();
  for (const d of sync?.scrape.recent ?? []) {
    const e = byDay.get(d.date) ?? { seen: 0, fresh: 0, relays: 0 };
    e.seen += d.seen;
    e.fresh += d.new;
    e.relays += 1;
    byDay.set(d.date, e);
  }
  const days = [...byDay.entries()].sort((a, b) => a[0].localeCompare(b[0]));
  const peak = Math.max(1, ...days.map(([, v]) => v.seen));
  const p = sync?.scrape;

  return (
    <>
      <div class="row tight" style={{ marginBottom: "12px" }}>
        <Chip tone="mute">{num(p?.relay_days)} relay-days done</Chip>
        <Chip tone="mute">{num(p?.days)} dates</Chip>
        <Chip tone="mute">{compact(p?.events_new)} events kept</Chip>
        {p?.oldest_day && p?.newest_day ? (
          <Chip tone="mute">
            {p.oldest_day} → {p.newest_day}
          </Chip>
        ) : null}
      </div>
      <Bars
        items={days.map(([date, v]) => ({
          key: date,
          height: v.seen / peak,
          fill: v.seen > 0 ? v.fresh / v.seen : 0,
          title: `${date} — ${num(v.seen)} returned, ${num(v.fresh)} new, ${v.relays} relay${v.relays === 1 ? "" : "s"}`,
        }))}
        height={72}
        left={days.length ? days[0][0] : undefined}
        center={days.length ? `peak ${compact(peak)} events/day` : undefined}
        right={days.length ? days[days.length - 1][0] : undefined}
        empty="The scraper has not finished a relay-day yet."
      />
    </>
  );
}

/**
 * The most recent relay-days, individually.
 *
 * `recent` was only ever aggregated into the bars above, which shows how much
 * arrived but not where from. Work is drawn at random now, so the useful
 * question while watching a pass is which relay is being hit right now and
 * whether it is returning anything -- a relay quietly returning zero looks
 * identical to a busy one in a per-date total.
 */
function RecentDays({ sync }: { sync: SyncStatus | null }) {
  const rows = (sync?.scrape.recent ?? []).slice(0, 12);
  if (rows.length === 0) return null;

  return (
    <>
      <hr class="hr" />
      <h3
        style={{
          font: "600 10px/1 var(--mono)",
          letterSpacing: "0.16em",
          textTransform: "uppercase",
          color: "var(--slate)",
          margin: "0 0 12px",
        }}
      >
        Recently scraped
        <span class="sub-note">newest first, updates as the pass runs</span>
      </h3>
      <ul class="feed">
        {rows.map((d) => (
          <li key={`${d.relay}|${d.date}`}>
            <span class="feed-when">{ago(d.at)}</span>
            <span class="feed-what" title={d.relay}>
              {d.relay.replace(/^wss?:\/\//, "").replace(/\/$/, "")}
            </span>
            <span class="feed-day">{d.date}</span>
            <span
              class="feed-num"
              title={`${num(d.seen)} returned, ${num(d.new)} new to the index`}
              style={{ color: d.seen === 0 ? "var(--slate)" : undefined }}
            >
              {compact(d.seen)}
              {d.new > 0 ? <em class="feed-new"> +{compact(d.new)}</em> : null}
            </span>
          </li>
        ))}
      </ul>
    </>
  );
}

function negentropy(v: boolean | null) {
  if (v === true) return <Chip tone="ok">Negentropy</Chip>;
  if (v === false) return <Chip tone="mute">Windowed REQ</Chip>;
  return <Chip tone="mute">Unprobed</Chip>;
}

export function Relays({
  sync,
  authed,
  gate,
  offset,
  onPage,
}: {
  sync: SyncStatus | null;
  authed: boolean;
  gate: string;
  offset: number;
  onPage: (next: number) => void;
}) {
  const notify = useNotify();
  const [relay, setRelay] = useState("");
  const [from, setFrom] = useState("");
  const [to, setTo] = useState("");
  const [match, setMatch] = useState<number | null>(null);

  const filters = () => ({
    relay: relay.trim() || undefined,
    from: from.trim() || undefined,
    to: to.trim() || undefined,
  });

  const preview = async () => {
    try {
      const r = await api.scrape(filters());
      setMatch(r.matching_days?.count ?? r.progress.relay_days);
    } catch (e) {
      notify("err", e instanceof Error ? e.message : String(e));
    }
  };

  const clearDays = async () => {
    try {
      const r = await api.resetScrape(filters());
      notify("ok", `${num(r.reset_days)} relay-days will be scraped again.`);
      setMatch(null);
    } catch (e) {
      notify("err", e instanceof Error ? e.message : String(e));
    }
  };

  const forgetRelay = async (url: string) => {
    try {
      const r = await api.resetRelay(url);
      notify("ok", r.detail);
    } catch (e) {
      notify("err", e instanceof Error ? e.message : String(e));
    }
  };

  const rows = sync?.relays.top ?? [];

  return (
    <Panel
      id="relays"
      label="Relays"
      aside={sync ? `${num(sync.relays.failing)} failing` : undefined}
      note="Relays discovered from published relay lists, ranked by how many people advertise them. Forgetting a relay clears what the scraper learned about it — horizon, failures and page size — and it starts over."
    >
      <HorizonPanel sync={sync} />
      <Backfill sync={sync} />
      <RecentDays sync={sync} />

      <hr class="hr" />

      <div class="row tight" style={{ marginBottom: "16px" }}>
        <Chip tone="mute">{num(sync?.relays.total)} known</Chip>
        <Chip tone="ok">{num(sync?.relays.negentropy)} negentropy</Chip>
        <Chip tone="mute">{num(sync?.relays.no_negentropy)} windowed</Chip>
        <Chip tone="mute">{num(sync?.relays.unprobed)} unprobed</Chip>
        {sync && sync.relays.failing > 0 ? <Chip tone="bad">{num(sync.relays.failing)} failing</Chip> : null}
      </div>

      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Relay</th>
              <th title="Inside the usage-weight cut, and therefore scraped">
                Scope
              </th>
              <th>Sync method</th>
              <th class="num">Advertised by</th>
              <th class="num">Days</th>
              <th class="num">Events</th>
              <th class="num" title="new to the index">New</th>
              <th class="num">Page size</th>
              <th class="num">Fails</th>
              <th>Last success</th>
              {authed ? <th /> : null}
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td colSpan={authed ? 11 : 10} class="empty">
                  No relays discovered yet.
                </td>
              </tr>
            ) : (
              rows.map((r) => (
                <tr key={r.url}>
                  <td class="trunc" title={r.url}>
                    {r.url}
                  </td>
                  <td>
                    {/* The list holds every relay ever discovered; only a few
                        dozen are in scope. Without saying which, a relay
                        sitting at zero reads as broken rather than simply not
                        being worked on. */}
                    {r.in_scope ? (
                      <Chip tone="ok">scraping</Chip>
                    ) : (
                      <Chip tone="mute">out of scope</Chip>
                    )}
                  </td>
                  <td>{negentropy(r.negentropy)}</td>
                  <td class="num">{num(r.sources)}</td>
                  <td class="num">{num(r.days)}</td>
                  <td class="num">{compact(r.events_seen)}</td>
                  <td class="num" title="new to the index">{compact(r.events_new)}</td>
                  <td class="num">{num(r.cap)}</td>
                  <td class="num" style={{ color: r.fails > 0 ? "var(--rust)" : undefined }}>
                    {r.dead_until && r.dead_until * 1000 > Date.now() ? (
                      <span title={`dead, retrying ${ago(r.dead_until)}`}>dead</span>
                    ) : (
                      num(r.fails)
                    )}
                  </td>
                  <td>{ago(r.last_ok)}</td>
                  {authed ? (
                    <td>
                      <ConfirmButton
                        tiny
                        label="Forget"
                        confirmLabel="Confirm forget"
                        onConfirm={() => forgetRelay(r.url)}
                      />
                    </td>
                  ) : null}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {sync && sync.relays.total > rows.length ? (
        <div class="pager">
          <button
            class="btn tiny"
            disabled={offset <= 0}
            onClick={() => onPage(Math.max(0, offset - sync.relays.limit))}
          >
            &larr; Prev
          </button>
          <span class="pager-at">
            {num(offset + 1)}&ndash;{num(offset + rows.length)} of{" "}
            {num(sync.relays.total)}
          </span>
          <button
            class="btn tiny"
            disabled={offset + rows.length >= sync.relays.total}
            onClick={() => onPage(offset + sync.relays.limit)}
          >
            Next &rarr;
          </button>
        </div>
      ) : null}

      <hr class="hr" />

      <h3
        style={{
          font: "600 10px/1 var(--mono)",
          letterSpacing: "0.16em",
          textTransform: "uppercase",
          color: "var(--slate)",
          margin: "0 0 12px",
        }}
      >
        Scrape again
      </h3>
      <p class="panel-note">
        Clearing a day record makes the scraper re-fetch that relay and date on its next pass. Check
        the match count first — with every field empty, this clears the entire backfill history.
      </p>

      {!authed ? (
        <p class="empty">{gate}</p>
      ) : (
        <div class="row">
          <label class="field">
            Relay
            <input
              placeholder="wss://relay.example"
              value={relay}
              onInput={(e) => {
                setRelay((e.target as HTMLInputElement).value);
                setMatch(null);
              }}
              size={26}
            />
          </label>
          <label class="field">
            From
            <input
              placeholder="2024-01-01"
              value={from}
              onInput={(e) => {
                setFrom((e.target as HTMLInputElement).value);
                setMatch(null);
              }}
              size={12}
            />
          </label>
          <label class="field">
            To
            <input
              placeholder="2024-12-31"
              value={to}
              onInput={(e) => {
                setTo((e.target as HTMLInputElement).value);
                setMatch(null);
              }}
              size={12}
            />
          </label>
          <button onClick={preview}>Count matches</button>
          <ConfirmButton
            label={match === null ? "Clear day records" : `Clear ${num(match)} day records`}
            confirmLabel="Confirm clear"
            onConfirm={clearDays}
          />
          {match !== null ? (
            <span class="mono-key" style={{ paddingBottom: "10px" }}>
              {num(match)} records match these filters
            </span>
          ) : null}
        </div>
      )}
    </Panel>
  );
}
