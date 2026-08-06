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
      <Backfill sync={sync} />

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
              <th>Sync method</th>
              <th class="num">Advertised by</th>
              <th class="num">Page size</th>
              <th class="num">Fails</th>
              <th>Last success</th>
              {authed ? <th /> : null}
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td colSpan={authed ? 7 : 6} class="empty">
                  No relays discovered yet.
                </td>
              </tr>
            ) : (
              rows.map((r) => (
                <tr key={r.url}>
                  <td class="trunc" title={r.url}>
                    {r.url}
                  </td>
                  <td>{negentropy(r.negentropy)}</td>
                  <td class="num">{num(r.sources)}</td>
                  <td class="num">{num(r.cap)}</td>
                  <td class="num" style={{ color: r.fails > 0 ? "var(--rust)" : undefined }}>
                    {num(r.fails)}
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
