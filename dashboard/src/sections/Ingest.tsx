import { useRef } from "preact/hooks";
import { api } from "../api";
import type { ReplayStatus } from "../types";
import { Chip, ConfirmButton, Meter, Panel, num, useNotify, usePoll } from "../ui";

/**
 * Read rate and time remaining, from successive polls.
 *
 * The node reports counters, not speed, and a 490 GiB archive is the case this
 * panel exists for: without a rate there is no way to tell a slow ingest from a
 * stalled one, and no way to answer "how long".
 */
type ReadRate =
  | { state: "measuring" }
  | { state: "moving"; eventsPerSec: number; etaSecs: number | null }
  | { state: "stalled"; stillSecs: number };

/** No new events for this long, while running, is worth saying out loud. */
const STALL_SECS = 30;

function useReadRate(st: ReplayStatus | undefined, corpus: number | null): ReadRate | null {
  const last = useRef<{ seen: number; at: number } | null>(null);
  const movedAt = useRef<number>(Date.now());
  const rate = useRef<number | null>(null);

  if (!st?.running) {
    last.current = null;
    rate.current = null;
    return null;
  }

  const now = Date.now();
  const prev = last.current;

  if (!prev) {
    last.current = { seen: st.seen, at: now };
    movedAt.current = now;
  } else {
    const delta = st.seen - prev.seen;
    const secs = (now - prev.at) / 1000;
    if (delta > 0 && secs >= 1) {
      const sample = delta / secs;
      // Smooth it: one slow poll should not make the estimate jump.
      rate.current = rate.current === null ? sample : rate.current * 0.7 + sample * 0.3;
      last.current = { seen: st.seen, at: now };
      movedAt.current = now;
    }
  }

  const stillSecs = (now - movedAt.current) / 1000;
  if (stillSecs > STALL_SECS) return { state: "stalled", stillSecs };
  if (!rate.current || rate.current <= 0) return { state: "measuring" };

  // The archive's unique-event count is the only total available, and it is a
  // floor: the passes after the first re-read the same events to feed the
  // dependent reports, so an estimate covers this pass, not the whole run.
  const left = corpus && corpus > st.seen ? corpus - st.seen : null;
  return {
    state: "moving",
    eventsPerSec: rate.current,
    etaSecs: left === null ? null : left / rate.current,
  };
}

function duration(secs: number): string {
  if (!isFinite(secs)) return "—";
  if (secs < 90) return `${Math.round(secs)}s`;
  if (secs < 5400) return `${Math.round(secs / 60)}m`;
  if (secs < 172800) return `${(secs / 3600).toFixed(1)}h`;
  return `${(secs / 86400).toFixed(1)}d`;
}

function Progress({ st, corpus }: { st: ReplayStatus; corpus: number | null }) {
  const rate = useReadRate(st, corpus);

  return (
    <div class="stack">
      <div class="row tight">
        <Chip tone={st.running ? "warn" : st.cancelled ? "bad" : "ok"} dot={st.running}>
          {st.running ? "Ingesting" : st.cancelled ? "Stopped" : "Idle"}
        </Chip>
        {/* The archive is read once per dependency stage: the follow graph is
            built first, then the reports that label events using it. Only the
            first pass writes to the index, so counters legitimately stop
            rising on later passes. */}
        {st.passes > 1 ? (
          <Chip tone="mute">
            pass {st.pass + 1}/{st.passes}
            {st.pass === 0 ? " · indexing" : " · folding reports"}
          </Chip>
        ) : null}
        <Chip tone="mute">{num(st.seen)} read</Chip>
        <Chip tone="mute">{num(st.indexed)} indexed</Chip>
        {st.skipped > 0 ? <Chip tone="mute">{num(st.skipped)} already had</Chip> : null}
      </div>

      {corpus ? (
        <div class="stack tight">
          <Meter value={Math.min(st.seen, corpus)} max={corpus} />
          <div class="mono-key">
            {num(st.seen)} of ~{num(corpus)} events in the archive
          </div>
        </div>
      ) : null}

      {st.running ? (
        <div class="mono-key">
          {rate?.state === "moving"
            ? `${num(Math.round(rate.eventsPerSec))} events/s` +
              (rate.etaSecs !== null ? ` · ~${duration(rate.etaSecs)} left this pass` : "")
            : rate?.state === "stalled"
              ? `no new events for ${duration(rate.stillSecs)}`
              : "measuring rate…"}
        </div>
      ) : st.finished_at > 0 ? (
        <div class="mono-key">
          finished {new Date(st.finished_at * 1000).toLocaleString()}
        </div>
      ) : null}
    </div>
  );
}

export function Ingest({ authed, gate }: { authed: boolean; gate: string }) {
  const notify = useNotify();

  const status = usePoll<ReplayStatus>(api.ingest, 2000, authed);
  const archive = usePoll(api.archiveStats, 60_000, true);
  const corpus = archive.data?.total_events ?? null;

  const start = async (dedupe: boolean) => {
    try {
      const r = await api.startIngest(dedupe);
      notify("ok", r.detail);
      status.refresh();
    } catch (e) {
      notify("err", e instanceof Error ? e.message : String(e));
    }
  };

  const stop = async () => {
    try {
      await api.cancelIngest();
      notify("ok", "Ingest will stop at the next chunk boundary.");
      status.refresh();
    } catch (e) {
      notify("err", e instanceof Error ? e.message : String(e));
    }
  };

  const running = status.data?.running ?? false;
  const unsupported = status.error?.includes("archive directory");

  return (
    <Panel
      id="ingest"
      label="Archive ingest"
      aside={running ? "running" : undefined}
      note="Reads the whole archive into this node, using the same engine as the ingest CLI. Runs alongside live traffic."
    >
      {!authed ? (
        <p class="empty">{gate}</p>
      ) : unsupported ? (
        <p class="empty">This node has no archive directory, so it cannot ingest.</p>
      ) : (
        <div class="stack">
          {status.data ? (
            <Progress st={status.data} corpus={corpus} />
          ) : (
            <p class="empty">Loading ingest status…</p>
          )}

          <hr class="hr" />

          <div class="row tight">
            <button class="primary" disabled={running} onClick={() => start(true)}>
              Ingest missing events
            </button>
            {/* Re-indexing everything is the repair for a dedupe store that has
                drifted ahead of the index. While it is ahead, the normal path
                skips the very events that are missing, so no number of ordinary
                runs will ever close the gap. It is also hours of work, hence the
                confirmation. */}
            <ConfirmButton
              label="Re-index everything"
              confirmLabel="Ignores the dedupe store — hours of work, confirm"
              disabled={running}
              onConfirm={() => start(false)}
            />
            <ConfirmButton
              label="Stop"
              confirmLabel="Confirm stop"
              disabled={!running}
              onConfirm={stop}
            />
          </div>
        </div>
      )}
    </Panel>
  );
}
