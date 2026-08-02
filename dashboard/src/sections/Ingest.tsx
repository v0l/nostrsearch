import { useMemo, useRef, useState } from "preact/hooks";
import { api } from "../api";
import type { ArchiveFileInfo, FileProgress, ReplayStatus } from "../types";
import { Chip, ConfirmButton, Meter, Panel, bytes, num, plural, useNotify, usePoll } from "../ui";

/**
 * Read rate and time remaining for the file being read, from successive polls.
 *
 * The node reports bytes, not speed, and a 122 GiB dump is the case this panel
 * exists for: without a rate there is no way to tell a slow replay from a
 * stalled one, and no way to answer "how long".
 */
type ReadRate =
  | { state: "measuring" }
  | { state: "moving"; bytesPerSec: number; etaSecs: number }
  | { state: "stalled"; stillSecs: number };

/** No new bytes for this long, while running, is worth saying out loud. */
const STALL_SECS = 20;

function useReadRate(current: FileProgress | null | undefined): ReadRate | null {
  const last = useRef<{ name: string; bytes: number; at: number } | null>(null);
  const movedAt = useRef<number>(Date.now());
  const rate = useRef<number | null>(null);

  if (!current) {
    last.current = null;
    rate.current = null;
    return null;
  }

  const now = Date.now();
  const prev = last.current;

  if (!prev || prev.name !== current.name) {
    last.current = { name: current.name, bytes: current.bytes_read, at: now };
    movedAt.current = now;
    rate.current = null;
  } else {
    const delta = current.bytes_read - prev.bytes;
    const secs = (now - prev.at) / 1000;
    if (delta > 0 && secs >= 1) {
      const sample = delta / secs;
      // Smooth it: one slow poll should not make the estimate jump.
      rate.current = rate.current === null ? sample : rate.current * 0.7 + sample * 0.3;
      last.current = { name: current.name, bytes: current.bytes_read, at: now };
      movedAt.current = now;
    }
  }

  const stillSecs = (now - movedAt.current) / 1000;
  // A replay that has stopped reading looks identical to a slow one unless the
  // panel says so; this is the whole reason the node reports live bytes.
  if (stillSecs > STALL_SECS) return { state: "stalled", stillSecs };
  if (!rate.current || rate.current <= 0) return { state: "measuring" };

  const left = Math.max(0, current.bytes_total - current.bytes_read);
  return { state: "moving", bytesPerSec: rate.current, etaSecs: left / rate.current };
}

function duration(secs: number): string {
  if (!Number.isFinite(secs)) return "—";
  if (secs < 90) return `${Math.round(secs)}s`;
  if (secs < 5400) return `${Math.round(secs / 60)}m`;
  if (secs < 172_800) return `${(secs / 3600).toFixed(1)}h`;
  return `${(secs / 86_400).toFixed(1)}d`;
}

function FileRow({
  f,
  checked,
  toggle,
}: {
  f: ArchiveFileInfo;
  checked: boolean;
  toggle: (name: string) => void;
}) {
  return (
    <label class="filerow">
      <input type="checkbox" checked={checked} onChange={() => toggle(f.name)} />
      <span class="nm">{f.name}</span>
      <span class="sz">{bytes(f.size)}</span>
    </label>
  );
}

/** The file being read right now, with rate and time remaining. */
function Current({ f }: { f: FileProgress }) {
  const rate = useReadRate(f);
  const pct = f.bytes_total > 0 ? (f.bytes_read / f.bytes_total) * 100 : 0;

  return (
    <div class="current">
      <div class="row tight" style={{ justifyContent: "space-between" }}>
        <span class="trunc" style={{ fontWeight: 600 }}>
          {f.name}
        </span>
        <span class="pct">{pct.toFixed(1)}%</span>
      </div>

      <Meter value={f.bytes_read} max={f.bytes_total} tone={f.error ? "rust" : undefined} />

      <div class="row tight" style={{ justifyContent: "space-between", gap: "16px" }}>
        <span class="mono-key">
          {bytes(f.bytes_read)} of {bytes(f.bytes_total)}
        </span>
        <span
          class="mono-key"
          style={{ color: rate?.state === "stalled" ? "var(--rust)" : undefined }}
        >
          {rate?.state === "moving"
            ? `${bytes(rate.bytesPerSec)}/s · ${duration(rate.etaSecs)} left`
            : rate?.state === "stalled"
              ? `no new bytes for ${duration(rate.stillSecs)}`
              : "measuring rate…"}
        </span>
      </div>

      <div class="row tight">
        <Chip tone="mute">{num(f.events)} read</Chip>
        <Chip tone="mute">{num(f.new)} new</Chip>
        {f.malformed > 0 ? <Chip tone="warn">{num(f.malformed)} malformed</Chip> : null}
        {f.error ? <Chip tone="bad">{f.error}</Chip> : null}
      </div>
    </div>
  );
}

function Progress({ st }: { st: ReplayStatus }) {
  const cur = st.current_progress;
  // The top-level counters only cover finished files, so a replay of one huge
  // dump would otherwise read zero for hours.
  const events = st.events + (cur?.events ?? 0);
  const fresh = st.new + (cur?.new ?? 0);
  const malformed = st.malformed + (cur?.malformed ?? 0);
  const done = st.files.filter((f) => f.complete && !f.error);
  const failed = st.files.filter((f) => f.error);

  return (
    <div class="stack">
      <div class="row tight">
        <Chip tone={st.running ? "warn" : st.cancelled ? "bad" : "ok"} dot={st.running}>
          {st.running ? "Replaying" : st.cancelled ? "Stopped" : "Idle"}
        </Chip>
        <Chip tone="mute">
          {num(st.files_done)}/{num(st.files_total)} files
        </Chip>
        <Chip tone="mute">{num(events)} read</Chip>
        <Chip tone="mute">{num(fresh)} new</Chip>
        {malformed > 0 ? <Chip tone="bad">{num(malformed)} malformed</Chip> : null}
      </div>

      {cur ? (
        <Current f={cur} />
      ) : st.running ? (
        <div class="mono-key">Opening {st.current ?? "the next dump"}…</div>
      ) : null}

      {done.length > 0 || failed.length > 0 ? (
        <details class="finished">
          <summary>
            {plural(done.length, "file")} finished
            {failed.length > 0 ? `, ${plural(failed.length, "failed")}` : ""}
          </summary>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>File</th>
                  <th class="num">Read</th>
                  <th class="num">Events</th>
                  <th class="num">New</th>
                  <th>Outcome</th>
                </tr>
              </thead>
              <tbody>
                {[...failed, ...done].map((f) => (
                  <tr key={f.name}>
                    <td class="trunc" title={f.name}>
                      {f.name}
                    </td>
                    <td class="num">{bytes(f.bytes_read)}</td>
                    <td class="num">{num(f.events)}</td>
                    <td class="num">{num(f.new)}</td>
                    <td>
                      {f.error ? (
                        <span style={{ color: "var(--rust)" }}>{f.error}</span>
                      ) : f.malformed > 0 ? (
                        `${num(f.malformed)} malformed lines`
                      ) : (
                        "clean"
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </details>
      ) : null}
    </div>
  );
}

export function Ingest({ authed, gate }: { authed: boolean; gate: string }) {
  const notify = useNotify();
  const [picked, setPicked] = useState<Set<string>>(new Set());
  const [busy, setBusy] = useState(false);

  const status = usePoll<ReplayStatus>(api.ingest, 2000, authed);
  const files = usePoll<ArchiveFileInfo[]>(api.archiveFiles, 60_000, true);

  const list = useMemo(
    () => (files.data ?? []).slice().sort((a, b) => b.timestamp - a.timestamp),
    [files.data],
  );

  const toggle = (name: string) =>
    setPicked((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });

  const start = async () => {
    setBusy(true);
    try {
      const r = await api.startIngest([...picked]);
      notify("ok", r.detail);
      status.refresh();
    } catch (e) {
      notify("err", e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const stop = async () => {
    try {
      await api.cancelIngest();
      notify("ok", "Replay will stop at the next batch boundary.");
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
      label="Replay"
      aside={running ? "running" : undefined}
      note="Re-read archive dumps through the live writer to fill gaps. Events already in the index are skipped, and the replay yields to live traffic."
    >
      {!authed ? (
        <p class="empty">{gate}</p>
      ) : unsupported ? (
        <p class="empty">This node has no archive directory, so it cannot replay.</p>
      ) : (
        <div class="stack">
          {status.data ? <Progress st={status.data} /> : <p class="empty">Loading replay status…</p>}

          <hr class="hr" />

          <div class="row" style={{ justifyContent: "space-between" }}>
            <label class="field" style={{ flex: 1, minWidth: "280px" }}>
              Files to replay — none selected means every dump
              <div class="filelist">
                {list.length === 0 ? (
                  <div class="empty" style={{ padding: "12px" }}>
                    No dumps published by this node.
                  </div>
                ) : (
                  list.map((f) => (
                    <FileRow key={f.name} f={f} checked={picked.has(f.name)} toggle={toggle} />
                  ))
                )}
              </div>
            </label>
          </div>

          <div class="row tight">
            <button class="primary" disabled={busy || running} onClick={start}>
              {picked.size === 0 ? "Replay every dump" : `Replay ${picked.size} selected`}
            </button>
            {picked.size > 0 ? (
              <button class="tiny" onClick={() => setPicked(new Set())}>
                Clear selection
              </button>
            ) : null}
            <ConfirmButton
              label="Stop replay"
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
