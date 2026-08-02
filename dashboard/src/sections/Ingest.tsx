import { useMemo, useState } from "preact/hooks";
import { api } from "../api";
import type { ArchiveFileInfo, ReplayStatus } from "../types";
import { Chip, ConfirmButton, Meter, Panel, bytes, num, useNotify, usePoll } from "../ui";

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

function Progress({ st }: { st: ReplayStatus }) {
  const active = st.files.filter((f) => !f.complete || f.error);
  const rows = active.length ? active : st.files.slice(0, 5);
  return (
    <div class="stack">
      <div class="row tight">
        <Chip tone={st.running ? "warn" : st.cancelled ? "bad" : "ok"} dot={st.running}>
          {st.running ? "Replaying" : st.cancelled ? "Stopped" : "Idle"}
        </Chip>
        <Chip tone="mute">
          {num(st.files_done)}/{num(st.files_total)} files
        </Chip>
        <Chip tone="mute">{num(st.events)} read</Chip>
        <Chip tone="mute">{num(st.new)} new</Chip>
        {st.malformed > 0 ? <Chip tone="bad">{num(st.malformed)} malformed</Chip> : null}
      </div>

      {st.current ? <div class="mono-key">Reading {st.current}</div> : null}

      {rows.length > 0 ? (
        <div class="stack" style={{ gap: "10px" }}>
          {rows.map((f) => (
            <div key={f.name}>
              <div
                class="row tight"
                style={{ justifyContent: "space-between", fontSize: "11px", marginBottom: "4px" }}
              >
                <span class="trunc">{f.name}</span>
                <span style={{ color: "var(--slate)" }}>
                  {bytes(f.bytes_read)} / {bytes(f.bytes_total)} · {num(f.events)} events
                </span>
              </div>
              <Meter value={f.bytes_read} max={f.bytes_total} tone={f.error ? "rust" : undefined} />
              {f.error ? <div style={{ color: "var(--rust)", fontSize: "11px" }}>{f.error}</div> : null}
            </div>
          ))}
        </div>
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
