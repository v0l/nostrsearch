import { useEffect, useRef, useState } from "preact/hooks";
import {
  ApiError,
  NO_SIGNER,
  api,
  hasSigner,
  waitForSigner,
  type Submission,
  type SubmitLimits,
} from "../api";
import { Bars, Chip, ago, bytes, num, useNotify } from "../ui";

const ACCEPT = ".jsonl,.jsonl.zst,.jsonl.gz,.jsonl.bz2,.mdb,.mdb.zst,.mdb.gz";

function Counts({ s }: { s: Submission }) {
  const rows: [string, number][] = [
    ["new", s.new],
    ["duplicate", s.duplicate],
    ["bad signature", s.bad_signature],
    ["malformed", s.malformed],
    ["undecodable", s.undecodable],
    ["ephemeral", s.ephemeral],
    ["future", s.future],
  ];
  const max = Math.max(1, ...rows.map(([, n]) => n));
  return (
    <div class="stack">
      <div class="row tight">
        <Chip tone={s.state === "done" ? "ok" : s.state === "failed" || s.state === "interrupted" ? "bad" : "warn"} dot={s.state === "processing"}>
          {s.state}
        </Chip>
        <Chip tone="mute">{s.state === "queued" ? "waiting in queue" : `${Math.floor(s.percent)}% read`}</Chip>
        <Chip tone="mute">{num(s.lines)} lines</Chip>
        <Chip tone="mute">{num(s.valid)} valid</Chip>
      </div>
      <Bars
        height={72}
        items={rows.map(([label, n]) => ({
          key: label,
          height: n / max,
          fill: n > 0 ? 1 : 0,
          title: `${label}: ${num(n)}`,
          gap: n === 0,
        }))}
        left={rows.map(([l, n]) => `${l} ${num(n)}`).join(" · ")}
      />
      {s.error ? <p class="empty">{s.error}</p> : null}
    </div>
  );
}

export function Submit({ onDone }: { onDone?: () => void }) {
  const notify = useNotify();
  const input = useRef<HTMLInputElement>(null);
  const [sent, setSent] = useState<{ sent: number; total: number } | null>(null);
  const [result, setResult] = useState<Submission | null>(null);
  const busy = useRef(false);

  const follow = async (id: string) => {
    for (;;) {
      const s = await api.submission(id);
      setResult(s);
      if (s.state === "done" || s.state === "failed" || s.state === "interrupted") {
        onDone?.();
        return;
      }
      await new Promise((r) => setTimeout(r, 1500));
    }
  };

  const go = async () => {
    const file = input.current?.files?.[0];
    if (!file || busy.current) return;
    if (!hasSigner()) {
      notify("err", NO_SIGNER);
      return;
    }
    busy.current = true;
    setResult(null);
    setSent({ sent: 0, total: file.size });
    try {
      const { id } = await api.uploadDump(file, (s, t) => setSent({ sent: s, total: t }));
      notify("ok", "Upload received and queued. It is processed in the background.");
      await follow(id);
    } catch (e) {
      notify("err", e instanceof ApiError || e instanceof Error ? e.message : String(e));
    } finally {
      busy.current = false;
      setSent(null);
    }
  };

  return (
    <div class="stack upload-form">
      <div class="row tight">
        <input ref={input} type="file" accept={ACCEPT} disabled={busy.current} />
        <button class="primary" disabled={!!sent} onClick={go}>
          Upload
        </button>
      </div>
      {sent ? (
        <div class="mono-key">
          uploading {bytes(sent.sent)} of {bytes(sent.total)}
        </div>
      ) : null}
      {result ? <Counts s={result} /> : null}
    </div>
  );
}

const gb = (n: number): string => `${Number((n / 1e9).toFixed(1))} GB`;

function History({ reload }: { reload: number }) {
  const [rows, setRows] = useState<Submission[] | null>(null);
  const [err, setErr] = useState<string | null>(null);
  useEffect(() => {
    if (!hasSigner()) return;
    let alive = true;
    api
      .submissions()
      .then((r) => alive && setRows(r))
      .catch((e) => alive && setErr(e instanceof Error ? e.message : String(e)));
    return () => {
      alive = false;
    };
  }, [reload]);

  if (!hasSigner()) return <p class="empty">Connect a NIP-07 signer to see your past submissions.</p>;
  if (err) return <p class="empty">{err}</p>;
  if (!rows) return <p class="empty">Loading your submissions…</p>;
  if (rows.length === 0) return <p class="empty">You have not submitted anything yet.</p>;
  return (
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>File</th>
            <th>State</th>
            <th class="num">Progress</th>
            <th class="num">Size</th>
            <th class="num">Events</th>
            <th class="num">New</th>
            <th class="num">Rejected</th>
            <th>When</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((s) => (
            <tr key={s.id}>
              <td>{s.name}</td>
              <td>
                <Chip tone={s.state === "done" ? "ok" : s.state === "failed" || s.state === "interrupted" ? "bad" : "warn"}>{s.state}</Chip>
              </td>
              <td class="num">{s.state === "processing" || s.state === "done" ? `${Math.floor(s.percent)}%` : "-"}</td>
              <td class="num">{bytes(s.bytes)}</td>
              <td class="num">{num(s.lines)}</td>
              <td class="num">{num(s.new)}</td>
              <td class="num">{num(s.bad_signature + s.malformed + s.undecodable + s.ephemeral + s.future)}</td>
              <td>{ago(s.started_at)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function UploadPage() {
  const [limits, setLimits] = useState<SubmitLimits | null>(null);
  const [reload, setReload] = useState(0);
  const [, setSigner] = useState(false);
  useEffect(() => {
    api.submitLimits().then(setLimits).catch(() => {});
    waitForSigner().then(setSigner);
  }, []);

  return (
    <div class="upload-page">
      <header class="upload-head">
        <a class="wordmark" href="/">
          nostr<span>search</span>
          <small>Archive</small>
        </a>
        <a href="/">Back to the console</a>
      </header>

      <section class="panel">
        <div class="plate">
          <span>Send us your nostr data</span>
        </div>
        <p class="lede">
          Relays prune old events, and nobody recorded everything. If you run a relay or kept a backup, your copy may
          hold events the rest of the network has lost, especially from October 2024 to August 2025. Upload it here and
          anything we do not already have joins the public archive and search.
        </p>

        <div class="upload-grid">
          <div>
            <h3 class="subhead">What you can send</h3>
            <ul class="plain">
              <li>
                A JSON lines file with one nostr event per line: <code>.jsonl</code>, optionally <code>.zst</code>,{" "}
                <code>.gz</code> or <code>.bz2</code> compressed.
              </li>
              <li>
                A strfry database: the <code>data.mdb</code> file from your <code>strfry-db</code> folder, as is or
                compressed with zstd or gzip. Stop strfry first, or copy the file while it is idle.
              </li>
            </ul>
            <h3 class="subhead">How to export</h3>
            <ul class="plain">
              <li>
                strfry: <code>strfry export &gt; events.jsonl</code>, or send <code>data.mdb</code> directly.
              </li>
              <li>
                Any relay or client: <code>nak req --paginate wss://your.relay &gt; events.jsonl</code>.
              </li>
            </ul>
          </div>
          <div>
            <h3 class="subhead">What happens to it</h3>
            <ul class="plain">
              <li>Every event's signature is checked. Anything forged or altered is dropped.</li>
              <li>Events we already hold are counted and skipped.</li>
              <li>
                Ephemeral events and events dated in the future are dropped. Old dates are kept, including backdated
                imports.
              </li>
              <li>
                The file is stored as you send it, then checked and indexed in a background queue. The stored copy is
                deleted when it is done, and only the valid events are kept.
              </li>
              <li>You sign the upload with your nostr key, so each submission is tied to a pubkey.</li>
            </ul>
            {limits ? (
              <>
                <h3 class="subhead">Limits</h3>
                <ul class="plain">
                  <li>No size limit, as long as it fits above the free space floor.</li>
                  <li>{limits.disk_note}</li>
                  <li>{limits.resume_note}</li>
                  <li>
                    Uploads stop if free disk space drops below {gb(limits.min_free_bytes)}. {gb(limits.free_bytes)} is
                    free now, so up to {gb(limits.acceptable_bytes)} can be accepted.
                  </li>
                  <li>
                    Queue: {limits.queued_jobs} waiting or processing, {gb(limits.queued_bytes)} stored.
                  </li>
                  <li>{limits.max_concurrent} uploads at a time, {limits.process_concurrency} processed at a time.</li>
                </ul>
              </>
            ) : null}
          </div>
        </div>

        {limits && !limits.accepting ? (
          <p class="empty">The archive is short on disk space and is not accepting uploads right now.</p>
        ) : (
          <Submit onDone={() => setReload((n) => n + 1)} />
        )}
      </section>

      <section class="panel">
        <div class="plate">
          <span>Your submissions</span>
        </div>
        <History reload={reload} />
      </section>
    </div>
  );
}
