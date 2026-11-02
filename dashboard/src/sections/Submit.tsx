import { useRef, useState } from "preact/hooks";
import { ApiError, NO_SIGNER, api, hasSigner, type Submission } from "../api";
import { Bars, Chip, Panel, bytes, num, useNotify } from "../ui";

const ACCEPT = ".jsonl,.jsonl.zst,.jsonl.gz,.jsonl.bz2";

function Counts({ s }: { s: Submission }) {
  const rows: [string, number][] = [
    ["new", s.new],
    ["duplicate", s.duplicate],
    ["bad signature", s.bad_signature],
    ["malformed", s.malformed],
    ["ephemeral", s.ephemeral],
    ["future", s.future],
  ];
  const max = Math.max(1, ...rows.map(([, n]) => n));
  return (
    <div class="stack">
      <div class="row tight">
        <Chip tone={s.state === "done" ? "ok" : s.state === "failed" ? "bad" : "warn"} dot={s.state === "processing"}>
          {s.state}
        </Chip>
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

export function Submit() {
  const notify = useNotify();
  const input = useRef<HTMLInputElement>(null);
  const [sent, setSent] = useState<{ sent: number; total: number } | null>(null);
  const [result, setResult] = useState<Submission | null>(null);
  const busy = useRef(false);

  const follow = async (id: string) => {
    for (;;) {
      const s = await api.submission(id);
      setResult(s);
      if (s.state === "done" || s.state === "failed") return;
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
      notify("ok", "Upload received. Checking signatures and indexing.");
      await follow(id);
    } catch (e) {
      notify("err", e instanceof ApiError || e instanceof Error ? e.message : String(e));
    } finally {
      busy.current = false;
      setSent(null);
    }
  };

  return (
    <Panel
      id="submit"
      label="Submit a dump"
      note="Send a relay export or personal backup (.jsonl, .jsonl.zst, .jsonl.gz, .jsonl.bz2). Every event's signature is checked and only events we do not have are added. You sign the upload with your nostr key."
    >
      <div class="stack">
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
    </Panel>
  );
}
