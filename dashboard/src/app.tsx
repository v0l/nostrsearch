import { useCallback, useEffect, useState } from "preact/hooks";
import { api } from "./api";
import { useReports } from "./reports";
import { gateMessage, useSession, type Session } from "./session";
import type { RegistryStats, SyncStatus } from "./types";
import { Analyses } from "./sections/Analyses";
import { Corpus } from "./sections/Corpus";
import { Ingest } from "./sections/Ingest";
import { IndexPanel } from "./sections/IndexPanel";
import { Relays } from "./sections/Relays";
import { Reports } from "./sections/Reports";
import { Today } from "./sections/Today";
import { Chip, Toasts, ago, num, shortKey, usePoll, useNotify } from "./ui";

const SECTIONS = [
  { id: "today", label: "Today" },
  { id: "reports", label: "Reports" },
  { id: "corpus", label: "Corpus" },
  { id: "ingest", label: "Replay" },
  { id: "analyses", label: "Analyses" },
  { id: "relays", label: "Relays" },
  { id: "index", label: "Index" },
] as const;

function Identity({ session }: { session: Session }) {
  const s = session.state;

  switch (s.status) {
    case "loading":
      return <span class="mono-key">Looking for a signer…</span>;

    case "no-signer":
      return (
        <div class="gate">
          <Chip tone="mute">No signer</Chip>
          <span>
            Admin data needs a NIP-07 signer. Install{" "}
            <a href="https://getalby.com" target="_blank" rel="noreferrer">
              Alby
            </a>{" "}
            or nos2x, then reload.
          </span>
          <button class="tiny" onClick={() => location.reload()}>
            Reload
          </button>
        </div>
      );

    case "checking":
      return (
        <div class="gate">
          <Chip tone="warn" dot>
            Checking key
          </Chip>
          <span class="mono-key">{shortKey(s.pubkey)}</span>
        </div>
      );

    case "denied":
      return (
        <div class="gate">
          <Chip tone="bad">Not an admin</Chip>
          <span class="mono-key" title={s.pubkey}>
            {shortKey(s.pubkey)}
          </span>
          <span>
            This node refused the key: {s.reason}. Add it to ADMIN_PUBKEYS, or sign in with a key
            that is already there.
          </span>
          <div class="row tight">
            <button class="tiny" onClick={session.retry}>
              Try again
            </button>
            <button class="tiny" onClick={session.signOut}>
              Use another key
            </button>
          </div>
        </div>
      );

    case "signed-in":
      return (
        <div class="stack" style={{ gap: "8px" }}>
          <Chip tone="ok">Admin</Chip>
          <div class="mono-key" title={s.pubkey}>
            {shortKey(s.pubkey)}
          </div>
          <button class="tiny" onClick={session.signOut}>
            Sign out
          </button>
        </div>
      );

    case "signed-out":
      return (
        <div class="gate">
          <span>Admin data needs your nostr key.</span>
          <button class="primary" onClick={() => void session.signIn()}>
            Sign in with nostr
          </button>
        </div>
      );
  }
}

function Rail(props: {
  session: Session;
  live: boolean;
  lastChange: { name: string; at: number } | null;
  counts: Record<string, string>;
}) {
  const [active, setActive] = useState<string>("corpus");

  useEffect(() => {
    const obs = new IntersectionObserver(
      (entries) => {
        const shown = entries.filter((e) => e.isIntersecting);
        if (shown.length) setActive(shown[0].target.id);
      },
      { rootMargin: "-10% 0px -70% 0px" },
    );
    for (const s of SECTIONS) {
      const el = document.getElementById(s.id);
      if (el) obs.observe(el);
    }
    return () => obs.disconnect();
  }, []);

  return (
    <aside class="rail">
      <div class="wordmark">
        nostr<span>search</span>
        <small>Node console</small>
      </div>

      <nav class="nav">
        {SECTIONS.map((s) => (
          <a key={s.id} href={`#${s.id}`} aria-current={active === s.id ? "true" : undefined}>
            {s.label}
            <b>{props.counts[s.id] ?? ""}</b>
          </a>
        ))}
      </nav>

      <div class="rail-foot">
        <div>
          <Chip tone={props.live ? "ok" : "mute"} dot={props.live}>
            {props.live ? "Reports streaming" : "Stream down"}
          </Chip>
          {props.lastChange ? (
            <div class="mono-key" style={{ marginTop: "8px" }}>
              {props.lastChange.name} updated {ago(props.lastChange.at)}
            </div>
          ) : null}
        </div>

        <Identity session={props.session} />
      </div>
    </aside>
  );
}

function Console() {
  const notify = useNotify();
  const onAuthError = useCallback((msg: string) => notify("err", msg), [notify]);
  const session = useSession(onAuthError);

  const stats = usePoll<RegistryStats>(api.stats, 5000);
  // Relay paging: the scraper can discover thousands, so the list is a window.
  const [relayOffset, setRelayOffset] = useState(0);
  const sync = usePoll<SyncStatus>(
    () => api.sync(relayOffset, 50),
    5000,
    true,
    relayOffset,
  );
  const reports = useReports();

  const authed = session.authed;
  const gate = gateMessage(session.state);

  const counts: Record<string, string> = {
    reports: reports.names.length ? num(reports.names.length) : "",
    corpus: stats.data ? `${num(stats.data.shard_count)} shards` : "",
    relays: sync.data ? num(sync.data.relays.total) : "",
  };

  return (
    <div class="shell">
      <Rail
        session={session}
        live={reports.live}
        lastChange={
          reports.updatedName
            ? { name: reports.updatedName, at: reports.updatedAt }
            : null
        }
        counts={counts}
      />
      <main class="main">
        <Today reports={reports} />
        <Reports reports={reports} />
        <Corpus stats={stats.data} />
        {/* Operator controls, not public status: these panels are actions
            with a status readout attached, so they are hidden entirely rather
            than shown disabled. */}
        {authed ? <Ingest authed={authed} gate={gate} /> : null}
        {authed ? <Analyses authed={authed} gate={gate} /> : null}
        <Relays
          sync={sync.data}
          authed={authed}
          gate={gate}
          offset={relayOffset}
          onPage={setRelayOffset}
        />
        <IndexPanel stats={stats.data} />
      </main>
    </div>
  );
}

export function App() {
  return (
    <Toasts>
      <Console />
    </Toasts>
  );
}
