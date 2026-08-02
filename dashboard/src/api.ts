import type {
  AdminScrapeState,
  AnalysisStatus,
  ArchiveFileInfo,
  RegistryStats,
  ReplayStatus,
  ReportDelta,
  ReportIndex,
  SyncStatus,
} from "./types";

// --- NIP-07 signer ---------------------------------------------------------

interface UnsignedEvent {
  kind: number;
  created_at: number;
  tags: string[][];
  content: string;
}

interface SignedEvent extends UnsignedEvent {
  id: string;
  pubkey: string;
  sig: string;
}

interface Nip07 {
  getPublicKey(): Promise<string>;
  signEvent(e: UnsignedEvent): Promise<SignedEvent>;
}

declare global {
  interface Window {
    nostr?: Nip07;
  }
}

export const NO_SIGNER =
  "No signer in this browser. Install a NIP-07 extension (Alby, nos2x, Nostore) and reload.";

export function hasSigner(): boolean {
  return typeof window.nostr?.signEvent === "function";
}

/**
 * Extensions inject `window.nostr` on document idle, which can land after the
 * app has already rendered. Wait briefly before concluding there is no signer,
 * otherwise a fast page load looks like a missing extension.
 */
export function waitForSigner(timeoutMs = 2500): Promise<boolean> {
  if (hasSigner()) return Promise.resolve(true);
  return new Promise((resolve) => {
    const started = Date.now();
    const h = setInterval(() => {
      if (hasSigner()) {
        clearInterval(h);
        resolve(true);
      } else if (Date.now() - started > timeoutMs) {
        clearInterval(h);
        resolve(false);
      }
    }, 120);
  });
}

export async function connect(): Promise<string> {
  if (!(await waitForSigner())) throw new Error(NO_SIGNER);
  const pk = await window.nostr!.getPublicKey();
  if (!/^[0-9a-f]{64}$/i.test(pk)) throw new Error("Signer returned an unusable public key.");
  return pk.toLowerCase();
}

/** NIP-98 header: a fresh kind-27235 event naming this exact URL and method. */
async function nip98(url: string, method: string): Promise<string> {
  if (!hasSigner()) throw new Error(NO_SIGNER);
  const event = await window.nostr!.signEvent({
    kind: 27235,
    created_at: Math.floor(Date.now() / 1000),
    tags: [
      ["u", url],
      ["method", method.toUpperCase()],
    ],
    content: "",
  });
  return "Nostr " + btoa(JSON.stringify(event));
}

// --- transport -------------------------------------------------------------

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }

  /** 401 from the NIP-98 gate: wrong key, stale clock, or replayed header. */
  get isAuth(): boolean {
    return this.status === 401;
  }
}

async function unwrap<T>(res: Response): Promise<T> {
  const text = await res.text();
  let body: unknown;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = null;
  }
  if (!res.ok) {
    const msg =
      (body as { error?: string } | null)?.error ?? text.slice(0, 200) ?? res.statusText;
    throw new ApiError(res.status, msg || `request failed (${res.status})`);
  }
  return body as T;
}

/** Public endpoints: no auth, safe to poll. */
async function open<T>(path: string): Promise<T> {
  return unwrap<T>(await fetch(path, { headers: { accept: "application/json" } }));
}

/**
 * Admin endpoints. The auth event names the absolute URL, so it must be built
 * from the same origin the request goes to, and signed per request — the node
 * refuses a header it has already seen.
 */
async function signed<T>(path: string, method: "GET" | "POST" = "GET"): Promise<T> {
  const url = new URL(path, window.location.origin).toString();
  let authorization: string;
  try {
    authorization = await nip98(url, method);
  } catch (e) {
    // A rejected signing prompt is a user decision, not a node failure — say so
    // rather than surfacing the extension's own wording.
    const msg = e instanceof Error ? e.message : String(e);
    throw new ApiError(0, msg === NO_SIGNER ? msg : `Signing was refused: ${msg}`);
  }
  const res = await fetch(url, {
    method,
    headers: { accept: "application/json", authorization },
  });
  return unwrap<T>(res);
}

// --- endpoints -------------------------------------------------------------

export const api = {
  stats: () => open<RegistryStats>("/stats"),
  // No trailing slashes: axum's nest("/sync", …) with a "/" child matches the
  // prefix exactly, and 404s on "/sync/".
  sync: () => open<SyncStatus>("/sync"),
  archiveFiles: () => open<ArchiveFileInfo[]>("/archive/files"),

  analyses: () => signed<AnalysisStatus[]>("/admin/analyses"),
  resetAnalysis: (name: string) =>
    signed<{ reset: boolean; detail: string }>(
      `/admin/analyses/${encodeURIComponent(name)}/reset`,
      "POST",
    ),

  ingest: () => signed<ReplayStatus>("/admin/ingest"),
  startIngest: (files: string[]) => {
    const qs = files.map((f) => `file=${encodeURIComponent(f)}`).join("&");
    return signed<{ started: boolean; detail: string }>(
      `/admin/ingest${qs ? `?${qs}` : ""}`,
      "POST",
    );
  },
  cancelIngest: () => signed<{ cancelled: boolean }>("/admin/ingest/cancel", "POST"),

  scrape: (q: { relay?: string; from?: string; to?: string } = {}) =>
    signed<AdminScrapeState>(`/admin/scrape${qstr(q)}`),
  resetScrape: (q: { relay?: string; from?: string; to?: string }) =>
    signed<{ reset_days: number; detail: string }>(`/admin/scrape/reset${qstr(q)}`, "POST"),
  resetRelay: (relay: string) =>
    signed<{ reset: boolean; detail: string }>(
      `/admin/scrape/relay/reset?relay=${encodeURIComponent(relay)}`,
      "POST",
    ),
};

function qstr(q: Record<string, string | undefined>): string {
  const parts = Object.entries(q)
    .filter(([, v]) => v)
    .map(([k, v]) => `${k}=${encodeURIComponent(v as string)}`);
  return parts.length ? `?${parts.join("&")}` : "";
}

/** The names the writer has published, and when. */
export const reportIndex = () => open<ReportIndex>("/reports");

/** One report's full snapshot, the shape its deltas patch. */
export const report = (name: string) =>
  open<unknown>(`/reports/${encodeURIComponent(name)}`);

/** Live report deltas. Returns a teardown function. */
export function streamReports(h: {
  onDelta: (d: ReportDelta) => void;
  onLagged: (dropped: number) => void;
  onState: (up: boolean) => void;
}): () => void {
  const es = new EventSource("/reports/stream");
  es.addEventListener("open", () => h.onState(true));
  es.addEventListener("error", () => h.onState(false));
  es.addEventListener("delta", (e) => {
    try {
      h.onDelta(JSON.parse((e as MessageEvent).data) as ReportDelta);
    } catch {
      /* ignore a malformed frame rather than tearing down the stream */
    }
  });
  // The node sends this when it drops us as a slow consumer: the stream is
  // gapped from here, so the only correct move is to re-seed.
  es.addEventListener("lagged", (e) =>
    h.onLagged(Number((e as MessageEvent).data) || 0),
  );
  return () => es.close();
}
