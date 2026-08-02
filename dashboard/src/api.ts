import type {
  AdminScrapeState,
  AnalysisStatus,
  ArchiveFileInfo,
  RegistryStats,
  ReplayStatus,
  ReportDelta,
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

export function hasSigner(): boolean {
  return typeof window.nostr?.signEvent === "function";
}

export async function connect(): Promise<string> {
  if (!window.nostr) throw new Error("No signer found. Install a nostr browser extension.");
  return window.nostr.getPublicKey();
}

/** NIP-98 header: a fresh kind-27235 event naming this exact URL and method. */
async function nip98(url: string, method: string): Promise<string> {
  if (!window.nostr) throw new Error("No signer found. Install a nostr browser extension.");
  const event = await window.nostr.signEvent({
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
  const res = await fetch(url, {
    method,
    headers: {
      accept: "application/json",
      authorization: await nip98(url, method),
    },
  });
  return unwrap<T>(res);
}

// --- endpoints -------------------------------------------------------------

export const api = {
  stats: () => open<RegistryStats>("/stats"),
  sync: () => open<SyncStatus>("/sync/"),
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

/** Live report deltas. Returns a teardown function. */
export function streamReports(
  onDelta: (d: ReportDelta) => void,
  onState: (up: boolean) => void,
): () => void {
  const es = new EventSource("/reports/stream");
  es.addEventListener("open", () => onState(true));
  es.addEventListener("error", () => onState(false));
  es.addEventListener("delta", (e) => {
    try {
      onDelta(JSON.parse((e as MessageEvent).data) as ReportDelta);
    } catch {
      /* ignore malformed frame */
    }
  });
  return () => es.close();
}
