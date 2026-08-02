import { useEffect, useRef, useState } from "preact/hooks";
import { reportIndex, report, streamReports } from "./api";

/**
 * The published analysis reports, kept live.
 *
 * The node offers both a full snapshot per report and a stream of merge patches
 * over that same shape, so this seeds once from `GET /reports/{name}` and then
 * applies every frame from `GET /reports/stream`. That is the difference
 * between a page that shows numbers and a page where the numbers move — and it
 * costs a few hundred bytes a tick instead of refetching a multi-year report.
 *
 * A `lagged` frame means the node dropped us as a slow consumer; the only
 * correct response is to re-seed rather than keep patching a gapped state.
 */

/** JSON merge patch (RFC 7386), matching the node's `merge_patch`. */
export function mergePatch(target: unknown, patch: unknown): unknown {
  if (patch === null) return undefined;
  if (typeof patch !== "object" || Array.isArray(patch)) return patch;
  const base: Record<string, unknown> =
    typeof target === "object" && target !== null && !Array.isArray(target)
      ? { ...(target as Record<string, unknown>) }
      : {};
  for (const [k, v] of Object.entries(patch as Record<string, unknown>)) {
    if (v === null) delete base[k];
    else base[k] = mergePatch(base[k], v);
  }
  return base;
}

export interface Reports {
  /** Report name → latest snapshot, patched in place as frames arrive. */
  data: Record<string, unknown>;
  names: string[];
  generatedAt: number;
  /** Unix seconds of the last patch applied, for the freshness readout. */
  updatedAt: number;
  /** Which report that patch was for. */
  updatedName: string | null;
  live: boolean;
  loading: boolean;
  /** Set when the node could not be reached; distinct from "nothing published". */
  error: string | null;
}

export function useReports(): Reports {
  const [data, setData] = useState<Record<string, unknown>>({});
  const [names, setNames] = useState<string[]>([]);
  const [generatedAt, setGeneratedAt] = useState(0);
  const [updated, setUpdated] = useState<{ at: number; name: string } | null>(null);
  const [live, setLive] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const seeding = useRef(false);

  useEffect(() => {
    let alive = true;

    const seed = async () => {
      if (seeding.current) return;
      seeding.current = true;
      try {
        const idx = await retry(reportIndex);
        if (!alive) return;
        setNames(idx.reports);
        setGeneratedAt(idx.generated_at);
        setError(null);

        // Sequential on purpose. Fanning these out is a burst of simultaneous
        // connections to the same host, which is the pattern DDoS scrubbing
        // drops SYNs from — and a report that silently fails to load looks
        // exactly like an analysis with nothing in it.
        for (const name of idx.reports) {
          const v = await retry(() => report(name)).catch(() => null);
          if (!alive) return;
          if (v !== null) setData((prev) => ({ ...prev, [name]: v }));
        }
      } catch (e) {
        // Say the node is unreachable rather than letting every panel claim
        // its analysis has published nothing.
        if (alive) setError(e instanceof Error ? e.message : String(e));
      } finally {
        if (alive) setLoading(false);
        seeding.current = false;
      }
    };

    void seed();
    // Re-seed periodically as well: new analyses appear in the index only on a
    // full publish, and it repairs any drift from a missed frame.
    const h = setInterval(seed, 120_000);

    const stop = streamReports({
      onDelta: (d) => {
        setData((prev) => ({ ...prev, [d.name]: mergePatch(prev[d.name], d.patch) }));
        setUpdated({ at: Math.floor(Date.now() / 1000), name: d.name });
      },
      onLagged: () => void seed(),
      onState: setLive,
    });

    return () => {
      alive = false;
      clearInterval(h);
      stop();
    };
  }, []);

  return {
    data,
    names,
    generatedAt,
    updatedAt: updated?.at ?? 0,
    updatedName: updated?.name ?? null,
    live,
    loading,
    error,
  };
}

/** One retry with a short pause: a dropped SYN is not a missing report. */
async function retry<T>(fn: () => Promise<T>): Promise<T> {
  try {
    return await fn();
  } catch {
    await new Promise((r) => setTimeout(r, 600));
    return fn();
  }
}

// --- report shapes ---------------------------------------------------------
// Mirrors each analysis's `snapshot()` in nostrsearch-stats.

export interface TrustedCount {
  trusted: number;
  untrusted: number;
}

export const total = (c: TrustedCount | undefined): number =>
  c ? c.trusted + c.untrusted : 0;

/** `activity`: unix day start → what happened that day. */
export interface DailyActivity {
  zaps_sent_sats: TrustedCount;
  zaps_received_sats: TrustedCount;
  zap_count: number;
  kinds: Record<string, TrustedCount>;
}

/** `client_tags`: client name → totals. */
export interface ClientStats {
  sum: number;
  last_note: number;
  kinds: Record<string, number>;
}

/** `trending_hashtags`: already ranked, highest score first. */
export interface TrendingTag {
  tag: string;
  score: number;
  mentions: number;
}

/** `active_users`: bucket start → unique publishers, from HLL sketches. */
export interface ActiveUsersBucket {
  start: number;
  users: TrustedCount;
}

export interface ActiveUsersReport {
  daily: Record<string, ActiveUsersBucket>;
  weekly: Record<string, ActiveUsersBucket>;
}

export const asRecord = <T>(v: unknown): Record<string, T> =>
  typeof v === "object" && v !== null && !Array.isArray(v) ? (v as Record<string, T>) : {};

export const asArray = <T>(v: unknown): T[] => (Array.isArray(v) ? (v as T[]) : []);
