import { createContext, type ComponentChildren } from "preact";
import { useCallback, useContext, useEffect, useRef, useState } from "preact/hooks";

// --- formatting ------------------------------------------------------------

const NF = new Intl.NumberFormat("en-US");

export const num = (n: number | null | undefined): string =>
  n === null || n === undefined ? "—" : NF.format(n);

/** Short form for headline readouts: 12.4M, 831k. */
export function compact(n: number | null | undefined): string {
  if (n === null || n === undefined) return "—";
  if (n < 1000) return String(n);
  if (n < 1e6) return `${(n / 1e3).toFixed(n < 1e4 ? 1 : 0)}k`;
  if (n < 1e9) return `${(n / 1e6).toFixed(n < 1e7 ? 1 : 0)}M`;
  return `${(n / 1e9).toFixed(1)}B`;
}

export function bytes(b: number): string {
  const u = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  let v = b;
  while (v >= 1024 && i < u.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(v < 10 && i > 0 ? 1 : 0)} ${u[i]}`;
}

export function ago(unix: number | null | undefined): string {
  if (!unix) return "never";
  const s = Math.max(0, Math.floor(Date.now() / 1000) - unix);
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
  return `${Math.floor(s / 86400)}d ago`;
}

export const shortKey = (hex: string): string =>
  hex.length > 16 ? `${hex.slice(0, 8)}…${hex.slice(-8)}` : hex;

// --- toasts ----------------------------------------------------------------

export interface Toast {
  id: number;
  kind: "ok" | "err";
  text: string;
}

type Notify = (kind: Toast["kind"], text: string) => void;

const NotifyCtx = createContext<Notify>(() => {});
export const useNotify = () => useContext(NotifyCtx);

export function Toasts({ children }: { children: ComponentChildren }) {
  const [items, setItems] = useState<Toast[]>([]);
  const seq = useRef(0);

  const notify = useCallback<Notify>((kind, text) => {
    const id = ++seq.current;
    setItems((xs) => [...xs, { id, kind, text }]);
    setTimeout(() => setItems((xs) => xs.filter((x) => x.id !== id)), kind === "err" ? 9000 : 5000);
  }, []);

  return (
    <NotifyCtx.Provider value={notify}>
      {children}
      <div class="toasts" role="status" aria-live="polite">
        {items.map((t) => (
          <div key={t.id} class={`toast ${t.kind === "err" ? "err" : ""}`}>
            <b>{t.kind === "err" ? "Failed" : "Done"}</b>
            <span>{t.text}</span>
          </div>
        ))}
      </div>
    </NotifyCtx.Provider>
  );
}

// --- polling ---------------------------------------------------------------

export interface Poll<T> {
  data: T | null;
  error: string | null;
  loading: boolean;
  refresh: () => void;
}

/** Poll an endpoint on an interval. `enabled: false` parks it entirely. */
export function usePoll<T>(fn: () => Promise<T>, ms: number, enabled = true): Poll<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(enabled);
  const [tick, setTick] = useState(0);
  const call = useRef(fn);
  call.current = fn;

  useEffect(() => {
    if (!enabled) return;
    let alive = true;
    const run = async () => {
      try {
        const v = await call.current();
        if (!alive) return;
        setData(v);
        setError(null);
      } catch (e) {
        if (alive) setError(e instanceof Error ? e.message : String(e));
      } finally {
        if (alive) setLoading(false);
      }
    };
    run();
    const h = setInterval(run, ms);
    return () => {
      alive = false;
      clearInterval(h);
    };
  }, [ms, enabled, tick]);

  return { data, error, loading, refresh: useCallback(() => setTick((t) => t + 1), []) };
}

// --- primitives ------------------------------------------------------------

export function Panel(props: {
  id?: string;
  label: string;
  note?: string;
  aside?: ComponentChildren;
  children: ComponentChildren;
}) {
  return (
    <section class="panel" id={props.id}>
      <div class="plate">
        <span>{props.label}</span>
        {props.aside ? <em>{props.aside}</em> : null}
      </div>
      {props.note ? <p class="panel-note">{props.note}</p> : null}
      {props.children}
    </section>
  );
}

export function Readout(props: { value: string; label: string; sub?: string }) {
  return (
    <div class="readout">
      <div class="v">{props.value}</div>
      <div class="k">{props.label}</div>
      {props.sub ? <div class="s">{props.sub}</div> : null}
    </div>
  );
}

export function Chip(props: { tone?: "ok" | "warn" | "bad" | "mute"; dot?: boolean; children: ComponentChildren }) {
  return (
    <span class={`chip ${props.tone ?? ""}`}>
      {props.dot ? <i class="dot live" /> : null}
      {props.children}
    </span>
  );
}

export function Meter(props: { value: number; max: number; tone?: "brass" | "rust" }) {
  const pct = props.max > 0 ? Math.min(100, (props.value / props.max) * 100) : 0;
  return (
    <div class={`meter ${props.tone ?? ""}`}>
      <i style={{ width: `${pct}%` }} />
    </div>
  );
}

export interface BarDatum {
  key: string;
  /** Bar height as a fraction of the tallest bar, 0–1. */
  height: number;
  /** Solid share rising from the baseline, 0–1. */
  fill: number;
  title: string;
}

/**
 * The house chart: one bar per bucket, height for the total and a solid fill
 * for the part that counts (new events, trusted publishers). A tall hollow bar
 * is the interesting failure in both readings.
 */
export function Bars(props: {
  items: BarDatum[];
  height?: number;
  left?: string;
  center?: string;
  right?: string;
  empty?: string;
}) {
  if (props.items.length === 0) {
    return <div class="ribbon-empty">{props.empty ?? "Nothing recorded yet."}</div>;
  }
  return (
    <>
      <div class="bars" style={{ height: `${props.height ?? 132}px` }}>
        {props.items.map((d, i) => (
          <div
            key={d.key}
            class="bar"
            style={{
              height: `${Math.max(2, d.height * 100)}%`,
              animationDelay: `${i * 14}ms`,
            }}
            title={d.title}
          >
            <i style={{ height: `${Math.min(100, d.fill * 100)}%` }} />
          </div>
        ))}
      </div>
      {props.left || props.center || props.right ? (
        <div class="scale">
          <span>{props.left}</span>
          <span>{props.center}</span>
          <span>{props.right}</span>
        </div>
      ) : null}
    </>
  );
}

/** Two-step confirm on a destructive control — no modal, no lost context. */
export function ConfirmButton(props: {
  label: string;
  confirmLabel: string;
  onConfirm: () => void;
  disabled?: boolean;
  tiny?: boolean;
}) {
  const [armed, setArmed] = useState(false);
  useEffect(() => {
    if (!armed) return;
    const h = setTimeout(() => setArmed(false), 4000);
    return () => clearTimeout(h);
  }, [armed]);

  return (
    <button
      class={`${armed ? "danger" : ""} ${props.tiny ? "tiny" : ""}`}
      disabled={props.disabled}
      onClick={() => {
        if (armed) {
          setArmed(false);
          props.onConfirm();
        } else {
          setArmed(true);
        }
      }}
    >
      {armed ? props.confirmLabel : props.label}
    </button>
  );
}
