import type { RegistryStats } from "../types";
import { Meter, Panel, compact, num } from "../ui";

function Gauge(props: {
  label: string;
  value: number | null | undefined;
  max: number | null | undefined;
  unit?: string;
  note?: string;
}) {
  const v = props.value ?? 0;
  const max = props.max ?? 0;
  const pct = max > 0 ? (v / max) * 100 : 0;
  const tone = pct > 90 ? "rust" : pct > 70 ? "brass" : undefined;
  return (
    <div class="stack" style={{ gap: "6px" }}>
      <div class="row tight" style={{ justifyContent: "space-between" }}>
        <span style={{ fontSize: "11px", letterSpacing: "0.1em", textTransform: "uppercase", color: "var(--slate)" }}>
          {props.label}
        </span>
        <span style={{ fontVariantNumeric: "tabular-nums" }}>
          {num(props.value)}
          {max > 0 ? ` / ${num(max)}` : ""} {props.unit ?? ""}
        </span>
      </div>
      <Meter value={v} max={max} tone={tone} />
      {props.note ? <span class="mono-key">{props.note}</span> : null}
    </div>
  );
}

/** A node with a shard per month has hundreds; list the ones that matter. */
const SHARDS_SHOWN = 12;

export function IndexPanel({ stats }: { stats: RegistryStats | null }) {
  const m = stats?.memory;
  const all = (stats?.shards ?? []).slice().sort((a, b) => b.docs - a.docs);
  const shards = all.slice(0, SHARDS_SHOWN);
  const rest = all.slice(SHARDS_SHOWN);
  const restDocs = rest.reduce((n, s) => n + s.docs, 0);

  return (
    <Panel
      id="index"
      label="Index"
      aside={stats ? `${num(stats.shard_count)} shards` : undefined}
      note="Tantivy memory-maps every segment, so resident size counts page cache the kernel can reclaim. Anonymous memory is the number that means pressure."
    >
      {!stats ? (
        <p class="empty">Loading index stats…</p>
      ) : (
        <div class="stack">
          <div
            style={{
              display: "grid",
              gap: "18px 32px",
              gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))",
            }}
          >
            <Gauge
              label="Shard readers open"
              value={stats.open_readers}
              max={stats.max_open_readers}
            />
            <Gauge
              label="File descriptors"
              value={stats.open_fds}
              max={stats.nofile_soft}
              note="soft limit for this process"
            />
            <Gauge
              label="Anonymous memory"
              value={m?.cgroup_anon_mb}
              max={m?.cgroup_limit_mb}
              unit="MB"
              note="heaps and writer arenas"
            />
            <Gauge
              label="Page cache"
              value={m?.cgroup_file_mb}
              max={m?.cgroup_limit_mb}
              unit="MB"
              note={`resident ${num(m?.rss_mb)} MB, peak ${num(m?.peak_rss_mb)} MB`}
            />
          </div>

          <hr class="hr" />

          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Largest shards</th>
                  <th class="num">Documents</th>
                  <th class="num">Share</th>
                </tr>
              </thead>
              <tbody>
                {shards.map((s) => (
                  <tr key={s.shard}>
                    <td>{s.shard}</td>
                    <td class="num">{num(s.docs)}</td>
                    <td class="num">
                      {stats.total_docs > 0
                        ? `${((s.docs / stats.total_docs) * 100).toFixed(1)}%`
                        : "—"}
                    </td>
                  </tr>
                ))}
                {shards.length === 0 ? (
                  <tr>
                    <td colSpan={3} class="empty">
                      No shards mounted.
                    </td>
                  </tr>
                ) : (
                  <>
                    {rest.length > 0 ? (
                      <tr>
                        <td style={{ color: "var(--slate)" }}>{num(rest.length)} smaller shards</td>
                        <td class="num" style={{ color: "var(--slate)" }}>
                          {num(restDocs)}
                        </td>
                        <td class="num" style={{ color: "var(--slate)" }}>
                          {stats.total_docs > 0
                            ? `${((restDocs / stats.total_docs) * 100).toFixed(1)}%`
                            : "—"}
                        </td>
                      </tr>
                    ) : null}
                    <tr>
                      <td style={{ fontWeight: 600 }}>Total</td>
                      <td class="num" style={{ fontWeight: 600 }}>
                        {compact(stats.total_docs)}
                      </td>
                      <td />
                    </tr>
                  </>
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </Panel>
  );
}
