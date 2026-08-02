import { api } from "../api";
import type { AnalysisStatus } from "../types";
import { Chip, ConfirmButton, Panel, ago, num, useNotify, usePoll } from "../ui";

export function Analyses({ authed, gate }: { authed: boolean; gate: string }) {
  const notify = useNotify();
  const poll = usePoll<AnalysisStatus[]>(api.analyses, 10_000, authed);

  const reset = async (name: string) => {
    try {
      const r = await api.resetAnalysis(name);
      notify("ok", r.detail);
      poll.refresh();
    } catch (e) {
      notify("err", e instanceof Error ? e.message : String(e));
    }
  };

  const rows = poll.data ?? [];
  const pending = rows.filter((r) => !r.backfilled).length;

  return (
    <Panel
      id="analyses"
      label="Analyses"
      aside={authed && rows.length ? `${pending} still deriving` : undefined}
      note="Each analysis consumes the event stream and publishes a report. Re-deriving clears its accumulated state; the report is incomplete until it catches up."
    >
      {!authed ? (
        <p class="empty">{gate}</p>
      ) : poll.error ? (
        <p class="empty">{poll.error}</p>
      ) : rows.length === 0 ? (
        <p class="empty">{poll.loading ? "Loading analyses…" : "No analyses registered."}</p>
      ) : (
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Analysis</th>
                <th>State</th>
                <th class="num">Epoch</th>
                <th class="num">Observed</th>
                <th class="num">Consumed</th>
                <th class="num">Filtered</th>
                <th>Watermark</th>
                <th>Depends on</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {rows.map((a) => (
                <tr key={a.name}>
                  <td style={{ fontWeight: 600 }}>{a.name}</td>
                  <td>
                    <Chip tone={a.backfilled ? "ok" : "warn"}>
                      {a.backfilled ? "Caught up" : "Deriving"}
                    </Chip>
                  </td>
                  <td class="num">{a.epoch}</td>
                  <td class="num">{num(a.observed)}</td>
                  <td class="num">{num(a.consumed)}</td>
                  <td class="num">{num(a.filtered)}</td>
                  <td title={a.watermark ? new Date(a.watermark * 1000).toISOString() : "none"}>
                    {ago(a.watermark)}
                  </td>
                  <td style={{ color: "var(--slate)" }}>{a.deps.length ? a.deps.join(", ") : "—"}</td>
                  <td>
                    <ConfirmButton
                      tiny
                      label="Re-derive"
                      confirmLabel="Confirm re-derive"
                      onConfirm={() => reset(a.name)}
                    />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  );
}
