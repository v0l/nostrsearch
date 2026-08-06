import { api } from "../api";
import type { AnalysisStatus } from "../types";
import { Chip, ConfirmButton, Panel, ago, num, useNotify, usePoll, isoDay } from "../ui";

export function Analyses({ authed, gate }: { authed: boolean; gate: string }) {
  const notify = useNotify();
  const poll = usePoll<AnalysisStatus[]>(api.analyses, 10_000, authed);

  const reset = async (name: string) => {
    try {
      const r = await api.resetAnalysis(name);
      notify(r.rebuild ? "ok" : "err", r.detail);
      poll.refresh();
    } catch (e) {
      notify("err", e instanceof Error ? e.message : String(e));
    }
  };

  const rows = poll.data ?? [];
  const pending = rows.filter((r) => !r.backfilled).length;

  const resetAll = async () => {
    try {
      const r = await api.resetAllAnalyses();
      notify(r.rebuild ? "ok" : "err", r.detail);
      poll.refresh();
    } catch (e) {
      notify("err", e instanceof Error ? e.message : String(e));
    }
  };

  /**
   * Everything that would be cleared along with `name`, transitively.
   *
   * A reset cascades to dependents on the server, so showing only the analysis
   * clicked would understate what the button does -- re-deriving follow_graph
   * also empties activity and active_users, and the reports they publish.
   */
  const blastRadius = (name: string): string[] => {
    const out: string[] = [];
    const queue = [name];
    while (queue.length) {
      const cur = queue.pop()!;
      for (const r of rows) {
        if (r.deps.includes(cur) && !out.includes(r.name)) {
          out.push(r.name);
          queue.push(r.name);
        }
      }
    }
    return out;
  };

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
        <div class="stack">
        {/* The one-click rebuild. Everything goes, including the follow
            graph's on-disk store, then the archive is folded back in staged
            passes -- graph first, then the reports that label events with it.
            This is the honest fix when numbers are suspect: per-analysis
            resets leave the others holding totals folded from state that no
            longer exists. */}
        <div class="row tight">
          <ConfirmButton
            label="Rebuild all reports from archive"
            confirmLabel="Clears everything, then hours of rebuild — confirm"
            onConfirm={resetAll}
          />
        </div>
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
                  <td
                    title={
                      a.unhealthy ?? (a.watermark ? isoDay(a.watermark) : "none")
                    }
                  >
                    {/* A report whose producer could not derive an answer is
                        not evidence of anything; say so rather than showing a
                        confident timestamp over hollow output. */}
                    {a.unhealthy ? (
                      <span style={{ color: "var(--rust)", fontWeight: 600 }}>
                        unhealthy
                      </span>
                    ) : (
                      ago(a.watermark)
                    )}
                  </td>
                  <td style={{ color: "var(--slate)" }}>{a.deps.length ? a.deps.join(", ") : "—"}</td>
                  <td>
                    <ConfirmButton
                      tiny
                      label="Re-derive"
                      confirmLabel={
                        blastRadius(a.name).length
                          ? `Also clears ${blastRadius(a.name).join(", ")}`
                          : "Confirm re-derive"
                      }
                      onConfirm={() => reset(a.name)}
                    />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        </div>
      )}
    </Panel>
  );
}
