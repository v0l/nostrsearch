import type { RegistryStats } from "../types";
import { Bars, Panel, Readout, compact, num } from "../ui";

/** Shards are named `YYYY-MM`; anything else is not a month we can place. */
const MONTH = /^(\d{4})-(\d{2})$/;

/**
 * The index laid out in time: one bar per monthly shard, height for documents.
 *
 * Nostr started in late 2020, so shards dated before that hold events whose
 * `created_at` is wrong — a 1970 bucket is a broken clock, not history. They
 * are counted and named rather than silently folded in, because they are the
 * reason a shard count looks larger than the corpus's real span.
 */
function Timeline({ stats }: { stats: RegistryStats | null }) {
  const shards = (stats?.shards ?? [])
    .filter((s) => MONTH.test(s.shard))
    .sort((a, b) => a.shard.localeCompare(b.shard));

  const era = shards.filter((s) => s.shard >= "2020-01");
  const preEra = shards.filter((s) => s.shard < "2020-01");
  const preDocs = preEra.reduce((n, s) => n + s.docs, 0);
  const peak = Math.max(1, ...era.map((s) => s.docs));

  return (
    <div class="ribbon">
      <div class="ribbon-head">
        <span>Documents per monthly shard</span>
        <b>{era.length ? `${era.length} months` : "no shards mounted"}</b>
      </div>

      <Bars
        items={era.map((s) => ({
          key: s.shard,
          height: s.docs / peak,
          fill: 1,
          title: `${s.shard} — ${num(s.docs)} documents`,
        }))}
        left={era.length ? era[0].shard : undefined}
        center={era.length ? `peak ${compact(peak)} in one month` : undefined}
        right={era.length ? era[era.length - 1].shard : undefined}
        empty="No shards mounted yet."
      />

      {preEra.length > 0 ? (
        <div class="mono-key" style={{ padding: "0 0 12px" }}>
          {num(preEra.length)} shards dated before nostr existed hold {num(preDocs)} documents —
          events with a broken `created_at`, kept but not shown here.
        </div>
      ) : null}
    </div>
  );
}

export function Corpus({ stats }: { stats: RegistryStats | null }) {
  const shards = (stats?.shards ?? []).filter((s) => MONTH.test(s.shard));
  const era = shards.filter((s) => s.shard >= "2020-01").sort((a, b) => a.shard.localeCompare(b.shard));
  const busiest = shards.slice().sort((a, b) => b.docs - a.docs)[0];

  return (
    <Panel
      id="corpus"
      label="Corpus"
      aside={stats ? `${num(stats.shard_count)} shards` : undefined}
      note="Events are indexed into one Tantivy shard per month, so a query bounded in time only ever touches the shards it needs."
    >
      <Timeline stats={stats} />

      <div class="readouts" style={{ marginTop: "18px" }}>
        <Readout
          value={compact(stats?.total_docs)}
          label="Documents indexed"
          sub={stats ? `${num(stats.shard_count)} shards on disk` : undefined}
        />
        <Readout
          value={num(era.length)}
          label="Months covered"
          sub={era.length ? `${era[0].shard} onward` : undefined}
        />
        <Readout
          value={compact(busiest?.docs)}
          label="Busiest month"
          sub={busiest?.shard}
        />
      </div>
    </Panel>
  );
}
