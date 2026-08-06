// Wire shapes, mirroring the server's serde types.

export interface MemoryStats {
  rss_mb: number;
  peak_rss_mb: number;
  cgroup_current_mb: number | null;
  cgroup_anon_mb: number | null;
  cgroup_file_mb: number | null;
  cgroup_limit_mb: number | null;
}

export interface ShardStat {
  shard: string;
  docs: number;
}

export interface RegistryStats {
  total_docs: number;
  shard_count: number;
  open_readers: number;
  max_open_readers: number;
  open_fds: number | null;
  nofile_soft: number;
  memory: MemoryStats;
  shards: ShardStat[];
}

export interface DayEntry {
  date: string;
  relay: string;
  seen: number;
  new: number;
  at: number;
}

export interface ScrapeProgress {
  days: number;
  relay_days: number;
  events_seen: number;
  events_new: number;
  oldest_day: string | null;
  newest_day: string | null;
  recent: DayEntry[];
}

export interface SyncRelay {
  /** Days scraped from this relay, and what they yielded. */
  days: number;
  events_seen: number;
  events_new: number;
  /** Set while the relay is being left alone after repeated failures. */
  dead_until?: number;
  url: string;
  sources: number;
  negentropy: boolean | null;
  cap: number;
  fails: number;
  last_ok: number;
  birthday: number | null;
}

export interface SyncStatus {
  relays: {
    total: number;
    offset: number;
    limit: number;
    negentropy: number;
    no_negentropy: number;
    unprobed: number;
    failing: number;
    top: SyncRelay[];
  };
  scrape: ScrapeProgress;
}

export interface AnalysisStatus {
  /** Set when the analysis could not derive a real answer from its input. */
  unhealthy?: string;
  name: string;
  epoch: number;
  backfilled: boolean;
  watermark: number;
  events: number;
  observed: number;
  consumed: number;
  filtered: number;
  deps: string[];
}

/**
 * Progress of an archive ingest.
 *
 * There is no per-file breakdown: the reader walks the whole directory across
 * several threads at once, so "the current file" is plural and a file-by-file
 * bar would describe one arbitrary worker. Event counts describe the run.
 */
export interface ReplayStatus {
  running: boolean;
  cancelled: boolean;
  /**
   * Dependency-stage pass, 0-based, and how many this run makes.
   *
   * The archive is read once per stage: analyses that label events using the
   * follow graph cannot fold in the same pass that builds it, or everything
   * lands untrusted. Only pass 0 writes to the index.
   */
  pass: number;
  passes: number;
  /** Events handed to the index during the indexing pass. */
  indexed: number;
  /** Events read, including those skipped as already known. */
  seen: number;
  /** Events skipped because the dedupe store already had them. */
  skipped: number;
  finished_at: number;
}

export interface ArchiveFileInfo {
  name: string;
  size: number;
  timestamp: number;
}

export interface AdminScrapeState {
  relays: SyncRelay[];
  progress: ScrapeProgress;
  matching_days: { count: number; sample: DayEntry[]; detail: string } | null;
}

export interface ReportIndex {
  generated_at: number;
  reports: string[];
}

export interface ReportDelta {
  name: string;
  patch: unknown;
}

/** kind-0 metadata, reduced to what a list needs. */
export interface Profile {
  pubkey: string;
  name?: string;
  display_name?: string;
  picture?: string;
  nip05?: string;
}
