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
    negentropy: number;
    no_negentropy: number;
    unprobed: number;
    failing: number;
    top: SyncRelay[];
  };
  scrape: ScrapeProgress;
}

export interface AnalysisStatus {
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

export interface FileProgress {
  name: string;
  bytes_total: number;
  bytes_read: number;
  malformed: number;
  events: number;
  new: number;
  /** Lines fast-forwarded past while resuming an interrupted rebuild. */
  skipped: number;
  complete: boolean;
  error: string | null;
}

export interface ReplayStatus {
  running: boolean;
  cancelled: boolean;
  started_at: number;
  finished_at: number;
  files_total: number;
  files_done: number;
  /** Totals across *completed* files only. */
  events: number;
  new: number;
  malformed: number;
  current: string | null;
  /** Live progress for the file being read, absent between files. */
  current_progress: FileProgress | null;
  files: FileProgress[];
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
