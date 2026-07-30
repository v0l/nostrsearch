//! Process memory reporting.
//!
//! Backfills have been OOM-killed with no indication of which structure was
//! growing. Reading the kernel's own accounting is cheap and makes the next run
//! diagnostic instead of guesswork.

/// Current and peak resident set size in MB, from `/proc/self/status`.
/// Returns `(rss_mb, peak_mb)`; zeroes on platforms without procfs.
pub fn rss_mb() -> (u64, u64) {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return (0, 0);
    };
    let mut rss = 0;
    let mut peak = 0;
    for line in status.lines() {
        let parse = |l: &str| -> u64 {
            l.split_whitespace()
                .nth(1)
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0)
                / 1024
        };
        if line.starts_with("VmRSS:") {
            rss = parse(line);
        } else if line.starts_with("VmHWM:") {
            peak = parse(line);
        }
    }
    (rss, peak)
}

/// Container memory limit in MB, if running under a cgroup with one set.
pub fn cgroup_limit_mb() -> Option<u64> {
    for p in [
        "/sys/fs/cgroup/memory.max",                  // cgroup v2
        "/sys/fs/cgroup/memory/memory.limit_in_bytes", // cgroup v1
    ] {
        if let Ok(v) = std::fs::read_to_string(p) {
            let v = v.trim();
            if v == "max" {
                return None;
            }
            if let Ok(bytes) = v.parse::<u64>() {
                // v1 reports a sentinel when unlimited.
                if bytes < u64::MAX / 2 {
                    return Some(bytes / 1_048_576);
                }
            }
        }
    }
    None
}

/// What the cgroup actually accounts against `memory.max`, in MB.
///
/// This is the number the OOM killer uses, and it is **not** RSS: it includes
/// the page cache. A process reading dumps and writing an index can sit at a
/// few GB of RSS while the cgroup approaches its limit purely through cached
/// file pages, then get killed with no apparent growth in RSS.
///
/// Returns `(current_mb, anon_mb, file_mb)` where `anon` is real process memory
/// and `file` is page cache.
pub fn cgroup_usage_mb() -> Option<(u64, u64, u64)> {
    let current = std::fs::read_to_string("/sys/fs/cgroup/memory.current")
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()?
        / 1_048_576;

    let (mut anon, mut file) = (0, 0);
    if let Ok(stat) = std::fs::read_to_string("/sys/fs/cgroup/memory.stat") {
        for line in stat.lines() {
            let mut it = line.split_whitespace();
            match (it.next(), it.next().and_then(|v| v.parse::<u64>().ok())) {
                (Some("anon"), Some(v)) => anon = v / 1_048_576,
                (Some("file"), Some(v)) => file = v / 1_048_576,
                _ => {}
            }
        }
    }
    Some((current, anon, file))
}
