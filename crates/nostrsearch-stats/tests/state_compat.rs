//! A bad checkpoint must never stop the node from starting.
//!
//! `Registry::load` used to propagate a `restore_bin` failure straight out of
//! `Pipeline::new`, so an analysis whose serialized shape had changed since the
//! last run would take the whole process down at startup — in Kubernetes, a
//! CrashLoopBackOff that looks exactly like "still booting" from outside.
//!
//! This is not hypothetical: `ActiveUsers` switched from `HashSet<Pubkey>` to a
//! HyperLogLog sketch, and its `epoch()` (the framework's own mechanism for
//! "discard my old state") was not bumped, so the stale bytes passed the epoch
//! check and then failed to deserialize.

use nostrsearch_stats::analyses::{ActiveUsers, Clients, FollowGraph};
use nostrsearch_stats::{Analysis, Progress, Registry, StatStore};

fn tempdir(tag: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "nsstate-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

#[test]
fn incompatible_checkpoint_does_not_prevent_startup() {
    let dir = tempdir("incompat");
    let store = StatStore::new(&dir).unwrap();

    // Simulate state written by an older build whose layout no longer matches:
    // right name, right epoch, bytes that cannot deserialize into the current
    // type.
    let epoch = ActiveUsers::default().epoch();
    store
        .save(
            "active_users",
            &[0xde, 0xad, 0xbe, 0xef, 0x00, 0x01, 0x02],
            &Progress::fresh(epoch),
        )
        .unwrap();

    let mut reg = Registry::new();
    reg.register(FollowGraph::default())
        .register(ActiveUsers::default())
        .register(Clients::default());

    // Must not error: the node has to boot even with an unreadable checkpoint.
    reg.load(&store)
        .expect("a corrupt checkpoint must not stop startup");

    // The affected analysis falls back to a fresh backfill rather than
    // silently carrying garbage forward.
    let entry = reg
        .entries()
        .iter()
        .find(|e| e.name() == "active_users")
        .unwrap();
    assert!(
        entry.needs_backfill(),
        "analysis with unreadable state must re-backfill"
    );

    // Unaffected analyses are untouched.
    assert_eq!(reg.entries().len(), 3);
}

/// Changing a serialized shape must come with an epoch bump, which is what
/// makes the discard deliberate rather than accidental.
#[test]
fn active_users_epoch_reflects_the_hll_layout_change() {
    assert!(
        ActiveUsers::default().epoch() >= 1,
        "ActiveUsers switched to HyperLogLog; its epoch must be bumped so \
         pre-sketch checkpoints are discarded instead of mis-parsed"
    );
}

/// Progress written before `rebuild` existed must still load.
///
/// Progress is persisted with bincode, which is positional and stores no field
/// names, so `#[serde(default)]` does nothing for it: a struct that gains a
/// trailing field runs off the end of older bytes and fails with "unexpected
/// end of file". Adding one made every existing .progress.bin undecodable and
/// the node refused to start:
///
///     Error: decoding /data/stats/follow_graph.progress.bin
///     Caused by: io error: unexpected end of file
///
/// State that is merely older must not be a startup failure.
#[test]
fn progress_written_before_rebuild_tracking_still_loads() {
    use nostrsearch_stats::{Progress, StatStore};

    /// Exactly the old on-disk layout: everything except `rebuild`.
    #[derive(serde::Serialize)]
    struct ProgressV0 {
        epoch: u32,
        watermark: u64,
        boundary: std::collections::HashSet<[u8; 32]>,
        events: u64,
        backfilled: bool,
        last_refresh_wall: u64,
        counters: nostrsearch_stats::metrics::Counters,
    }

    let dir = std::env::temp_dir().join(format!(
        "nscompat-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let store = StatStore::new(&dir).unwrap();

    let old = ProgressV0 {
        epoch: 1,
        watermark: 1_700_000_000,
        boundary: std::collections::HashSet::new(),
        events: 987_654,
        backfilled: true,
        last_refresh_wall: 1_700_000_100,
        counters: Default::default(),
    };
    std::fs::write(dir.join("activity.state.bin"), b"state").unwrap();
    std::fs::write(
        dir.join("activity.progress.bin"),
        bincode::serialize(&old).unwrap(),
    )
    .unwrap();

    let (state, p): (Vec<u8>, Progress) = store
        .load("activity")
        .expect("older progress must load, not abort startup")
        .expect("present");

    assert_eq!(state, b"state");
    assert_eq!(p.watermark, 1_700_000_000, "watermark must survive");
    assert_eq!(p.events, 987_654, "totals must survive");
    assert!(p.backfilled, "backfilled must survive");
    assert!(
        p.rebuild.file.is_empty() && p.rebuild.completed.is_empty(),
        "the new field defaults"
    );

    std::fs::remove_dir_all(&dir).ok();
}
