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
