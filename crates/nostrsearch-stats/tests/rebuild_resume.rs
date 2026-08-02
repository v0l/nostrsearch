//! Rebuild progress is per-analysis, and resuming folds each event once.
//!
//! Rebuilds fold the whole archive and run for hours, so a deploy lands in the
//! middle of one routinely. Resuming has to be exact in both directions: a
//! resume point ahead of what was folded silently drops events, one behind
//! folds them twice. Both corrupt every counter, and neither is visible
//! afterwards -- the numbers just quietly stop matching the corpus.
//!
//! The position is per-analysis because resets are. A single shared position
//! cannot say "this one needs the whole archive and that one needs none", and
//! clearing it on one analysis destroyed the resume point of a rebuild running
//! for another.

use nostrsearch_core::event::NostrEvent;
use nostrsearch_stats::analyses::{Activity, KindBreakdown};
use nostrsearch_stats::{RebuildPlan, Registry, StatStore, World};

const DUMP: &str = "combined.jsonl.zst";

fn tempdir(tag: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "nsrebuild-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn note(n: u16) -> NostrEvent {
    NostrEvent {
        id: format!("{n:04x}").repeat(16),
        pubkey: "ab".repeat(32),
        created_at: 1_700_000_000 + n as u64,
        kind: 1,
        tags: vec![],
        content: format!("note {n}"),
        sig: "c".repeat(128),
    }
}

fn registry() -> Registry {
    let mut r = Registry::new();
    r.register(KindBreakdown::default());
    r.register(Activity::default());
    r
}

/// Fold a dump, stopping after `stop_after` events as an interruption would.
fn fold(reg: &mut Registry, events: &[NostrEvent], stop_after: usize) {
    let world = World::new();
    for ev in events.iter().take(stop_after) {
        reg.observe_rebuild(ev, 1_700_100_000, &world, DUMP);
    }
}

#[test]
fn a_resumed_rebuild_folds_each_event_exactly_once() {
    let dir = tempdir("once");
    let store = StatStore::new(&dir).unwrap();
    let events: Vec<NostrEvent> = (0..10).map(note).collect();

    // Reference: one uninterrupted pass.
    let mut whole = registry();
    whole.load(&store).unwrap();
    assert_eq!(whole.begin_rebuild(DUMP), RebuildPlan::FoldAll);
    fold(&mut whole, &events, 10);
    let expected = whole
        .snapshots()
        .into_iter()
        .find(|(n, _)| *n == "kind_breakdown")
        .unwrap()
        .1;

    // Interrupted after 4, persisted, reloaded, resumed.
    let dir2 = tempdir("resume");
    let store2 = StatStore::new(&dir2).unwrap();
    let mut part = registry();
    part.load(&store2).unwrap();
    assert_eq!(part.begin_rebuild(DUMP), RebuildPlan::FoldAll);
    fold(&mut part, &events, 4);
    part.persist(&store2).unwrap();

    let mut resumed = registry();
    resumed.load(&store2).unwrap();
    // Both analyses stopped at the same event, so the reader can fast-forward
    // without parsing.
    match resumed.begin_rebuild(DUMP) {
        RebuildPlan::ResumeAfter(id) => assert_eq!(id, events[3].id, "resume after the 4th"),
        other => panic!("expected a shared resume point, got {other:?}"),
    }
    // Replaying the file from the top: the first four are skipped internally.
    fold(&mut resumed, &events, 10);

    assert_eq!(
        resumed
            .snapshots()
            .into_iter()
            .find(|(n, _)| *n == "kind_breakdown")
            .unwrap()
            .1,
        expected,
        "a resumed rebuild must match an uninterrupted one, with nothing \
         double-counted or dropped"
    );

    std::fs::remove_dir_all(&dir).ok();
    std::fs::remove_dir_all(&dir2).ok();
}

#[test]
fn resetting_one_analysis_leaves_another_rebuild_untouched() {
    let dir = tempdir("independent");
    let store = StatStore::new(&dir).unwrap();
    let events: Vec<NostrEvent> = (0..10).map(note).collect();

    let mut reg = registry();
    reg.load(&store).unwrap();
    reg.begin_rebuild(DUMP);
    fold(&mut reg, &events, 6);

    // Reset one analysis while the other is mid-rebuild. Under the old global
    // checkpoint this wiped the shared position and the survivor re-folded from
    // the top, double-counting everything it had already taken.
    reg.reset("activity").expect("activity exists");

    // They now disagree, so the reader must hand over every event and let each
    // skip on its own.
    assert_eq!(
        reg.begin_rebuild(DUMP),
        RebuildPlan::FoldAll,
        "analyses at different points cannot share a reader-level skip"
    );

    let before = reg
        .snapshots()
        .into_iter()
        .find(|(n, _)| *n == "kind_breakdown")
        .unwrap()
        .1;
    fold(&mut reg, &events, 10);
    let after = reg
        .snapshots()
        .into_iter()
        .find(|(n, _)| *n == "kind_breakdown")
        .unwrap()
        .1;

    // The untouched analysis folded only the four it had not seen.
    let count = |v: &serde_json::Value| {
        v["1"]["trusted"].as_u64().unwrap_or(0) + v["1"]["untrusted"].as_u64().unwrap_or(0)
    };
    assert_eq!(
        count(&after) - count(&before),
        4,
        "the analysis that was not reset must resume, not restart: {before} -> {after}"
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_completed_dump_is_not_folded_again() {
    let dir = tempdir("complete");
    let store = StatStore::new(&dir).unwrap();
    let events: Vec<NostrEvent> = (0..5).map(note).collect();

    let mut reg = registry();
    reg.load(&store).unwrap();
    reg.begin_rebuild(DUMP);
    fold(&mut reg, &events, 5);
    reg.finish_rebuild_file(DUMP);
    reg.persist(&store).unwrap();

    let mut again = registry();
    again.load(&store).unwrap();
    assert_eq!(
        again.begin_rebuild(DUMP),
        RebuildPlan::SkipFile,
        "a dump every analysis has folded must be skipped, not re-read"
    );

    std::fs::remove_dir_all(&dir).ok();
}
