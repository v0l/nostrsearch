//! Rebuild runs are staged, per-analysis, and resume exactly.
//!
//! Rebuilds fold the whole archive and run for hours, so a deploy lands in the
//! middle of one routinely. Resuming has to be exact in both directions: a
//! resume point ahead of what was folded silently drops events, one behind
//! folds them twice. Both corrupt every counter, and neither is visible
//! afterwards.
//!
//! Staging matters just as much: dependents label events using the world their
//! dependency builds, so folding them in the same pass records everything
//! against a world that does not exist yet -- on a cold graph, every event
//! permanently untrusted.

use nostrsearch_core::event::NostrEvent;
use nostrsearch_stats::analyses::{Activity, FollowGraph, KindBreakdown};
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

fn pk(seed: u8) -> String {
    format!("{seed:02x}").repeat(32)
}

fn note(n: u16, author: &str) -> NostrEvent {
    NostrEvent {
        id: format!("{n:04x}").repeat(16),
        pubkey: author.into(),
        created_at: 1_700_000_000 + n as u64,
        kind: 1,
        tags: vec![],
        content: format!("note {n}"),
        sig: "c".repeat(128),
    }
}

fn contacts(n: u16, author: &str, follows: &str) -> NostrEvent {
    NostrEvent {
        id: format!("{n:04x}").repeat(16),
        pubkey: author.into(),
        created_at: 1_700_000_000 + n as u64,
        kind: 3,
        tags: vec![vec!["p".into(), follows.into()]],
        content: String::new(),
        sig: "c".repeat(128),
    }
}

fn registry(store: &StatStore) -> Registry {
    let mut r = Registry::new();
    r.register(FollowGraph::default());
    r.register(KindBreakdown::default());
    r.load(store).unwrap();
    r
}

fn snapshot(reg: &Registry, name: &str) -> serde_json::Value {
    reg.snapshots()
        .into_iter()
        .find(|(n, _)| *n == name)
        .unwrap()
        .1
}

/// Fold a dump through the rebuild path, stopping early as an interruption
/// would.
fn fold(reg: &mut Registry, events: &[NostrEvent], stop_after: usize) {
    let world = World::new();
    for ev in events.iter().take(stop_after) {
        reg.observe_rebuild(ev, 1_700_100_000, &world, DUMP);
    }
}

#[test]
fn a_resumed_rebuild_folds_each_event_exactly_once() {
    let events: Vec<NostrEvent> = (0..10).map(|n| note(n, &pk(1))).collect();

    // Reference: one uninterrupted pass.
    let dir = tempdir("whole");
    let store = StatStore::new(&dir).unwrap();
    let mut whole = registry(&store);
    whole.set_rebuild_files(vec![DUMP.into()]).unwrap();
    assert_eq!(whole.begin_rebuild(DUMP), RebuildPlan::FoldAll);
    fold(&mut whole, &events, 10);
    let expected = snapshot(&whole, "kind_breakdown");
    drop(whole);
    std::fs::remove_dir_all(&dir).ok();

    // Interrupted after 4, persisted, reloaded, resumed.
    let dir2 = tempdir("resume");
    let store2 = StatStore::new(&dir2).unwrap();
    let mut part = registry(&store2);
    part.set_rebuild_files(vec![DUMP.into()]).unwrap();
    assert_eq!(part.begin_rebuild(DUMP), RebuildPlan::FoldAll);
    fold(&mut part, &events, 4);
    part.persist(&store2).unwrap();
    drop(part);

    let mut resumed = registry(&store2);
    resumed.set_rebuild_files(vec![DUMP.into()]).unwrap();
    // Both analyses stopped at the same event, so the reader can fast-forward
    // without parsing.
    match resumed.begin_rebuild(DUMP) {
        RebuildPlan::ResumeAfter(id) => assert_eq!(id, events[3].id, "resume after the 4th"),
        other => panic!("expected a shared resume point, got {other:?}"),
    }
    // Replaying the file from the top: the first four are skipped internally.
    fold(&mut resumed, &events, 10);

    assert_eq!(
        snapshot(&resumed, "kind_breakdown"),
        expected,
        "a resumed rebuild must match an uninterrupted one, with nothing \
         double-counted or dropped"
    );

    std::fs::remove_dir_all(&dir2).ok();
}

#[test]
fn resetting_one_analysis_leaves_another_rebuild_untouched() {
    let dir = tempdir("independent");
    let store = StatStore::new(&dir).unwrap();
    let events: Vec<NostrEvent> = (0..10).map(|n| note(n, &pk(1))).collect();

    let mut reg = registry(&store);
    reg.set_rebuild_files(vec![DUMP.into()]).unwrap();
    reg.begin_rebuild(DUMP);
    fold(&mut reg, &events, 6);

    // Reset one analysis while the other is mid-rebuild. Under the old global
    // checkpoint this wiped the shared position and the survivor re-folded
    // from the top, double-counting everything it had already taken.
    reg.reset("follow_graph").expect("exists");

    // They now disagree, so the reader must hand over every event and let each
    // skip on its own.
    assert_eq!(
        reg.begin_rebuild(DUMP),
        RebuildPlan::FoldAll,
        "analyses at different points cannot share a reader-level skip"
    );

    let before = snapshot(&reg, "kind_breakdown");
    fold(&mut reg, &events, 10);
    let after = snapshot(&reg, "kind_breakdown");

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
fn a_finished_run_reports_finished_and_clears_its_positions() {
    let dir = tempdir("complete");
    let store = StatStore::new(&dir).unwrap();
    let events: Vec<NostrEvent> = (0..5).map(|n| note(n, &pk(1))).collect();

    let mut reg = registry(&store);
    reg.set_rebuild_files(vec![DUMP.into()]).unwrap();
    reg.begin_rebuild(DUMP);
    fold(&mut reg, &events, 5);
    reg.finish_rebuild_file(DUMP);

    // Every analysis has folded every dump: the run is over, and the reader is
    // told so directly instead of discovering it one SkipFile at a time.
    assert_eq!(reg.begin_rebuild(DUMP), RebuildPlan::Finished);

    reg.finish_rebuild_run();
    assert!(
        reg.rebuilding().is_empty(),
        "positions must clear when the run ends, or every startup would spawn \
         a rebuild that does nothing"
    );

    std::fs::remove_dir_all(&dir).ok();
}

/// The staged pass order is what makes the trust split right.
///
/// `activity` depends on `follow_graph`: it labels each event trusted or
/// untrusted from the world the graph builds. In a single-pass rebuild the
/// graph is still cold while activity folds, so every event is recorded
/// untrusted, permanently -- the same bug the staged ingest exists to prevent,
/// reintroduced through the rebuild path.
#[test]
fn dependents_wait_for_their_own_pass() {
    let dir = tempdir("staged");
    let store = StatStore::new(&dir).unwrap();

    let mut reg = Registry::new();
    reg.register(FollowGraph::default());
    reg.register(Activity::default());
    reg.load(&store).unwrap();
    reg.set_rebuild_files(vec![DUMP.into()]).unwrap();

    // The archive interleaves contact lists and notes, as a real dump does.
    let star = pk(200);
    let mut events: Vec<NostrEvent> = Vec::new();
    for i in 0..10u16 {
        events.push(contacts(100 + i, &pk(i as u8), &star));
    }
    for i in 0..5u16 {
        events.push(note(200 + i, &star));
    }

    // Pass 1: follow_graph only. Activity must fold nothing yet.
    assert_eq!(reg.rebuild_stage(), Some(0));
    reg.begin_rebuild(DUMP);
    let world = World::new(); // cold, as it is during pass 1
    for ev in &events {
        reg.observe_rebuild(ev, 1_700_100_000, &world, DUMP);
    }
    reg.finish_rebuild_file(DUMP);

    let mid = snapshot(&reg, "activity");
    assert!(
        mid.as_object().map(|o| o.is_empty()).unwrap_or(true),
        "activity must not fold during the graph's pass: {mid}"
    );

    // Between passes the world is materialized from what pass 1 built.
    let mut world = World::new();
    reg.materialize_all(1_700_100_000, &mut world).unwrap();

    // Pass 2: activity folds with a real world.
    assert_eq!(reg.rebuild_stage(), Some(1), "the run must advance a stage");
    reg.begin_rebuild(DUMP);
    for ev in &events {
        reg.observe_rebuild(ev, 1_700_100_000, &world, DUMP);
    }
    reg.finish_rebuild_file(DUMP);
    assert_eq!(reg.begin_rebuild(DUMP), RebuildPlan::Finished);

    let day = 1_700_000_000u64 - (1_700_000_000 % 86_400);
    let act = snapshot(&reg, "activity");
    assert_eq!(
        act[day.to_string()]["kinds"]["1"]["trusted"],
        5,
        "notes from a followed pubkey must be trusted after the staged \
         rebuild; in a single pass they are all untrusted: {act}"
    );

    std::fs::remove_dir_all(&dir).ok();
}
