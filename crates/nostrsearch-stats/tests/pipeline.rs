//! End-to-end: dependency staging, publisher filtering, additive backfill,
//! resumable binary persistence, and realtime metrics emission.

use nostrsearch_stats::analyses::{FollowGraph, KindBreakdown, Pagerank};
use nostrsearch_stats::metrics::MetricsEvent;
use nostrsearch_stats::{
    BufferObserver, PublisherFilter, Registry, StatStore, backfill_in_memory,
};
use nostrsearch_core::event::NostrEvent;
use std::sync::Arc;

fn pk(seed: u8) -> String {
    format!("{seed:02x}").repeat(32)
}
fn id(seed: u16) -> String {
    format!("{seed:04x}").repeat(16)
}
fn ev(idv: &str, pubkey: &str, kind: u16, created_at: u64, p: &[String]) -> NostrEvent {
    NostrEvent {
        id: idv.into(),
        pubkey: pubkey.into(),
        created_at,
        kind,
        tags: p.iter().map(|x| vec!["p".to_string(), x.clone()]).collect(),
        content: String::new(),
        sig: "c".repeat(128),
    }
}

/// `star` (pk 200) gets 10 followers (tier 1); `nobody` (pk 201) gets 0.
fn corpus() -> Vec<NostrEvent> {
    let mut events = Vec::new();
    for i in 0..10u8 {
        events.push(ev(&id(i as u16), &pk(i), 3, 100 + i as u64, &[pk(200)]));
    }
    for i in 0..3u16 {
        events.push(ev(&id(1000 + i), &pk(200), 1, 200 + i as u64, &[]));
    }
    for i in 0..2u16 {
        events.push(ev(&id(2000 + i), &pk(201), 1, 300 + i as u64, &[]));
    }
    events
}

#[test]
fn dependency_staging_publisher_filter_and_metrics() {
    let events = corpus();
    let obs = Arc::new(BufferObserver::new(256));

    let mut reg = Registry::new();
    reg.set_observer(obs.clone());
    reg.register(FollowGraph::default())
        .register(Pagerank::default())
        .register(KindBreakdown::filtered(PublisherFilter::min_followers(10)));

    // follow_graph + pagerank in stage 0, filtered kind_breakdown in stage 1.
    let stages = reg.stages().unwrap();
    assert_eq!(stages.len(), 2);

    let world = backfill_in_memory(&mut reg, 1_000, 2_000_000_000, &events).unwrap();
    assert_eq!(world.follower_count(&pk_hash(200)), 10);

    let snaps: std::collections::HashMap<_, _> = reg.snapshots().into_iter().collect();
    let kb = &snaps["kind_breakdown"];
    let k1 = kb["1"]["trusted"].as_u64().unwrap() + kb["1"]["untrusted"].as_u64().unwrap();
    assert_eq!(k1, 3, "only star's 3 notes pass the >=10 follower filter");

    // pagerank ran its scheduled refresh during backfill
    let refreshed = obs
        .recent()
        .iter()
        .any(|e| matches!(e, MetricsEvent::Refreshed { name, .. } if *name == "pagerank"));
    assert!(refreshed, "pagerank should have emitted a Refreshed event");

    // an initial Snapshot was emitted, and metrics show throughput + filtering
    assert!(matches!(obs.recent().first(), Some(MetricsEvent::Snapshot(_))));
    let m = reg.metrics(world.len());
    assert!(m.total_events > 0);
    let kbm = m.analyses.iter().find(|a| a.name == "kind_breakdown").unwrap();
    assert_eq!(kbm.consumed, 3, "only star's 3 notes cleared the filter");
    // 10 kind-3 authors (0 followers) + nobody's 2 notes are all filtered.
    assert_eq!(kbm.filtered, 12);
}

#[test]
fn additive_backfill_and_resume_binary_store() {
    let events = corpus();
    let dir = tempdir();
    let store = StatStore::new(&dir).unwrap();

    // Round 1: only follow graph.
    let observed_round1;
    {
        let mut reg = Registry::new();
        reg.register(FollowGraph::default());
        reg.load(&store).unwrap();
        assert!(reg.needs_backfill());
        backfill_in_memory(&mut reg, 1_000, 2_000_000_000, &events).unwrap();
        assert!(!reg.needs_backfill());
        observed_round1 = reg.metrics(0).analyses[0].observed;
        assert_eq!(observed_round1, 10, "10 kind-3 events observed");
        reg.persist(&store).unwrap();
    }
    // binary checkpoint on disk
    assert!(dir.join("follow_graph.state.bin").exists());

    // Counters persist: reload follow_graph alone and check its cumulative total.
    {
        let mut reg = Registry::new();
        reg.register(FollowGraph::default());
        reg.load(&store).unwrap();
        let fg = reg.metrics(0).analyses.into_iter().next().unwrap();
        assert_eq!(fg.observed, observed_round1, "counters survived restart");
        assert!(!reg.needs_backfill());
    }

    // Round 2: restart; follow graph resumes, new analysis backfills alone.
    {
        let mut reg = Registry::new();
        reg.register(FollowGraph::default())
            .register(KindBreakdown::filtered(PublisherFilter::min_followers(10)));
        reg.load(&store).unwrap();

        let e = reg.entries();
        let fg = e.iter().find(|x| x.name() == "follow_graph").unwrap();
        let kb = e.iter().find(|x| x.name() == "kind_breakdown").unwrap();
        assert!(!fg.needs_backfill(), "follow_graph restored from disk");
        assert!(kb.needs_backfill(), "new analysis must backfill");

        let world = backfill_in_memory(&mut reg, 1_000, 2_000_000_000, &events).unwrap();
        assert_eq!(world.follower_count(&pk_hash(200)), 10);
    }
}

fn pk_hash(seed: u8) -> nostrsearch_stats::Pubkey {
    nostrsearch_stats::Pubkey::from_hex(&pk(seed)).unwrap()
}

fn tempdir() -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "nsstats-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// Re-running a backfill over *newer* dumps must keep updating stats.
///
/// Exercises `observe_backfill`, the path the `ingest` binary uses. An earlier
/// version skipped every already-backfilled analysis outright, so a second
/// ingest over new daily dumps indexed the events but silently never updated
/// stats/WoT again.
#[test]
fn rerunning_backfill_over_newer_events_still_updates_stats() {
    let dir = tempdir();
    let store = StatStore::new(&dir).unwrap();
    let world = nostrsearch_stats::World::new();

    // Round 1: original corpus (star gets 10 followers).
    {
        let mut reg = Registry::new();
        reg.register(FollowGraph::default());
        reg.load(&store).unwrap();
        for e in corpus() {
            reg.observe_backfill(&e, 1_000, &world);
        }
        let mut w = nostrsearch_stats::World::new();
        reg.materialize_all(2_000_000_000, &mut w).unwrap();
        reg.mark_all_backfilled().unwrap();
        reg.persist(&store).unwrap();
        assert_eq!(w.follower_count(&pk_hash(200)), 10);
    }

    // Round 2: a later dump adds 5 more followers for `star`.
    let mut later = Vec::new();
    for i in 10..15u8 {
        later.push(ev(&id(500 + i as u16), &pk(i), 3, 1_000 + i as u64, &[pk(200)]));
    }

    let mut reg = Registry::new();
    reg.register(FollowGraph::default());
    reg.load(&store).unwrap();
    assert!(!reg.entries()[0].needs_backfill(), "restored as backfilled");

    for e in &later {
        reg.observe_backfill(e, 2_000, &world);
    }
    let mut w = nostrsearch_stats::World::new();
    reg.materialize_all(2_000_000_000, &mut w).unwrap();

    assert_eq!(
        w.follower_count(&pk_hash(200)),
        15,
        "a later dump must be folded into the existing follow graph"
    );
}
