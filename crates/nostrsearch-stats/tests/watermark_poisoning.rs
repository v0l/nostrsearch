//! A single future-dated event must not stop an analysis forever.
//!
//! Reproduces what happened on archive.v0l.io: the activity report had counted
//! exactly **2 events** against an index of 470M documents. `created_at` is
//! publisher-supplied and unvalidated, the live corpus contains events dated
//! past the year 55000, and `Progress::should_consume` rejects anything with
//! `created_at < watermark`. So the first future-dated event parked the
//! watermark ahead of real time and every subsequent genuine event was dropped.

use nostrsearch_core::event::NostrEvent;
use nostrsearch_stats::analyses::Clients;
use nostrsearch_stats::{Analysis, Progress, Registry, World};

const DAY: u64 = 86_400;
const NOW: u64 = 1_785_676_000;

fn ev(id_seed: u16, created_at: u64) -> NostrEvent {
    NostrEvent {
        id: format!("{id_seed:04x}").repeat(16),
        pubkey: "b".repeat(64),
        created_at,
        kind: 1,
        tags: vec![vec!["client".into(), "snort".into()]],
        content: String::new(),
        sig: "c".repeat(128),
    }
}

fn counted(reg: &Registry) -> u64 {
    reg.entries()
        .iter()
        .find(|e| e.name() == "client_tags")
        .map(|e| e.progress.counters.consumed)
        .unwrap_or(0)
}

#[test]
fn one_future_dated_event_does_not_stall_the_analysis() {
    let world = World::new();
    let mut reg = Registry::new();
    reg.register(Clients::default());

    // A normal live event.
    reg.observe(&ev(1, NOW), NOW, &world);

    // Then the poison: an event dated in the year 55913, exactly like the ones
    // sitting in the production index.
    let year_55913 = 1_705_000_000_000u64;
    reg.observe(&ev(2, year_55913), NOW, &world);

    // ...followed by ordinary traffic, which is what used to vanish.
    for i in 3..50u16 {
        reg.observe(&ev(i, NOW + i as u64), NOW, &world);
    }

    let n = counted(&reg);
    assert!(
        n >= 48,
        "analysis stalled after a future-dated event: only {n} consumed"
    );
}

#[test]
fn mildly_future_events_do_not_stall_it_either() {
    // The actual production symptom was subtler than year 55913: an event
    // dated three weeks out parked the watermark there, so everything from
    // "now" was rejected for three weeks.
    let world = World::new();
    let mut reg = Registry::new();
    reg.register(Clients::default());

    reg.observe(&ev(1, NOW + 21 * DAY), NOW, &world);
    for i in 2..30u16 {
        reg.observe(&ev(i, NOW + i as u64), NOW, &world);
    }

    let n = counted(&reg);
    assert!(n >= 28, "3-week-ahead event stalled the analysis: {n}");
}

#[test]
fn events_within_clock_skew_still_advance_normally() {
    let world = World::new();
    let mut reg = Registry::new();
    reg.register(Clients::default());

    // A publisher a minute fast is ordinary drift, not poison; it should
    // advance the watermark as usual.
    reg.observe(&ev(1, NOW + 60), NOW, &world);
    // An event a second older than that is then legitimately behind the
    // watermark and skipped -- the count-once guarantee still holds.
    reg.observe(&ev(2, NOW + 59), NOW, &world);
    assert_eq!(counted(&reg), 1);
}

#[test]
fn a_poisoned_watermark_is_repaired_rather_than_waited_out() {
    // State persisted before the fix carries the bad watermark, so a restart
    // alone would not recover.
    let mut p = Progress::fresh(0);
    p.watermark = 1_705_000_000_000; // year 55913
    let max_ts = NOW + 300;

    assert!(p.clamp_watermark(max_ts), "should report a repair");
    assert_eq!(p.watermark, max_ts);
    assert!(p.boundary.is_empty(), "stale boundary ids must be dropped");

    // A sane watermark is left alone.
    let mut ok = Progress::fresh(0);
    ok.watermark = NOW - DAY;
    assert!(!ok.clamp_watermark(max_ts));
    assert_eq!(ok.watermark, NOW - DAY);
}
