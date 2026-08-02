//! Out-of-order delivery must not silently discard almost everything.
//!
//! archive.v0l.io counted 70 events into the activity report while the index
//! was taking hundreds of thousands a day and the gap scraper reported 468,941
//! events seen. Two bugs combined:
//!
//! 1. `should_consume` rejected anything with `created_at < watermark`. The
//!    watermark ratchets to the highest timestamp ever seen, so on a jittery
//!    live stream only successive record-highs survived, and the scraper's
//!    history -- which walks *backwards* -- was rejected wholesale.
//! 2. An analysis was marked "backfilled" once it had consumed a single event,
//!    which is what put it on that watermark path in the first place.

use nostrsearch_core::event::NostrEvent;
use nostrsearch_stats::analyses::Clients;
use nostrsearch_stats::{Analysis, Registry, World};

const NOW: u64 = 1_785_680_000;

fn ev(seed: u32, created_at: u64) -> NostrEvent {
    NostrEvent {
        id: format!("{seed:08x}").repeat(8),
        pubkey: "b".repeat(64),
        created_at,
        kind: 1,
        tags: vec![vec!["client".into(), "snort".into()]],
        content: String::new(),
        sig: "c".repeat(128),
    }
}

fn consumed(reg: &Registry) -> u64 {
    reg.entries()
        .iter()
        .find(|e| e.name() == "client_tags")
        .map(|e| e.progress.counters.consumed)
        .unwrap_or(0)
}

/// A live relay delivers events a few seconds either side of "now".
#[test]
fn jittery_live_delivery_is_not_thrown_away() {
    let world = World::new();
    let mut reg = Registry::new();
    reg.register(Clients::default());

    // Deterministic jitter of up to a minute, the shape of a real firehose.
    let mut seed = 12345u64;
    let n = 5_000u32;
    for i in 0..n {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let jitter = (seed >> 33) % 60;
        reg.observe(&ev(i, NOW + i as u64 - jitter), NOW, &world);
    }

    let got = consumed(&reg);
    assert!(
        got >= (n as u64) * 99 / 100,
        "out-of-order live stream lost events: {got} of {n}"
    );
}

/// The gap scraper walks backwards through history, so every event it returns
/// is far below the watermark set by live traffic.
#[test]
fn scraper_history_is_folded_not_rejected() {
    let world = World::new();
    let mut reg = Registry::new();
    reg.register(Clients::default());

    // Live traffic first, pushing the watermark to now.
    for i in 0..100u32 {
        reg.observe(&ev(i, NOW + i as u64), NOW, &world);
    }
    let after_live = consumed(&reg);

    // Then a day the scraper has just reconciled, hours back. An analysis that
    // has genuinely backfilled should still take recent history.
    for i in 1_000..1_500u32 {
        reg.observe(&ev(i, NOW - 3 * 3600 + i as u64), NOW, &world);
    }

    assert!(
        consumed(&reg) >= after_live + 500,
        "scraper history was rejected: {} folded",
        consumed(&reg) - after_live
    );
}

/// Count-once still holds: the same event twice is folded once.
#[test]
fn duplicates_are_still_counted_once() {
    let world = World::new();
    let mut reg = Registry::new();
    reg.register(Clients::default());

    let e = ev(42, NOW);
    reg.observe(&e, NOW, &world);
    reg.observe(&e, NOW, &world);
    reg.observe(&e, NOW, &world);
    assert_eq!(consumed(&reg), 1, "duplicate events must count once");
}

/// Genuinely ancient events stay rejected -- the window is a tolerance, not an
/// invitation to re-fold the whole corpus on every restart.
#[test]
fn events_far_below_the_window_are_still_skipped() {
    let world = World::new();
    let mut reg = Registry::new();
    reg.register(Clients::default());

    reg.observe(&ev(1, NOW), NOW, &world);
    let before = consumed(&reg);
    // A year old, well outside LIVE_LAG_SECS.
    reg.observe(&ev(2, NOW - 365 * 86_400), NOW, &world);
    assert_eq!(consumed(&reg), before, "ancient event should be skipped");
}

/// An analysis is only "backfilled" when something actually replayed the
/// corpus -- never inferred from having seen a couple of live events.
#[test]
fn backfill_is_not_inferred_from_live_traffic() {
    let world = World::new();
    let mut reg = Registry::new();
    reg.register(Clients::default());

    reg.observe(&ev(1, NOW), NOW, &world);
    reg.observe(&ev(2, NOW + 1), NOW, &world);

    assert_eq!(
        reg.outstanding_backfills(),
        vec!["client_tags"],
        "two live events must not count as having backfilled the corpus"
    );
}
