//! Resetting follow_graph must not destroy the web of trust.
//!
//! The adjacency lives in a shared on-disk store that is deliberately excluded
//! from the analysis's serialized state, so a reset clears the in-memory
//! follower counts while leaving the graph intact. Those counts drive every WoT
//! tier -- search ranking, and the trusted/untrusted split in every report --
//! and re-observing kind-3 contact lists to rebuild them would take longer than
//! collecting the corpus did. They are a pure function of the adjacency, so
//! they must be derived from it on attach.

use nostrsearch_core::event::NostrEvent;
use nostrsearch_stats::analyses::FollowGraph;
use nostrsearch_stats::{Registry, StatStore, World};

fn tempdir(tag: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "nsreset-{tag}-{}-{}",
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

fn contacts(author: &str, follows: &[String], created_at: u64) -> NostrEvent {
    NostrEvent {
        id: format!("{:04x}", created_at).repeat(16),
        pubkey: author.into(),
        created_at,
        kind: 3,
        tags: follows
            .iter()
            .map(|f| vec!["p".into(), f.clone()])
            .collect(),
        content: String::new(),
        sig: "c".repeat(128),
    }
}

#[test]
fn losing_the_checkpoint_keeps_the_web_of_trust() {
    let dir = tempdir("wot");
    let store = StatStore::new(&dir).unwrap();
    let world = World::new();
    let star = pk(200);

    let mut reg = Registry::new();
    reg.register(FollowGraph::default());
    reg.load(&store).unwrap();

    // Twenty pubkeys follow `star`.
    for i in 0..20u8 {
        reg.observe(
            &contacts(&pk(i), std::slice::from_ref(&star), 1000 + i as u64),
            2000,
            &world,
        );
    }

    let mut before = World::new();
    reg.materialize_all(2000, &mut before).unwrap();
    let star_key = nostrsearch_stats::Pubkey::from_hex(&star).unwrap();
    assert_eq!(before.follower_count(&star_key), 20);
    assert!(before.wot_tier(&star_key) >= 1, "should be trusted");

    // A restart that loses the serialized state but keeps the graph: a corrupt
    // checkpoint, or an epoch bump. The adjacency on disk is untouched, so the
    // follower counts are derivable and must not be thrown away -- rebuilding
    // them from contact lists would take longer than collecting the corpus did.
    //
    // This is deliberately *not* a reset. A reset clears the graph as well, so
    // the web of trust is empty until the rebuild refills it; that is what
    // reset means, and `reset_lets_the_graph_re_derive_from_replayed_events`
    // covers it.
    // Drop the first registry: RocksDB holds an exclusive lock, so this is
    // also what makes it a restart rather than two live handles.
    drop(reg);

    let mut restarted = Registry::new();
    restarted.register(FollowGraph::default());
    restarted.attach_all(store.dir()).unwrap();

    let mut after = World::new();
    restarted.materialize_all(2000, &mut after).unwrap();

    assert_eq!(
        after.follower_count(&star_key),
        20,
        "follower counts must be rebuilt from the on-disk graph, not lost"
    );
    assert_eq!(
        after.wot_tier(&star_key),
        before.wot_tier(&star_key),
        "losing the checkpoint must not silently drop the web of trust"
    );

    std::fs::remove_dir_all(&dir).ok();
}

/// Resetting a dependency must reset everything built on top of it.
///
/// `activity` and `active_users` label each event trusted or untrusted using
/// the world `follow_graph` builds, and fold that judgement into stored totals
/// as they go. Reset `follow_graph` alone and those reports keep counts derived
/// from a graph that no longer exists -- nothing recomputes them on read, so
/// the stale split survives any amount of re-ingesting.
#[test]
fn resetting_a_dependency_cascades_to_its_dependents() {
    use nostrsearch_stats::analyses::{ActiveUsers, Activity};

    let dir = tempdir("cascade");
    let store = StatStore::new(&dir).unwrap();
    let world = World::new();

    let mut reg = Registry::new();
    reg.register(FollowGraph::default());
    reg.register(Activity::default());
    reg.register(ActiveUsers::default());
    reg.load(&store).unwrap();

    let star = pk(200);
    for i in 0..20u8 {
        reg.observe(
            &contacts(&pk(i), std::slice::from_ref(&star), 1000 + i as u64),
            2000,
            &world,
        );
    }

    let reset = reg.reset("follow_graph").expect("follow_graph exists");
    assert!(
        reset.contains(&"activity") && reset.contains(&"active_users"),
        "resetting follow_graph must cascade to its dependents, got {reset:?}"
    );
    assert!(reset.contains(&"follow_graph"), "and include itself");

    // Resetting a leaf must not drag anything else down with it.
    let leaf = reg.reset("activity").expect("activity exists");
    assert_eq!(
        leaf,
        vec!["activity"],
        "a leaf reset must not touch unrelated analyses"
    );

    assert!(reg.reset("nope").is_none(), "unknown names are reported");

    std::fs::remove_dir_all(&dir).ok();
}

/// After a reset, replaying the same contact lists must rebuild the graph.
///
/// Contact lists are replaceable, so `FollowGraph::observe` drops any event not
/// newer than what the store already holds. Reset cleared the in-memory struct
/// and left the RocksDB adjacency fully populated, so every replayed list was
/// rejected as stale and the graph re-derived nothing -- on the live node,
/// `observed=1102, consumed=0` after a reset and an archive rebuild, with the
/// report simply staying empty.
#[test]
fn reset_lets_the_graph_re_derive_from_replayed_events() {
    let dir = tempdir("rederive");
    let store = StatStore::new(&dir).unwrap();
    let world = World::new();
    let star = pk(200);

    let mut reg = Registry::new();
    reg.register(FollowGraph::default());
    reg.load(&store).unwrap();

    let events: Vec<_> = (0..20u8)
        .map(|i| contacts(&pk(i), std::slice::from_ref(&star), 1000 + i as u64))
        .collect();
    for ev in &events {
        reg.observe(ev, 2000, &world);
    }

    let mut before = World::new();
    reg.materialize_all(2000, &mut before).unwrap();
    let star_key = nostrsearch_stats::Pubkey::from_hex(&star).unwrap();
    assert_eq!(before.follower_count(&star_key), 20);

    // Reset, then replay exactly the same events, as a rebuild over the
    // archive does.
    reg.reset("follow_graph").expect("exists");
    reg.attach_all(store.dir()).unwrap();

    for ev in &events {
        reg.observe(ev, 2000, &world);
    }
    let consumed: u64 = reg
        .status()
        .into_iter()
        .find(|s| s.name == "follow_graph")
        .map(|s| s.consumed)
        .unwrap_or(0);
    assert!(
        consumed > 0,
        "a reset graph must accept replayed contact lists, not reject them as stale \
         (observed them and consumed none is exactly the live symptom)"
    );

    let mut after = World::new();
    reg.materialize_all(2000, &mut after).unwrap();
    assert_eq!(
        after.follower_count(&star_key),
        20,
        "the graph must re-derive to the same shape from the same events"
    );

    std::fs::remove_dir_all(&dir).ok();
}
