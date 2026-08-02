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
use nostrsearch_stats::{Analysis, Registry, StatStore, World};

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
fn resetting_follow_graph_keeps_the_web_of_trust() {
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
            &contacts(&pk(i), &[star.clone()], 1000 + i as u64),
            2000,
            &world,
        );
    }

    let mut before = World::new();
    reg.materialize_all(2000, &mut before).unwrap();
    let star_key = nostrsearch_stats::Pubkey::from_hex(&star).unwrap();
    assert_eq!(before.follower_count(&star_key), 20);
    assert!(before.wot_tier(&star_key) >= 1, "should be trusted");

    // Operator resets the analysis, then it is re-attached as on restart.
    assert!(reg.reset("follow_graph"));
    reg.attach_all(store.dir()).unwrap();

    let mut after = World::new();
    reg.materialize_all(2000, &mut after).unwrap();

    assert_eq!(
        after.follower_count(&star_key),
        20,
        "follower counts must be rebuilt from the on-disk graph, not lost"
    );
    assert_eq!(
        after.wot_tier(&star_key),
        before.wot_tier(&star_key),
        "resetting an analysis must not silently drop the web of trust"
    );

    std::fs::remove_dir_all(&dir).ok();
}
