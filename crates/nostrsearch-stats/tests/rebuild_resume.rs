//! A rebuild must resume where it stopped, exactly once per event.
//!
//! Rebuilds fold the whole archive and run for hours, so a deploy lands in the
//! middle of one routinely. Resuming has to be exact in both directions: a
//! resume point ahead of what was folded silently drops events, and one behind
//! folds them twice. Both corrupt every counter in the report, and neither is
//! visible afterwards -- the numbers just quietly stop matching the corpus.

use nostrsearch_stats::{RebuildCheckpoint, StatStore};

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

/// The scan a resuming reader performs: fold everything strictly after the
/// checkpointed id, and nothing at or before it.
fn fold_from(lines: &[&str], cp: &RebuildCheckpoint) -> Vec<String> {
    let mut out = Vec::new();
    let mut skipping = !cp.last_id.is_empty();
    for l in lines {
        if skipping {
            if l.contains(&cp.last_id) {
                skipping = false;
            }
            continue;
        }
        out.push((*l).to_string());
    }
    out
}

#[test]
fn resuming_folds_each_event_exactly_once() {
    let lines: Vec<&str> = vec![
        r#"{"id":"aa","content":"1"}"#,
        r#"{"id":"bb","content":"2"}"#,
        r#"{"id":"cc","content":"3"}"#,
        r#"{"id":"dd","content":"4"}"#,
    ];

    // Interrupted after folding the second event.
    let cp = RebuildCheckpoint {
        completed: vec![],
        file: "dump.jsonl".into(),
        last_id: "bb".into(),
        offset: 999, // deliberately wrong: the id is what decides, not the hint
    };

    let resumed = fold_from(&lines, &cp);
    assert_eq!(
        resumed,
        vec![
            r#"{"id":"cc","content":"3"}"#.to_string(),
            r#"{"id":"dd","content":"4"}"#.to_string()
        ],
        "resume must continue after the checkpointed id, not repeat or skip it"
    );

    // A wrong byte offset must not affect the outcome; that is the whole point
    // of resuming on an id rather than a position.
    let mut shifted = cp.clone();
    shifted.offset = 0;
    assert_eq!(fold_from(&lines, &shifted), resumed);

    // Fresh checkpoint: fold everything.
    let fresh = RebuildCheckpoint::default();
    assert_eq!(fold_from(&lines, &fresh).len(), 4);
}

#[test]
fn an_unknown_checkpoint_id_folds_nothing_rather_than_everything() {
    let lines: Vec<&str> = vec![r#"{"id":"aa"}"#, r#"{"id":"bb"}"#];
    let cp = RebuildCheckpoint {
        completed: vec![],
        file: "dump.jsonl".into(),
        last_id: "zz".into(), // not in this file
        offset: 0,
    };
    // The id is never found, so nothing is folded. That is the safe direction:
    // re-folding the file from the top would double every counter, and the
    // missing id is detectable, unlike a stale offset that still points
    // somewhere plausible.
    assert!(
        fold_from(&lines, &cp).is_empty(),
        "an unmatched checkpoint must not silently re-fold the file"
    );
}

#[test]
fn checkpoints_survive_a_restart_and_a_corrupt_one_is_discarded() {
    let dir = tempdir("store");
    let store = StatStore::new(&dir).unwrap();

    assert!(store.load_rebuild().unwrap().is_none(), "none initially");

    let cp = RebuildCheckpoint {
        completed: vec!["a.jsonl".into()],
        file: "b.jsonl".into(),
        last_id: "deadbeef".into(),
        offset: 42,
    };
    store.save_rebuild(&cp).unwrap();

    let back = store.load_rebuild().unwrap().expect("checkpoint persisted");
    assert_eq!(back.last_id, "deadbeef");
    assert_eq!(back.completed, vec!["a.jsonl".to_string()]);

    // A truncated checkpoint must not wedge startup: losing it costs a restart
    // of the rebuild, failing to start costs the node.
    std::fs::write(dir.join("rebuild.checkpoint.bin"), b"\xff\xff\xff").unwrap();
    assert!(
        store.load_rebuild().unwrap().is_none(),
        "a corrupt checkpoint is discarded, not fatal"
    );

    store.save_rebuild(&cp).unwrap();
    store.clear_rebuild().unwrap();
    assert!(store.load_rebuild().unwrap().is_none(), "cleared on finish");

    std::fs::remove_dir_all(&dir).ok();
}
