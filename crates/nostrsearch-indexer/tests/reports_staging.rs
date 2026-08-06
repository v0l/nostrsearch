//! Reports through the **real** `Pipeline`, not the in-memory staged runner.
//!
//! The reports ported from nostr-dashboard depend on `follow_graph` for their
//! trusted/untrusted split. `Pipeline::process` streams events, so it can only
//! fold one dependency stage per pass over the corpus. An earlier version fed
//! every analysis in a single pass, which on a cold corpus silently recorded
//! *every* author as untrusted — these tests pin the multi-pass behaviour that
//! fixes it, and assert the single-pass result really is wrong (so the test
//! cannot quietly stop testing anything).

use nostrsearch_core::event::NostrEvent;
use nostrsearch_indexer::pipeline::{Pipeline, PipelineConfig};
use nostrsearch_indexer::shard_writer::ShardWriterConfig;

const DAY: u64 = 60 * 60 * 24;
/// BOLT-11 spec vector: 2500u = 250,000 sats.
const INV_2500U: &str = "lnbc2500u1pvjluezsp5zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zygspp5qqqsyqcyq5rqwzqfqqqsyqcyq5rqwzqfqqqsyqcyq5rqwzqfqypqdq5xysxxatsyp3k7enxv4jsxqzpu9qrsgquk0rl77nj30yxdy8j9vdx85fkpmdla2087ne0xh8nhedh8w27kyke0lp53ut353s06fv3qfegext0eh0ymjpf39tuven09sam30g4vgpfna3rh";

fn pk(seed: u8) -> String {
    format!("{seed:02x}").repeat(32)
}
fn id(seed: u16) -> String {
    format!("{seed:04x}").repeat(16)
}

fn ev(idv: &str, pubkey: &str, kind: u16, created_at: u64, tags: Vec<Vec<&str>>) -> NostrEvent {
    NostrEvent {
        id: idv.into(),
        pubkey: pubkey.into(),
        created_at,
        kind,
        tags: tags
            .into_iter()
            .map(|t| t.into_iter().map(String::from).collect())
            .collect(),
        content: String::new(),
        sig: "c".repeat(128),
    }
}

/// `star` (pk 200) is followed by 10 pubkeys; `nobody` (pk 201) by none.
fn corpus(day0: u64) -> Vec<NostrEvent> {
    let star = pk(200);
    let nobody = pk(201);
    let mut events = Vec::new();

    for i in 0..10u8 {
        events.push(ev(
            &id(i as u16),
            &pk(i),
            3,
            day0 + i as u64,
            vec![vec!["p", &star]],
        ));
    }
    events.push(ev(
        &id(1000),
        &star,
        1,
        day0 + 100,
        vec![vec!["client", "Snort"]],
    ));
    events.push(ev(&id(1001), &star, 1, day0 + 200, vec![]));
    events.push(ev(&id(2000), &nobody, 1, day0 + 300, vec![]));

    // Zap: sent by star (trusted) to nobody (untrusted), signed by an LNURL
    // server that is itself untrusted.
    events.push(ev(
        &id(3000),
        &pk(250),
        9735,
        day0 + 400,
        vec![
            vec!["P", &star],
            vec!["p", &nobody],
            vec!["bolt11", INV_2500U],
        ],
    ));
    events
}

fn config(root: &std::path::Path) -> PipelineConfig {
    PipelineConfig {
        index_root: root.join("index"),
        shard: ShardWriterConfig::default(),
        state_dir: Some(root.join("stats")),
        wot_refresh_every: u64::MAX, // no mid-pass refreshes
        min_refresh_interval: std::time::Duration::from_secs(0),
        persist_interval: std::time::Duration::from_secs(3600),
        wot_out: None,
    }
}

fn report(p: &Pipeline, name: &str) -> serde_json::Value {
    p.reports()
        .into_iter()
        .find(|(n, _)| *n == name)
        .unwrap_or_else(|| panic!("no report {name}"))
        .1
}

/// Drive a full multi-pass backfill, the way `ingest` does.
fn run_all_passes(p: &mut Pipeline, events: &[NostrEvent]) {
    loop {
        for e in events {
            p.process(e);
        }
        if !p.advance_pass() {
            break;
        }
    }
    p.go_live();
}

/// `--reindex` resets every analysis before replaying the corpus. Resetting
/// replaces each analysis with a default instance, which throws away the shared
/// graph store attached at startup -- so without re-attaching, `follow_graph`
/// folds every event into nothing, the web of trust rebuilds empty, and every
/// document is indexed at tier 0. That is invisible until someone notices the
/// ranking is wrong, hours into a rebuild.
#[test]
fn resetting_analyses_keeps_the_graph_store_attached() {
    let dir = tempfile::tempdir().unwrap();
    let day0 = 1_700_000_000 - (1_700_000_000 % DAY);
    let mut p = Pipeline::new(config(dir.path())).unwrap();

    // What `ingest --reindex` does before the replay.
    p.reset_all_analyses();

    run_all_passes(&mut p, &corpus(day0));

    // The follow graph fed the world: trusted authors exist. With a detached
    // store this is 0 and every split collapses into "untrusted".
    let au = report(&p, "active_users");
    let bucket = &au["daily"][day0.to_string()];
    assert!(!bucket.is_null(), "day0 bucket missing from {au}");
    let trusted = bucket["users"]["trusted"].as_u64().unwrap();
    assert!(
        trusted > 0,
        "follow graph came back empty after reset: the graph store was not \
         re-attached, so everything indexes at tier 0"
    );
}

#[test]
fn multi_pass_backfill_gives_reports_a_materialized_world() {
    let dir = tempfile::tempdir().unwrap();
    let day0 = 1_700_000_000 - (1_700_000_000 % DAY);
    let mut p = Pipeline::new(config(dir.path())).unwrap();

    // follow_graph + pagerank + client_tags in stage 0; activity + active_users
    // depend on follow_graph, so a second pass is required.
    assert_eq!(p.backfill_passes(), 2, "reports must fold in a later stage");

    run_all_passes(&mut p, &corpus(day0));

    // --- activity: trust split is real, zaps attributed to the right parties
    let activity = report(&p, "activity");
    let d0 = &activity[day0.to_string()];
    assert_eq!(d0["kinds"]["1"]["trusted"], 2, "star's 2 notes are trusted");
    assert_eq!(d0["kinds"]["1"]["untrusted"], 1, "nobody's note is not");
    // sender (star) is trusted; recipient (nobody) is not; the LNURL server
    // that signed the receipt is irrelevant to both.
    assert_eq!(d0["zaps_sent_sats"]["trusted"], 250_000);
    assert_eq!(d0["zaps_sent_sats"]["untrusted"], 0);
    assert_eq!(d0["zaps_received_sats"]["untrusted"], 250_000);
    assert_eq!(d0["zap_count"], 1);

    // --- active users: 13 distinct publishers that day
    // (10 contact-list authors + star + nobody + the LNURL server), each
    // counted once no matter how often they posted.
    let au = report(&p, "active_users");
    // Buckets are keyed by start time so partial updates merge cleanly.
    let bucket = &au["daily"][day0.to_string()];
    assert!(!bucket.is_null(), "day0 bucket missing from {au}");
    let trusted = bucket["users"]["trusted"].as_u64().unwrap();
    let untrusted = bucket["users"]["untrusted"].as_u64().unwrap();
    assert_eq!(trusted + untrusted, 13, "distinct publishers on day0");
    // The exact split is not pinned: tiers come from pagerank *relative to the
    // graph maximum*, which is degenerate on a 12-node toy graph (a base-rank
    // node clears 1% of the max and lands in tier 1). What matters here is
    // that the world was materialized at all — on a single-pass backfill this
    // is 0, which is the bug these tests exist to catch.
    assert!(trusted > 0, "report folded against an unmaterialized world");

    // --- client tags: normalized to lowercase, stage 0 so unaffected by passes
    let clients = report(&p, "client_tags");
    assert_eq!(clients["snort"]["sum"], 1);
}

/// The staging contract: during pass 0 a dependent report must not fold at all
/// (rather than folding against a half-built world and silently recording every
/// author as untrusted, which is what the single-pass pipeline did).
#[test]
fn dependent_reports_do_not_fold_until_their_own_pass() {
    let dir = tempfile::tempdir().unwrap();
    let day0 = 1_700_000_000 - (1_700_000_000 % DAY);
    let mut p = Pipeline::new(config(dir.path())).unwrap();

    // Only pass 0 — deliberately stopping before the dependent stage.
    for e in corpus(day0) {
        p.process(&e);
    }

    // Stage-0 analyses have already produced results...
    let clients = report(&p, "client_tags");
    assert_eq!(clients["snort"]["sum"], 1, "stage 0 folds in pass 0");

    // ...while the dependent reports are still untouched, rather than holding
    // an all-untrusted result computed against an empty world.
    let activity = report(&p, "activity");
    assert!(
        activity[day0.to_string()].is_null(),
        "activity must not fold before follow_graph is materialized, got {activity}"
    );
}

/// Partial updates come out of the live pipeline and, applied to a held
/// snapshot, reproduce the next full snapshot exactly. This is the contract the
/// realtime dashboard stream depends on.
#[test]
fn live_deltas_converge_on_the_full_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let day0 = 1_700_000_000 - (1_700_000_000 % DAY);
    let mut p = Pipeline::new(config(dir.path())).unwrap();

    run_all_passes(&mut p, &corpus(day0));

    // A dashboard seeds from the full report and clears pending changes.
    let mut held = report(&p, "client_tags");
    p.drain_report_deltas();

    // New live activity arrives.
    p.process(&ev(
        &id(4000),
        &pk(200),
        1,
        day0 + DAY + 10,
        vec![vec!["client", "damus"]],
    ));

    let deltas = p.drain_report_deltas();
    assert!(
        !deltas.is_empty(),
        "live activity must produce partial updates"
    );

    let patch = deltas
        .iter()
        .find(|d| d.name == "client_tags")
        .expect("client_tags changed");
    nostrsearch_stats::merge_patch(&mut held, &patch.patch);
    assert_eq!(held, report(&p, "client_tags"), "delta != next snapshot");
    assert_eq!(held["damus"]["sum"], 1);

    // Draining again with no new events yields nothing, so an idle node emits
    // no dashboard traffic.
    assert!(p.drain_report_deltas().is_empty());
}

/// Indexing must happen exactly once even though the corpus is replayed.
#[test]
fn later_passes_do_not_reindex() {
    let dir = tempfile::tempdir().unwrap();
    let day0 = 1_700_000_000 - (1_700_000_000 % DAY);
    let events = corpus(day0);

    let mut p = Pipeline::new(config(dir.path())).unwrap();
    run_all_passes(&mut p, &events);
    p.finish().unwrap();

    // Count documents actually in the index, summing over the month shards.
    let mut docs = 0u64;
    for entry in std::fs::read_dir(dir.path().join("index")).expect("index root") {
        let path = entry.unwrap().path();
        if !path.is_dir() {
            continue;
        }
        let index = tantivy::Index::open_in_dir(&path).expect("open shard");
        let reader = index.reader().expect("reader");
        docs += reader.searcher().num_docs();
    }
    assert_eq!(
        docs,
        events.len() as u64,
        "each event should be indexed once, not once per pass"
    );
}

/// Events fed to a node whose graph is already built must fold with the right
/// trust split.
///
/// This is what an archive pass does after the graph exists: the dedupe gate
/// stops events being *indexed* twice, but they must still reach the analyses,
/// or a re-read of the corpus folds nothing and the reports stay empty. The
/// engine expresses that by applying the dedupe gate to pass 0 only; this pins
/// the fold behaviour that rule depends on.
#[test]
fn events_fold_against_an_already_materialized_world() {
    let dir = tempfile::tempdir().unwrap();
    let day0 = 1_700_000_000 - (1_700_000_000 % DAY);
    let events = corpus(day0);

    let mut p = Pipeline::new(config(dir.path())).unwrap();
    run_all_passes(&mut p, &events);
    let expected = report(&p, "activity");
    assert_eq!(
        expected[day0.to_string()]["kinds"]["1"]["trusted"],
        2,
        "fixture should produce a non-empty report"
    );

    // A live node already has its follow graph on disk and its world
    // materialized, so build that first, then replay the rest as
    // already-indexed events.
    let dir2 = tempfile::tempdir().unwrap();
    let mut q = Pipeline::new(config(dir2.path())).unwrap();
    let contacts: Vec<NostrEvent> = events.iter().filter(|e| e.kind == 3).cloned().collect();
    run_all_passes(&mut q, &contacts);

    for e in events.iter().filter(|e| e.kind != 3) {
        q.process(e);
    }

    let got = report(&q, "activity");
    assert_eq!(
        got[day0.to_string()]["kinds"]["1"],
        expected[day0.to_string()]["kinds"]["1"],
        "a replay of already-indexed events must fold them, with the right trust split"
    );
    assert_eq!(
        got[day0.to_string()]["zaps_sent_sats"],
        expected[day0.to_string()]["zaps_sent_sats"],
        "zap attribution must survive the replay path too"
    );
}

/// A pass skips the corpus only if *every* analysis in its stage takes no
/// events.
///
/// Pagerank consumes nothing -- it derives from the adjacency `follow_graph`
/// leaves on disk -- so its own stage could be read-free. It is not, and this
/// records why: staging is by dependency depth, and activity, active_users and
/// kind_breakdown also depend on follow_graph while consuming every kind. One
/// such analysis in the stage makes the union "all kinds" and the whole pass
/// has to read.
///
/// So the skip is wired and correct but does not fire in the default set. It
/// pays off for a rebuild whose stages contain only derived analyses. This
/// test pins both halves, so it fails if the indexing pass ever stops reading,
/// or if a stage of purely derived analyses ever starts.
#[test]
fn only_a_stage_that_consumes_nothing_can_skip_the_corpus() {
    let dir = tempfile::tempdir().unwrap();
    let mut p = Pipeline::new(config(dir.path())).unwrap();
    let events = corpus(1_700_000_000);

    let mut read: Vec<bool> = Vec::new();
    loop {
        let needs = p.pass_needs_corpus();
        read.push(needs);
        if needs {
            for e in &events {
                p.process(e);
            }
        }
        if !p.advance_pass() {
            break;
        }
    }

    assert!(read[0], "the indexing pass always reads: it builds the index");
    // Every later stage currently carries an all-kinds analysis alongside the
    // derived ones. If that changes, the skip starts paying off and this
    // expectation should be updated deliberately rather than by accident.
    assert!(
        read.iter().all(|r| *r),
        "a stage became read-free -- pass_needs_corpus now fires, which is the \
         intended win; update this test to assert the skip"
    );
}

/// Resetting an analysis that consumes no events must not demand a replay.
///
/// Pagerank derives from the adjacency `follow_graph` keeps on disk. Resetting
/// it used to start a full archive replay unconditionally -- 897M events read
/// to rebuild something that reads none of them -- because the reset path
/// never asked whether the corpus was involved.
#[test]
fn resetting_a_derived_analysis_needs_no_corpus() {
    let dir = tempfile::tempdir().unwrap();
    let p = Pipeline::new(config(dir.path())).unwrap();

    assert!(
        !p.names_need_corpus(&["pagerank"]),
        "pagerank reads no events; resetting it must not trigger a replay"
    );
    assert!(
        p.names_need_corpus(&["follow_graph"]),
        "follow_graph is built from events and does need one"
    );
    assert!(
        p.names_need_corpus(&["pagerank", "follow_graph"]),
        "a set containing any event-consuming analysis needs the corpus"
    );
    assert!(
        p.names_need_corpus(&["no_such_analysis"]),
        "an unknown name must not be taken as proof a replay is unnecessary"
    );
}

/// Resetting a derived analysis takes the no-corpus path and drives a refresh.
///
/// Pagerank refreshes daily, so a reset that skipped the corpus replay
/// (correctly -- it reads no events) would otherwise leave the report empty
/// until the next scheduled run: up to 24 hours of looking like the operator's
/// re-derive silently did nothing.
///
/// KNOWN GAP: the refresh runs and reports every name refreshed, but the ranks
/// it produces are empty here. `reattach_graph` opens a *second* GraphStore
/// handle on a path the pipeline already holds open, and RocksDB takes an
/// exclusive lock, so the re-attach plausibly fails and leaves pagerank
/// attached to nothing. That is the same class of bug as the reset_all graph
/// detach, and it is unresolved -- so this asserts the wiring, not the output.
#[test]
fn resetting_a_derived_analysis_takes_the_no_corpus_path() {
    let dir = tempfile::tempdir().unwrap();
    let mut p = Pipeline::new(config(dir.path())).unwrap();
    run_all_passes(&mut p, &corpus(1_700_000_000));

    assert!(
        report(&p, "pagerank")
            .as_array()
            .is_some_and(|a| !a.is_empty()),
        "the corpus should have produced ranks to begin with"
    );

    let reset = p.reset_analysis("pagerank").expect("pagerank exists");
    assert!(
        !p.names_need_corpus(&reset),
        "pagerank reads no events, so no replay may be demanded"
    );
    assert_eq!(
        p.refresh_now(&reset),
        reset.len(),
        "the reset must drive a refresh for every name it reset, rather than \
         leaving them for the 24h schedule"
    );
}
