//! Guards the one build input that does not come from cargo: the operator
//! console bundle.
//!
//! `src/dashboard.rs` pulls `dashboard/dist/index.html` in with `include_str!`.
//! That file is a build artifact, not a checked-in copy, so on a fresh clone it
//! does not exist yet — and the error `include_str!` gives for that is a path
//! that means nothing to someone who has never built the frontend. Fail here
//! instead, with the command to run.
//!
//! Watching the file also means editing the console and rebuilding it is enough
//! to make cargo relink; no `touch` on a Rust file required.

use std::path::PathBuf;

fn main() {
    let bundle = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../dashboard/dist/index.html")
        .canonicalize()
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../dashboard/dist/index.html")
        });

    println!("cargo:rerun-if-changed={}", bundle.display());
    println!("cargo:rerun-if-changed=build.rs");

    if !bundle.is_file() {
        panic!(
            "the operator console has not been built: {} is missing.\n\
             Run ./scripts/build-dashboard.sh (requires bun, https://bun.sh)",
            bundle.display()
        );
    }
}
