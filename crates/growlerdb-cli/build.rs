// Bakes the version reported by `growlerdb --version` and the System gRPC service (task-156).
//
// Release builds export `GROWLERDB_VERSION` from the git tag (see RELEASING.md / ADR D29 — release
// artifacts are tag-derived while the in-tree workspace version stays `0.0.0`); every other build
// falls back to the crate's `CARGO_PKG_VERSION` (the workspace `0.0.0`), so a local `--version`
// honestly reports an unreleased build.
fn main() {
    let version = std::env::var("GROWLERDB_VERSION")
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| std::env::var("CARGO_PKG_VERSION").unwrap());
    println!("cargo:rustc-env=GROWLERDB_BUILD_VERSION={version}");
    println!("cargo:rerun-if-env-changed=GROWLERDB_VERSION");
}
