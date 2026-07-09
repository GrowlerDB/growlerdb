//! Generate Rust types + tonic services from the `growlerdb.v1` protos at build time.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Point prost/tonic at a vendored protoc unless one is explicitly provided, so the build works
    // regardless of environment — CI, local, and especially `cross` containers, whose distro protoc
    // is ancient (proto2-only) or absent. An explicit PROTOC override still wins.
    if std::env::var_os("PROTOC").is_none() {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }
    tonic_prost_build::configure().compile_protos(
        &[
            "proto/growlerdb/v1/common.proto",
            "proto/growlerdb/v1/system.proto",
            "proto/growlerdb/v1/write.proto",
            "proto/growlerdb/v1/search.proto",
            "proto/growlerdb/v1/admin.proto",
            "proto/growlerdb/v1/control.proto",
        ],
        &["proto"],
    )?;
    Ok(())
}
