//! Compiles proto/gateway/v1/gateway.proto and proto/dataplane/v1/
//! dataplane.proto into Rust message types at build time (Phase-01 Step
//! E1). Message-only (`prost_build`, not `tonic_build`) — normalize just
//! needs `SubmitRequest` et al. as values; the gRPC client/service stub
//! codegen is added in Step E3 when the actual Submit() call is wired.
//!
//! Not committed generated code (unlike dataplane-stub's Python stubs,
//! generate_proto.sh): Rust's idiomatic path is compile-time codegen via
//! build.rs, and unlike protobuf's Python runtime (which enforces a strict
//! generator/runtime version match — see dataplane-stub/generate_proto.sh's
//! header for that saga), prost has no such constraint forcing a pinned,
//! committed alternative here.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::compile_protos(
        &[
            "../../proto/dataplane/v1/dataplane.proto",
            "../../proto/gateway/v1/gateway.proto",
        ],
        &["../../proto"],
    )?;
    Ok(())
}
