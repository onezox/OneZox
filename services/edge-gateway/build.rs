//! Compiles proto/gateway/v1/gateway.proto and proto/dataplane/v1/
//! dataplane.proto into Rust types at build time. Step E1 used prost-build
//! (message types only); Step E3 switches to tonic-prost-build (message
//! types plus gRPC client/server stub codegen) now that the gRPC client to
//! dataplane-stub is actually needed. `build_server(false)`: edge-gateway
//! only ever calls dataplane-stub's Submit RPC, never serves
//! DataplaneService itself.
//!
//! tonic 0.14 split what used to be one `tonic-build` crate in two:
//! `tonic-build` is now low-level service-stub-only codegen, and message
//! compilation plus the familiar `configure().compile_protos(...)` API
//! moved to this new `tonic-prost-build` crate.
//!
//! Not committed generated code (unlike dataplane-stub's Python stubs,
//! generate_proto.sh): Rust's idiomatic path is compile-time codegen via
//! build.rs, and unlike protobuf's Python runtime (which enforces a strict
//! generator/runtime version match — see dataplane-stub/generate_proto.sh's
//! header for that saga), prost/tonic have no such constraint forcing a
//! pinned, committed alternative here.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(false)
        .compile_protos(
            &[
                "../../proto/dataplane/v1/dataplane.proto",
                "../../proto/gateway/v1/gateway.proto",
            ],
            &["../../proto"],
        )?;
    Ok(())
}
