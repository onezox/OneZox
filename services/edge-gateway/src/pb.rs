//! Generated protobuf message + gRPC client types, committed rather than
//! generated at build time (Step F reversal of Step E1/E3's original
//! choice). Reason: Docker's build context for this service is scoped to
//! services/edge-gateway/ (matching the other two services and CI's
//! existing `docker build ... services/${matrix.service}` pattern), so
//! proto/ at the repo root isn't reachable from inside a container build —
//! the same underlying constraint (Docker build context scoping) that
//! already led dataplane-stub to commit its generated Python stubs
//! (generate_proto.sh) rather than generate them at Docker build time;
//! it just took until Step F's actual containerization to surface here,
//! since local `cargo build` never hit it (proto/ is reachable from a
//! normal checkout).
//!
//! Regenerate via `cargo test -p edge-gateway -- --ignored
//! regenerate_proto` (needs repo-root proto/ access — a local dev-time
//! operation, same "run manually, not in CI" convention as
//! dataplane_client.rs's #[ignore] live-check test).

pub mod gateway {
    pub mod v1 {
        include!("pb_generated/gateway.v1.rs");
    }
}

pub mod dataplane {
    pub mod v1 {
        include!("pb_generated/dataplane.v1.rs");
    }
}

#[cfg(test)]
mod regenerate {
    #[test]
    #[ignore]
    fn regenerate_proto() {
        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let repo_root = manifest_dir
            .parent()
            .and_then(|p| p.parent())
            .expect("services/edge-gateway should be two levels under the repo root");
        let out_dir = manifest_dir.join("src/pb_generated");
        std::fs::create_dir_all(&out_dir).unwrap();

        tonic_prost_build::configure()
            .build_server(false)
            .out_dir(&out_dir)
            .compile_protos(
                &[
                    repo_root.join("proto/dataplane/v1/dataplane.proto"),
                    repo_root.join("proto/gateway/v1/gateway.proto"),
                ],
                &[repo_root.join("proto")],
            )
            .expect("proto regeneration failed");
    }
}
