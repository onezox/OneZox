//! Generated protobuf message types (build.rs, prost-build) from
//! proto/gateway/v1/gateway.proto and proto/dataplane/v1/dataplane.proto.

pub mod gateway {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/gateway.v1.rs"));
    }
}

pub mod dataplane {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/dataplane.v1.rs"));
    }
}
