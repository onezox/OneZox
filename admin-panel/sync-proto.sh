#!/usr/bin/env bash
# Re-copies proto/admin/v1/admin.proto from the repo root into this
# service's own tree.
#
# admin-panel loads the .proto at RUNTIME (@grpc/proto-loader, lib/grpc.ts)
# rather than generating TypeScript stubs from it — so unlike the Go and
# Rust services, what has to be committed here is the .proto file itself,
# not generated code. Same underlying convention either way: this
# service's Docker build context is scoped to admin-panel/ and cannot
# reach proto/ at the repo root, so the contract is vendored in and
# refreshed by this script when admin.proto changes.
#
# Runtime loading rather than codegen is deliberate for this one service:
# the panel calls exactly six unary RPCs, and a generated-types toolchain
# (ts-proto/buf + its plugin set) would be a substantially larger
# dependency surface than the contract it protects. Every request/response
# crossing this boundary is still shaped by lib/grpc.ts's own explicit
# TypeScript interfaces, so the call sites remain typed.
set -euo pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"  # repo root

mkdir -p admin-panel/proto/admin/v1
cp proto/admin/v1/admin.proto admin-panel/proto/admin/v1/admin.proto

echo "Synced admin-panel/proto/admin/v1/admin.proto from proto/admin/v1/"
