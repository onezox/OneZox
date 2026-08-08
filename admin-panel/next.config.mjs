/**
 * admin-panel — Next.js config (Phase-05 Step U2).
 *
 * output: 'standalone' produces a self-contained server bundle plus a
 * minimal node_modules subset, so the runtime Docker stage copies a
 * built artifact instead of installing dependencies again — the same
 * "build it once, ship only what runs" shape every other service's
 * multi-stage Dockerfile in this project already uses.
 *
 * serverExternalPackages: @grpc/grpc-js and @grpc/proto-loader load
 * native/dynamic resources at runtime (proto files read from disk) and
 * must not be bundled — they stay real node_modules requires. This is
 * what makes the server-side gRPC path work at all in a standalone
 * build.
 *
 * poweredByHeader off: an operator console shouldn't advertise its
 * framework, the same reflex as every other service here declining to
 * leak internals over the wire.
 */
/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'standalone',
  poweredByHeader: false,
  serverExternalPackages: ['@grpc/grpc-js', '@grpc/proto-loader'],
};

export default nextConfig;
