# admin-panel — Phase-05 Step U2. Multi-stage, same shape as every other
# service here: build in a full toolchain image, ship only what runs.
#
# `npm ci`, not `npm install`: package-lock.json is committed, and ci
# installs exactly that lock — a build cannot silently pick up a
# different transitive version than the one tested. Same reasoning as
# go.sum and Cargo.lock elsewhere in this repo.
#
# `COPY . .` rather than a per-directory list: this is the exact gap
# admin-api's own Dockerfile header documents hitting twice (a named
# main.go missing server.go; a per-subdirectory internal/ copy). A
# Next.js app/ tree grows a route directory per section, so an explicit
# list here would be the same trap with more chances to spring it.
# .dockerignore is what keeps node_modules/.next out.
FROM docker.io/library/node:24-bookworm-slim AS builder
WORKDIR /build
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM docker.io/library/node:24-bookworm-slim
WORKDIR /app
ENV NODE_ENV=production
# Next's standalone output already contains the trimmed node_modules it
# needs, so nothing is installed in this stage.
COPY --from=builder /build/.next/standalone ./
COPY --from=builder /build/.next/static ./.next/static
# The .proto is loaded from disk at runtime by @grpc/proto-loader
# (lib/grpc.ts resolves it against process.cwd()), so it must be a real
# file in the runtime image — it is not part of the JS bundle.
COPY --from=builder /build/proto ./proto

# Runs as an unprivileged user. The node image ships a `node` user for
# exactly this; nothing here needs root, and the panel holds admin
# sessions, so it gets the same least-privilege posture as everything
# else in this cluster.
USER node

EXPOSE 3000
ENV PORT=3000 HOSTNAME=0.0.0.0
CMD ["node", "server.js"]
