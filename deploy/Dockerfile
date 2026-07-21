# Multi-stage build of the `growlerdb` binary (control-plane / node / gateway — one binary,
# selected by subcommand). Build context is the repo root; see .dockerignore.
# Bases pinned by digest so the released, signed image is byte-reproducible; tag comments track version.
# trixie (glibc 2.41), not bookworm (2.36): pyke's prebuilt ONNX Runtime 1.24 references glibc-2.38+
# symbols (e.g. __isoc23_strtoull), so bookworm can't link/run it. The runtime base is bumped to match.
FROM rust:1-trixie@sha256:9a2cd304a852f05d3352f75bc2775242371c0169a72dbb40d5d881379d571989 AS build
# protoc is required by tonic-build/prost-build (the gRPC wire layer).
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
# Release version baked into the binary's `--version`. Passed by release.yml from the git
# tag; empty for a local `docker build`, where build.rs falls back to the in-tree 0.0.0.
ARG GROWLERDB_VERSION
# BuildKit cache mounts persist the cargo registry/git + the target/ dir ACROSS image builds, so a
# rebuild after a source change recompiles only the changed crates (incremental) instead of every
# dependency from scratch — turning a multi-minute rebuild into seconds in the dev loop.
# We build the lean `dist` profile (release minus thin-LTO, stripped): a fraction of the disk +
# wall-time of a full release build, which is what overflowed small dev VMs. The caches still live
# in Docker's (VM) storage — reclaim with `docker builder prune` if space gets tight.
# target/ is a cache mount (not part of the image), so copy the binary out within the same RUN.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    RUSTFLAGS="-C link-arg=-lstdc++" \
    GROWLERDB_VERSION="${GROWLERDB_VERSION}" cargo build --profile dist -p growlerdb-cli \
    && cp target/dist/growlerdb /usr/local/bin/growlerdb

# Build the UI SPA — the gateway serves it via --ui-dir.
FROM node:22-bookworm-slim@sha256:53ada149d435c38b14476cb57e4a7da73c15595aba79bd6971b547ceb6d018bf AS ui-build
WORKDIR /ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci
COPY ui/ ./
RUN npm run build

FROM debian:trixie-slim@sha256:020c0d20b9880058cbe785a9db107156c3c75c2ac944a6aa7ab59f2add76a7bd AS runtime
# ca-certificates for TLS to the catalog/object store; curl for the container healthcheck;
# libstdc++6 for the statically-linked ONNX Runtime (its C++ stdlib refs are linked dynamically).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -u 10001 -m growlerdb
COPY --from=build /usr/local/bin/growlerdb /usr/local/bin/growlerdb
# The built UI SPA; the gateway serves it with `--ui-dir /usr/share/growlerdb/ui`.
COPY --from=ui-build /ui/dist /usr/share/growlerdb/ui
USER growlerdb
# Local index store lives here; mount a volume for persistence.
ENV GROWLERDB_DATA_DIR=/data
WORKDIR /data
ENTRYPOINT ["growlerdb"]
