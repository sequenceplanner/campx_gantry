# ---- build stage -------------------------------------------------------
FROM rust:1-bookworm AS builder

# micro_sp is declared with an ssh:// URL in Cargo.toml, but the repo is
# public. Rewrite it to https so the build needs no SSH keys or agent.
# insteadOf is only honoured by the git CLI, so force cargo to use it.
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN git config --global url."https://github.com/".insteadOf "ssh://git@github.com/"

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# target/ and the cargo caches live on BuildKit cache mounts, so the binary
# has to be copied out before the mounts go away.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked && \
    cp target/release/gantry /usr/local/bin/gantry

# ---- runtime stage -----------------------------------------------------
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /usr/local/bin/gantry /usr/local/bin/gantry

# The OPC UA client generates a self-signed keypair under ./pki on first
# start; compose mounts a volume here so the cert survives restarts.
RUN mkdir -p /app/pki

ENV RUST_LOG=info
ENTRYPOINT ["gantry"]
