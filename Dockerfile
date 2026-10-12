ARG ROS_DISTRO=lyrical

# ---- build stage -------------------------------------------------------
FROM ros:${ROS_DISTRO}-ros-base AS builder

# clang/libclang are for r2r's bindgen step.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential ca-certificates clang curl git libclang-dev \
        libssl-dev pkg-config && \
    rm -rf /var/lib/apt/lists/*

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
        sh -s -- -y --default-toolchain 1.97.1 --profile minimal --no-modify-path

# micro_sp and r2r are declared with ssh:// URLs in Cargo.toml, but both repos
# are public. Rewrite them to https so the build needs no SSH keys or agent.
# insteadOf is only honoured by the git CLI, so force cargo to use it.
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN git config --global --add url."https://github.com/".insteadOf "ssh://git@github.com/" && \
    git config --global --add url."https://github.com/".insteadOf "git@github.com:"

# Bump to force everything below to rebuild, while keeping the toolchain
# layers above: --build-arg CACHE_BUST=$(date +%s)
ARG CACHE_BUST=0

# The action interface is a regular ROS package; r2r generates its Rust types
# from whatever is sourced, so it has to be built and sourced before cargo runs.
WORKDIR /build
COPY interfaces ./interfaces
RUN . "/opt/ros/$ROS_DISTRO/setup.sh" && \
    colcon build --base-paths interfaces --merge-install \
        --install-base /opt/gantry_interfaces

COPY Cargo.toml Cargo.lock ./
COPY src ./src

# target/ and the cargo caches live on BuildKit cache mounts, so the binaries
# have to be copied out before the mounts go away.
# r2r only regenerates its message types when the ROS env vars change, not when
# an interface file does, so drop its cached bindings whenever interfaces/ has
# changed since the last build.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    . "/opt/ros/$ROS_DISTRO/setup.sh" && \
    . /opt/gantry_interfaces/setup.sh && \
    stamp=$(find interfaces -type f | sort | xargs sha256sum | sha256sum) && \
    if [ "$stamp" != "$(cat target/.interfaces.sha 2>/dev/null)" ]; then \
        cargo clean --release -p r2r -p r2r_msg_gen; \
    fi && \
    cargo build --release --locked && \
    echo "$stamp" > target/.interfaces.sha && \
    cp target/release/gantry target/release/ros /usr/local/bin/

# ---- runtime stage -----------------------------------------------------
FROM ros:${ROS_DISTRO}-ros-base

WORKDIR /app
COPY --from=builder /opt/gantry_interfaces /opt/gantry_interfaces
COPY --from=builder /usr/local/bin/gantry /usr/local/bin/ros /usr/local/bin/
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh

# The OPC UA client generates a self-signed keypair under ./pki on first
# start; compose mounts a volume here so the cert survives restarts.
RUN mkdir -p /app/pki

ENV RUST_LOG=info
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["gantry"]
