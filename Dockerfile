# BASE
FROM rust:bookworm AS base

RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    cmake \
    mold \
    ca-certificates \
    curl \
    jq # used by polaris-setup.sh

# Used by minio-setup.sh
RUN curl -fsSL https://dl.min.io/client/mc/release/linux-amd64/mc -o /usr/local/bin/mc \
    && chmod +x /usr/local/bin/mc

# BUILDER
FROM base AS builder

WORKDIR /app

COPY . .
RUN cargo fetch --locked

ARG PACKAGE
RUN AWS_LC_SYS_CMAKE_BUILDER=1 cargo build --release -p ${PACKAGE} --locked


# RUNNER
FROM debian:bookworm-slim AS runner

RUN apt-get update && apt-get install -y \
    libssl-dev \
    ca-certificates

ARG PACKAGE

COPY --from=builder /app/target/release/${PACKAGE} /opt/${PACKAGE}/bin/${PACKAGE}

ENV PACKAGE=${PACKAGE}

CMD ["sh", "-c", "/opt/${PACKAGE}/bin/${PACKAGE}", "-c", "/opt/${PACKAGE}/etc/settings.toml", "server"]
