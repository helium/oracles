# BASE
FROM debian:bookworm AS base

RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    build-essential \
    protobuf-compiler \
    pkg-config \
    openssl \
    libssl-dev

COPY rust-toolchain.toml .
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"


# BUILDER
FROM base AS builder

WORKDIR /app

COPY . .
RUN cargo fetch

ARG PACKAGE
RUN cargo build --release -p ${PACKAGE}


# RUNNER
FROM debian:bookworm-slim AS runner

RUN apt-get update && apt-get install -y \
    libssl-dev \
    ca-certificates

ARG PACKAGE

COPY --from=builder /app/target/release/${PACKAGE} /opt/${PACKAGE}/bin/${PACKAGE}

ENV PACKAGE=${PACKAGE}

CMD ["/opt/${PACKAGE}/bin/${PACKAGE}", "-c", "/opt/${PACKAGE}/etc/settings.toml", "server"]