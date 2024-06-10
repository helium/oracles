FROM rust:latest AS builder

RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    protobuf-compiler \
    pkg-config \
    openssl \
    libssl-dev

# Copy Deps
COPY custom_tracing    custom_tracing/
COPY db_store          db_store/
COPY file_store        file_store/
COPY hex_assignments   hex_assignments/
COPY metrics           metrics/
COPY price             price/
COPY reward_scheduler  reward_scheduler/
COPY solana            solana/
COPY task_manager      task_manager/
COPY Cargo.toml        Cargo.toml
COPY Cargo.lock        Cargo.lock

# Copy mobile pakages
COPY ingest                 ingest/
COPY mobile_config          mobile_config/
COPY mobile_packet_verifier mobile_packet_verifier/
COPY mobile_verifier        mobile_verifier/
COPY reward_index           reward_index/

# Remove useless packages from toml
RUN sed -i \
    -e '/denylist/d' \
    -e '/iot_config/d' \
    -e '/iot_packet_verifier/d' \
    -e '/iot_verifier/d' \
    -e '/poc_entropy/d' \
    -e '/mobile_config_cli/d' \
    -e '/boost_manager/d' \
    -e '/test_mobile/d' \
    Cargo.toml

# Build releases
RUN cargo build --features "ingest/mobile-test,mobile-verifier/mobile-test" --release

# Package Runners
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    libssl-dev \
    ca-certificates

ARG PACKAGE

# $PACKAGE via build args
COPY --from=builder ./target/release/$PACKAGE /opt/$PACKAGE/bin/$PACKAGE

ENV PACKAGE=${PACKAGE}

CMD /opt/$PACKAGE/bin/$PACKAGE -c /opt/$PACKAGE/etc/settings.toml server