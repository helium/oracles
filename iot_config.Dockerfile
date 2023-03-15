FROM rust:1.67 AS builder

RUN apt-get update && apt-get install -y protobuf-compiler
RUN rustup toolchain install nightly

# Copy cargo file and workspace dependency crates to cache build
COPY Cargo.toml Cargo.lock ./
COPY db_store ./db_store/
COPY file_store ./file_store/
COPY metrics ./metrics/
COPY node_follower /node_follower/
COPY iot_config/Cargo.toml ./iot_config/Cargo.toml

RUN mkdir ./iot_config/src \
 # Create a dummy project file to build deps around
 && echo "fn main() {}" > ./iot_config/src/main.rs \
 # Remove unused members of the workspace to avoid compile error on missing members
 && sed -i -e '/ingest/d'       -e '/mobile_rewards/d' -e '/mobile_verifier/d' \
           -e '/poc_entropy/d'  -e '/iot_verifier/d'   -e '/poc_iot_injector/d' \
           -e '/reward_index/d' -e '/denylist/d'       -e '/iot_packet_verifier/d' \
           -e '/price/d'
           Cargo.toml \
 # Build on nightly cargo to use sparse-registry to avoid crates indexing infinite loop
 && cargo +nightly build --package iot-config --release -Z sparse-registry

COPY iot_config ./iot_config/
RUN cargo +nightly build --package iot-config --release -Z sparse-registry

FROM debian:bullseye-slim

COPY --from=builder ./target/release/iot-config /opt/iot_config/bin/iot-config

EXPOSE 8080

CMD ["/opt/iot_config/bin/iot-config", "server"]
