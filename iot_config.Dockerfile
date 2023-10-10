FROM rust:1.70 AS builder

RUN apt-get update && apt-get install -y protobuf-compiler

# Copy cargo file and workspace dependency crates to cache build
COPY Cargo.toml Cargo.lock ./
COPY db_store ./db_store/
COPY file_store ./file_store/
COPY task_manager ./task_manager/
COPY metrics ./metrics/
COPY iot_config/Cargo.toml ./iot_config/Cargo.toml

# Enable sparse registry to avoid crates indexing infinite loop
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

RUN mkdir ./iot_config/src \
 # Create a dummy project file to build deps around
 && echo "fn main() {}" > ./iot_config/src/main.rs \
 # Remove unused members of the workspace to avoid compile error on missing members
 && sed -i -e '/ingest/d'              -e '/mobile_config/d'    -e '/mobile_verifier/d' \
           -e '/poc_entropy/d'         -e '/iot_verifier/d'     -e '/price/d' \
           -e '/reward_index/d'        -e '/reward_scheduler/d' -e '/denylist/d' \
           -e '/iot_packet_verifier/d' -e '/solana/d'           -e '/mobile_packet_verifier/d' \
           -e '/mobile_config_cli/d' \
           Cargo.toml \
 && cargo build --package iot-config --release

COPY iot_config ./iot_config/
RUN cargo build --package iot-config --release

FROM debian:bullseye-slim

COPY --from=builder ./target/release/iot-config /opt/iot_config/bin/iot-config

EXPOSE 8080

CMD ["/opt/iot_config/bin/iot-config", "server"]
