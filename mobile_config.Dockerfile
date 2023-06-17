FROM rust:1.70 as builder

RUN apt-get update && apt-get install -y protobuf-compiler

# Copy cargo file and workspace dependency crates to cache build
COPY Cargo.toml Cargo.lock ./
COPY db_store ./db_store/
COPY file_store ./file_store/
COPY metrics ./metrics/
COPY mobile_config/Cargo.toml ./mobile_config/Cargo.toml

# Enable sparse registry to avoid crates indexing infinite loop
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

RUN mkdir ./mobile_config/src \
 # Create a dummy project file to build deps around
 && echo "fn main() {}" > ./mobile_config/src/main.rs \
 && sed -i -e '/ingest/d'              -e '/iot_config/d'       -e '/mobile_verifier/d' \
           -e '/poc_entropy/d'         -e '/iot_verifier/d'     -e '/price/d' \
           -e '/reward_index/d'        -e '/reward_scheduler/d' -e '/denylist/d' \
           -e '/iot_packet_verifier/d' -e '/solana/d'           -e '/mobile_packet_verifier/d' \
           -e '/mobile_config_cli/d' \
           Cargo.toml \
  && cargo build --package mobile-config --release

COPY mobile_config ./mobile_config/
RUN cargo build --package mobile-config --release

FROM debian:bullseye-slim

COPY --from=builder ./target/release/mobile-config /opt/mobile_config/bin/mobile-config

EXPOSE 8090

CMD ["/opt/mobile_config/bin/mobile-config", "server"]
