FROM rust:latest AS deps
# apt stuff
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    protobuf-compiler \
    pkg-config \
    openssl \
    libssl-dev
# Copy deps
COPY custom_tracing/    ./custom_tracing/
COPY db_store/          ./db_store/
COPY file_store/        ./file_store/
COPY metrics/           ./metrics/
COPY price/             ./price/
COPY reward_scheduler/  ./reward_scheduler/
COPY solana/            ./solana/
COPY task_manager/      ./task_manager/
COPY Cargo.toml         Cargo.toml
COPY Cargo.lock         Cargo.lock
# Copy service/binary cargo files for stub builds
COPY ingest/Cargo.toml                  ./ingest/Cargo.toml
COPY mobile_config/Cargo.toml           ./mobile_config/Cargo.toml
COPY mobile_packet_verifier/Cargo.toml  ./mobile_packet_verifier/Cargo.toml
COPY mobile_verifier/Cargo.toml         ./mobile_verifier/Cargo.toml
COPY reward_index/Cargo.toml            ./reward_index/Cargo.toml

# Create a dummy project files to build deps around
RUN bash -c 'mkdir {ingest,mobile_config,mobile_packet_verifier,mobile_verifier,reward_index}/src' \
    && echo "fn main() {}" > ./ingest/src/main.rs \
    && echo "fn main() {}" > ./mobile_config/src/main.rs \
    && echo "fn main() {}" > ./mobile_packet_verifier/src/main.rs \
    && echo "fn main() {}" > ./mobile_verifier/src/main.rs \
    && echo "fn main() {}" > ./reward_index/src/main.rs \
    # Remove unused workspace members to avoid compile error on missing members
    && sed -i \
    -e '/boost_manager/d' \
    -e '/denylist/d' \
    -e '/iot_config/d' \
    -e '/iot_packet_verifier/d' \
    -e '/iot_verifier/d' \
    -e '/poc_entropy/d' \
    -e '/mobile_config_cli/d' \
    -e '/test_mobile/d' \
    Cargo.toml
# Enable sparse registry to avoid crates indexing infinite loop
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
# Build deps releases
RUN cargo build --release

# Build ingest release
FROM deps as builder-ingest
COPY ingest ./ingest/
RUN cargo build --package ingest --release

# Build the mobile-config release
FROM deps as builder-mobile_config
COPY mobile_config ./mobile_config/
RUN cargo build --package mobile-config --release

# Build mobile-packet-verifier release
FROM builder-mobile_config as builder-mobile_packet_verifier
COPY mobile_packet_verifier ./mobile_packet_verifier/
RUN cargo build --package mobile-packet-verifier --release

# Build mobile-verifier release
FROM builder-mobile_config as builder-mobile_verifier
COPY mobile_verifier ./mobile_verifier/
RUN cargo build --package mobile-verifier --release

# Build reward-index release
FROM deps as builder-reward_index
COPY reward_index ./reward_index/
RUN cargo build --package reward-index --release

# Package Runners
FROM debian:bookworm-slim AS runner
COPY --from=builder-ingest                  ./target/release/ingest                 /tmp/ingest
COPY --from=builder-mobile_config           ./target/release/mobile-config          /tmp/mobile-config
COPY --from=builder-mobile_packet_verifier  ./target/release/mobile-packet-verifier /tmp/mobile-packet-verifier
COPY --from=builder-mobile_verifier         ./target/release/mobile-verifier        /tmp/mobile-verifier
COPY --from=builder-reward_index            ./target/release/reward-index           /tmp/reward-index
COPY --from=deps                            ./target/release/price                  /tmp/price

# Runners
FROM debian:bookworm-slim
ARG PACKAGE
RUN apt-get update && apt-get install -y \
    libssl-dev \
    ca-certificates
# $PACKAGE via build args
COPY --from=runner /tmp/$PACKAGE /opt/$PACKAGE/bin/$PACKAGE
# $PACKAGE via environment
CMD /opt/$PACKAGE/bin/$PACKAGE -c /opt/$PACKAGE/etc/settings.toml server