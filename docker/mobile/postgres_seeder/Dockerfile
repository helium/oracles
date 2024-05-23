FROM rust:latest

ENV DB_URL=postgres://postgres:postgres@postgres:5432

RUN cargo install sqlx-cli --no-default-features --features native-tls,postgres

COPY ./mobile_config/migrations /mobile_config/migrations
COPY ./mobile_packet_verifier/migrations /mobile_packet_verifier/migrations
COPY ./mobile_verifier/migrations /mobile_verifier/migrations
COPY ./reward_index/migrations /reward_index/migrations

COPY ./docker/mobile/postgres_seeder/mobile_config_pubkeys.sql /mobile_config/migrations/20240522174900_mobile_config_pubkeys.sql

COPY ./docker/mobile/postgres_seeder/run.sh /run.sh

CMD ["/run.sh"]