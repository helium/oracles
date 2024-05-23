FROM rust:latest

ENV DB_URL=postgres://postgres:postgres@postgres:5432

RUN apt-get update && apt-get install -y postgresql


RUN cargo install sqlx-cli --no-default-features --features native-tls,postgres

COPY ./mobile_config/migrations /mobile_config/migrations
COPY ./mobile_packet_verifier/migrations /mobile_packet_verifier/migrations
COPY ./mobile_verifier/migrations /mobile_verifier/migrations
COPY ./reward_index/migrations /reward_index/migrations

COPY ./docker/mobile/postgres_seeder/mobile_config /postgres_seeder/mobile_config

COPY ./docker/mobile/postgres_seeder/run.sh /run.sh

CMD ["/run.sh"]