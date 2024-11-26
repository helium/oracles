CREATE TABLE IF NOT EXISTS unique_connections (
    hotspot_pubkey      TEXT            NOT NULL,
    unique_connections  BIGINT          NOT NULL,
    start_timestamp     TIMESTAMPTZ     NOT NULL,
    end_timestamp       TIMESTAMPTZ     NOT NULL,
    recv_timestamp      TIMESTAMPTZ     NOT NULL
);