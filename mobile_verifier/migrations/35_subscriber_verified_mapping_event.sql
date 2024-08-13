CREATE TABLE IF NOT EXISTS subscriber_verified_mapping_event (
    subscriber_id BYTEA NOT NULL,
    total_reward_points BIGINT NOT NULL,
    received_timestamp TIMESTAMPTZ NOT NULL,
    inserted_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (subscriber_id, received_timestamp)
);
