CREATE TABLE IF NOT EXISTS subscriber_verified_mapping_event (
    subscriber_id BYTEA NOT NULL,
    total_reward_points INTEGER NOT NULL,
    received_timestamp TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (subscriber_id, received_timestamp)
);