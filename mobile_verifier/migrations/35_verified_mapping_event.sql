CREATE TABLE IF NOT EXISTS verified_mapping_event (
    subscriber_id TEXT NOT NULL,
    total_reward_points INTEGER NOT NULL,
    timestamp TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (subscriber_id, timestamp)
);