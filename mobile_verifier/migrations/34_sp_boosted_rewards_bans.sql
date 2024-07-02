CREATE TABLE IF NOT EXISTS sp_boosted_rewards_bans (
    radio_type radio_type NOT NULL,
    radio_key TEXT NOT NULL,
    received_timestamp TIMESTAMPTZ NOT NULL,
    until TIMESTAMPTZ NOT NULL,
    invalidated_at TIMESTAMPTZ,
    PRIMARY KEY (radio_type, radio_key)
);
