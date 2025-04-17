CREATE TABLE IF NOT EXISTS subscriber_mapping_activity (
    subscriber_id BYTEA NOT NULL,
    discovery_reward_shares BIGINT NOT NULL,
    verification_reward_shares BIGINT NOT NULL,
    received_timestamp TIMESTAMPTZ NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (subscriber_id, received_timestamp)
);

INSERT INTO subscriber_mapping_activity(subscriber_id, discovery_reward_shares, verification_reward_shares, received_timestamp, inserted_at)
SELECT subscriber_id, 30, 0, received_timestamp, created_at AS inserted_at
FROM subscriber_loc_verified;

UPDATE subscriber_mapping_activity sma
SET verification_reward_shares = svme.total_reward_points
FROM subscriber_verified_mapping_event svme 
WHERE sma.subscriber_id = svme.subscriber_id 
	AND sma.received_timestamp::date = svme.received_timestamp::date;
