CREATE TABLE IF NOT EXISTS promotion_reward (
       time_of_reward TIMESTAMPTZ NOT NULL,
       subscriber_id BYTEA NOT NULL,
       gateway_key TEXT NOT NULL,
       service_provider BIGINT NOT NULL,
       shares BIGINT NOT NULL,
       PRIMARY KEY (time_of_reward, subscriber_id, gateway_key, carrier_key)
);
