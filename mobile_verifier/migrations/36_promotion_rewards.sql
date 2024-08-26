CREATE TABLE IF NOT EXISTS promotion_rewards (
       time_of_reward TIMESTAMPTZ NOT NULL,
       subscriber_id BYTEA NOT NULL,
       gateway_key TEXT NOT NULL,
       service_provider BIGINT NOT NULL,
       shares BIGINT NOT NULL,
       PRIMARY KEY (time_of_reward, subscriber_id, gateway_key, service_provider)
);
