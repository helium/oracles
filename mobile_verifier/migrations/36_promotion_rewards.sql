CREATE TABLE IF NOT EXISTS subscriber_promotion_rewards (
       time_of_reward TIMESTAMPTZ NOT NULL,
       subscriber_id BYTEA NOT NULL,
       carrier_key NOT NULL,
       shares BIGINT NOT NULL,
       PRIMARY KEY (time_of_reward, subscriber_id, carrier_key)
);

CREATE TABLE IF NOT EXISTS gateway_promotion_rewads (
       time_of_reward TIMESTAMPTZ NOT NULL,
       gateway_key TEXT NOT NULL,
       carrier_key TEXT NOT NULL,
       shares BIGINT NOT NULL,
       PRIMARY KEY (time_of_reward, gateway_key, carrier_key) 
);
