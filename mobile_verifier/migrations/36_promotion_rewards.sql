DO $$ BEGIN
CREATE TYPE entity_type AS ENUM (
       'subscriber',
       'gateway'
);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS promotion_rewards (
       time_of_reward TIMESTAMPTZ NOT NULL,
       entity TEXT NOT NULL,
       entity_type entity_type NOT NULL,
       service_provider INTEGER NOT NULL,
       shares BIGINT NOT NULL,
       PRIMARY KEY (time_of_reward, entity, entity_type, service_provider)
);

