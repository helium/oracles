CREATE TABLE IF NOT EXISTS escrow_durations (
    address         TEXT            PRIMARY KEY NOT NULL,
    duration_days   BIGINT          NOT NULL,
    inserted_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    expires_on      DATE
);

CREATE TABLE IF NOT EXISTS escrow_rewards (
    address         TEXT            NOT NULL,
    amount          BIGINT          NOT NULL DEFAULT 0,
    inserted_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    reward_type     reward_type     NOT NULL,
    unlocked        BOOLEAN         NOT NULL DEFAULT FALSE
);
