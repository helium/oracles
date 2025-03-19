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
    unlocked_at     TIMESTAMPTZ     DEFAULT NULL
);

-- Add the column claimable to reward_index and default it to the rewards column value
--
-- `reward_index.rewards` tracks the total lifetime earned rewards
-- `reward_index.claimable` tracks the total rewards allowed to be claimed
ALTER TABLE reward_index ADD COLUMN IF NOT EXISTS claimable BIGINT DEFAULT 0;
UPDATE reward_index SET claimable = rewards;