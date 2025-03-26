ALTER TABLE
    IF EXISTS hotspot_data_transfer_sessions
ADD
    COLUMN IF NOT EXISTS rewardable_bytes BIGINT;