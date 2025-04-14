-- Add burn_timestamp
ALTER TABLE
    hotspot_data_transfer_sessions
ADD
    COLUMN IF NOT EXISTS burn_timestamp TIMESTAMPTZ;

-- Update burn_timestamp to be equal to received_timestamp for old records
UPDATE
    hotspot_data_transfer_sessions
SET
    burn_timestamp = received_timestamp
WHERE
    burn_timestamp IS NULL;

-- Make burn_timestamp a NOT NULL value so it can be a primary key
ALTER TABLE
    hotspot_data_transfer_sessions
ALTER COLUMN
    burn_timestamp
SET
    NOT NULL;

-- Add primary key on pub_key, payer, burn_timestamp
ALTER TABLE
    hotspot_data_transfer_sessions
ADD
    CONSTRAINT hotspot_data_transfer_sessions_pk PRIMARY KEY (pub_key, payer, burn_timestamp);