-- 1. Drop the primary key constraint on address
ALTER TABLE
    gateways DROP CONSTRAINT IF EXISTS gateways_pkey;

-- 2. Rename column updated_at -> inserted_at
ALTER TABLE
    gateways RENAME COLUMN updated_at TO inserted_at;

-- 3. Backfill inserted_at with created_at values
UPDATE
    gateways
SET
    inserted_at = created_at;

-- 4. Ensure inserted_at is NOT NULL
ALTER TABLE
    gateways
ALTER COLUMN
    inserted_at
SET
    NOT NULL;

-- 5. Create an index on (address, inserted_at DESC)
CREATE INDEX IF NOT EXISTS gateways_address_inserted_idx ON gateways (address, inserted_at DESC);