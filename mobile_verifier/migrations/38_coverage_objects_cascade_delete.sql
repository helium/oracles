ALTER TABLE
    hexes DROP CONSTRAINT IF EXISTS hexes_uuid_fkey;

ALTER TABLE
    hexes
ADD
    CONSTRAINT hexes_uuid_fkey FOREIGN KEY (uuid) REFERENCES coverage_objects(uuid) ON DELETE CASCADE;