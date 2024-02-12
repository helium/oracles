ALTER TABLE cbrs_heartbeats ADD COLUMN asserted_hex BIGINT;
ALTER TABLE cbrs_heartbeats ADD COLUMN distance_to_asserted BIGINT;
ALTER TABLE wifi_heartbeats ADD COLUMN asserted_hex BIGINT;
