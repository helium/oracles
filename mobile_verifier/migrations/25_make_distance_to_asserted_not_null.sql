UPDATE wifi_heartbeats SET distance_to_asserted = 0 WHERE distance_to_asserted IS NULL;
ALTER TABLE wifi_heartbeats ALTER COLUMN distance_to_asserted SET NOT NULL;
