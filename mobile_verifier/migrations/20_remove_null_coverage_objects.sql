-- Remove NULL and invalid coverage objects from WiFi heartbeats:
DELETE FROM wifi_heartbeats WHERE coverage_object IS NULL;
DELETE FROM wifi_heartbeats WHERE coverage_object NOT IN (SELECT uuid FROM hex_coverage);

-- Remove NULL and invalid coverage objects from CBRS heartbeats:
DELETE FROM cbrs_heartbeats WHERE coverage_object IS NULL;
DELETE FROM cbrs_heartbeats WHERE coverage_object NOT IN (SELECT uuid FROM hex_coverage);

-- Add the NOT NULL constraint to coverage objects for wifi and cbrs heartbeats:
ALTER TABLE wifi_heartbeats ALTER COLUMN coverage_object SET NOT NULL;
ALTER TABLE cbrs_heartbeats ALTER COLUMN coverage_object SET NOT NULL;
