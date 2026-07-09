-- Coverage-object ingestion has been removed and heartbeats are no longer
-- validated against a coverage object, so a valid heartbeat may not carry a
-- coverage_object uuid. Allow the column to be null.
ALTER TABLE wifi_heartbeats ALTER COLUMN coverage_object DROP NOT NULL;
