-- Add a not null constraint to distance_to_asserted. We avoid deleting the
-- null columns before hand so that if there are any, this migration will
-- fail rather than silently deleting data.
ALTER TABLE wifi_heartbeats ALTER COLUMN distance_to_asserted SET NOT NULL;
