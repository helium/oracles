ALTER TABLE hexes ADD COLUMN IF NOT EXISTS service_provider_selected oracle_assignment;
ALTER TYPE data_set_type ADD VALUE IF NOT EXISTS 'service_provider_selected';
