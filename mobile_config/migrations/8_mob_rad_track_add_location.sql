ALTER TABLE mobile_radio_tracker 
ADD COLUMN asserted_location BIGINT,
ADD COLUMN asserted_location_changed_at TIMESTAMPTZ;
