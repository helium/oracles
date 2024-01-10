ALTER TABLE wifi_heartbeats ADD COLUMN location_trust_score_multiplier DECIMAL;

UPDATE TABLE wifi_heartbeats SET location_trust_score_multiplier =
       CASE WHEN location_validation_timestamp IS NULL THEN
       	    0.25
       WHEN distance_to_asserted > 100 THEN
       	    0.25
       ELSE
            1.0
       END;

ALTER TABLE wifi_heartbeats SET location_trust_score_multiplier NOT NULL;

ALTER TABLE cbrs_heartbeats ADD COLUMN location_trust_score_multiplier DECIMAL;

UPDATE TABLE cbrs_heartbeats SET location_trust_score_multiplier = 1.0;

ALTER TABLE cbrs_heartbeats SET location_trust_score_multiplier NOT NULL;
