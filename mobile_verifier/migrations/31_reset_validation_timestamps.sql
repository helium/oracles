UPDATE wifi_heartbeats SET location_validation_timestamp = NULL, lat = 0.0, lon = 0.0
WHERE distance_to_asserted > 200;
