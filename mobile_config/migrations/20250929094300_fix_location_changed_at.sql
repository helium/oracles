UPDATE
    gateways
SET
    location_changed_at = CASE
        WHEN location_asserts = 1 THEN created_at
        WHEN location_asserts > 1 THEN refreshed_at
        ELSE location_changed_at
    END
WHERE
    location_changed_at IS NULL
    AND location_asserts IS NOT NULL;