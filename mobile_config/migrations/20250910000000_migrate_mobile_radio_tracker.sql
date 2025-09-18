INSERT INTO
    gateways (
        address,
        gateway_type,
        created_at,
        updated_at,
        refreshed_at,
        last_changed_at,
        hash,
        location,
        location_changed_at
    )
SELECT
    mrt.entity_key AS address,
    'wifiDataOnly' :: gateway_type AS gateway_type,
    mrt.last_changed_at AS created_at,
    mrt.last_changed_at AS updated_at,
    mrt.last_checked_at AS refreshed_at,
    mrt.last_changed_at AS last_changed_at,
    mrt.hash AS hash,
    mrt.asserted_location AS location,
    mrt.asserted_location_changed_at AS location_changed_at
FROM
    mobile_radio_tracker AS mrt;