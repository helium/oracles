WITH latest_cbrs_hotspot AS (
    SELECT DISTINCT ON (cbsd_id)
        cbsd_id,
        hotspot_key
    FROM
        cbrs_heartbeats
    WHERE
        truncated_timestamp >= $1
        AND truncated_timestamp < $2
    ORDER BY
        cbsd_id,
        latest_timestamp DESC
),
heartbeats AS (
    SELECT
        lch.hotspot_key,
        ch.cbsd_id,
        ch.cell_type,
        CASE WHEN count(*) >= $3 THEN
            1.0
        ELSE
            0.0
        END AS heartbeat_multiplier,
        1.0 AS location_trust_multiplier
    FROM
        cbrs_heartbeats ch
        INNER JOIN latest_cbrs_hotspot lch ON ch.cbsd_id = lch.cbsd_id
    WHERE
        ch.truncated_timestamp >= $1
        AND ch.truncated_timestamp < $2
    GROUP BY
        ch.cbsd_id,
        lch.hotspot_key,
        ch.cell_type
    UNION
    SELECT
        hotspot_key,
        NULL AS cbsd_id,
        cell_type,
        CASE WHEN count(*) >= $3 THEN
            1.0
        ELSE
            0.0
        END AS heartbeat_multiplier,
        avg(
            CASE WHEN location_validation_timestamp IS NULL THEN
                0.25
            WHEN distance_to_asserted > $4 THEN
                0.25
            ELSE
                1.0
            END) AS location_trust_multiplier
FROM
    wifi_heartbeats
    WHERE
        truncated_timestamp >= $1
        AND truncated_timestamp < $2
    GROUP BY
        hotspot_key,
        cell_type
),
latest_uuids AS (( SELECT DISTINCT ON (hotspot_key,
            cbsd_id)
            hotspot_key,
            cbsd_id,
            coverage_object AS uuid
        FROM
            cbrs_heartbeats ch
        WHERE
            truncated_timestamp >= $1
            AND truncated_timestamp < $2
        ORDER BY
            hotspot_key,
            cbsd_id,
            truncated_timestamp DESC)
    UNION ( SELECT DISTINCT ON (hotspot_key)
            hotspot_key,
            NULL AS cbsd_id,
            coverage_object AS uuid
        FROM
            wifi_heartbeats wh
        WHERE
            truncated_timestamp >= $1
            AND truncated_timestamp < $2
        ORDER BY
            hotspot_key,
            truncated_timestamp DESC))
SELECT
    hb.hotspot_key,
    hb.cbsd_id,
    hb.cell_type,
    hb.location_trust_multiplier,
    u.uuid AS coverage_object
FROM
    heartbeats hb
    INNER JOIN latest_uuids u ON hb.hotspot_key = u.hotspot_key
        AND (hb.cbsd_id = u.cbsd_id or (hb.cbsd_id is null and u.cbsd_id is null))
WHERE
    hb.heartbeat_multiplier = 1.0
