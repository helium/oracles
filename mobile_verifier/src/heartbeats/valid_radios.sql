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
        AVG(ch.location_trust_score_multiplier) as location_trust_score_multiplier
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
        AVG(location_trust_score_multiplier) as location_trust_score_multiplier
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
            coverage_object
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
            coverage_object
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
    hb.location_trust_score_multiplier,
    u.coverage_object
FROM
    heartbeats hb
    INNER JOIN latest_uuids u ON hb.hotspot_key = u.hotspot_key
        AND (hb.cbsd_id = u.cbsd_id
            OR (hb.cbsd_id IS NULL
                AND u.cbsd_id IS NULL))
WHERE
    hb.heartbeat_multiplier = 1.0
