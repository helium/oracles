WITH heartbeats AS (
    SELECT
        hotspot_key,
        cell_type,
        CASE WHEN count(*) >= $3 THEN
            1.0
        ELSE
            0.0
        END AS heartbeat_multiplier,
        ARRAY_AGG(distance_to_asserted ORDER BY truncated_timestamp) as distances_to_asserted,
        ARRAY_AGG(location_trust_score_multiplier ORDER BY truncated_timestamp) as trust_score_multipliers
    FROM
        wifi_heartbeats
    WHERE
        truncated_timestamp >= $1
        AND truncated_timestamp < $2
    GROUP BY
        hotspot_key,
        cell_type
),
latest_uuids AS (
    SELECT DISTINCT ON (hotspot_key)
        hotspot_key,
        coverage_object
    FROM
        wifi_heartbeats wh
    WHERE
        truncated_timestamp >= $1
        AND truncated_timestamp < $2
    ORDER BY
        hotspot_key,
        truncated_timestamp DESC
)
SELECT
    hb.hotspot_key,
    hb.cell_type,
    hb.distances_to_asserted,
    hb.trust_score_multipliers,
    u.coverage_object
FROM
    heartbeats hb
    INNER JOIN latest_uuids u ON hb.hotspot_key = u.hotspot_key
WHERE
    hb.heartbeat_multiplier = 1.0
