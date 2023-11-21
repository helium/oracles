WITH cbrs_coverage_objs AS (
    SELECT
        t1.cbsd_id,
        t1.coverage_object,
        t1.latest_timestamp
    FROM
        cbrs_heartbeats t1
    WHERE
        t1.latest_timestamp = (
            SELECT
                MAX(t2.latest_timestamp)
            FROM
                cbrs_heartbeats t2
            WHERE
                t2.cbsd_id = t1.cbsd_id
                AND truncated_timestamp >= $1
                AND truncated_timestamp < $2)
),
wifi_coverage_objs AS (
    SELECT
        t1.hotspot_key,
        t1.coverage_object,
        t1.latest_timestamp
    FROM
        wifi_heartbeats t1
    WHERE
        t1.latest_timestamp = (
            SELECT
                MAX(t2.latest_timestamp)
            FROM
                wifi_heartbeats t2
            WHERE
                t2.hotspot_key = t1.hotspot_key
                AND truncated_timestamp >= $1
                AND truncated_timestamp < $2)
),
latest_hotspots AS (
    SELECT
        t1.cbsd_id,
        t1.hotspot_key,
        t1.latest_timestamp
    FROM
        cbrs_heartbeats t1
    WHERE
        t1.latest_timestamp = (
            SELECT
                MAX(t2.latest_timestamp)
            FROM
                cbrs_heartbeats t2
            WHERE
                t2.cbsd_id = t1.cbsd_id
                AND truncated_timestamp >= $1
                AND truncated_timestamp < $2))
SELECT
    latest_hotspots.hotspot_key,
    cbrs_heartbeats.cbsd_id,
    cell_type,
    cbrs_coverage_objs.coverage_object,
    cbrs_coverage_objs.latest_timestamp,
    NULL AS location_validation_timestamp,
    NULL AS distance_to_asserted
FROM
    cbrs_heartbeats
    LEFT JOIN latest_hotspots ON cbrs_heartbeats.cbsd_id = latest_hotspots.cbsd_id
    LEFT JOIN cbrs_coverage_objs ON cbrs_heartbeats.cbsd_id = cbrs_coverage_objs.cbsd_id
WHERE
    truncated_timestamp >= $1
    AND truncated_timestamp < $2
GROUP BY
    cbrs_heartbeats.cbsd_id,
    latest_hotspots.hotspot_key,
    cell_type,
    cbrs_coverage_objs.coverage_object,
    cbrs_coverage_objs.latest_timestamp
HAVING
    count(*) >= $3
UNION ALL
SELECT
    wifi_grouped.hotspot_key,
    NULL AS cbsd_id,
    cell_type,
    wifi_coverage_objs.coverage_object,
    wifi_coverage_objs.latest_timestamp,
    b.location_validation_timestamp,
    b.distance_to_asserted
FROM (
    SELECT
        hotspot_key,
        cell_type
    FROM
        wifi_heartbeats
    WHERE
        truncated_timestamp >= $1
        AND truncated_timestamp < $2
    GROUP BY
        hotspot_key,
        cell_type
    HAVING
        count(*) >= $3) AS wifi_grouped
    LEFT JOIN (
        SELECT
            hotspot_key,
            location_validation_timestamp,
            distance_to_asserted
        FROM
            wifi_heartbeats
        WHERE
            wifi_heartbeats.truncated_timestamp >= $1
            AND wifi_heartbeats.truncated_timestamp < $2) AS b ON b.hotspot_key = wifi_grouped.hotspot_key
    LEFT JOIN wifi_coverage_objs ON wifi_grouped.hotspot_key = wifi_coverage_objs.hotspot_key
