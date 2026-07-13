-- Cut the gateway service over to the dbt-maintained chain tables
-- (dbt.mobile_gateway_inventory + dbt.mobile_hotspot_history). The S3
-- change-report ingestion and the local `gateways` history table are retired.
DROP TABLE IF EXISTS gateways;

-- Local cache of WiFi deployment info (antenna, elevation), pulled periodically
-- from the on-chain metadata DB by DeploymentInfoTracker. These two fields are
-- absent from the dbt chain tables (only azimuth is on-chain), so they are
-- joined into gateway reads from here instead of hitting the metadata DB on
-- every request. `address` is the base58 helium public key, matching
-- dbt.mobile_gateway_inventory.address.
CREATE TABLE IF NOT EXISTS deployment_info (
    address      TEXT PRIMARY KEY,
    antenna      INTEGER,
    elevation    INTEGER,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
