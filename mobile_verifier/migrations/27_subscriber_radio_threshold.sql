CREATE TABLE IF NOT EXISTS radio_threshold (
  id SERIAL PRIMARY KEY,
  hotspot_pubkey TEXT NOT NULL,
  cbsd_id TEXT NULL,
  bytes_threshold BIGINT NOT NULL,
  subscriber_threshold INT NOT NULL,
  threshold_timestamp TIMESTAMPTZ NOT NULL,
  recv_timestamp TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS radio_threshold_hotspot_pubkey_cbsd_id_idx ON radio_threshold (hotspot_pubkey, cbsd_id);

-- temp table for grandfathered radio thresholds
CREATE TABLE IF NOT EXISTS grandfathered_radio_thresholds (
  id SERIAL PRIMARY KEY,
  hotspot_pubkey TEXT NOT NULL,
  cbsd_id TEXT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS grandfathered_radio_thresholds_hotspot_pubkey_cbsd_id_idx ON grandfathered_radio_thresholds (hotspot_pubkey, cbsd_id);
