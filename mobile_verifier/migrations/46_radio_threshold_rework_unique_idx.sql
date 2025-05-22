DELETE FROM radio_threshold WHERE cbsd_id IS NOT NULL;

DROP INDEX radio_threshold_hotspot_pubkey_cbsd_id_idx;

ALTER TABLE radio_threshold DROP COLUMN cbsd_id;

CREATE UNIQUE INDEX radio_threshold_hotspot_pubkey_idx ON radio_threshold (hotspot_pubkey);
