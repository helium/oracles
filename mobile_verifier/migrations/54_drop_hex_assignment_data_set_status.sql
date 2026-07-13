-- The oracle-boosting data-set downloader has been removed, so its file-tracking
-- bookkeeping table and the enums it used are no longer referenced by any code.
-- (The `hexes` assignment columns use the separate `oracle_assignment` enum and
-- are untouched here.)
DROP TABLE IF EXISTS hex_assignment_data_set_status;
DROP TYPE IF EXISTS data_set_status;
DROP TYPE IF EXISTS data_set_type;
