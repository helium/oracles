-- Remove the Proof-of-Coverage reward data model. PoC is no longer rewarded
-- (HIP-149); coverage-object ingestion and the coverage reward computation have
-- been removed, so these tables and their enums are unused. `sp_boosted_rewards_bans`
-- is the dead SP-boosted-hex ban table (no code references it) and is the last
-- user of `radio_type`, so it is dropped here to free that enum.
--
-- `hexes` is dropped before `coverage_objects` (it holds the foreign key), and the
-- enums are dropped after every table that used them. `wifi_heartbeats` uses the
-- separate `cell_type` enum and a bare `coverage_object` uuid column, so it is
-- unaffected.
DROP TABLE IF EXISTS hexes;
DROP TABLE IF EXISTS coverage_objects;
DROP TABLE IF EXISTS seniority;
DROP TABLE IF EXISTS sp_boosted_rewards_bans;

DROP TYPE IF EXISTS signal_level;
DROP TYPE IF EXISTS oracle_assignment;
DROP TYPE IF EXISTS radio_type;
