-- HIP-150: the multiplier granted to a hotspot, and when it took effect.
--
-- One row per grant, append-only. The burn needs to ask what was in force at a
-- past instant, not just what is in force now, so it needs the history.
--
-- Lives in Postgres because that is where data_transfer_sessions lives, and
-- pricing a session means joining the two.
--
-- Only tickets that verified are written here. The Iceberg history table holds
-- every ticket, including refusals, for auditing.
CREATE TABLE data_transfer_multipliers (
    hotspot_pubkey TEXT NOT NULL,
    multiplier NUMERIC NOT NULL,
    -- The timestamp the issuer signed, which is when the grant starts. The
    -- Iceberg history orders on the same value, so both tables agree on which
    -- ticket is current for a hotspot.
    effective_timestamp TIMESTAMPTZ NOT NULL,
    -- Keyed on the grant itself, so reprocessing a ticket file rewrites the same
    -- rows instead of adding to them. That is also what makes a replayed ticket
    -- a no-op rather than a way to reinstate a superseded multiplier.
    PRIMARY KEY (hotspot_pubkey, effective_timestamp)
);

-- The burn looks up the latest row for a hotspot at or before a given instant.
-- The primary key's index serves that by scanning backwards.
