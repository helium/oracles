-- HIP-150: record the multiplier an in-flight burn was priced at.
--
-- When a burn is submitted its amount is fixed on chain. The records written
-- when the transaction confirms are built from these rows, so they have to carry
-- the multipliers the amount was computed from. The value is written as rows
-- move in and travels with them.
--
-- Rows moving back out on a failed transaction drop it, and the next burn
-- resolves a fresh one from the ticket history.
--
-- Rows in flight when this runs were priced before HIP-150, so the default is
-- correct for them.
ALTER TABLE pending_data_transfer_sessions
    ADD COLUMN multiplier NUMERIC NOT NULL DEFAULT 1;

-- These are the per-file rows of migration 11, moved whole, so they carry the
-- same key.
ALTER TABLE pending_data_transfer_sessions
    DROP CONSTRAINT pending_data_transfer_sessions_pkey;

ALTER TABLE pending_data_transfer_sessions
    ADD PRIMARY KEY (pub_key, payer, last_timestamp);
