-- The block height a pending burn's blockhash expires at.
--
-- Past it the burn can never land, so a burn with no finalized status by then
-- has failed and its sessions can go into another transaction. Before this,
-- a burn was given up on one minute after it was submitted, whether or not it
-- could still land.
--
-- The default of 0 means "not recorded". It covers rows written by a binary
-- that doesn't know this column: rows in flight when this runs, and rows
-- written after a rollback to the binary before this one. The burner bounds a
-- 0 from the chain the first time it reads one, and stores the bound in its
-- place. This binary always writes the real value.
ALTER TABLE pending_txns
    ADD COLUMN last_valid_block_height BIGINT NOT NULL DEFAULT 0;
