ALTER TABLE pending_burns RENAME COLUMN payer TO escrow_key;
ALTER TABLE pending_txns RENAME COLUMN payer TO escrow_key;