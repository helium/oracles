ALTER TABLE sp_boosted_rewards_bans ADD COLUMN ban_type TEXT;

UPDATE sp_boosted_rewards_bans SET ban_type = 'boosted_hex';

ALTER TABLE sp_boosted_rewards_bans ALTER COLUMN ban_type SET NOT NULL;

ALTER TABLE sp_boosted_rewards_bans DROP CONSTRAINT sp_boosted_rewards_bans_pkey;

ALTER TABLE sp_boosted_rewards_bans ADD PRIMARY KEY (ban_type, radio_type, radio_key, received_timestamp);



