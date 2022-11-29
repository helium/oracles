DROP TABLE heartbeats;

CREATE TABLE heartbeats (
       cbsd_id TEXT NOT NULL,
       hotspot_key TEXT NOT NULL,
       reward_weight DECIMAL,
       hours_seen BOOLEAN[24] NOT NULL,
       PRIMARY KEY(hotspot_key, cbsd_id)
);
       
