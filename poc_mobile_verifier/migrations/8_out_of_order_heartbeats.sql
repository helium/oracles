DROP TABLE heartbeats;

CREATE TABLE heartbeats (
       cbsd_id TEXT NOT NULL PRIMARY KEY,
       hotspot_key TEXT NOT NULL,
       reward_weight DECIMAL,
       -- List of hours for which we have seen or have not seen a heartbeat, in order starting
       -- at the first hour of the day (00:00 to 00:59). Since SQL arrays are 1-indexed, this
       -- means that the index is one greater than the hour, e.g. hours_seen[2] corresponds to
       -- seeing a heartbeat sometime at 1 am.
       hours_seen BOOLEAN[24] NOT NULL
);
