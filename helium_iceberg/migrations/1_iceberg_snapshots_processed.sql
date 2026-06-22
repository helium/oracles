-- Watermark of Iceberg snapshots processed by the stream poller.
-- Applied to the per-poller SQLite db by `stream::open_watermark_db`.
-- One row per (process_name, table_name); the poller reads MAX(sequence_number).
CREATE TABLE IF NOT EXISTS iceberg_snapshots_processed (
    process_name        TEXT    NOT NULL DEFAULT 'default',
    table_name          TEXT    NOT NULL,
    snapshot_id         INTEGER NOT NULL,
    sequence_number     INTEGER NOT NULL,
    snapshot_timestamp  TEXT    NOT NULL,
    processed_at        TEXT    NOT NULL,
    PRIMARY KEY (process_name, table_name, sequence_number)
);
