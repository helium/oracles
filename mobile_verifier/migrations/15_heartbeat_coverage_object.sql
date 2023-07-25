-- Coverage object can be NULL
ALTER TABLE heartbeats ADD COLUMN coverage_object UUID;
