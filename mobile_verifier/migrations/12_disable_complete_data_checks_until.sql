INSERT INTO
   meta
VALUES
   ('disable_complete_data_checks_until', '0')
ON CONFLICT
   (key)
DO NOTHING;
