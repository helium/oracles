-- disable_complete_data_checks_until 0 last_rewarded_end_time 1710640800 last_verified_end_time 1686186000 next_rewarded_end_time 1710727200
INSERT INTO
    meta (key, value)
VALUES
    (
        'last_rewarded_end_time',
        (
            SELECT
                floor(
                    extract(
                        epoch
                        from
                            now() at time zone 'utc'
                    )
                )
        )
    ) ON CONFLICT (key) DO
UPDATE
SET
    value = (
        SELECT
            floor(
                extract(
                    epoch
                    from
                        now() at time zone 'utc'
                )
            )
    );