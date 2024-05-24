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