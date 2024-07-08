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
                ) - (24 * 60 * 60) + 180
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
            ) - (24 * 60 * 60) + 180
    );

INSERT INTO
    meta (key, value)
VALUES
    (
        'next_rewarded_end_time',
        (
            SELECT
                floor(
                    extract(
                        epoch
                        from
                            now() at time zone 'utc'
                    ) + 180 -- Adding 180 seconds
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
            ) + 180 -- Adding 180 seconds
    );

INSERT INTO
    meta (key, value)
VALUES
    (
        'disable_complete_data_checks_until',
        (
            SELECT
                floor(
                    extract(
                        epoch
                        from
                            now() at time zone 'utc'
                    ) + (24 * 60 * 60)
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
            ) + (24 * 60 * 60)
    );