INSERT INTO
    registered_keys (pubkey, key_role, created_at, updated_at, name)
VALUES
    (
        '131kC5gTPFfTyzziHbh2PWz2VSdF4gDvhoC5vqCz25N7LFtDocF',
        'administrator',
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP,
        'config'
    ) ON CONFLICT (pubkey, key_role) DO
UPDATE
SET
    updated_at = CURRENT_TIMESTAMP;

INSERT INTO
    registered_keys (pubkey, key_role, created_at, updated_at, name)
VALUES
    (
        '14c5dZUZgFEVcocB3mfcjhXEVqDuafnpzghgyr2i422okXVByPr',
        'oracle',
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP,
        'packet_verifier'
    ) ON CONFLICT (pubkey, key_role) DO
UPDATE
SET
    updated_at = CURRENT_TIMESTAMP;

INSERT INTO
    registered_keys (pubkey, key_role, created_at, updated_at, name)
VALUES
    (
        '14FGkBKPAdBuCtKGFkSnUmvoUBkJGjKVLrPrNLXKN3NgMiLTtwm',
        'oracle',
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP,
        'verifier'
    ) ON CONFLICT (pubkey, key_role) DO
UPDATE
SET
    updated_at = CURRENT_TIMESTAMP;

INSERT INTO
    registered_keys (pubkey, key_role, created_at, updated_at, name)
VALUES
    (
        '13te9quF3s24VNrQmBRHnoNSwWPg48Jh2hfJdqFQoiFYiDcDAsp',
        'pcs',
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP,
        'Authorized Coverage Object Key'
    ) ON CONFLICT (pubkey, key_role) DO
UPDATE
SET
    updated_at = CURRENT_TIMESTAMP;