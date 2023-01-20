CREATE TABLE meta (
    key text primary key not null,
    value text
);

INSERT INTO meta (key, value) VALUES ('last_verified_packet', '0');
