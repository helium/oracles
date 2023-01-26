CREATE TABLE pending_burns (
       id SERIAL PRIMARY KEY,
       payer TEXT UNIQUE NOT NULL,
       amount BIGINT NOT NULL,
       last_burn TIMESTAMP NOT NULL
);
