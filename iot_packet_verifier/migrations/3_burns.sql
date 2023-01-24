CREATE TABLE pending_burns (
       id SERIAL PRIMARY KEY,
       payer UNIQUE TEXT NOT NULL,
       amount BIGINT NOT NULL,
       last_burn TIMESTAMP NOT NULL
);
