CREATE TABLE pending_burns (
       payer TEXT PRIMARY KEY,
       amount BIGINT NOT NULL,
       last_burn TIMESTAMP NOT NULL
);
