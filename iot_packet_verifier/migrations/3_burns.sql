CREATE TABLE pending_burns (
       id SERIAL PRIMARY KEY,
       gateway UNIQUE TEXT NOT NULL,
       amount BIGINT NOT NULL
);
