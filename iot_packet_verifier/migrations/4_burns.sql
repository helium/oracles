CREATE TABLE pending_burns (
       id SERIAL PRIMARY KEY,
       gateway TEXT NOT NULL,
       amount BIGINT NOT NULL
);
