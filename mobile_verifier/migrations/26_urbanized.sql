CREATE TYPE oracle_assignment AS ENUM ('a', 'b', 'c');

ALTER TABLE hexes ADD COLUMN urbanized oracle_assignment;
