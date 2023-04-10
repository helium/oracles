CREATE TYPE reward_type as enum('mobile', 'iot_gateway', 'iot_operational');

ALTER TABLE reward_index ADD reward_type reward_type;
