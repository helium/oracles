#!/bin/bash

echo "#############################################"
echo "Running mobile-config migrations"
sqlx migrate run --database-url $DB_URL/mobile_config --source /mobile_config/migrations
echo "#############################################"

echo "Running mobile-packet-verifier migrations"
sqlx migrate run --database-url $DB_URL/mobile_packet_verifier --source /mobile_packet_verifier/migrations
echo "#############################################"

echo "Running mobile-verifier migrations"
sqlx migrate run --database-url $DB_URL/mobile_verifier --source /mobile_verifier/migrations
echo "#############################################"

echo "Running mobile reward-index migrations"
sqlx migrate run --database-url $DB_URL/mobile_index --source /reward_index/migrations
echo "#############################################"

echo "Done"
