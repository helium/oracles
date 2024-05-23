#!/bin/bash

echo "#############################################"
echo "Running mobile-config migrations"
sqlx migrate run --database-url postgres://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:5432/mobile_config --source /mobile_config/migrations
echo "#############################################"

echo "Running mobile-packet-verifier migrations"
sqlx migrate run --database-url postgres://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:5432/mobile_packet_verifier --source /mobile_packet_verifier/migrations
echo "#############################################"

echo "Running mobile-verifier migrations"
sqlx migrate run --database-url postgres://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:5432/mobile_verifier --source /mobile_verifier/migrations
echo "#############################################"

echo "Running mobile reward-index migrations"
sqlx migrate run --database-url postgres://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:5432/mobile_index --source /reward_index/migrations
echo "#############################################"

export PGPASSWORD=$POSTGRES_PASSWORD
MOBILE_CONFIG_FILES=/postgres_seeder/mobile_config/*

for file in $MOBILE_CONFIG_FILES; do
    echo "Running $file"
    psql -h $POSTGRES_HOST -U $POSTGRES_USER -d mobile_config -f $file
    echo "#############################################"
done
