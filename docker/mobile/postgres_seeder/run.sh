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

FILES=/postgres_seeder/post_migration/*
for file in $FILES; do
    readarray -d "-" -t array <<<"$file"
    db=$(echo ${array[0]} | sed 's|/postgres_seeder/post_migration/||g')
    echo "Running $file on db $db"

    psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $db -f $file
    echo "#############################################"
done
