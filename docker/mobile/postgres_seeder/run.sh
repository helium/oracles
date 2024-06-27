#!/bin/bash

# Description:
#   This Bash script performs database migrations and seeding for multiple PostgreSQL databases.
#
# Script Overview:
#   - Runs migrations for the 'mobile_config' database using sqlx migrate.
#   - Runs migrations for the 'mobile_packet_verifier' database using sqlx migrate.
#   - Runs migrations for the 'mobile_verifier' database using sqlx migrate.
#   - Runs migrations for the 'mobile_index' database using sqlx migrate.
#   - Executes SQL scripts for additional post-migration tasks in each database.
#       1. Define the path to SQL script files located in the /postgres_seeder/post_migration/ directory.
#       2. Iterate through each SQL script file found in the specified directory.
#       3. Extract the database name from the file path using delimiters and manipulation.
#       4. Output a message indicating the script being executed and the corresponding database.
#       5. Use psql to connect to the PostgreSQL database and execute the SQL script.
#
# Key Points:
#   - Database Migration: The script uses sqlx migrate to apply database migrations from specified sources.
#   - Database Seeding: It executes SQL scripts located in /postgres_seeder/post_migration/ directory for additional setup tasks.
#   - Environment Variables: Requires environment variables POSTGRES_USER, POSTGRES_PASSWORD, and POSTGRES_HOST to connect to PostgreSQL databases.
#   - Script Output: Displays status messages and execution results for each migration and seeding step.

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
