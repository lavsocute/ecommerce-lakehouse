#!/bin/bash
# PostgreSQL Init Script
# Creates databases and schemas for the lakehouse
set -e

echo "Creating additional databases..."

# Create Airflow and Metabase databases
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE airflow;
    CREATE DATABASE metabase;
EOSQL

# Create schemas in main database
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS bronze;
    CREATE SCHEMA IF NOT EXISTS silver;
    CREATE SCHEMA IF NOT EXISTS gold;
    GRANT ALL PRIVILEGES ON SCHEMA bronze TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON SCHEMA silver TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON SCHEMA gold TO $POSTGRES_USER;
EOSQL

echo "Database initialization complete!"
