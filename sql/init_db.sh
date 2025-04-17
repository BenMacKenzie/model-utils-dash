#!/bin/bash

# Load environment variables
source .env

# Create the database if it doesn't exist
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d postgres -c "CREATE DATABASE $DB_NAME;"

# Run the SQL script
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -f init_db.sql

echo "Database initialization complete!" 