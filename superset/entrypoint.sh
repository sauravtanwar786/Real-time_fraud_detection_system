#!/bin/bash

# # Wait for the database to be ready
# echo "Waiting for database..."
# while ! nc -z postgres 5432; do
#   sleep 1
# done

echo "Database is ready, running migrations..."
superset db upgrade

echo "Creating admin user..."
superset fab create-admin --username admin --firstname Superset --lastname Admin --email example@thisemailwillneverexist.xyz --password admin || true

echo "Initializing Superset..."
superset init

echo "Setting Trino database URI..."
superset set_database_uri -d trino_lakehouse -u trino://user@trino:8080/nessie/schema

echo "Starting Superset..."
exec gunicorn --workers 5 --timeout 120 --bind 0.0.0.0:8088 "superset.app:create_app()"
