#!/bin/bash

# Wait for Postgres to be ready
echo "Waiting for Postgres to be ready..."
until pg_isready -h postgres -U airflow; do
  sleep 2
done

# Initialize the database
if ! airflow db check; then
  echo "Initializing Airflow database..."
  airflow db init
fi

# Create the admin user if it doesn't exist
if ! airflow users list | grep -q "admin"; then
  echo "Creating admin user..."
  airflow users create \
    --username littlecapa \
    --firstname Little \
    --lastname Capa \
    --role Admin \
    --email littlecapa@googlemail.com \
    --password "airflowadmin"
else
  echo "Admin user already exists."
fi

# Start the requested Airflow component
exec airflow "$@"
