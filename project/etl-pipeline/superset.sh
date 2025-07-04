#!/bin/bash
set -e

# Initialize database and create admin user
superset db upgrade

# Only create admin user if not exists
superset fab create-admin \
    --username admin \
    --firstname Thu \
    --lastname Thuy \
    --email admin@example.com \
    --password admin123 || true

# Setup roles and permissions
superset init

# Start the web server
superset run -h 0.0.0.0 -p 8088
