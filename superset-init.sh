#!/bin/bash

echo "Starting Superset initialization..."

superset db upgrade

superset fab create-admin \
    --username admin \
    --firstname admin \
    --lastname admin \
    --email admin@superset.com \
    --password admin || true

superset init

echo "Starting Superset server..."

/usr/bin/run-server.sh