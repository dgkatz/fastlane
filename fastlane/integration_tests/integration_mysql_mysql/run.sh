#!/bin/bash
echo "Running fastlane integration test for MySQL -> MySQL"
pip install -r requirements.txt

echo Starting database containers...

docker-compose up -d
docker-compose start

echo "Waiting 30s for database containers to start"
sleep 30

python3 test.py

docker-compose down