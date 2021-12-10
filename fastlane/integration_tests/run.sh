#!/bin/bash
set -e

pip install wait-for-it

echo Starting web API containers...

docker-compose -f ../../fastlane_web/docker-compose.yml up -d
docker-compose -f ../../fastlane_web/docker-compose.yml start

for i in {1..5}
do
  sleep 5
  wait-for-it \
  --timeout 120 \
  --service localhost:8001/api/healthcheck \
  -- echo "web API containers responded for ${i} time."
done

for integration in "integration_"*
do
  cd "$integration" && source run.sh
  cd ..
done

docker-compose -f ../../fastlane_web/docker-compose.yml down
