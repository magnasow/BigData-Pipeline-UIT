#!/bin/bash
set -e

EXPECTED_WORKERS=2
MASTER_URL="http://spark-master:8080"

echo "Waiting for $EXPECTED_WORKERS Spark workers to be registered..."

until [ "$(curl -s $MASTER_URL/json | jq '.workers | length')" -ge "$EXPECTED_WORKERS" ]; do
  echo "Waiting for Spark workers to register..."
  sleep 5
done

echo "$EXPECTED_WORKERS workers are ready. Submitting Spark job..."
spark-submit --master spark://spark-master:7077 /scripts/clean_transform.py

echo "Job submitted"
