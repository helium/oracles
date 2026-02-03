#!/bin/sh

set -e

until mc alias set minio http://minio:9000 "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY"; do
  echo "...waiting for MinIO..."
  sleep 1
done

mc mb minio/iceberg || true
mc anonymous set public minio/iceberg
