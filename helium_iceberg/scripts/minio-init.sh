#!/bin/sh
set -e

if ! command -v mc > /dev/null 2>&1; then
  echo "mc not found, downloading..."
  curl -f#L https://dl.min.io/client/mc/release/linux-amd64/mc -o /usr/local/bin/mc
  chmod +x /usr/local/bin/mc
fi

MINIO_HOST="${MINIO_HOST:-minio}"
MINIO_URL="http://${MINIO_HOST}:9000"
MINIO_USER="${MINIO_ROOT_USER:-admin}"
MINIO_PASS="${MINIO_ROOT_PASSWORD:-password}"

until mc alias set minio "$MINIO_URL" "$MINIO_USER" "$MINIO_PASS"; do
  echo "...waiting for MinIO at $MINIO_URL..."
  sleep 1
done

mc mb minio/iceberg || true
mc mb minio/iceberg-test || true
mc anonymous set public minio/iceberg
mc anonymous set public minio/iceberg-test
mc ls minio
