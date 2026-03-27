#!/bin/sh
set -e

echo "Initializing RustFS buckets..."

# Create iceberg bucket
aws --endpoint-url http://rustfs:9000 s3 mb s3://iceberg 2>/dev/null || {
    echo "Bucket iceberg already exists"
}

# Create iceberg-test bucket for integration tests
aws --endpoint-url http://rustfs:9000 s3 mb s3://iceberg-test 2>/dev/null || {
    echo "Bucket iceberg-test already exists"
}

echo "RustFS initialization complete"
