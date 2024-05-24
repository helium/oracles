#!/usr/bin/env bash
echo "Init localstack s3"
awslocal s3 mb s3://mobile-ingest
awslocal s3 mb s3://mobile-verifier
awslocal s3 mb s3://mobile-packet-verifier
awslocal s3 mb s3://mobile-price
awslocal s3 mb s3://mobile-verifier-data-sets

DIRS=/tmp/data/*
for DIR in $DIRS; do
    readarray -d "-" -t array <<<"$DIR"
    bucket=$(echo $DIR | sed 's|/tmp/data/||g')
    echo "Uploading $DIR to bucket $bucket"

    awslocal s3 cp $DIR s3://$bucket/ --recursive
done
