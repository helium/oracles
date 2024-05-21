#!/usr/bin/env bash
echo "Init localstack s3"
awslocal s3 mb s3://mobile-ingest
awslocal s3 mb s3://mobile-verifier
awslocal s3 mb s3://mobile-packet-verifier
awslocal s3 mb s3://mobile-price
